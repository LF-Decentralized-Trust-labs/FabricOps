/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/yaml"
)

func TestCustodyCRDSchemasMatchChart(t *testing.T) {
	for _, resource := range []string{"fabricnetworks", "fabricparticipants"} {
		readSpec := func(path string, template bool) any {
			t.Helper()
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if template {
				// CRD Helm conditionals wrap metadata and the document, not the schema.
				var lines []string
				for line := range strings.SplitSeq(string(data), "\n") {
					if !strings.HasPrefix(strings.TrimSpace(line), "{{") {
						lines = append(lines, line)
					}
				}
				data = []byte(strings.Join(lines, "\n"))
			}
			var document struct {
				Spec map[string]any `json:"spec"`
			}
			if err := yaml.Unmarshal(data, &document); err != nil {
				t.Fatal(err)
			}
			if len(document.Spec) == 0 {
				t.Fatal("missing CRD spec")
			}
			return document.Spec
		}
		root := filepath.Join("..", "..")
		generated := readSpec(filepath.Join(root, "config", "crd", "bases", "fabricops.io_"+resource+".yaml"), false)
		chart := readSpec(filepath.Join(root, "dist", "chart", "templates", "crd", resource+".fabricops.io.yaml"), true)
		if !reflect.DeepEqual(generated, chart) {
			t.Errorf("%s chart CRD differs from its generated schema", resource)
		}
	}
}

func TestPeerMSPSigningFailsClosedInBothTopologies(t *testing.T) {
	var peer fabricopsv1alpha1.PeerConfig
	err := json.Unmarshal([]byte(`{"instances":2,"db":"LevelDB","prefix":"peer","mspSigning":{
		"provider":"PKCS11","pkcs11":{"library":"/usr/lib/libsofthsm2.so","tokenLabel":"peers",
		"pinSecretKeyRef":{"name":"peer-token-pin","key":"pin"}}}}`), &peer)
	if err != nil {
		t.Fatal(err)
	}
	net, _, _, _, _ := channelConfigOperationFixture()
	net.Spec.Orgs[1].Peer = &peer
	participant := fabricParticipantFixture("custody-contract")
	participant.Spec.Org.Peer = &peer
	for name, problems := range map[string][]string{
		"network":     validateFabricNetworkTopology(net),
		"participant": validateFabricParticipantTopology(participant),
	} {
		t.Run(name, func(t *testing.T) {
			message := strings.Join(problems, "; ")
			if !strings.Contains(message, "PKCS11 is unavailable") || !strings.Contains(message, "software fallback is disabled") {
				t.Fatalf("unsupported custody must block provisioning: %s", message)
			}
		})
	}
}

func peerPKCS11SigningFixture() *fabricopsv1alpha1.PeerMSPSigningConfig {
	return &fabricopsv1alpha1.PeerMSPSigningConfig{Provider: "PKCS11", PKCS11: &fabricopsv1alpha1.PeerPKCS11Config{
		Library: "/usr/lib/libsofthsm2.so", TokenLabel: "peers",
		PINSecretKeyRef: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "token-pin"}, Key: "pin"},
	}}
}

func TestPeerMSPSigningValidation(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*fabricopsv1alpha1.PeerMSPSigningConfig)
		want   string
	}{
		{"unknown provider", func(c *fabricopsv1alpha1.PeerMSPSigningConfig) { c.Provider = "sensitive-unsupported-provider" }, ".provider must"},
		{"missing token", func(c *fabricopsv1alpha1.PeerMSPSigningConfig) { c.PKCS11 = nil }, ".pkcs11 is required"},
		{"software with token", func(c *fabricopsv1alpha1.PeerMSPSigningConfig) { c.Provider = "Secret" }, ".pkcs11 must be omitted"},
		{"relative module", func(c *fabricopsv1alpha1.PeerMSPSigningConfig) { c.PKCS11.Library = "sensitive-relative-module" }, ".library must"},
		{"unclean module", func(c *fabricopsv1alpha1.PeerMSPSigningConfig) { c.PKCS11.Library = "/lib/../module.so" }, ".library must"},
		{"empty label", func(c *fabricopsv1alpha1.PeerMSPSigningConfig) { c.PKCS11.TokenLabel = " " }, ".tokenLabel must"},
		{"control label", func(c *fabricopsv1alpha1.PeerMSPSigningConfig) { c.PKCS11.TokenLabel = "sensitive\nlabel" }, ".tokenLabel must"},
		{"bad secret", func(c *fabricopsv1alpha1.PeerMSPSigningConfig) { c.PKCS11.PINSecretKeyRef.Name = "sensitive/secret" }, ".pinSecretKeyRef must"},
		{"missing key", func(c *fabricopsv1alpha1.PeerMSPSigningConfig) { c.PKCS11.PINSecretKeyRef.Key = "" }, ".pinSecretKeyRef must"},
		{"optional PIN", func(c *fabricopsv1alpha1.PeerMSPSigningConfig) { yes := true; c.PKCS11.PINSecretKeyRef.Optional = &yes }, ".pinSecretKeyRef must"},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := peerPKCS11SigningFixture()
			test.mutate(config)
			org := fabricopsv1alpha1.Org{Peer: &fabricopsv1alpha1.PeerConfig{MSPSigning: config}}
			message := strings.Join(validatePeerMSPSigning(org), "; ")
			if !strings.Contains(message, test.want) || strings.Contains(message, "sensitive") {
				t.Fatalf("unexpected or unredacted validation: %s", message)
			}
		})
	}
	for _, org := range []fabricopsv1alpha1.Org{{}, {Peer: &fabricopsv1alpha1.PeerConfig{}},
		{Peer: &fabricopsv1alpha1.PeerConfig{MSPSigning: &fabricopsv1alpha1.PeerMSPSigningConfig{Provider: "Secret"}}}} {
		if problems := validatePeerMSPSigning(org); len(problems) != 0 {
			t.Fatalf("default software custody changed: %v", problems)
		}
	}
}

var _ = Describe("Peer MSP signing custody contract", func() {
	It("persists requests but blocks both controllers before provisioning or reading PINs", func() {
		net, _, _, _, _ := channelConfigOperationFixture()
		net.Name = "msp-custody-gate"
		net.Spec.Channels, net.Spec.Chaincodes = []fabricopsv1alpha1.Channel{}, []fabricopsv1alpha1.Chaincode{}
		net.Spec.Orgs[1].Peer.MSPSigning = peerPKCS11SigningFixture()
		participant := fabricParticipantFixture("msp-custody-gate")
		participant.Spec.Org.Peer.MSPSigning = peerPKCS11SigningFixture()
		for _, object := range []client.Object{net, participant} {
			Expect(k8sClient.Create(ctx, object)).To(Succeed())
			DeferCleanup(func() {
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(object), object)).To(Succeed())
				object.SetFinalizers(nil)
				Expect(k8sClient.Update(ctx, object)).To(Succeed())
				Expect(k8sClient.Delete(ctx, object)).To(Succeed())
			})
			request := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(object)}
			var conditions []metav1.Condition
			var namespace string
			switch obj := object.(type) {
			case *fabricopsv1alpha1.FabricNetwork:
				r := &FabricNetworkReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
				_, err := r.Reconcile(ctx, request)
				Expect(err).NotTo(HaveOccurred())
				Expect(k8sClient.Get(ctx, request.NamespacedName, obj)).To(Succeed())
				conditions, namespace = obj.Status.Conditions, orgNamespaceName(obj, obj.Spec.Orgs[1])
				Expect(obj.Spec.Orgs[1].Peer.MSPSigning).To(Equal(peerPKCS11SigningFixture()))
			case *fabricopsv1alpha1.FabricParticipant:
				r := &FabricParticipantReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
				_, err := r.Reconcile(ctx, request)
				Expect(err).NotTo(HaveOccurred())
				Expect(k8sClient.Get(ctx, request.NamespacedName, obj)).To(Succeed())
				conditions, namespace = obj.Status.Conditions, participantOrgNamespace(obj)
				Expect(obj.Spec.Org.Peer.MSPSigning).To(Equal(peerPKCS11SigningFixture()))
			}
			ready := apiMeta.FindStatusCondition(conditions, conditionReady)
			Expect(ready).NotTo(BeNil())
			Expect(ready.Status).To(Equal(metav1.ConditionFalse))
			Expect(ready.Message).To(ContainSubstring("PKCS11 is unavailable"))
			Expect(ready.Message).To(ContainSubstring("software fallback is disabled"))
			Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKey{Name: namespace}, &corev1.Namespace{}))).To(BeTrue())
			var jobs batchv1.JobList
			Expect(k8sClient.List(ctx, &jobs, client.InNamespace(namespace))).To(Succeed())
			Expect(jobs.Items).To(BeEmpty())
		}
	})

	It("rejects ambiguous providers and incomplete PIN references in both CRD schemas", func() {
		for _, mutate := range []func(*fabricopsv1alpha1.PeerMSPSigningConfig){
			func(c *fabricopsv1alpha1.PeerMSPSigningConfig) { c.Provider = "Vault" },
			func(c *fabricopsv1alpha1.PeerMSPSigningConfig) { c.Provider = "Secret" },
			func(c *fabricopsv1alpha1.PeerMSPSigningConfig) { c.PKCS11 = nil },
			func(c *fabricopsv1alpha1.PeerMSPSigningConfig) { c.PKCS11.PINSecretKeyRef.Name = "" },
			func(c *fabricopsv1alpha1.PeerMSPSigningConfig) { c.PKCS11.PINSecretKeyRef.Key = "" },
			func(c *fabricopsv1alpha1.PeerMSPSigningConfig) { yes := true; c.PKCS11.PINSecretKeyRef.Optional = &yes },
		} {
			config := peerPKCS11SigningFixture()
			mutate(config)
			net, _, _, _, _ := channelConfigOperationFixture()
			net.Name = "invalid-msp-contract"
			net.Spec.Channels, net.Spec.Chaincodes = []fabricopsv1alpha1.Channel{}, []fabricopsv1alpha1.Chaincode{}
			net.Spec.Orgs[1].Peer.MSPSigning = config
			participant := fabricParticipantFixture(net.Name)
			participant.Spec.Org.Peer.MSPSigning = config.DeepCopy()
			for _, object := range []client.Object{net, participant} {
				err := k8sClient.Create(ctx, object)
				if err == nil {
					Expect(k8sClient.Delete(ctx, object)).To(Succeed())
				}
				Expect(apierrors.IsInvalid(err)).To(BeTrue(), "ambiguous custody must fail schema validation: %v", err)
			}
		}
	})
})
