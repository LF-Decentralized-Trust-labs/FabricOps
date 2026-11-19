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
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var _ = Describe("Key-free peer MSP material", func() {
	It("recognizes public material through the API without treating it as provider readiness or migrating keys", func() {
		net, _, org, _, _ := channelConfigOperationFixture()
		org.Peer.MSPSigning = peerPKCS11SigningFixture()
		const namespace = "peer-msp-handoff-api"
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, ns))).To(Succeed()) })
		authority, err := generateTestIdentityAuthority(org)
		Expect(err).NotTo(HaveOccurred())
		secret, err := buildTestMSPSecret(net, org, namespace, "peer0", componentPeer, secretKindMSP, authority)
		Expect(err).NotTo(HaveOccurred())
		keyPEM := secret.Data[mspKeyStoreKey]
		delete(secret.Data, mspKeyStoreKey)
		secret.Labels[labelIdentitySource] = identitySourceFabricCA
		tlsSecret, err := buildTestWorkloadTLSSecret(net, org, namespace, "peer0", componentPeer, nil, authority)
		Expect(err).NotTo(HaveOccurred())
		tlsSecret.Labels[labelIdentitySource] = identitySourceFabricCA
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		Expect(k8sClient.Create(ctx, tlsSecret)).To(Succeed())
		r := &FabricNetworkReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		materialReady, err := r.workloadIdentityEnrolled(ctx, net, org, namespace, "peer0", componentPeer)
		Expect(err).NotTo(HaveOccurred())
		Expect(materialReady).To(BeTrue())
		Expect(validatePeerMSPSigning(org)).To(ContainElement(ContainSubstring("PKCS11 is unavailable")))
		secret.Data[mspKeyStoreKey] = keyPEM
		Expect(k8sClient.Update(ctx, secret)).To(Succeed())
		materialReady, err = r.workloadIdentityEnrolled(ctx, net, org, namespace, "peer0", componentPeer)
		Expect(err).NotTo(HaveOccurred())
		Expect(materialReady).To(BeFalse())
		stored := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(secret), stored)).To(Succeed())
		Expect(stored.Data[mspKeyStoreKey]).To(Equal(keyPEM), "material checks must not migrate live software keys")
		delete(secret.Data, mspKeyStoreKey)
		Expect(k8sClient.Update(ctx, secret)).To(Succeed())
		delete(tlsSecret.Data, tlsServerKeyKey)
		Expect(k8sClient.Update(ctx, tlsSecret)).To(Succeed())
		materialReady, err = r.workloadIdentityEnrolled(ctx, net, org, namespace, "peer0", componentPeer)
		Expect(err).NotTo(HaveOccurred())
		Expect(materialReady).To(BeFalse(), "TLS still requires a software private key")
	})
})

func TestPKCS11PeerMSPHandoffOmitsPrivateKeys(t *testing.T) {
	net, _, org, _, _ := channelConfigOperationFixture()
	org.Peer.MSPSigning = peerPKCS11SigningFixture()
	const namespace = "org"
	peer := buildPeerDeployment(net, org, 0, namespace)
	for _, volume := range peer.Spec.Template.Spec.Volumes {
		if volume.Name == secretKindMSP {
			for _, item := range volume.Secret.Items {
				if item.Key == mspKeyStoreKey {
					t.Error("PKCS11 peer still requests a software MSP private key")
				}
			}
		}
	}
	authority, err := generateTestIdentityAuthority(org)
	if err != nil {
		t.Fatal(err)
	}
	secret, err := buildTestMSPSecret(net, org, namespace, "peer0", componentPeer, secretKindMSP, authority)
	if err != nil {
		t.Fatal(err)
	}
	delete(secret.Data, mspKeyStoreKey)
	for _, requirement := range requiredIdentitySecrets(net, org, namespace) {
		if requirement.name != secret.Name {
			continue
		}
		if slices.Contains(requirement.keys, mspKeyStoreKey) {
			t.Error("PKCS11 material readiness still requires a software private key")
		}
		if problem := identitySecretValidationError(*secret, requirement.kind, net.Spec.Global.TLS); problem != "" {
			t.Errorf("key-free peer MSP rejected: %s", problem)
		}
	}
	for _, component := range []string{componentPeer, componentOrderer} {
		job := buildWorkloadEnrollmentJob(net, org, namespace, "peer0", component, nil)
		enroll := job.Spec.Template.Spec.InitContainers[0].Command[2]
		if component == componentPeer && (!strings.Contains(enroll, "refusing software key generation") || strings.Contains(enroll, "fabric-ca-client enroll")) {
			t.Error("staged PKCS11 enrollment can generate a software key")
		}
		if component == componentOrderer && !strings.Contains(enroll, "fabric-ca-client enroll") {
			t.Error("software orderer enrollment changed")
		}
		script := job.Spec.Template.Spec.Containers[0].Command[2]
		hasKey := strings.Contains(script, "--from-file=keystore.pem=")
		if hasKey != (component == componentOrderer) {
			t.Errorf("incorrect private key publication for %s", component)
		}
	}
	if problem := identitySecretValidationError(*secret, secretKindAdminMSP, net.Spec.Global.TLS); !strings.Contains(problem, mspKeyStoreKey) {
		t.Fatalf("admin software key requirement changed: %s", problem)
	}
	org.Peer.MSPSigning = nil
	if script := buildWorkloadEnrollmentJob(net, org, namespace, "peer0", componentPeer, nil).Spec.Template.Spec.Containers[0].Command[2]; !strings.Contains(script, "--from-file=keystore.pem=") {
		t.Fatal("default software peer no longer publishes its private key")
	}
}

func TestPKCS11PeerMSPValidationAndEnrollmentStatus(t *testing.T) {
	for _, tlsEnabled := range []bool{false, true} {
		net, _, org, _, _ := channelConfigOperationFixture()
		net.Spec.Global.TLS = tlsEnabled
		org.Peer.MSPSigning = peerPKCS11SigningFixture()
		authority, err := generateTestIdentityAuthority(org)
		if err != nil {
			t.Fatal(err)
		}
		secret, err := buildTestMSPSecret(net, org, "org", "peer0", componentPeer, secretKindMSP, authority)
		if err != nil {
			t.Fatal(err)
		}
		privateKey := secret.Data[mspKeyStoreKey]
		delete(secret.Data, mspKeyStoreKey)
		secret.Labels[labelIdentitySource] = identitySourceFabricCA
		tlsSecret, err := buildTestWorkloadTLSSecret(net, org, "org", "peer0", componentPeer, nil, authority)
		if err != nil {
			t.Fatal(err)
		}
		tlsSecret.Labels[labelIdentitySource] = identitySourceFabricCA
		scheme := runtime.NewScheme()
		if err := corev1.AddToScheme(scheme); err != nil {
			t.Fatal(err)
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret, tlsSecret).Build()
		r := &FabricNetworkReconciler{Client: c, Scheme: scheme}
		ready, err := r.workloadIdentityEnrolled(t.Context(), net, org, "org", "peer0", componentPeer)
		if err != nil || !ready {
			t.Fatalf("key-free MSP material not recognized: %v", err)
		}
		ready, err = r.workloadIdentityEnrolled(t.Context(), net, org, "org", "peer0", componentOrderer)
		if err != nil || ready {
			t.Fatalf("orderer lost its private key requirement: %v", err)
		}
		for _, mutate := range []func(*corev1.Secret){
			func(s *corev1.Secret) { s.Data[mspKeyStoreKey] = privateKey },
			func(s *corev1.Secret) { s.Data["provider-pin"] = []byte("sensitive-test-value") },
			func(s *corev1.Secret) { s.Data[mspSignCertKey] = append(s.Data[mspSignCertKey], privateKey...) },
			func(s *corev1.Secret) {
				s.Data[mspConfigKey] = []byte("privateKey: |\n  " + strings.ReplaceAll(string(privateKey), "\n", "\n  "))
			},
			func(s *corev1.Secret) { delete(s.Data, mspCACertKey) },
			func(s *corev1.Secret) { s.Data[mspSignCertKey] = []byte("sensitive-test-value") },
		} {
			bad := secret.DeepCopy()
			mutate(bad)
			problem := identitySecretValidationError(*bad, secretKindPeerPKCS11MSP, tlsEnabled)
			if problem == "" || strings.Contains(problem, "sensitive-test-value") || strings.Contains(problem, string(privateKey)) {
				t.Fatal("invalid PKCS11 material was accepted or leaked in diagnostics")
			}
		}
	}
}

func TestPKCS11PeerMSPPublicationScript(t *testing.T) {
	for _, test := range []struct {
		name, existingKeys                           string
		localKey, embeddedKey, lookupFails, software bool
		wantOK                                       bool
	}{
		{name: "new public MSP", wantOK: true},
		{name: "existing public MSP", existingKeys: "config.yaml\ncacert.pem\nsigncert.pem\ntlscacert.pem", wantOK: true},
		{name: "existing software key", existingKeys: "keystore.pem"},
		{name: "unexpected provider credential", existingKeys: "provider-pin"},
		{name: "local software key", localKey: true},
		{name: "embedded private PEM", embeddedKey: true},
		{name: "failed lookup", lookupFails: true},
		{name: "software peer unchanged", localKey: true, software: true, wantOK: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			for _, path := range []string{"output/msp", "output/tls", "bin"} {
				if err := os.MkdirAll(filepath.Join(dir, path), 0700); err != nil {
					t.Fatal(err)
				}
			}
			files := map[string]string{
				"output/msp/config.yaml": "NodeOUs: {}", "output/msp/cacert.pem": "public-ca",
				"output/msp/signcert.pem": "public-cert", "output/msp/tlscacert.pem": "public-tls-ca",
				"output/tls/ca.crt": "public-ca", "output/tls/server.crt": "public-tls-cert", "output/tls/server.key": "test-tls-key",
				"bin/kubectl": `#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$PUBLICATION_TEST_DIR/commands"
shift 2
case "$1" in
  get) [ "$LOOKUP_FAILS" != true ]; printf '%s\n' "$EXISTING_KEYS" ;;
  create)
    for arg in "$@"; do
      case "$arg" in --from-file=*) file="${arg#--from-file=}"; test -s "${file#*=}" ;; esac
    done
    printf 'public-secret-payload\n'
    ;;
  apply) payload="$(cat)"; test -n "$payload"; touch "$PUBLICATION_TEST_DIR/applied" ;;
  label) ;;
  *) exit 1 ;;
esac
`,
			}
			if test.localKey {
				files["output/msp/keystore.pem"] = "sensitive-test-value"
			}
			if test.embeddedKey {
				files["output/msp/signcert.pem"] += "\n-----BEGIN PRIVATE KEY-----\nsensitive-test-value\n-----END PRIVATE KEY-----"
			}
			for path, data := range files {
				if err := os.WriteFile(filepath.Join(dir, path), []byte(data), 0700); err != nil {
					t.Fatal(err)
				}
			}
			script := strings.ReplaceAll(publishWorkloadIdentityScript(test.software), enrollmentWorkDir, dir)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "sh", "-ec", script)
			cmd.Env = append(os.Environ(), "PATH="+filepath.Join(dir, "bin")+string(os.PathListSeparator)+os.Getenv("PATH"),
				"PUBLICATION_TEST_DIR="+dir, "EXISTING_KEYS="+test.existingKeys,
				"LOOKUP_FAILS="+map[bool]string{true: "true", false: "false"}[test.lookupFails],
				"POD_NAMESPACE=org", "FABRICOPS_WORKLOAD_MSP_SECRET=peer0-msp", "FABRICOPS_WORKLOAD_TLS_SECRET=peer0-tls",
				"FABRICOPS_WORKLOAD_NAME=peer0", "FABRICOPS_TLS_ENABLED=true")
			logs, err := cmd.CombinedOutput()
			if (err == nil) != test.wantOK || strings.Contains(string(logs), "sensitive-test-value") {
				t.Fatalf("publication success=%v, want %v: %s", err == nil, test.wantOK, logs)
			}
			commands, _ := os.ReadFile(filepath.Join(dir, "commands"))
			if !test.wantOK {
				if strings.Contains(string(commands), "create secret") || strings.Contains(string(commands), "apply") {
					t.Fatal("invalid or legacy material reached a Secret write")
				}
				return
			}
			if strings.Contains(string(commands), "--from-file=keystore.pem=") != test.software || !strings.Contains(string(commands), "--from-file=server.key=") {
				t.Fatal("MSP private-key publication or TLS private-key regression")
			}
			if _, err := os.Stat(filepath.Join(dir, "applied")); err != nil {
				t.Fatal(err)
			}
		})
	}
}
