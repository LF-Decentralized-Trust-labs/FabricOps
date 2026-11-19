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
	"reflect"
	"strings"
	"testing"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestPKCS11ClaimReferencesBlockNamespaceDeletion(t *testing.T) {
	net, _, _, _, _ := channelConfigOperationFixture()
	net.Spec.Orgs[1] = pkcs11RuntimeOrgFixture(t)
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	first, protected := buildOrgNamespace(net, net.Spec.Orgs[0]), buildOrgNamespace(net, net.Spec.Orgs[1])
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(first, protected).Build()
	r := &FabricNetworkReconciler{Client: c, Scheme: scheme}
	if err := r.cleanupFabricNetwork(t.Context(), net); err == nil || !strings.Contains(err.Error(), "token state") {
		t.Fatal("cleanup did not refuse referenced token state")
	}
	for _, namespace := range []*corev1.Namespace{first, protected} {
		if err := c.Get(t.Context(), client.ObjectKeyFromObject(namespace), &corev1.Namespace{}); err != nil {
			t.Fatal("cleanup deleted a namespace before checking token retention")
		}
	}
	// Without a recorded hold, only the declared-reference fallback applies.
	net.Spec.Orgs[1].Peer.MSPSigning.PKCS11.Runtime.StateClaims = nil
	if err := r.cleanupFabricNetwork(t.Context(), net); err != nil {
		t.Fatal(err)
	}
}

func pkcs11RuntimeOrgFixture(t interface {
	Helper()
	Fatal(...any)
}) fabricopsv1alpha1.Org {
	t.Helper()
	_, _, org, _, _ := channelConfigOperationFixture()
	org.Peer.Instances = 2
	org.Peer.MSPSigning = peerPKCS11SigningFixture()
	err := json.Unmarshal([]byte(`{"library":"/usr/lib/softhsm/libsofthsm2.so","tokenLabel":"peers",
"pinSecretKeyRef":{"name":"token-pin","key":"pin"},"runtime":{
"enrollmentImage":"enrollment:development","peerImage":"peer:development",
"configSecretKeyRef":{"name":"provider-config","key":"softhsm2.conf"},
"configEnv":"SOFTHSM2_CONF","runAsUser":1001,"fsGroup":1001,
"stateClaims":[{"peer":"peer0","claimName":"token-peer0"},{"peer":"peer1","claimName":"token-peer1"}]}}`), org.Peer.MSPSigning.PKCS11)
	if err != nil {
		t.Fatal(err)
	}
	return org
}

func TestPKCS11RuntimeInvalidAccessFailsBeforeMounts(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*fabricopsv1alpha1.PeerPKCS11Config)
	}{
		{"missing runtime", func(p *fabricopsv1alpha1.PeerPKCS11Config) { p.Runtime = nil }},
		{"missing PIN", func(p *fabricopsv1alpha1.PeerPKCS11Config) { p.PINSecretKeyRef.Name = "" }},
		{"missing image", func(p *fabricopsv1alpha1.PeerPKCS11Config) { p.Runtime.PeerImage = "" }},
		{"invalid image", func(p *fabricopsv1alpha1.PeerPKCS11Config) { p.Runtime.EnrollmentImage = "sensitive bad image" }},
		{"optional config", func(p *fabricopsv1alpha1.PeerPKCS11Config) { yes := true; p.Runtime.ConfigSecretKeyRef.Optional = &yes }},
		{"PIN as config", func(p *fabricopsv1alpha1.PeerPKCS11Config) { p.Runtime.ConfigSecretKeyRef = p.PINSecretKeyRef }},
		{"unsafe environment", func(p *fabricopsv1alpha1.PeerPKCS11Config) { p.Runtime.ConfigEnv = "CORE_PEER_BCCSP_DEFAULT" }},
		{"root", func(p *fabricopsv1alpha1.PeerPKCS11Config) { p.Runtime.RunAsUser = 0 }},
		{"root group", func(p *fabricopsv1alpha1.PeerPKCS11Config) { p.Runtime.FSGroup = 0 }},
		{"missing claim", func(p *fabricopsv1alpha1.PeerPKCS11Config) { p.Runtime.StateClaims = p.Runtime.StateClaims[:1] }},
		{"duplicate peer", func(p *fabricopsv1alpha1.PeerPKCS11Config) { p.Runtime.StateClaims[1].Peer = "peer0" }},
		{"unknown peer", func(p *fabricopsv1alpha1.PeerPKCS11Config) { p.Runtime.StateClaims[1].Peer = "peer9" }},
		{"shared claim", func(p *fabricopsv1alpha1.PeerPKCS11Config) { p.Runtime.StateClaims[1].ClaimName = "token-peer0" }},
		{"ledger claim", func(p *fabricopsv1alpha1.PeerPKCS11Config) { p.Runtime.StateClaims[0].ClaimName = "peer0" }},
		{"CA claim", func(p *fabricopsv1alpha1.PeerPKCS11Config) { p.Runtime.StateClaims[0].ClaimName = "banka-ca" }},
		{"database claim", func(p *fabricopsv1alpha1.PeerPKCS11Config) { p.Runtime.StateClaims[0].ClaimName = couchDBName("peer0") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			org := pkcs11RuntimeOrgFixture(t)
			test.mutate(org.Peer.MSPSigning.PKCS11)
			pod := corev1.PodSpec{}
			before := pod.DeepCopy()
			err := applyPKCS11Runtime(&pod, org, "peer0", true)
			if err == nil || strings.Contains(err.Error(), "sensitive") || !reflect.DeepEqual(*before, pod) {
				t.Fatal("invalid access was accepted, leaked values, or mutated the pod")
			}
			net, _, _, _, _ := channelConfigOperationFixture()
			job := buildWorkloadEnrollmentJob(net, org, "org", "peer0", componentPeer, nil)
			command := strings.Join(job.Spec.Template.Spec.InitContainers[0].Command, " ")
			if strings.Contains(command, "fabric-ca-client enroll") || !strings.Contains(command, "refusing software key generation") {
				t.Fatal("invalid runtime access selected software enrollment")
			}
		})
	}
	org := pkcs11RuntimeOrgFixture(t)
	if problems := validatePKCS11Runtime(org); len(problems) != 0 {
		t.Fatal(problems)
	}
	if problems := validatePeerMSPSigning(org); len(problems) != 1 || !strings.Contains(problems[0], "PKCS11 is unavailable") {
		t.Fatal("complete access must not bypass the public capability gate")
	}
}

var _ = Describe("Reserved PKCS11 provider access", func() {
	It("persists access references in both CRDs and accepts the staged Job/Deployment shapes", func() {
		org := pkcs11RuntimeOrgFixture(GinkgoT())
		net, _, _, _, _ := channelConfigOperationFixture()
		net.Name = "pkcs-runtime-contract"
		net.Spec.Orgs[1] = org
		net.Spec.Channels, net.Spec.Chaincodes = []fabricopsv1alpha1.Channel{}, []fabricopsv1alpha1.Chaincode{}
		participant := fabricParticipantFixture(net.Name)
		participant.Spec.Org = *org.DeepCopy()
		for _, object := range []client.Object{net, participant} {
			Expect(k8sClient.Create(ctx, object)).To(Succeed())
			DeferCleanup(func() {
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(object), object)).To(Succeed())
				object.SetFinalizers(nil)
				Expect(k8sClient.Update(ctx, object)).To(Succeed())
				Expect(k8sClient.Delete(ctx, object)).To(Succeed())
			})
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(object), object)).To(Succeed())
		}
		Expect(net.Spec.Orgs[1].Peer.MSPSigning.PKCS11.Runtime).To(Equal(org.Peer.MSPSigning.PKCS11.Runtime))
		Expect(participant.Spec.Org.Peer.MSPSigning.PKCS11.Runtime).To(Equal(org.Peer.MSPSigning.PKCS11.Runtime))
		Expect(validatePeerMSPSigning(org)).To(ContainElement(ContainSubstring("PKCS11 is unavailable")))
		request := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(net)}
		r := &FabricNetworkReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err := r.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, request.NamespacedName, net)).To(Succeed())
		Expect(net.Status.Message).To(ContainSubstring("PKCS11 is unavailable"))
		pr := &FabricParticipantReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err = pr.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(participant)})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(participant), participant)).To(Succeed())
		Expect(participant.Status.Message).To(ContainSubstring("PKCS11 is unavailable"))
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "pkcs-runtime-shapes"}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, ns)).To(Succeed()) })
		job := buildWorkloadEnrollmentJob(net, org, ns.Name, "peer1", componentPeer, nil)
		deployment := buildPeerDeployment(net, org, 1, ns.Name)
		Expect(k8sClient.Create(ctx, job)).To(Succeed())
		Expect(k8sClient.Create(ctx, deployment)).To(Succeed())
		Expect(job.Spec.Template.Spec.Containers[0].VolumeMounts[0].SubPath).To(Equal("output"))
	})
	It("rejects root execution, environment overrides, and optional config at schema admission", func() {
		for _, mutate := range []func(*fabricopsv1alpha1.PeerPKCS11Runtime){
			func(r *fabricopsv1alpha1.PeerPKCS11Runtime) { r.RunAsUser = 0 },
			func(r *fabricopsv1alpha1.PeerPKCS11Runtime) { r.FSGroup = 0 },
			func(r *fabricopsv1alpha1.PeerPKCS11Runtime) { r.ConfigEnv = "PATH" },
			func(r *fabricopsv1alpha1.PeerPKCS11Runtime) { yes := true; r.ConfigSecretKeyRef.Optional = &yes },
			func(r *fabricopsv1alpha1.PeerPKCS11Runtime) { r.ConfigSecretKeyRef.Key = "" },
			func(r *fabricopsv1alpha1.PeerPKCS11Runtime) { r.StateClaims[1].Peer = r.StateClaims[0].Peer },
		} {
			participant := fabricParticipantFixture("invalid-pkcs-runtime")
			participant.Spec.Org = pkcs11RuntimeOrgFixture(GinkgoT())
			mutate(participant.Spec.Org.Peer.MSPSigning.PKCS11.Runtime)
			err := k8sClient.Create(ctx, participant)
			if err == nil {
				Expect(k8sClient.Delete(ctx, participant)).To(Succeed())
			}
			Expect(apierrors.IsInvalid(err)).To(BeTrue(), "invalid access accepted: %v", err)
		}
	})
})

func TestPKCS11RuntimeMountsStayWithKeyActors(t *testing.T) {
	net, _, _, _, _ := channelConfigOperationFixture()
	org := pkcs11RuntimeOrgFixture(t)
	job := buildWorkloadEnrollmentJob(net, org, "org", "peer1", componentPeer, nil)
	pod := job.Spec.Template.Spec
	enroller, publisher := pod.InitContainers[0], pod.Containers[0]
	if pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken || pod.ServiceAccountName == enrollmentServiceAccountName(org) {
		t.Fatal("PKCS11 enrollment still shares the broad enrollment API identity")
	}
	if enroller.Image != "enrollment:development" || !strings.Contains(strings.Join(enroller.Command, " "), "fabricops-enroll-peer") {
		t.Error("provider-aware enrollment executable/image was not selected")
	}
	for _, container := range []corev1.Container{enroller, buildPeerDeployment(net, org, 1, "org").Spec.Template.Spec.Containers[0]} {
		pinFound := false
		for _, env := range container.Env {
			if env.Name == "FABRICOPS_PKCS11_PIN" {
				pinFound = env.Value == "" && env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil && env.ValueFrom.SecretKeyRef.Name == "token-pin"
			}
		}
		if !pinFound {
			t.Error("key actor did not receive an org-local PIN Secret reference")
		}
	}
	for _, env := range publisher.Env {
		if strings.Contains(env.Name, "PKCS11") || env.Name == "SOFTHSM2_CONF" {
			t.Fatal("publisher received provider environment")
		}
	}
	for _, mount := range publisher.VolumeMounts {
		if mount.Name == enrollmentPublisherTokenVolume && mount.ReadOnly && mount.MountPath == "/var/run/secrets/kubernetes.io/serviceaccount" {
			continue
		}
		if mount.Name != enrollmentVolumeName || mount.SubPath != "output" || !mount.ReadOnly {
			t.Error("publisher can access private working/provider material")
		}
	}
	claimFound := false
	for _, volume := range pod.Volumes {
		if volume.PersistentVolumeClaim != nil {
			claimFound = volume.PersistentVolumeClaim.ClaimName == "token-peer1"
		}
		if volume.Name == enrollmentVolumeName && (volume.EmptyDir == nil || volume.EmptyDir.Medium != corev1.StorageMediumMemory) {
			t.Error("enrollment working material is not memory-backed")
		}
	}
	if !claimFound {
		t.Error("peer1 did not select its own pre-provisioned token claim")
	}
	if pod.SecurityContext == nil || pod.SecurityContext.FSGroup == nil || *pod.SecurityContext.FSGroup != 1001 {
		t.Error("explicit shared group is missing")
	}
	if pod.SecurityContext == nil || pod.SecurityContext.SeccompProfile == nil || pod.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Error("runtime-default seccomp profile is missing")
	}
}
