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
	"reflect"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestPKCS11PublisherAccessBoundaries(t *testing.T) {
	net, _, _, _, _ := channelConfigOperationFixture()
	org := pkcs11RuntimeOrgFixture(t)
	for _, tls := range []bool{false, true} {
		net.Spec.Global.TLS = tls
		account, role, binding := pkcs11PublisherRBAC(net, org, "org", "peer0")
		names := []string{"peer0-msp"}
		if tls {
			names = append(names, "peer0-tls")
		}
		if account.Name != "peer0-identity-publisher" || account.AutomountServiceAccountToken == nil || *account.AutomountServiceAccountToken || len(role.Rules) != 1 {
			t.Fatal("publisher identity is not explicit and non-automounting")
		}
		rule := role.Rules[0]
		if !reflect.DeepEqual(rule.ResourceNames, names) || !reflect.DeepEqual(rule.Verbs, []string{"get", "patch"}) || !reflect.DeepEqual(rule.Resources, []string{"secrets"}) || !reflect.DeepEqual(rule.APIGroups, []string{""}) {
			t.Fatal("publisher role permits more than its prepared outputs")
		}
		if len(binding.Subjects) != 1 || binding.Subjects[0].Name != account.Name || binding.Subjects[0].Namespace != "org" || binding.RoleRef.Name != role.Name {
			t.Fatal("publisher binding targets a shared identity")
		}
		pod := buildWorkloadEnrollmentJob(net, org, "org", "peer0", componentPeer, nil).Spec.Template.Spec
		projected := 0
		for _, volume := range pod.Volumes {
			if volume.Projected == nil {
				continue
			}
			projected++
			if volume.Name != enrollmentPublisherTokenVolume || len(volume.Projected.Sources) != 3 || volume.Projected.Sources[0].ServiceAccountToken == nil || *volume.Projected.Sources[0].ServiceAccountToken.ExpirationSeconds != 3600 {
				t.Fatal("publisher token is not an explicit bounded projection")
			}
		}
		if projected != 1 {
			t.Fatal("publisher token projection is missing or duplicated")
		}
		for _, mount := range pod.InitContainers[0].VolumeMounts {
			if mount.Name == enrollmentPublisherTokenVolume || strings.Contains(mount.MountPath, "serviceaccount") {
				t.Fatal("key actor received Kubernetes API credentials")
			}
		}
		peerPod := buildPeerDeployment(net, org, 0, "org").Spec.Template.Spec
		for _, volume := range peerPod.Volumes {
			if volume.Projected != nil {
				t.Fatal("peer runtime received publisher API credentials")
			}
		}
	}
	for _, name := range []string{"peer0-msp", "peer0-tls", "peer1-msp", "peer1-tls"} {
		for _, pin := range []bool{true, false} {
			aliased := *org.DeepCopy()
			if pin {
				aliased.Peer.MSPSigning.PKCS11.PINSecretKeyRef.Name = name
			} else {
				aliased.Peer.MSPSigning.PKCS11.Runtime.ConfigSecretKeyRef.Name = name
			}
			if len(validatePKCS11Runtime(aliased)) == 0 {
				t.Fatal("provider credential alias was accepted")
			}
		}
	}
}

func TestPKCS11PublisherPreparationPreservesDestinations(t *testing.T) {
	net, namespace, r := pkcs11RetentionFixture(t)
	if err := rbacv1.AddToScheme(r.Scheme); err != nil {
		t.Fatal(err)
	}
	org := net.Spec.Orgs[0]
	prepare := func() error { return r.preparePKCS11Publisher(t.Context(), net, org, namespace.Name, "peer0") }
	if err := prepare(); err != nil {
		t.Fatal(err)
	}
	var destination corev1.Secret
	key := client.ObjectKey{Namespace: namespace.Name, Name: "peer0-msp"}
	if err := r.Get(t.Context(), key, &destination); err != nil {
		t.Fatal(err)
	}
	if len(destination.Data) != 0 || destination.Annotations[annotationFabricNetworkUID] != string(net.UID) {
		t.Fatal("destination contains generated material or lacks ownership")
	}
	version := destination.ResourceVersion
	if err := prepare(); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(t.Context(), key, &destination); err != nil || destination.ResourceVersion != version {
		t.Fatal("preparing an existing destination rewrote it")
	}
	destination.Data = map[string][]byte{"keystore.pem": []byte("do-not-replace")}
	if err := r.Update(t.Context(), &destination); err != nil {
		t.Fatal(err)
	}
	before := destination.DeepCopy()
	if err := prepare(); err == nil || strings.Contains(err.Error(), "do-not-replace") {
		t.Fatal("software material was accepted or leaked")
	}
	if err := r.Get(t.Context(), key, &destination); err != nil || !reflect.DeepEqual(before, &destination) {
		t.Fatal("existing identity material was modified")
	}
}

func TestPKCS11PublisherRejectsForeignOwnership(t *testing.T) {
	for _, kind := range []string{"Secret", "ServiceAccount", "Role", "RoleBinding"} {
		t.Run(kind, func(t *testing.T) {
			net, namespace, r := pkcs11RetentionFixture(t)
			if err := rbacv1.AddToScheme(r.Scheme); err != nil {
				t.Fatal(err)
			}
			org := net.Spec.Orgs[0]
			account, role, binding := pkcs11PublisherRBAC(net, org, namespace.Name, "peer0")
			var object client.Object
			switch kind {
			case "Secret":
				object = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "peer0-msp", Namespace: namespace.Name}}
			case "ServiceAccount":
				object = account
			case "Role":
				object = role
			case "RoleBinding":
				object = binding
			}
			object.SetAnnotations(map[string]string{annotationFabricNetworkUID: "foreign-owner"})
			if err := r.Create(t.Context(), object); err != nil {
				t.Fatal(err)
			}
			before := object.DeepCopyObject()
			if err := r.preparePKCS11Publisher(t.Context(), net, org, namespace.Name, "peer0"); err == nil {
				t.Fatal("foreign destination or publisher identity was adopted")
			}
			if err := r.Get(t.Context(), client.ObjectKeyFromObject(object), object); err != nil || !reflect.DeepEqual(before, object) {
				t.Fatal("foreign object was changed")
			}
		})
	}
}

func TestPKCS11PublisherRepairsOwnedPermissionDrift(t *testing.T) {
	net, namespace, r := pkcs11RetentionFixture(t)
	if err := rbacv1.AddToScheme(r.Scheme); err != nil {
		t.Fatal(err)
	}
	org := net.Spec.Orgs[0]
	prepare := func() error { return r.preparePKCS11Publisher(t.Context(), net, org, namespace.Name, "peer0") }
	if err := prepare(); err != nil {
		t.Fatal(err)
	}
	account, role, binding := pkcs11PublisherRBAC(net, org, namespace.Name, "peer0")
	for _, object := range []client.Object{account, role, binding} {
		if err := r.Get(t.Context(), client.ObjectKeyFromObject(object), object); err != nil {
			t.Fatal(err)
		}
	}
	automount := true
	account.AutomountServiceAccountToken = &automount
	role.Rules[0].ResourceNames = nil
	role.Rules[0].Verbs = []string{"*"}
	binding.Subjects = append(binding.Subjects, rbacv1.Subject{Kind: rbacv1.ServiceAccountKind, Name: "unexpected", Namespace: namespace.Name})
	for _, object := range []client.Object{account, role, binding} {
		if err := r.Update(t.Context(), object); err != nil {
			t.Fatal(err)
		}
	}
	if err := prepare(); err != nil {
		t.Fatal(err)
	}
	expectedAccount, expectedRole, expectedBinding := pkcs11PublisherRBAC(net, org, namespace.Name, "peer0")
	for _, object := range []client.Object{account, role, binding} {
		if err := r.Get(t.Context(), client.ObjectKeyFromObject(object), object); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(account.AutomountServiceAccountToken, expectedAccount.AutomountServiceAccountToken) ||
		!reflect.DeepEqual(role.Rules, expectedRole.Rules) || !reflect.DeepEqual(binding.Subjects, expectedBinding.Subjects) {
		t.Fatal("reconciliation did not restore narrow publisher permissions")
	}
}

var _ = Describe("PKCS11 publisher authorization", func() {
	It("permits only named destination reads and patches under real RBAC", func() {
		net, _, _, _, _ := channelConfigOperationFixture()
		net.Name, net.UID = "pkcs-publisher-auth", "publisher-network-uid"
		org := pkcs11RuntimeOrgFixture(GinkgoT())
		ns := buildOrgNamespace(net, org)
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, ns)).To(Succeed()) })
		r := &FabricNetworkReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		Expect(r.preparePKCS11Publisher(ctx, net, org, ns.Name, "peer0")).To(Succeed())
		Expect(r.preparePKCS11Publisher(ctx, net, org, ns.Name, "peer1")).To(Succeed())
		for _, name := range []string{"token-pin", "provider-config", caBootstrapSecretName(org), adminEnrollmentSecretName(org), identityEnrollmentSecretName("peer0")} {
			Expect(k8sClient.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns.Name}})).To(Succeed())
		}
		config := rest.CopyConfig(cfg)
		config.Impersonate = rest.ImpersonationConfig{UserName: "system:serviceaccount:" + ns.Name + ":" + pkcs11PublisherName("peer0"), Groups: []string{"system:serviceaccounts", "system:serviceaccounts:" + ns.Name, "system:authenticated"}}
		publisher, err := client.New(config, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		var destination corev1.Secret
		Eventually(func() error {
			return publisher.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: "peer0-msp"}, &destination)
		}, 10*time.Second, 100*time.Millisecond).Should(Succeed())
		before := destination.DeepCopy()
		destination.Data = map[string][]byte{"config.yaml": []byte("public-output")}
		Expect(publisher.Patch(ctx, &destination, client.MergeFrom(before))).To(Succeed())
		Expect(publisher.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: "peer0-tls"}, &corev1.Secret{})).To(Succeed())
		for _, name := range []string{"peer1-msp", "peer1-tls", "token-pin", "provider-config", caBootstrapSecretName(org), adminEnrollmentSecretName(org), identityEnrollmentSecretName("peer0")} {
			Expect(apierrors.IsForbidden(publisher.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: name}, &corev1.Secret{}))).To(BeTrue(), name)
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns.Name}}
			Expect(apierrors.IsForbidden(publisher.Patch(ctx, secret, client.RawPatch("application/merge-patch+json", []byte(`{"data":{}}`))))).To(BeTrue(), name)
		}
		Expect(apierrors.IsForbidden(publisher.List(ctx, &corev1.SecretList{}, client.InNamespace(ns.Name)))).To(BeTrue())
		Expect(apierrors.IsForbidden(publisher.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "new-secret", Namespace: ns.Name}}))).To(BeTrue())
		Expect(apierrors.IsForbidden(publisher.Update(ctx, &destination))).To(BeTrue())
		Expect(apierrors.IsForbidden(publisher.Delete(ctx, &destination))).To(BeTrue())
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "publisher-admission", Namespace: ns.Name}, Spec: buildWorkloadEnrollmentJob(net, org, ns.Name, "peer0", componentPeer, nil).Spec.Template.Spec}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		Expect(pod.Spec.AutomountServiceAccountToken).To(HaveValue(BeFalse()))
		for _, mount := range pod.Spec.InitContainers[0].VolumeMounts {
			Expect(mount.Name).NotTo(Equal(enrollmentPublisherTokenVolume))
			Expect(mount.MountPath).NotTo(ContainSubstring("serviceaccount"))
		}
	})
})
