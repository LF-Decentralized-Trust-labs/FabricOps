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
	"fmt"
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

func TestEnrollmentRoleCannotReadProviderCredentials(t *testing.T) {
	net, _, org, _, _ := channelConfigOperationFixture()
	role := buildEnrollmentRole(net, org, "org")
	for _, rule := range role.Rules {
		if len(rule.ResourceNames) == 0 {
			t.Fatal("enrollment still has namespace-wide Secret access")
		}
		for _, verb := range rule.Verbs {
			if verb != "get" && verb != "patch" {
				t.Fatal("enrollment still permits unrestricted Secret creation or replacement")
			}
		}
	}
	for _, pod := range []corev1.PodSpec{
		buildAdminEnrollmentJob(net, org, "org").Spec.Template.Spec,
		buildWorkloadEnrollmentJob(net, org, "org", "peer0", componentPeer, nil).Spec.Template.Spec,
		buildCARegistrarRotationJob(net, org, "org", "rotate-1").Spec.Template.Spec,
	} {
		if pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken {
			t.Fatal("CA-facing containers still automount Kubernetes API credentials")
		}
		for _, container := range pod.InitContainers {
			for _, mount := range container.VolumeMounts {
				if mount.Name == enrollmentPublisherTokenVolume {
					t.Fatal("enrollment key actor received publisher API credentials")
				}
			}
		}
	}
}

func TestEnrollmentOutputAllowlist(t *testing.T) {
	net, _, org, _, _ := channelConfigOperationFixture()
	for _, tls := range []bool{false, true} {
		net.Spec.Global.TLS = tls
		for _, hsm := range []bool{false, true} {
			org.Peer.MSPSigning = nil
			if hsm {
				org = pkcs11RuntimeOrgFixture(t)
			}
			expected := []string{"banka-admin-msp"}
			if tls {
				expected = append(expected, "banka-admin-tls")
			}
			if !hsm {
				for i := 0; i < org.Peer.Instances; i++ {
					peer := fmt.Sprintf("peer%d", i)
					expected = append(expected, peer+"-msp")
					if tls {
						expected = append(expected, peer+"-tls")
					}
				}
			}
			rule := buildEnrollmentRole(net, org, "org").Rules[0]
			if !reflect.DeepEqual(rule.ResourceNames, expected) {
				t.Fatalf("unexpected TLS=%v PKCS11=%v allowlist: %v", tls, hsm, rule.ResourceNames)
			}
		}
	}
	org = pkcs11RuntimeOrgFixture(t)
	for _, pin := range []bool{false, true} {
		aliased := *org.DeepCopy()
		if pin {
			aliased.Peer.MSPSigning.PKCS11.PINSecretKeyRef.Name = "banka-admin-msp"
		} else {
			aliased.Peer.MSPSigning.PKCS11.Runtime.ConfigSecretKeyRef.Name = "banka-admin-tls"
		}
		if len(validatePKCS11Runtime(aliased)) == 0 {
			t.Fatal("provider reference aliases a software enrollment destination")
		}
	}
}

func TestEnrollmentOutputPreparationPreservesLegacyAndForeignData(t *testing.T) {
	for _, foreign := range []bool{false, true} {
		net, ns, r := pkcs11RetentionFixture(t)
		org := net.Spec.Orgs[0]
		org.Peer.MSPSigning = nil
		legacy := enrollmentOutputSecrets(net, org, ns.Name)[0]
		legacy.Labels = map[string]string{labelIdentityKind: secretKindAdminMSP, labelWorkload: adminIdentityName(org), labelIdentitySource: identitySourceFabricCA}
		legacy.Annotations = nil
		legacy.Data = map[string][]byte{"existing-key": []byte("never-reset")}
		if foreign {
			legacy.Annotations = map[string]string{annotationFabricNetworkUID: "other-network"}
		}
		if err := r.Create(t.Context(), legacy); err != nil {
			t.Fatal(err)
		}
		before := legacy.DeepCopy()
		err := r.ensureEnrollmentOutputs(t.Context(), net, org, ns.Name)
		if (err != nil) != foreign {
			t.Fatalf("unexpected preservation decision for foreign=%v: %v", foreign, err)
		}
		if err := r.Get(t.Context(), client.ObjectKeyFromObject(legacy), legacy); err != nil || !reflect.DeepEqual(before, legacy) {
			t.Fatal("preparation modified existing identity material")
		}
	}
}

func TestEnrollmentRevokesBroadRoleEvenWhenOutputIsForeign(t *testing.T) {
	net, ns, r := pkcs11RetentionFixture(t)
	org := net.Spec.Orgs[0]
	org.Peer.MSPSigning = nil
	if err := rbacv1.AddToScheme(r.Scheme); err != nil {
		t.Fatal(err)
	}
	role := buildEnrollmentRole(net, org, ns.Name)
	role.Rules[0].ResourceNames = nil
	role.Rules[0].Verbs = []string{"get", "create", "update", "patch"}
	if err := r.Create(t.Context(), role); err != nil {
		t.Fatal(err)
	}
	foreign := enrollmentOutputSecrets(net, org, ns.Name)[0]
	foreign.Annotations[annotationFabricNetworkUID] = "foreign-network"
	if err := r.Create(t.Context(), foreign); err != nil {
		t.Fatal(err)
	}
	if err := r.ensureEnrollmentRBAC(t.Context(), net, org, ns.Name); err == nil {
		t.Fatal("foreign output was accepted")
	}
	if err := r.Get(t.Context(), client.ObjectKeyFromObject(role), role); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(role.Rules, buildEnrollmentRole(net, org, ns.Name).Rules) {
		t.Fatal("destination failure left the legacy broad grant in place")
	}
}

var _ = Describe("Software enrollment authorization", func() {
	It("narrows the legacy Role without exposing provider or enrollment credentials", func() {
		net, _, org, _, _ := channelConfigOperationFixture()
		net.Name, net.UID = "software-enrollment-access", "software-access-uid"
		ns := buildOrgNamespace(net, org)
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, ns)).To(Succeed()) })
		r := &FabricNetworkReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		legacyRole := buildEnrollmentRole(net, org, ns.Name)
		legacyRole.Rules = []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"get", "create", "update", "patch"}}}
		Expect(k8sClient.Create(ctx, legacyRole)).To(Succeed())
		Expect(r.ensureEnrollmentRBAC(ctx, net, org, ns.Name)).To(Succeed())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(legacyRole), legacyRole)).To(Succeed())
		Expect(legacyRole.Rules).To(Equal(buildEnrollmentRole(net, org, ns.Name).Rules))
		credentials := []string{"token-pin", "provider-config", caBootstrapSecretName(org), adminEnrollmentSecretName(org), identityEnrollmentSecretName("peer0")}
		for _, name := range credentials {
			Expect(k8sClient.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns.Name}})).To(Succeed())
		}
		config := rest.CopyConfig(cfg)
		config.Impersonate = rest.ImpersonationConfig{UserName: "system:serviceaccount:" + ns.Name + ":" + enrollmentServiceAccountName(org), Groups: []string{"system:serviceaccounts", "system:serviceaccounts:" + ns.Name, "system:authenticated"}}
		publisher, err := client.New(config, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		for _, output := range enrollmentOutputSecrets(net, org, ns.Name) {
			var secret corev1.Secret
			Eventually(func() error { return publisher.Get(ctx, client.ObjectKeyFromObject(output), &secret) }, 10*time.Second, 100*time.Millisecond).Should(Succeed())
			Expect(secret.Data).To(BeEmpty())
			before := secret.DeepCopy()
			secret.Data = map[string][]byte{"sentinel": []byte("published")}
			Expect(publisher.Patch(ctx, &secret, client.MergeFrom(before))).To(Succeed())
		}
		for _, name := range credentials {
			Expect(apierrors.IsForbidden(publisher.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: name}, &corev1.Secret{}))).To(BeTrue(), name)
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns.Name}}
			Expect(apierrors.IsForbidden(publisher.Patch(ctx, secret, client.RawPatch("application/merge-patch+json", []byte(`{"data":{}}`))))).To(BeTrue(), name)
		}
		Expect(apierrors.IsForbidden(publisher.List(ctx, &corev1.SecretList{}, client.InNamespace(ns.Name)))).To(BeTrue())
		Expect(apierrors.IsForbidden(publisher.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "unexpected", Namespace: ns.Name}}))).To(BeTrue())
		for _, job := range []*corev1.PodSpec{
			&buildAdminEnrollmentJob(net, org, ns.Name).Spec.Template.Spec,
			&buildWorkloadEnrollmentJob(net, org, ns.Name, "peer0", componentPeer, nil).Spec.Template.Spec,
		} {
			for _, container := range job.InitContainers {
				for _, mount := range container.VolumeMounts {
					Expect(mount.MountPath).NotTo(ContainSubstring("serviceaccount"))
				}
			}
			for _, volume := range job.Volumes {
				if volume.Projected != nil {
					Expect(volume.Projected.DefaultMode).To(HaveValue(Equal(int32(0444))))
				}
			}
			Expect(strings.Join(job.Containers[0].Command, " ")).To(ContainSubstring("--dry-run=client"))
		}
	})
})
