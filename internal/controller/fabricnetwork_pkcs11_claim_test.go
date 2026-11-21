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
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestPKCS11MissingStateClaimBlocksEnrollment(t *testing.T) {
	net, ns, r := pkcs11RetentionFixture(t)
	if err := batchv1.AddToScheme(r.Scheme); err != nil {
		t.Fatal(err)
	}
	if err := rbacv1.AddToScheme(r.Scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(r.Scheme); err != nil {
		t.Fatal(err)
	}
	err := r.reconcileWorkloadEnrollment(t.Context(), net, net.Spec.Orgs[0], ns.Name, "peer0", componentPeer, nil)
	if err == nil {
		t.Fatal("enrollment accepted a missing token claim")
	}
	if _, err := r.reconcilePeers(t.Context(), net, net.Spec.Orgs[0], ns.Name); err == nil {
		t.Fatal("peer scheduling accepted a missing token claim")
	}
	for _, list := range []client.ObjectList{&batchv1.JobList{}, &corev1.SecretList{}, &rbacv1.RoleList{}, &appsv1.DeploymentList{}, &corev1.PersistentVolumeClaimList{}} {
		if err := r.List(t.Context(), list); err != nil {
			t.Fatal(err)
		}
		switch objects := list.(type) {
		case *batchv1.JobList:
			if len(objects.Items) != 0 {
				t.Fatal("enrollment Job created before claim validation")
			}
		case *corev1.SecretList:
			if len(objects.Items) != 0 {
				t.Fatal("publisher destinations created before claim validation")
			}
		case *rbacv1.RoleList:
			if len(objects.Items) != 0 {
				t.Fatal("publisher privileges created before claim validation")
			}
		case *appsv1.DeploymentList:
			if len(objects.Items) != 0 {
				t.Fatal("peer Deployment created before claim validation")
			}
		case *corev1.PersistentVolumeClaimList:
			if len(objects.Items) != 0 {
				t.Fatal("storage provisioned before token claim validation")
			}
		}
	}
}

func preparedPKCS11Claim(namespace string, networkUID types.UID) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "token-peer0", Namespace: namespace, UID: "claim-uid",
		Annotations: map[string]string{annotationFabricNetworkUID: string(networkUID), annotationPKCS11Peer: "peer0"}},
		Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "prepared-token-volume", AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Mi")}}},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}
}

func TestPKCS11StateClaimPolicy(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*corev1.PersistentVolumeClaim)
	}{
		{"missing UID", func(c *corev1.PersistentVolumeClaim) { c.UID = "" }},
		{"foreign network", func(c *corev1.PersistentVolumeClaim) { c.Annotations[annotationFabricNetworkUID] = "foreign" }},
		{"wrong peer", func(c *corev1.PersistentVolumeClaim) { c.Annotations[annotationPKCS11Peer] = "peer1" }},
		{"pending", func(c *corev1.PersistentVolumeClaim) { c.Status.Phase = corev1.ClaimPending }},
		{"lost", func(c *corev1.PersistentVolumeClaim) { c.Status.Phase = corev1.ClaimLost }},
		{"no volume", func(c *corev1.PersistentVolumeClaim) { c.Spec.VolumeName = "" }},
		{"raw block", func(c *corev1.PersistentVolumeClaim) { mode := corev1.PersistentVolumeBlock; c.Spec.VolumeMode = &mode }},
		{"shared access", func(c *corev1.PersistentVolumeClaim) {
			c.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
		}},
		{"read only", func(c *corev1.PersistentVolumeClaim) {
			c.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadOnlyMany}
		}},
		{"ambiguous access", func(c *corev1.PersistentVolumeClaim) {
			c.Spec.AccessModes = append(c.Spec.AccessModes, corev1.ReadWriteMany)
		}},
		{"controller-owned", func(c *corev1.PersistentVolumeClaim) {
			yes := true
			c.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: "owner", UID: "owner", Controller: &yes}}
		}},
		{"deleting", func(c *corev1.PersistentVolumeClaim) {
			now := metav1.Now()
			c.DeletionTimestamp = &now
			c.Finalizers = []string{"test-protection"}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			net, ns, r := pkcs11RetentionFixture(t)
			claim := preparedPKCS11Claim(ns.Name, net.UID)
			test.mutate(claim)
			if err := r.Create(t.Context(), claim); err != nil {
				t.Fatal(err)
			}
			if test.name == "deleting" {
				if err := r.Delete(t.Context(), claim); err != nil {
					t.Fatal(err)
				}
				if err := r.Get(t.Context(), client.ObjectKeyFromObject(claim), claim); err != nil {
					t.Fatal(err)
				}
			}
			before := claim.DeepCopy()
			if err := r.validateAndBindPKCS11StateClaim(t.Context(), net, net.Spec.Orgs[0], ns.Name, "peer0"); err == nil {
				t.Fatal("unsafe or unprepared token claim was accepted")
			}
			if err := r.Get(t.Context(), client.ObjectKeyFromObject(claim), claim); err != nil || !reflect.DeepEqual(before, claim) {
				t.Fatal("claim validation modified user-owned storage")
			}
			if err := r.Get(t.Context(), client.ObjectKeyFromObject(ns), ns); err != nil {
				t.Fatal(err)
			}
			if _, exists := ns.Annotations[annotationPKCS11StateRetention]; exists {
				t.Fatal("invalid claim was durably bound")
			}
		})
	}
}

func TestPKCS11ClaimBindingIsPersistentAndIdempotent(t *testing.T) {
	for _, access := range []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce, corev1.ReadWriteOncePod} {
		net, ns, r := pkcs11RetentionFixture(t)
		claim := preparedPKCS11Claim(ns.Name, net.UID)
		claim.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{access}
		if err := r.Create(t.Context(), claim); err != nil {
			t.Fatal(err)
		}
		validate := func() error {
			return r.validateAndBindPKCS11StateClaim(t.Context(), net, net.Spec.Orgs[0], ns.Name, "peer0")
		}
		if err := validate(); err != nil {
			t.Fatal(err)
		}
		if err := r.Get(t.Context(), client.ObjectKeyFromObject(ns), ns); err != nil {
			t.Fatal(err)
		}
		before := ns.DeepCopy()
		if err := validate(); err != nil {
			t.Fatal(err)
		}
		if err := r.Get(t.Context(), client.ObjectKeyFromObject(ns), ns); err != nil || !reflect.DeepEqual(before, ns) {
			t.Fatal("unchanged claim binding caused an update")
		}
		if err := r.recordPKCS11StateRetention(t.Context(), net); err != nil {
			t.Fatal(err)
		}
		if err := r.Get(t.Context(), client.ObjectKeyFromObject(ns), ns); err != nil || !reflect.DeepEqual(before, ns) {
			t.Fatal("reference refresh lost the claim UID binding")
		}
		if err := r.Delete(t.Context(), claim); err != nil {
			t.Fatal(err)
		}
		claim.UID, claim.ResourceVersion = "replacement-claim-uid", ""
		if err := r.Create(t.Context(), claim); err != nil {
			t.Fatal(err)
		}
		if err := validate(); err == nil {
			t.Fatal("a recreated claim was silently adopted")
		}
		if err := r.Get(t.Context(), client.ObjectKeyFromObject(ns), ns); err != nil || !reflect.DeepEqual(before, ns) {
			t.Fatal("replacement failure rewrote retention evidence")
		}
	}
}

func TestPKCS11ClaimCannotMoveBetweenPeers(t *testing.T) {
	net, ns, r := pkcs11RetentionFixture(t)
	claim := preparedPKCS11Claim(ns.Name, net.UID)
	if err := r.Create(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	if err := r.validateAndBindPKCS11StateClaim(t.Context(), net, net.Spec.Orgs[0], ns.Name, "peer0"); err != nil {
		t.Fatal(err)
	}
	claim.Annotations[annotationPKCS11Peer] = "peer1"
	if err := r.Update(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	states := net.Spec.Orgs[0].Peer.MSPSigning.PKCS11.Runtime.StateClaims
	states[0].ClaimName, states[1].ClaimName = states[1].ClaimName, states[0].ClaimName
	if err := r.validateAndBindPKCS11StateClaim(t.Context(), net, net.Spec.Orgs[0], ns.Name, "peer1"); err == nil {
		t.Fatal("retained token storage was reassigned to a different peer")
	}
}

var _ = Describe("Prepared PKCS11 claim binding", func() {
	It("persists API-assigned PVC UIDs and refuses replacement without changing storage", func() {
		net, _, _, _, _ := channelConfigOperationFixture()
		net.Name, net.UID = "pkcs-claim-binding", "claim-network-uid"
		org := pkcs11RuntimeOrgFixture(GinkgoT())
		net.Spec.Orgs[1] = org
		ns := buildOrgNamespace(net, org)
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, ns)).To(Succeed()) })
		claim := preparedPKCS11Claim(ns.Name, net.UID)
		claim.UID, claim.Status = "", corev1.PersistentVolumeClaimStatus{}
		Expect(k8sClient.Create(ctx, claim)).To(Succeed())
		claim.Status.Phase = corev1.ClaimBound
		Expect(k8sClient.Status().Update(ctx, claim)).To(Succeed())
		r := &FabricNetworkReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		Expect(r.validateAndBindPKCS11StateClaim(ctx, net, org, ns.Name, "peer0")).To(Succeed())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(ns), ns)).To(Succeed())
		var record pkcs11StateRetention
		Expect(json.Unmarshal([]byte(ns.Annotations[annotationPKCS11StateRetention]), &record)).To(Succeed())
		Expect(record.ClaimBindings[claim.Name]).To(Equal(pkcs11ClaimBinding{UID: claim.UID, Peer: "peer0"}))
		original := claim.UID
		claim.Finalizers = nil
		Expect(k8sClient.Update(ctx, claim)).To(Succeed())
		Expect(k8sClient.Delete(ctx, claim)).To(Succeed())
		claim = preparedPKCS11Claim(ns.Name, net.UID)
		claim.UID, claim.Status = "", corev1.PersistentVolumeClaimStatus{}
		Expect(k8sClient.Create(ctx, claim)).To(Succeed())
		claim.Status.Phase = corev1.ClaimBound
		Expect(k8sClient.Status().Update(ctx, claim)).To(Succeed())
		Expect(claim.UID).NotTo(Equal(original))
		Expect(r.validateAndBindPKCS11StateClaim(ctx, net, org, ns.Name, "peer0")).NotTo(Succeed())
	})
})
