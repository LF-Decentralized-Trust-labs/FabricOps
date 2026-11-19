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
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestPKCS11RetentionSurvivesRemovedOrg(t *testing.T) {
	net, _, _, _, _ := channelConfigOperationFixture()
	first := buildOrgNamespace(net, net.Spec.Orgs[0])
	protected := buildOrgNamespace(net, net.Spec.Orgs[1])
	protected.Annotations["fabricops.io/pkcs11-state-retention"] = "requires-review"
	net.Spec.Orgs = net.Spec.Orgs[:1]
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(first, protected).Build()
	r := &FabricNetworkReconciler{Client: c, Scheme: scheme}
	if err := r.cleanupFabricNetwork(t.Context(), net); err == nil {
		t.Fatal("cleanup ignored retained token state after org removal")
	}
	for _, namespace := range []*corev1.Namespace{first, protected} {
		if err := c.Get(t.Context(), client.ObjectKeyFromObject(namespace), &corev1.Namespace{}); err != nil {
			t.Fatal("cleanup deleted a namespace before checking retained token state")
		}
	}
}

func pkcs11RetentionFixture(t *testing.T) (*fabricopsv1alpha1.FabricNetwork, *corev1.Namespace, *FabricNetworkReconciler) {
	t.Helper()
	net, _, _, _, _ := channelConfigOperationFixture()
	net.UID = "network-uid"
	net.Spec.Orgs = []fabricopsv1alpha1.Org{pkcs11RuntimeOrgFixture(t)}
	namespace := buildOrgNamespace(net, net.Spec.Orgs[0])
	namespace.UID = "namespace-uid"
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(namespace).Build()
	return net, namespace, &FabricNetworkReconciler{Client: c, Scheme: scheme}
}

func TestPKCS11RetentionIsMonotonicAndIdempotent(t *testing.T) {
	net, namespace, r := pkcs11RetentionFixture(t)
	read := func() pkcs11StateRetention {
		t.Helper()
		if err := r.Get(t.Context(), client.ObjectKeyFromObject(namespace), namespace); err != nil {
			t.Fatal(err)
		}
		var record pkcs11StateRetention
		if err := json.Unmarshal([]byte(namespace.Annotations[annotationPKCS11StateRetention]), &record); err != nil {
			t.Fatal(err)
		}
		return record
	}
	if err := r.recordPKCS11StateRetention(t.Context(), net); err != nil {
		t.Fatal(err)
	}
	expected := pkcs11StateRetention{Version: 1, NetworkUID: net.UID, NamespaceUID: namespace.UID, Claims: []string{"token-peer0", "token-peer1"}}
	if got := read(); !reflect.DeepEqual(got, expected) {
		t.Fatalf("unexpected public retention record: %+v", got)
	}
	version := namespace.ResourceVersion
	if err := r.recordPKCS11StateRetention(t.Context(), net); err != nil {
		t.Fatal(err)
	}
	read()
	if namespace.ResourceVersion != version {
		t.Fatal("unchanged retention record caused an update")
	}
	net.Spec.Orgs[0].Peer.MSPSigning.PKCS11.Runtime.StateClaims[0].ClaimName = "replacement-token"
	if err := r.recordPKCS11StateRetention(t.Context(), net); err != nil {
		t.Fatal(err)
	}
	expected.Claims = []string{"replacement-token", "token-peer0", "token-peer1"}
	if got := read(); !reflect.DeepEqual(got, expected) {
		t.Fatalf("claim change discarded retained references: %+v", got)
	}
	net.Spec.Orgs[0].Peer.MSPSigning = nil
	if err := r.recordPKCS11StateRetention(t.Context(), net); err != nil {
		t.Fatal(err)
	}
	if err := r.ensureNamespace(t.Context(), buildOrgNamespace(net, net.Spec.Orgs[0])); err != nil {
		t.Fatal(err)
	}
	if got := read(); !reflect.DeepEqual(got, expected) {
		t.Fatal("metadata refresh or provider removal changed retained state")
	}
	if err := r.cleanupFabricNetwork(t.Context(), net); err == nil {
		t.Fatal("provider removal released the hold")
	}
	delete(namespace.Annotations, annotationPKCS11StateRetention)
	if err := r.Update(t.Context(), namespace); err != nil {
		t.Fatal(err)
	}
	if err := r.cleanupFabricNetwork(t.Context(), net); err != nil {
		t.Fatal("explicit reviewed release did not permit ordinary cleanup:", err)
	}
}

func TestPKCS11RetentionRejectsUnsafeRecordsAndOwnership(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*fabricopsv1alpha1.FabricNetwork, *corev1.Namespace)
	}{
		{"empty", func(_ *fabricopsv1alpha1.FabricNetwork, n *corev1.Namespace) {
			n.Annotations[annotationPKCS11StateRetention] = ""
		}},
		{"malformed", func(_ *fabricopsv1alpha1.FabricNetwork, n *corev1.Namespace) {
			n.Annotations[annotationPKCS11StateRetention] = "private-value-not-for-errors"
		}},
		{"missing bindings", func(_ *fabricopsv1alpha1.FabricNetwork, n *corev1.Namespace) {
			n.Annotations[annotationPKCS11StateRetention] = `{"claims":["token"]}`
		}},
		{"unknown field", func(_ *fabricopsv1alpha1.FabricNetwork, n *corev1.Namespace) {
			n.Annotations[annotationPKCS11StateRetention] = `{"version":1,"networkUID":"network-uid","namespaceUID":"namespace-uid","claims":["token"],"unknown":true}`
		}},
		{"trailing JSON", func(_ *fabricopsv1alpha1.FabricNetwork, n *corev1.Namespace) {
			n.Annotations[annotationPKCS11StateRetention] += `{}`
		}},
		{"unsupported version", func(_ *fabricopsv1alpha1.FabricNetwork, n *corev1.Namespace) {
			n.Annotations[annotationPKCS11StateRetention] = `{"version":2,"networkUID":"network-uid","namespaceUID":"namespace-uid","claims":["token"]}`
		}},
		{"old network", func(_ *fabricopsv1alpha1.FabricNetwork, n *corev1.Namespace) {
			n.Annotations[annotationPKCS11StateRetention] = `{"version":1,"networkUID":"old-network","namespaceUID":"namespace-uid","claims":["token"]}`
		}},
		{"old namespace", func(_ *fabricopsv1alpha1.FabricNetwork, n *corev1.Namespace) {
			n.Annotations[annotationPKCS11StateRetention] = `{"version":1,"networkUID":"network-uid","namespaceUID":"old-namespace","claims":["token"]}`
		}},
		{"invalid recorded claim", func(_ *fabricopsv1alpha1.FabricNetwork, n *corev1.Namespace) {
			n.Annotations[annotationPKCS11StateRetention] = `{"version":1,"networkUID":"network-uid","namespaceUID":"namespace-uid","claims":["INVALID"]}`
		}},
		{"foreign UID annotation", func(_ *fabricopsv1alpha1.FabricNetwork, n *corev1.Namespace) {
			n.Annotations[annotationFabricNetworkUID] = "someone-else"
		}},
		{"missing network UID", func(net *fabricopsv1alpha1.FabricNetwork, _ *corev1.Namespace) { net.UID = "" }},
		{"missing namespace UID", func(_ *fabricopsv1alpha1.FabricNetwork, n *corev1.Namespace) { n.UID = "" }},
		{"foreign labels", func(_ *fabricopsv1alpha1.FabricNetwork, n *corev1.Namespace) { n.Labels[labelOrg] = "foreign" }},
		{"invalid declared claim", func(net *fabricopsv1alpha1.FabricNetwork, _ *corev1.Namespace) {
			net.Spec.Orgs[0].Peer.MSPSigning.PKCS11.Runtime.StateClaims[0].ClaimName = "INVALID"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			net, namespace, r := pkcs11RetentionFixture(t)
			if err := r.recordPKCS11StateRetention(t.Context(), net); err != nil {
				t.Fatal(err)
			}
			if err := r.Get(t.Context(), client.ObjectKeyFromObject(namespace), namespace); err != nil {
				t.Fatal(err)
			}
			test.mutate(net, namespace)
			if err := r.Update(t.Context(), namespace); err != nil {
				t.Fatal(err)
			}
			before := namespace.DeepCopy()
			if err := r.recordPKCS11StateRetention(t.Context(), net); err == nil {
				t.Fatal("unsafe record or ownership was accepted")
			}
			if err := r.Get(t.Context(), client.ObjectKeyFromObject(namespace), namespace); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, namespace) {
				t.Fatal("unsafe record was modified")
			}
			net.Spec.Orgs = nil
			if err := r.cleanupFabricNetwork(t.Context(), net); err == nil {
				t.Fatal("cleanup ignored the retained marker")
			}
		})
	}
}

func TestPKCS11RetentionRetriesConflictsAndRejectsNamespaceReplacement(t *testing.T) {
	for _, replaced := range []bool{false, true} {
		net, namespace, r := pkcs11RetentionFixture(t)
		updates := 0
		r.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				updates++
				if updates == 1 {
					if replaced {
						if err := c.Delete(ctx, namespace); err != nil {
							return err
						}
						namespace.UID, namespace.ResourceVersion = "replacement-namespace", ""
						if err := c.Create(ctx, namespace); err != nil {
							return err
						}
					}
					return apierrors.NewConflict(schema.GroupResource{Resource: "namespaces"}, obj.GetName(), errors.New("conflict"))
				}
				return c.Update(ctx, obj, opts...)
			},
		})
		err := r.recordPKCS11StateRetention(t.Context(), net)
		if replaced {
			if err == nil || updates != 1 {
				t.Fatal("namespace replacement was adopted during retry")
			}
		} else if err != nil || updates != 2 {
			t.Fatal("conflict was not retried:", err)
		}
	}
}

var _ = Describe("PKCS11 namespace retention", func() {
	It("records holds for both gated APIs without enabling provider workloads", func() {
		org := pkcs11RuntimeOrgFixture(GinkgoT())
		net, _, _, _, _ := channelConfigOperationFixture()
		net.Name = "pkcs-retention"
		net.Spec.Orgs = []fabricopsv1alpha1.Org{org}
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
		}
		r := &FabricNetworkReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		local := participantLocalFabricNetwork(participant)
		for _, owner := range []*fabricopsv1alpha1.FabricNetwork{net, local} {
			ns := buildOrgNamespace(owner, org)
			Expect(k8sClient.Create(ctx, ns)).To(Succeed())
			DeferCleanup(func() { Expect(k8sClient.Delete(ctx, ns)).To(Succeed()) })
		}
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(net)})
		Expect(err).NotTo(HaveOccurred())
		pr := &FabricParticipantReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err = pr.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(participant)})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(net), net)).To(Succeed())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(participant), participant)).To(Succeed())
		Expect(net.Status.Message).To(ContainSubstring("PKCS11 is unavailable"))
		Expect(participant.Status.Message).To(ContainSubstring("PKCS11 is unavailable"))
		for _, owner := range []*fabricopsv1alpha1.FabricNetwork{net, local} {
			var ns corev1.Namespace
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: orgNamespaceName(owner, org)}, &ns)).To(Succeed())
			var record pkcs11StateRetention
			Expect(json.Unmarshal([]byte(ns.Annotations[annotationPKCS11StateRetention]), &record)).To(Succeed())
			Expect(record.NetworkUID).To(Equal(owner.UID))
			Expect(record.NamespaceUID).To(Equal(ns.UID))
			Expect(record.Claims).To(Equal([]string{"token-peer0", "token-peer1"}))
			owner.Spec.Orgs = nil
			Expect(r.cleanupFabricNetwork(ctx, owner)).NotTo(Succeed())
		}
	})
})
