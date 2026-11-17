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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

func TestRestoreHoldPreventsReconciliationAndCleanup(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := fabricopsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	for _, deleting := range []bool{false, true} {
		net := &fabricopsv1alpha1.FabricNetwork{ObjectMeta: metav1.ObjectMeta{
			Name: "held", Namespace: "default", Finalizers: []string{fabricNetworkFinalizer},
			Annotations: map[string]string{fabricopsv1alpha1.RestoreHoldAnnotation: "archive-digest"},
		}}
		if deleting {
			now := metav1.Now()
			net.DeletionTimestamp = &now
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(net).Build()
		r := &FabricNetworkReconciler{Client: c, Scheme: scheme}
		if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(net)}); err != nil {
			t.Fatal(err)
		}
		actual := &fabricopsv1alpha1.FabricNetwork{}
		if err := c.Get(t.Context(), client.ObjectKeyFromObject(net), actual); err != nil {
			t.Fatal(err)
		}
		if len(actual.Finalizers) != 1 {
			t.Fatal("hold allowed finalizer cleanup")
		}
		namespaces := &corev1.NamespaceList{}
		if err := c.List(t.Context(), namespaces); err != nil {
			t.Fatal(err)
		}
		if len(namespaces.Items) != 0 {
			t.Fatal("hold allowed workload namespace creation")
		}
	}
}
