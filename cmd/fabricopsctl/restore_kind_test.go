//go:build restore_e2e

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

package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
	"github.com/LF-Decentralized-Trust-labs/FabricOps/internal/controller"
)

func TestMetadataRestoreKind(t *testing.T) {
	const target = "kind-fabricops-restore-e2e"
	if os.Getenv("FABRICOPS_RESTORE_KIND_CONTEXT") != target {
		t.Skip("requires dedicated restore Kind context")
	}
	kube := kubeOptions{namespace: "default", context: target}
	c, err := newClient(kube)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	mustCreate := func(obj ctrlclient.Object) {
		t.Helper()
		if err := c.Create(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	mustCreate(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "restore-operator"}})
	zero := int32(0)
	manager := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "manager", Namespace: "restore-operator"},
		Spec: appsv1.DeploymentSpec{Replicas: &zero,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "manager"}},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "manager"}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "manager", Image: "registry.k8s.io/pause:3.10"}}}}}}
	mustCreate(manager)
	for {
		if err := c.Get(ctx, ctrlclient.ObjectKeyFromObject(manager), manager); err != nil {
			t.Fatal(err)
		}
		if manager.Status.ObservedGeneration == manager.Generation {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("stopped manager did not settle")
		case <-time.After(time.Second):
		}
	}
	net := &fabricopsv1alpha1.FabricNetwork{ObjectMeta: metav1.ObjectMeta{Name: "restore-smoke", Namespace: "default"},
		Spec: fabricopsv1alpha1.FabricNetworkSpec{Global: fabricopsv1alpha1.GlobalConfig{FabricVersion: "3.1.0", TLS: true},
			Channels: []fabricopsv1alpha1.Channel{}, Chaincodes: []fabricopsv1alpha1.Chaincode{},
			Orgs: []fabricopsv1alpha1.Org{{Organization: fabricopsv1alpha1.OrgMeta{
				Name: "BankA", Domain: "banka.example.com", MSPName: "BankAMSP"}, CA: fabricopsv1alpha1.CAConfig{DB: "sqlite"}}}}}
	mustCreate(net)
	sourceUID := net.UID
	org := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "fo-restore-smoke-banka"}}
	bindRestoredOwner(org, net)
	mustCreate(org)
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "channel-evidence", Namespace: org.Name,
		Labels: map[string]string{"fabricops.io/component": "channel"}}, Data: map[string]string{"receipt": "preserve-this"}}
	mustCreate(cm)
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "banka-admin-msp", Namespace: org.Name,
		Labels: map[string]string{"fabricops.io/identity-kind": "admin-msp", "fabricops.io/identity-source": "fabric-ca",
			"fabricops.io/workload": "admin"}}, Data: map[string][]byte{"test-key": []byte("synthetic-identity")}}
	mustCreate(secret)
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "ledger", Namespace: org.Name},
		Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Mi")}}}}
	bindRestoredOwner(pvc, net)
	mustCreate(pvc)
	dir := t.TempDir()
	archive := filepath.Join(dir, "archive.json")
	var stdout, stderr bytes.Buffer
	if err := run([]string{"backup", "export", "--context", target, "--include-secrets", "--out", archive, net.Name},
		&stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	bundle, data, err := loadRestoreArchive(archive)
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.Secrets) != 1 || len(bundle.ConfigMaps) != 1 || len(bundle.Volumes) != 1 {
		t.Fatal("fixture export incomplete")
	}
	// Simulate metadata loss only in this disposable cluster; no Fabric workload has run.
	if err := c.Delete(ctx, net); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, org); err != nil {
		t.Fatal(err)
	}
	for {
		err := c.Get(ctx, ctrlclient.ObjectKey{Name: org.Name}, &corev1.Namespace{})
		if apierrors.IsNotFound(err) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("fixture namespace deletion timed out")
		case <-time.After(time.Second):
		}
	}
	common := []string{"--context", target,
		"--operator-namespace", manager.Namespace, "--operator-deployment", manager.Name,
		"--file", archive}
	planArgs := append([]string{"backup", "plan-restore", "--metadata-only",
		"--out", filepath.Join(dir, "plan.json")}, common...)
	if err := run(planArgs, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	restoreArgs := append([]string{"backup", "restore-metadata", "--out", filepath.Join(dir, "receipt.json"),
		"--confirm-sha256", restoreArchiveDigest(data), "--confirm-network", "default/" + net.Name,
		"--acknowledge-incomplete-recovery"}, common...)
	assertIncorrectDigestRejected(t, ctx, c, net, restoreArgs, filepath.Join(dir, "receipt.json"))
	if err := run(restoreArgs, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	actual := &fabricopsv1alpha1.FabricNetwork{}
	if err := c.Get(ctx, ctrlclient.ObjectKeyFromObject(net), actual); err != nil {
		t.Fatal(err)
	}
	if actual.UID == sourceUID ||
		actual.Annotations[fabricopsv1alpha1.RestoreHoldAnnotation] != restoreArchiveDigest(data) {
		t.Fatal("fresh UID or persistent hold missing")
	}
	reconciler := controller.FabricNetworkReconciler{Client: c, Scheme: cliScheme}
	_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: ctrlclient.ObjectKeyFromObject(actual)})
	if err != nil {
		t.Fatal(err)
	}
	actualSecret := &corev1.Secret{}
	if err := c.Get(ctx, ctrlclient.ObjectKeyFromObject(secret), actualSecret); err != nil {
		t.Fatal(err)
	}
	if !backupOwnedBy(actualSecret, actual) || !bytes.Equal(actualSecret.Data["test-key"], secret.Data["test-key"]) {
		t.Fatal("restored identity changed or has stale ownership")
	}
	actualCM := &corev1.ConfigMap{}
	if err := c.Get(ctx, ctrlclient.ObjectKeyFromObject(cm), actualCM); err != nil {
		t.Fatal(err)
	}
	if !backupOwnedBy(actualCM, actual) || actualCM.Data["receipt"] != "preserve-this" {
		t.Fatal("evidence was not restored")
	}
	err = c.Get(ctx, ctrlclient.ObjectKeyFromObject(pvc), &corev1.PersistentVolumeClaim{})
	if !apierrors.IsNotFound(err) {
		t.Fatal("metadata restore created volume inventory as a PVC")
	}
	deployments := &appsv1.DeploymentList{}
	if err := c.List(ctx, deployments, ctrlclient.InNamespace(org.Name)); err != nil {
		t.Fatal(err)
	}
	if len(deployments.Items) != 0 {
		t.Fatal("held reconciliation created workloads")
	}
	if err := c.Get(ctx, ctrlclient.ObjectKeyFromObject(manager), manager); err != nil {
		t.Fatal(err)
	}
	if *manager.Spec.Replicas != 0 {
		t.Fatal("manager was resumed")
	}
	jobs := &batchv1.JobList{}
	if err := c.List(ctx, jobs, ctrlclient.InNamespace(org.Name)); err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 0 {
		t.Fatal("held reconciliation created helper Jobs")
	}
	replay := append([]string{}, restoreArgs...)
	for i := range replay {
		if replay[i] == "--out" {
			replay[i+1] = filepath.Join(dir, "replay-receipt.json")
			break
		}
	}
	if err := run(replay, &stdout, &stderr); err == nil {
		t.Fatal("restore accepted an existing target")
	}
	if err := c.Delete(ctx, actual); err != nil {
		t.Fatal(err)
	}
	_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: ctrlclient.ObjectKeyFromObject(actual)})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, ctrlclient.ObjectKeyFromObject(actual), actual); err != nil {
		t.Fatal(err)
	}
	if actual.DeletionTimestamp == nil || len(actual.Finalizers) == 0 {
		t.Fatal("hold did not block deletion cleanup")
	}
	if err := c.Get(ctx, ctrlclient.ObjectKey{Name: org.Name}, &corev1.Namespace{}); err != nil {
		t.Fatal(err)
	}
	t.Log("Metadata restored intact under a fresh UID; bad digest, replay, PVC creation and cleanup rejected")
}

func assertIncorrectDigestRejected(t *testing.T, ctx context.Context, c ctrlclient.Reader,
	net *fabricopsv1alpha1.FabricNetwork, args []string, receiptPath string) {
	t.Helper()
	wrongDigest := append([]string{}, args...)
	for i := range wrongDigest {
		if wrongDigest[i] == "--confirm-sha256" {
			wrongDigest[i+1] = "wrong-digest"
			break
		}
	}
	var stdout, stderr bytes.Buffer
	if err := run(wrongDigest, &stdout, &stderr); err == nil {
		t.Fatal("incorrect digest was accepted")
	}
	err := c.Get(ctx, ctrlclient.ObjectKeyFromObject(net), &fabricopsv1alpha1.FabricNetwork{})
	if !apierrors.IsNotFound(err) {
		t.Fatal("failed confirmation created a target network")
	}
	if _, err := os.Stat(receiptPath); !os.IsNotExist(err) {
		t.Fatal("failed confirmation reserved the receipt file")
	}
}
