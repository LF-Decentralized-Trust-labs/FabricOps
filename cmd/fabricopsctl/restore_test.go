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
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

func restoreFixture(t *testing.T) *backupBundle {
	t.Helper()
	net := &fabricopsv1alpha1.FabricNetwork{ObjectMeta: metav1.ObjectMeta{
		Name: "sample", Namespace: "default", UID: "source-uid",
	}}
	client := fake.NewClientBuilder().WithScheme(cliScheme).WithObjects(net).Build()
	bundle, err := collectBackup(t.Context(), client, net, true)
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

func TestRestoreArchiveValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*backupBundle)
	}{
		{"kind", func(b *backupBundle) { b.Kind = "List" }},
		{"source UID", func(b *backupBundle) { b.SourceUID = "" }},
		{"network", func(b *backupBundle) { b.Network = nil }},
		{"namespace", func(b *backupBundle) { b.Namespaces = []string{"other"} }},
		{"secret opt-in", func(b *backupBundle) {
			b.SecretsIncluded = false
			b.Secrets = []corev1.Secret{{ObjectMeta: metav1.ObjectMeta{Name: "unexpected", Namespace: "default"}}}
		}},
		{"foreign resource", func(b *backupBundle) {
			b.ConfigMaps = []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: "other"}}}
		}},
		{"unproven namespace artifact", func(b *backupBundle) {
			b.NamespaceScopedArtifacts = []string{"ConfigMap/default/missing"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := restoreFixture(t)
			tc.change(bundle)
			if err := validateRestoreArchive(bundle); err == nil {
				t.Fatal("invalid archive accepted")
			}
		})
	}
	if err := validateRestoreArchive(restoreFixture(t)); err != nil {
		t.Fatal(err)
	}
}

func TestRestorePlanIsReadOnlyAndDoesNotExposeSecrets(t *testing.T) {
	bundle := restoreFixture(t)
	bundle.Secrets = []corev1.Secret{{ObjectMeta: metav1.ObjectMeta{
		Name: "identity", Namespace: "default", Annotations: map[string]string{
			"fabricops.io/fabricnetwork": "sample", "fabricops.io/fabricnetwork-namespace": "default",
			"fabricops.io/fabricnetwork-uid": "source-uid",
		}}, Data: map[string][]byte{"private-key": []byte("do-not-print")}}}
	zero := int32(0)
	manager := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "manager", Namespace: "operator"},
		Spec: appsv1.DeploymentSpec{Replicas: &zero}}
	client := fake.NewClientBuilder().WithScheme(cliScheme).WithObjects(manager,
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "finished", Namespace: "default"},
			Status: corev1.PodStatus{Phase: corev1.PodSucceeded}},
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "finished", Namespace: "default"},
			Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{
				{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
			}}},
	).Build()
	plan, err := buildRestorePlan(t.Context(), client, bundle, "recovery", ctrlclient.ObjectKeyFromObject(manager))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Blockers) != 0 || plan.ApplySupported {
		t.Fatalf("unexpected plan: %+v", plan)
	}
	data, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"do-not-print", "ZG8tbm90LXByaW50", "private-key"} {
		if bytes.Contains(data, []byte(forbidden)) {
			t.Fatal("plan contains Secret data")
		}
	}
	objects := &fabricopsv1alpha1.FabricNetworkList{}
	if err := client.List(t.Context(), objects); err != nil {
		t.Fatal(err)
	}
	if len(objects.Items) != 0 {
		t.Fatal("planner created a network")
	}
}

func TestRestorePlanBlocksLiveSourceAndExistingObjects(t *testing.T) {
	bundle := restoreFixture(t)
	client := fake.NewClientBuilder().WithScheme(cliScheme).WithObjects(
		&fabricopsv1alpha1.FabricNetwork{ObjectMeta: metav1.ObjectMeta{
			Name: "sample", Namespace: "default", UID: "source-uid",
		}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "active", Namespace: "default"}},
	).Build()
	plan, err := buildRestorePlan(t.Context(), client, bundle, "recovery",
		ctrlclient.ObjectKey{Namespace: "operator", Name: "missing"})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(plan.Blockers, "\n")
	for _, want := range []string{"manager", "source UID", "Pod/default/active"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing blocker %q: %s", want, joined)
		}
	}
}

func TestRestorePlanPreflightBlockers(t *testing.T) {
	for _, tc := range []struct {
		name    string
		manager func(*appsv1.Deployment)
		bundle  func(*backupBundle)
		objects []ctrlclient.Object
		want    string
	}{
		{name: "running manager", manager: func(d *appsv1.Deployment) { one := int32(1); d.Spec.Replicas = &one },
			want: "not fully stopped"},
		{name: "stale manager status", manager: func(d *appsv1.Deployment) { d.Generation = 2 },
			want: "not fully stopped"},
		{name: "unfinished Job", objects: []ctrlclient.Object{&batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Name: "bootstrap", Namespace: "default"}}}, want: "Job/default/bootstrap"},
		{name: "namespace adoption", bundle: func(b *backupBundle) { b.Namespaces = append(b.Namespaces, "org-a") },
			objects: []ctrlclient.Object{&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "org-a"}}},
			want:    "Namespace/org-a"},
		{name: "missing identities", bundle: func(b *backupBundle) { b.SecretsIncluded = false }, want: "omits Secrets"},
		{name: "volume recovery", bundle: func(b *backupBundle) {
			b.Volumes = []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{
				Name: "ledger", Namespace: "default", Annotations: map[string]string{
					"fabricops.io/fabricnetwork": "sample", "fabricops.io/fabricnetwork-namespace": "default",
					"fabricops.io/fabricnetwork-uid": "source-uid",
				}}}}
		}, want: "volume data recovery is not verified"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := restoreFixture(t)
			if tc.bundle != nil {
				tc.bundle(bundle)
			}
			zero := int32(0)
			manager := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "manager", Namespace: "operator"},
				Spec: appsv1.DeploymentSpec{Replicas: &zero}}
			if tc.manager != nil {
				tc.manager(manager)
			}
			objects := append([]ctrlclient.Object{manager}, tc.objects...)
			client := fake.NewClientBuilder().WithScheme(cliScheme).WithObjects(objects...).Build()
			plan, err := buildRestorePlan(t.Context(), client, bundle, "recovery", ctrlclient.ObjectKeyFromObject(manager))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(strings.Join(plan.Blockers, "\n"), tc.want) {
				t.Fatalf("missing blocker %s: %+v", tc.want, plan.Blockers)
			}
		})
	}
}

func TestRestoreDecoderRejectsUnknownOrTrailingJSONWithoutEchoingPayload(t *testing.T) {
	for _, data := range []string{`{"secret-value":"sensitive"}`, `{} {}`, `null`, `{"kind":`} {
		_, err := decodeRestoreArchive([]byte(data))
		if err == nil {
			t.Fatal("malformed archive accepted")
		}
		if strings.Contains(err.Error(), "sensitive") || strings.Contains(err.Error(), "secret-value") {
			t.Fatal("decoder error echoed archive contents")
		}
	}
	data, err := json.Marshal(restoreFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeRestoreArchive(data); err != nil {
		t.Fatal(err)
	}
}

func TestRestoreCommandRequiresExplicitContextAndFile(t *testing.T) {
	for _, args := range [][]string{
		{"backup", "plan-restore"},
		{"backup", "plan-restore", "--file", "archive.json", "--out", "plan.json"},
		{"backup", "plan-restore", "--context", "recovery", "--file", "archive.json", "--out", "-"},
	} {
		var stdout, stderr bytes.Buffer
		if err := run(args, &stdout, &stderr); !errors.Is(err, errUsage) {
			t.Fatalf("expected usage error before reading an archive or cluster, got %v", err)
		}
		if stdout.Len() != 0 {
			t.Fatal("invalid command wrote plan")
		}
	}
}

func TestRestorePlanFailsClosedOnPermissionErrors(t *testing.T) {
	denied := errors.New("read denied")
	client := fake.NewClientBuilder().WithScheme(cliScheme).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(context.Context, ctrlclient.WithWatch, ctrlclient.ObjectList, ...ctrlclient.ListOption) error {
				return denied
			},
		}).Build()
	plan, err := buildRestorePlan(t.Context(), client, restoreFixture(t), "recovery",
		ctrlclient.ObjectKey{Namespace: "operator", Name: "manager"})
	if !errors.Is(err, denied) || plan != nil {
		t.Fatalf("permission failure produced a plan: %+v, %v", plan, err)
	}
}

func TestRestorePlanNeverOverwritesExistingConfigMap(t *testing.T) {
	bundle := restoreFixture(t)
	cm := corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "evidence", Namespace: "default",
		Annotations: map[string]string{
			"fabricops.io/fabricnetwork": "sample", "fabricops.io/fabricnetwork-namespace": "default",
			"fabricops.io/fabricnetwork-uid": "source-uid",
		}}, Data: map[string]string{"receipt": "archived"}}
	bundle.ConfigMaps = []corev1.ConfigMap{cm}
	cm.Data = map[string]string{"receipt": "live"}
	client := fake.NewClientBuilder().WithScheme(cliScheme).WithObjects(&cm).Build()
	plan, err := buildRestorePlan(t.Context(), client, bundle, "recovery",
		ctrlclient.ObjectKey{Namespace: "operator", Name: "manager"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(plan.Blockers, "\n"), "ConfigMap/default/evidence already exists") {
		t.Fatalf("existing destination not blocked: %+v", plan)
	}
	actual := &corev1.ConfigMap{}
	if err := client.Get(t.Context(), ctrlclient.ObjectKeyFromObject(&cm), actual); err != nil {
		t.Fatal(err)
	}
	if actual.Data["receipt"] != "live" {
		t.Fatal("destination was modified")
	}
}

func TestMetadataRestoreCreatesHeldNetworkAndRebindsOwnership(t *testing.T) {
	bundle := restoreFixture(t)
	bundle.Namespaces = append(bundle.Namespaces, "org-a")
	bundle.Secrets = []corev1.Secret{{ObjectMeta: metav1.ObjectMeta{Name: "identity", Namespace: "org-a",
		Labels: map[string]string{"fabricops.io/identity-kind": "msp", "fabricops.io/identity-source": "fabric-ca",
			"fabricops.io/workload": "peer0"}}, Data: map[string][]byte{"key": []byte("preserved")}}}
	bundle.NamespaceScopedArtifacts = []string{"Secret/org-a/identity"}
	zero := int32(0)
	manager := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "manager", Namespace: "operator"},
		Spec: appsv1.DeploymentSpec{Replicas: &zero}}
	c := fake.NewClientBuilder().WithScheme(cliScheme).WithObjects(manager).
		WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context, c ctrlclient.WithWatch,
			obj ctrlclient.Object, opts ...ctrlclient.CreateOption) error {
			obj.SetUID("restored-uid")
			return c.Create(ctx, obj, opts...)
		}}).Build()
	receipt, err := restoreMetadata(t.Context(), c, bundle, "digest", "recovery", ctrlclient.ObjectKeyFromObject(manager))
	if err != nil {
		t.Fatal(err)
	}
	if !receipt.Complete || receipt.NetworkUID == bundle.SourceUID {
		t.Fatalf("invalid restore receipt: %+v", receipt)
	}
	net := &fabricopsv1alpha1.FabricNetwork{}
	if err := c.Get(t.Context(), ctrlclient.ObjectKeyFromObject(bundle.Network), net); err != nil {
		t.Fatal(err)
	}
	if net.Annotations[fabricopsv1alpha1.RestoreHoldAnnotation] != "digest" {
		t.Fatal("network is not held")
	}
	secret := &corev1.Secret{}
	if err := c.Get(t.Context(), ctrlclient.ObjectKey{Namespace: "org-a", Name: "identity"}, secret); err != nil {
		t.Fatal(err)
	}
	if !backupOwnedBy(secret, net) || string(secret.Data["key"]) != "preserved" {
		t.Fatal("identity was not rebound intact")
	}
	if bundle.Secrets[0].Annotations != nil {
		t.Fatal("restore mutated archive provenance")
	}
	_, err = restoreMetadata(t.Context(), c, bundle, "digest", "recovery", ctrlclient.ObjectKeyFromObject(manager))
	if err == nil {
		t.Fatal("restore accepted an existing target")
	}
}

func TestMetadataRestoreStopsWhenManagerRestartsAndRetainsHeldPartialState(t *testing.T) {
	bundle := restoreFixture(t)
	bundle.Namespaces = append(bundle.Namespaces, "org-a")
	zero := int32(0)
	manager := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "manager", Namespace: "operator"},
		Spec: appsv1.DeploymentSpec{Replicas: &zero}}
	c := fake.NewClientBuilder().WithScheme(cliScheme).WithObjects(manager).
		WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context, c ctrlclient.WithWatch,
			obj ctrlclient.Object, opts ...ctrlclient.CreateOption) error {
			obj.SetUID("restored-uid")
			if err := c.Create(ctx, obj, opts...); err != nil {
				return err
			}
			if _, network := obj.(*fabricopsv1alpha1.FabricNetwork); network {
				current := &appsv1.Deployment{}
				if err := c.Get(ctx, ctrlclient.ObjectKeyFromObject(manager), current); err != nil {
					return err
				}
				one := int32(1)
				current.Spec.Replicas = &one
				return c.Update(ctx, current)
			}
			return nil
		}}).Build()
	receipt, err := restoreMetadata(t.Context(), c, bundle, "digest", "recovery", ctrlclient.ObjectKeyFromObject(manager))
	if err == nil || receipt.Complete || receipt.NetworkUID == "" {
		t.Fatalf("guard failed: %+v, %v", receipt, err)
	}
	net := &fabricopsv1alpha1.FabricNetwork{}
	if err := c.Get(t.Context(), ctrlclient.ObjectKeyFromObject(bundle.Network), net); err != nil {
		t.Fatal(err)
	}
	if net.Annotations[fabricopsv1alpha1.RestoreHoldAnnotation] != "digest" {
		t.Fatal("partial network lost hold")
	}
	if err := c.Get(t.Context(), ctrlclient.ObjectKey{Name: "org-a"}, &corev1.Namespace{}); err == nil {
		t.Fatal("restore continued writing after manager restart")
	}
}

func TestRestoreRunnerRefusesExistingClusterWithoutDeletingIt(t *testing.T) {
	dir := t.TempDir()
	kind := filepath.Join(dir, "kind")
	log := filepath.Join(dir, "unexpected-commands")
	mock := `#!/bin/sh
if [ "$1" = get ] && [ "$2" = clusters ]; then
  echo fabricops-restore-e2e
  exit 0
fi
echo "$*" >> "$TEST_KIND_LOG"
exit 9
`
	if err := os.WriteFile(kind, []byte(mock), 0o700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", "../../hack/test-restore-e2e.sh")
	cmd.Env = append(os.Environ(), "KIND="+kind, "TEST_KIND_LOG="+log)
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "refusing to reuse") {
		t.Fatalf("runner did not refuse the existing cluster: %v\n%s", err, out)
	}
	if _, err := os.Stat(log); !os.IsNotExist(err) {
		t.Fatal("runner attempted to change an existing cluster")
	}
}
