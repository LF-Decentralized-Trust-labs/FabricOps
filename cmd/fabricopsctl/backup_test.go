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
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

func TestBackupExportOwnershipAndSecretOptIn(t *testing.T) {
	net := &fabricopsv1alpha1.FabricNetwork{ObjectMeta: metav1.ObjectMeta{
		Name: "sample", Namespace: "default", UID: types.UID("current"), ResourceVersion: "9",
	}}
	owned := metav1.ObjectMeta{Name: "evidence", Namespace: "org-a", UID: "server-uid",
		ResourceVersion: "7", Finalizers: []string{"test"}, Annotations: map[string]string{
			"fabricops.io/fabricnetwork": "sample", "fabricops.io/fabricnetwork-namespace": "default",
			"fabricops.io/fabricnetwork-uid": "current", "kubectl.kubernetes.io/last-applied-configuration": "private",
		}}
	stale := owned.DeepCopy()
	stale.Name = "stale"
	stale.Annotations["fabricops.io/fabricnetwork-uid"] = "old"
	token := owned.DeepCopy()
	token.Name = "api-token"
	ns := owned.DeepCopy()
	ns.Name, ns.Namespace = "org-a", ""
	client := fake.NewClientBuilder().WithScheme(cliScheme).WithObjects(net,
		&corev1.Namespace{ObjectMeta: *ns},
		&corev1.ConfigMap{ObjectMeta: owned, Data: map[string]string{"receipt": "retained"}},
		&corev1.ConfigMap{ObjectMeta: *stale, Data: map[string]string{"receipt": "wrong-network"}},
		&corev1.Secret{ObjectMeta: owned, Data: map[string][]byte{"key": []byte("private-key")}},
		&corev1.Secret{ObjectMeta: *token, Type: corev1.SecretTypeServiceAccountToken},
		&corev1.PersistentVolumeClaim{ObjectMeta: owned},
	).Build()
	for _, include := range []bool{false, true} {
		bundle, err := collectBackup(t.Context(), client, net, include)
		if err != nil {
			t.Fatal(err)
		}
		if len(bundle.ConfigMaps) != 1 || bundle.ConfigMaps[0].Data["receipt"] != "retained" {
			t.Fatalf("unexpected ConfigMaps: %+v", bundle.ConfigMaps)
		}
		if (len(bundle.Secrets) == 1) != include {
			t.Fatalf("secret opt-in not respected: %+v", bundle.Secrets)
		}
		if include && string(bundle.Secrets[0].Data["key"]) != "private-key" {
			t.Fatal("secret payload was not preserved")
		}
		if len(bundle.Volumes) != 1 {
			t.Fatal("missing PVC inventory")
		}
		if bundle.Network.UID != "" || bundle.ConfigMaps[0].ResourceVersion != "" ||
			len(bundle.ConfigMaps[0].Finalizers) != 0 {
			t.Fatal("server metadata retained")
		}
		data, err := json.Marshal(bundle)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), "last-applied-configuration") {
			t.Fatal("last-applied annotation retained")
		}
	}
	if net.ResourceVersion != "9" {
		t.Fatal("export mutated source")
	}
}

func TestBackupDefaultDoesNotReadSecretsAndFailsOnListErrors(t *testing.T) {
	net := &fabricopsv1alpha1.FabricNetwork{ObjectMeta: metav1.ObjectMeta{
		Name: "sample", Namespace: "default", UID: "network-uid",
	}}
	denied := errors.New("read denied")
	client := fake.NewClientBuilder().WithScheme(cliScheme).WithObjects(net).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, client ctrlclient.WithWatch, list ctrlclient.ObjectList,
				opts ...ctrlclient.ListOption) error {
				if _, secretList := list.(*corev1.SecretList); secretList {
					return denied
				}
				return client.List(ctx, list, opts...)
			},
		}).Build()
	if _, err := collectBackup(t.Context(), client, net, false); err != nil {
		t.Fatalf("default export tried reading Secrets: %v", err)
	}
	if _, err := collectBackup(t.Context(), client, net, true); !errors.Is(err, denied) {
		t.Fatalf("secret list error was swallowed: %v", err)
	}
}

func TestBackupRejectsChangedOrDeletingNetwork(t *testing.T) {
	net := &fabricopsv1alpha1.FabricNetwork{ObjectMeta: metav1.ObjectMeta{
		Name: "sample", Namespace: "default", UID: "network-uid", Generation: 1,
	}}
	latest := net.DeepCopy()
	latest.Generation++
	client := fake.NewClientBuilder().WithScheme(cliScheme).WithObjects(latest).Build()
	if _, err := collectBackup(t.Context(), client, net, false); err == nil {
		t.Fatal("export accepted a changed spec")
	}
	now := metav1.Now()
	net.DeletionTimestamp = &now
	if _, err := collectBackup(t.Context(), client, net, false); err == nil {
		t.Fatal("export accepted a deleting resource")
	}
}

func TestBackupCommandRequiresExplicitFile(t *testing.T) {
	for _, args := range [][]string{
		{"backup"}, {"backup", "restore"}, {"backup", "export", "sample"},
		{"backup", "export", "--out", "-", "sample"},
	} {
		var stdout, stderr bytes.Buffer
		if err := run(args, &stdout, &stderr); !errors.Is(err, errUsage) {
			t.Fatalf("%v: expected usage error, got %v", args, err)
		}
		if stdout.Len() != 0 {
			t.Fatal("invalid command wrote an archive to stdout")
		}
	}
}

func TestBackupIncludesJobOutputsOnlyInsideOwnedNamespace(t *testing.T) {
	net := &fabricopsv1alpha1.FabricNetwork{ObjectMeta: metav1.ObjectMeta{
		Name: "sample", Namespace: "default", UID: "current",
	}}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "org-a", Annotations: map[string]string{
		"fabricops.io/fabricnetwork": "sample", "fabricops.io/fabricnetwork-namespace": "default",
		"fabricops.io/fabricnetwork-uid": "current",
	}}}
	meta := metav1.ObjectMeta{Name: "install-result", Namespace: "org-a",
		Labels: map[string]string{"fabricops.io/component": "chaincode"}}
	outside := meta.DeepCopy()
	outside.Namespace = "default"
	conflict := meta.DeepCopy()
	conflict.Name = "other-owner"
	conflict.Annotations = map[string]string{"fabricops.io/fabricnetwork-uid": "old"}
	identity := metav1.ObjectMeta{Name: "peer0-msp", Namespace: "org-a", Labels: map[string]string{
		"fabricops.io/identity-kind": "msp", "fabricops.io/identity-source": "fabric-ca", "fabricops.io/workload": "peer0",
	}}
	client := fake.NewClientBuilder().WithScheme(cliScheme).WithObjects(net, ns,
		&corev1.ConfigMap{ObjectMeta: meta}, &corev1.ConfigMap{ObjectMeta: *outside},
		&corev1.ConfigMap{ObjectMeta: *conflict},
		&corev1.Secret{ObjectMeta: identity, Data: map[string][]byte{"key": []byte("identity")}},
	).Build()
	bundle, err := collectBackup(t.Context(), client, net, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.ConfigMaps) != 1 || len(bundle.Secrets) != 1 || len(bundle.NamespaceScopedArtifacts) != 2 {
		t.Fatalf("incorrect Job artifact selection: %+v", bundle)
	}
}

func TestBackupFileIsPrivateAndNeverOverwritten(t *testing.T) {
	path := filepath.Join(t.TempDir(), "backup.json")
	if err := writeBackupFile(path, []byte("original")); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("unsafe permissions: %v", info.Mode())
	}
	if err := writeBackupFile(path, []byte("replacement")); err == nil {
		t.Fatal("overwrote existing export")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "original" {
		t.Fatal("existing export changed")
	}
}
