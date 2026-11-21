//go:build restore_e2e

// Copyright contributors to FabricOps. SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func testBackupCustodyKind(t *testing.T, ctx context.Context, c ctrlclient.Client,
	target string, manager ctrlclient.ObjectKey) {
	t.Helper()
	org := backupCustodyOrg()
	org.Organization.Domain, org.CA.DB, org.Peer.Prefix = "banka.example.com", "sqlite", "peer"
	org.Peer.DB = "LevelDb"
	org.Peer.MSPSigning.PKCS11.Runtime = nil
	net := &fabricopsv1alpha1.FabricNetwork{
		ObjectMeta: metav1.ObjectMeta{Name: "custody-backup-proof", Namespace: "default"},
		Spec: fabricopsv1alpha1.FabricNetworkSpec{
			Global: fabricopsv1alpha1.GlobalConfig{FabricVersion: "3.1.0", TLS: true},
			Orgs:   []fabricopsv1alpha1.Org{org}, Channels: []fabricopsv1alpha1.Channel{},
			Chaincodes: []fabricopsv1alpha1.Chaincode{},
		}}
	if err := c.Create(ctx, net); err != nil {
		t.Fatal(err)
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "fo-custody-backup-proof-banka"}}
	bindRestoredOwner(ns, net)
	ns.Annotations[backupCustodyRetention] = "damaged-retention-must-not-be-copied"
	if err := c.Create(ctx, ns); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "provider-pin", Namespace: ns.Name},
		Data: map[string][]byte{"pin": []byte("synthetic-custody-secret")}}
	bindRestoredOwner(secret, net)
	if err := c.Create(ctx, secret); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	var stdout, stderr bytes.Buffer
	for _, retainedOnly := range []bool{false, true} {
		if retainedOnly {
			net.Spec.Orgs[0].Peer.MSPSigning = nil
			if err := c.Update(ctx, net); err != nil {
				t.Fatal(err)
			}
		}
		forbidden := filepath.Join(dir, "forbidden.json")
		exportArgs := []string{"backup", "export", "--context", target, "--include-secrets", "--out", forbidden, net.Name}
		if err := run(exportArgs, &stdout, &stderr); err == nil {
			t.Fatal("custody Secret export succeeded")
		}
		if _, err := os.Stat(forbidden); !os.IsNotExist(err) {
			t.Fatal("rejected custody export created an archive")
		}
		bundle, err := collectBackup(ctx, c, net, false)
		if err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(bundle)
		if err != nil {
			t.Fatal(err)
		}
		encodedPIN := []byte(base64.StdEncoding.EncodeToString(secret.Data["pin"]))
		if bytes.Contains(data, secret.Data["pin"]) || bytes.Contains(data, encodedPIN) ||
			bytes.Contains(data, []byte(ns.Annotations[backupCustodyRetention])) ||
			bundle.ExternalCustody == nil || len(bundle.Secrets) != 0 {
			t.Fatal("custody archive omitted recovery evidence or exported private data")
		}
		if !retainedOnly {
			continue
		}
		// Exercise the custody blocker independently of the ordinary omitted-
		// Secrets blocker, as if loading a legacy secret-inclusive archive.
		bundle.SecretsIncluded = true
		data, err = json.Marshal(bundle)
		if err != nil {
			t.Fatal(err)
		}
		archive := filepath.Join(dir, "retained-custody.json")
		if err := writeBackupFile(archive, data); err != nil {
			t.Fatal(err)
		}
		for _, object := range []ctrlclient.Object{net, ns} {
			if err := c.Delete(ctx, object); err != nil {
				t.Fatal(err)
			}
		}
		for {
			err := c.Get(ctx, ctrlclient.ObjectKeyFromObject(ns), &corev1.Namespace{})
			if apierrors.IsNotFound(err) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-ctx.Done():
				t.Fatal("custody fixture cleanup timed out")
			case <-time.After(time.Second):
			}
		}
		planPath, receiptPath := filepath.Join(dir, "plan.json"), filepath.Join(dir, "receipt.json")
		common := []string{"--context", target, "--file", archive,
			"--operator-namespace", manager.Namespace, "--operator-deployment", manager.Name}
		planArgs := append([]string{"backup", "plan-restore", "--metadata-only", "--out", planPath}, common...)
		if err := run(planArgs, &stdout, &stderr); err == nil {
			t.Fatal("custody restore plan was allowed")
		}
		planData, err := os.ReadFile(planPath)
		if err != nil {
			t.Fatal(err)
		}
		var plan restorePlan
		if json.Unmarshal(planData, &plan) != nil || plan.ApplySupported ||
			!strings.Contains(strings.Join(plan.Blockers, " "), "external custody") {
			t.Fatal("custody blocker was not reported")
		}
		args := append([]string{"backup", "restore-metadata", "--out", receiptPath,
			"--confirm-sha256", restoreArchiveDigest(data), "--confirm-network", "default/" + net.Name,
			"--acknowledge-incomplete-recovery"}, common...)
		if err := run(args, &stdout, &stderr); err == nil {
			t.Fatal("custody metadata restore succeeded")
		}
		err = c.Get(ctx, ctrlclient.ObjectKeyFromObject(net), &fabricopsv1alpha1.FabricNetwork{})
		if !apierrors.IsNotFound(err) {
			t.Fatal("blocked restore recreated the network")
		}
		if err := c.Get(ctx, ctrlclient.ObjectKeyFromObject(ns), &corev1.Namespace{}); !apierrors.IsNotFound(err) {
			t.Fatal("blocked restore recreated the custody namespace")
		}
	}
	if strings.Contains(stdout.String()+stderr.String(), string(secret.Data["pin"])) {
		t.Fatal("CLI output exposed provider credentials")
	}
	t.Log("Custody export excludes credentials; retained-state restore is blocked without UID rebinding")
}
