// Copyright contributors to FabricOps. SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func backupCustodyOrg() fabricopsv1alpha1.Org {
	return fabricopsv1alpha1.Org{
		Organization: fabricopsv1alpha1.OrgMeta{Name: "BankA", MSPName: "BankAMSP"},
		Peer: &fabricopsv1alpha1.PeerConfig{Instances: 1, MSPSigning: &fabricopsv1alpha1.PeerMSPSigningConfig{
			Provider: "PKCS11", PKCS11: &fabricopsv1alpha1.PeerPKCS11Config{
				Library: "/provider/module.so", TokenLabel: "bank-token",
				PINSecretKeyRef: corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "provider-pin"}, Key: "pin"},
				Runtime: &fabricopsv1alpha1.PeerPKCS11Runtime{
					ConfigSecretKeyRef: corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "provider-config"}, Key: "config"},
					StateClaims: []fabricopsv1alpha1.PeerPKCS11StateClaim{{Peer: "peer0", ClaimName: "token-peer0"}},
				},
			},
		}}}
}

func TestBackupCustodyNeverExportsProviderSecrets(t *testing.T) {
	for _, evidence := range []string{"spec", "retained-namespace", "artifact"} {
		net := &fabricopsv1alpha1.FabricNetwork{
			ObjectMeta: metav1.ObjectMeta{Name: "sample", Namespace: "default", UID: "source"}}
		if evidence == "spec" {
			net.Spec.Orgs = []fabricopsv1alpha1.Org{backupCustodyOrg()}
		}
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "org-a"}}
		bindRestoredOwner(namespace, net)
		if evidence == "retained-namespace" {
			namespace.Annotations["fabricops.io/pkcs11-state-retention"] = "do-not-copy-raw-retention"
		}
		objects := []ctrlclient.Object{net, namespace}
		if evidence == "artifact" {
			meta := namespace.ObjectMeta.DeepCopy()
			meta.Name, meta.Namespace = "retained-evidence", namespace.Name
			meta.Annotations[backupCustodyRetention] = "do-not-copy-raw-retention"
			objects = append(objects, &corev1.ConfigMap{ObjectMeta: *meta})
		}
		secretReads := 0
		c := fake.NewClientBuilder().WithScheme(cliScheme).WithObjects(objects...).WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c ctrlclient.WithWatch, list ctrlclient.ObjectList,
				opts ...ctrlclient.ListOption) error {
				if _, ok := list.(*corev1.SecretList); ok {
					secretReads++
					return errors.New("sensitive-reader-error")
				}
				return c.List(ctx, list, opts...)
			},
		}).Build()
		bundle, err := collectBackup(t.Context(), c, net, false)
		if err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(bundle)
		if err != nil {
			t.Fatal(err)
		}
		var archive map[string]json.RawMessage
		if err := json.Unmarshal(data, &archive); err != nil {
			t.Fatal(err)
		}
		if len(archive["externalCustody"]) == 0 {
			t.Fatal("metadata export loses external recovery requirements")
		}
		if strings.Contains(string(data), "do-not-copy-raw-retention") || len(bundle.Secrets) != 0 {
			t.Fatal("custody export copied sensitive or stale retention data")
		}
		if evidence == "spec" && bundle.Network.Spec.Orgs[0].Peer.MSPSigning.PKCS11.PINSecretKeyRef.Name != "provider-pin" {
			t.Fatal("public provider references were lost")
		}
		if _, err := decodeRestoreArchive(data); err != nil {
			t.Fatal("custody metadata archive did not round-trip:", err)
		}
		if _, err := collectBackup(t.Context(), c, net, true); err == nil ||
			strings.Contains(err.Error(), "sensitive-reader-error") {
			t.Fatal("secret-inclusive custody backup was not rejected before Secret reads")
		}
		if secretReads != 0 {
			t.Fatal("custody backup read Secret payloads")
		}
	}
}

func TestBackupCustodyLegacyIdentitySecretRefused(t *testing.T) {
	for _, directOwner := range []bool{false, true} {
		net := &fabricopsv1alpha1.FabricNetwork{
			ObjectMeta: metav1.ObjectMeta{Name: "sample", Namespace: "default", UID: "source"},
		}
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "org-a"}}
		bindRestoredOwner(ns, net)
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "peer0-msp", Namespace: ns.Name,
			Labels: map[string]string{"fabricops.io/identity-kind": "peer-pkcs11-msp",
				"fabricops.io/identity-source": "fabric-ca", "fabricops.io/workload": "peer0"}},
			Data: map[string][]byte{"unexpected": []byte("private-custody-payload")}}
		if directOwner {
			bindRestoredOwner(secret, net)
		}
		c := fake.NewClientBuilder().WithScheme(cliScheme).WithObjects(net, ns, secret).Build()
		bundle, err := collectBackup(t.Context(), c, net, true)
		if err == nil || bundle != nil || !strings.Contains(err.Error(), externalCustodyExportError) ||
			strings.Contains(err.Error(), "private-custody-payload") {
			t.Fatal("legacy custody identity was exported or leaked in an error")
		}
	}
}

func TestRestoreExternalCustodyCannotWriteOrRebind(t *testing.T) {
	for _, evidence := range []string{"spec", "retention", "identity", "token-volume", "inventory"} {
		t.Run(evidence, func(t *testing.T) {
			bundle := restoreFixture(t)
			meta := metav1.ObjectMeta{Name: "custody", Namespace: "default", Annotations: map[string]string{
				"fabricops.io/fabricnetwork": "sample", "fabricops.io/fabricnetwork-namespace": "default",
				"fabricops.io/fabricnetwork-uid": bundle.SourceUID,
			}}
			switch evidence {
			case "spec":
				bundle.Network.Spec.Orgs = []fabricopsv1alpha1.Org{backupCustodyOrg()}
			case "retention":
				bundle.Network.Annotations = map[string]string{"fabricops.io/pkcs11-state-retention": "damaged-retention-evidence"}
			case "identity":
				meta.Labels = map[string]string{"fabricops.io/identity-kind": "peer-pkcs11-msp"}
				bundle.Secrets = []corev1.Secret{{ObjectMeta: meta,
					Data: map[string][]byte{"signcert.pem": []byte("public certificate")}}}
			case "token-volume":
				meta.Annotations["fabricops.io/pkcs11-peer"] = "peer0"
				bundle.Volumes = []corev1.PersistentVolumeClaim{{ObjectMeta: meta}}
			case "inventory":
				bundle.ExternalCustody = &backupExternalCustody{Version: 1, RetainedNamespaces: []string{"default"}}
			}
			zero, writes := int32(0), 0
			manager := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "manager", Namespace: "operator"},
				Spec: appsv1.DeploymentSpec{Replicas: &zero}}
			c := fake.NewClientBuilder().WithScheme(cliScheme).WithObjects(manager).WithInterceptorFuncs(interceptor.Funcs{
				Create: func(context.Context, ctrlclient.WithWatch, ctrlclient.Object, ...ctrlclient.CreateOption) error {
					writes++
					return errors.New("unexpected mutation")
				},
			}).Build()
			plan, err := buildMetadataRestorePlan(t.Context(), c, bundle, "recovery", ctrlclient.ObjectKeyFromObject(manager))
			if err != nil {
				t.Fatal(err)
			}
			if plan.ApplySupported || !strings.Contains(strings.Join(plan.Blockers, " "), "external custody") {
				t.Fatal("metadata-only planning bypassed external custody recovery")
			}
			receipt, err := restoreMetadata(t.Context(), c, bundle, "digest", "recovery",
				ctrlclient.ObjectKeyFromObject(manager))
			if err == nil || writes != 0 || len(receipt.Created) != 0 || receipt.NetworkUID != "" {
				t.Fatal("restore wrote metadata or rebound ownership despite custody blockers")
			}
		})
	}
}

func TestBackupCustodyChangesDuringExport(t *testing.T) {
	for _, change := range []string{"retention-added", "namespace-added", "network-retention-added"} {
		net := &fabricopsv1alpha1.FabricNetwork{
			ObjectMeta: metav1.ObjectMeta{Name: "sample", Namespace: "default", UID: "source"},
		}
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "org-a"}}
		bindRestoredOwner(ns, net)
		changed := false
		c := fake.NewClientBuilder().WithScheme(cliScheme).WithObjects(net, ns).WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c ctrlclient.WithWatch, list ctrlclient.ObjectList,
				opts ...ctrlclient.ListOption) error {
				if _, ok := list.(*corev1.ConfigMapList); ok && !changed {
					changed = true
					switch change {
					case "network-retention-added":
						var current fabricopsv1alpha1.FabricNetwork
						if err := c.Get(ctx, ctrlclient.ObjectKeyFromObject(net), &current); err != nil {
							return err
						}
						current.Annotations = map[string]string{backupCustodyRetention: "private-retention-evidence"}
						if err := c.Update(ctx, &current); err != nil {
							return err
						}
					case "retention-added":
						var current corev1.Namespace
						if err := c.Get(ctx, ctrlclient.ObjectKeyFromObject(ns), &current); err != nil {
							return err
						}
						current.Annotations[backupCustodyRetention] = "private-retention-evidence"
						if err := c.Update(ctx, &current); err != nil {
							return err
						}
					default:
						other := ns.DeepCopy()
						other.Name, other.ResourceVersion = "org-b", ""
						if err := c.Create(ctx, other); err != nil {
							return err
						}
					}
				}
				return c.List(ctx, list, opts...)
			},
		}).Build()
		bundle, err := collectBackup(t.Context(), c, net, false)
		if err == nil || bundle != nil || !strings.Contains(err.Error(), "changed during export") ||
			strings.Contains(err.Error(), "private-retention-evidence") {
			t.Fatal("export accepted changed custody metadata or leaked evidence")
		}
	}
}

func TestBackupCustodyInventoryValidation(t *testing.T) {
	for _, custody := range []*backupExternalCustody{
		{Version: 2}, {Version: 1, RetainedNamespaces: []string{"missing"}},
		{Version: 1, RetainedNamespaces: []string{"default", "default"}},
	} {
		bundle := restoreFixture(t)
		bundle.ExternalCustody = custody
		if err := validateRestoreArchive(bundle); err == nil {
			t.Fatal("accepted malformed custody inventory")
		}
	}
}
