// Copyright contributors to FabricOps. SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestJoinBundleCustodyRejectsSignerAndConfigSources(t *testing.T) {
	for _, participant := range []bool{false, true} {
		for _, evidence := range []string{"identity", "retention", "config", "private-key"} {
			objects := joinBundleTestSecrets()
			if participant {
				objects = joinBundleTestParticipantObjects()
			}
			secret := objects[0].(*corev1.Secret)
			switch evidence {
			case "identity":
				secret.Labels = map[string]string{"fabricops.io/identity-kind": "peer-pkcs11-msp"}
			case "retention":
				secret.Annotations = map[string]string{backupCustodyRetention: ""}
			case "config":
				secret.Data[mspConfigKey] = []byte(joinBundleTestMSPConfigYAML +
					"\nBCCSP:\n  Default: PKCS11\n  PKCS11:\n    Pin: never-export-this-pin\n")
			case "private-key":
				secret.Data[mspCACertKey] = []byte("-----BEGIN PRIVATE KEY-----\nnever-export-this-key\n")
			}
			c := fake.NewClientBuilder().WithScheme(cliScheme).WithObjects(objects...).Build()
			var bundle joinBundle
			var err error
			if participant {
				bundle, err = buildParticipantJoinBundle(t.Context(), c, joinBundleTestParticipant())
			} else {
				bundle, err = buildJoinBundle(t.Context(), c, joinBundleTestNetwork(), "BankA")
			}
			data, _ := json.Marshal(bundle)
			if err == nil || bundle.Kind != "" || strings.Contains(string(data)+err.Error(), "never-export-this") {
				t.Fatalf("participant=%t evidence=%s: unsafe public export or sensitive error", participant, evidence)
			}
		}
	}
}

func TestJoinBundleCustodyLeavesProviderStateLocal(t *testing.T) {
	for _, participant := range []bool{false, true} {
		objects := joinBundleTestSecrets()
		if participant {
			objects = joinBundleTestParticipantObjects()
		}
		c := fake.NewClientBuilder().WithScheme(cliScheme).WithObjects(objects...).WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c ctrlclient.WithWatch, key ctrlclient.ObjectKey,
				obj ctrlclient.Object, opts ...ctrlclient.GetOption) error {
				if key.Name == "provider-pin" || key.Name == "provider-config" || key.Name == "token-peer0" ||
					key.Name == "peer0-msp" || key.Name == "peer1-msp" {
					t.Error("public membership export requested provider/peer signing material")
					return errors.New("unexpected custody read")
				}
				return c.Get(ctx, key, obj, opts...)
			},
			List: func(context.Context, ctrlclient.WithWatch, ctrlclient.ObjectList, ...ctrlclient.ListOption) error {
				t.Error("public membership export listed cluster resources")
				return errors.New("unexpected list")
			},
		}).Build()
		var bundle joinBundle
		var err error
		if participant {
			p := joinBundleTestParticipant()
			p.Spec.Org.Peer.MSPSigning = backupCustodyOrg().Peer.MSPSigning
			bundle, err = buildParticipantJoinBundle(t.Context(), c, p)
		} else {
			n := joinBundleTestNetwork()
			n.Spec.Orgs[1].Peer = backupCustodyOrg().Peer
			bundle, err = buildJoinBundle(t.Context(), c, n, "BankA")
		}
		if err != nil {
			t.Fatal(err)
		}
		data, err := marshalJoinBundle(bundle)
		if err != nil {
			t.Fatal(err)
		}
		for _, value := range []string{"provider-pin", "provider-config", "token-peer0", "/provider/module.so",
			"bank-token", "mspSigning", "PRIVATE_KEY_SHOULD_NOT_EXPORT"} {
			if strings.Contains(data, value) {
				t.Fatal("bundle included local custody or private identity material")
			}
		}
		if _, err := buildJoinBundlePlan(bundle, nil); err != nil {
			t.Fatal("public membership plan must remain independent of peer custody:", err)
		}
	}
}

func TestJoinBundleCustodyRejectsImportedProviderConfig(t *testing.T) {
	bundle := joinBundleTestBundle(t)
	bundle.Exported.AdminMSP.ConfigYAML += "\nBCCSP:\n  PKCS11:\n    Pin: never-export-this-pin\n"
	if _, err := validateJoinBundle(bundle); err == nil || strings.Contains(err.Error(), "never-export-this-pin") {
		t.Fatal("import accepted local signer configuration or leaked credentials")
	}
	if _, err := buildJoinBundlePlan(bundle, nil); err == nil {
		t.Fatal("plan accepted local signer configuration")
	}
	if _, err := buildJoinBundleApplicationOrgGroup(bundle, bundle.Channels[0]); err == nil {
		t.Fatal("render-org accepted local signer configuration")
	}
	options := joinBundleRenderUpdateOptions{channel: "settlement"}
	if _, err := buildJoinBundleConfigUpdateRecipe(bundle, options); err == nil {
		t.Fatal("render-update accepted local signer configuration")
	}
}

func TestJoinBundlePublicConfigRejectsHiddenDocuments(t *testing.T) {
	for _, suffix := range []string{
		"\n---\nBCCSP:\n  PKCS11:\n    Pin: never-export-this-pin\n",
		"\nnever-export-this-pin: secret\n",
		"\nNodeOUs:\n  Enable: false\n",
	} {
		bundle := joinBundleTestBundle(t)
		bundle.Exported.AdminMSP.ConfigYAML += suffix
		if data, err := marshalJoinBundle(bundle); err == nil || data != "" ||
			strings.Contains(err.Error(), "never-export-this-pin") {
			t.Fatal("MSP config bypassed public-only serialization or leaked parser details")
		}
	}
}

func TestJoinBundleCustodyRejectsImportedPrivatePEM(t *testing.T) {
	for _, target := range []string{"msp", "msp-tls", "admin", "peer", "orderer"} {
		bundle := joinBundleTestBundle(t)
		var value *string
		switch target {
		case "msp":
			value = &bundle.Exported.AdminMSP.CACertPEM
		case "msp-tls":
			value = &bundle.Exported.AdminMSP.TLSCACertPEM
		case "admin":
			value = &bundle.Exported.AdminTLS.CACertPEM
		case "peer":
			value = &bundle.Exported.Peers[0].TLSRoot.CACertPEM
		case "orderer":
			value = &bundle.Orderers[0].TLSRoot.CACertPEM
		}
		*value += "\n-----BEGIN EC PRIVATE KEY-----\nnever-export-this-key\n"
		if _, err := validateJoinBundle(bundle); err == nil || strings.Contains(err.Error(), "never-export-this-key") {
			t.Fatal("import accepted private PEM or leaked it")
		}
		if data, err := marshalJoinBundle(bundle); err == nil || data != "" {
			t.Fatal("serialization accepted private PEM")
		}
	}
}

func TestJoinBundleParticipantTLSRootCustodyGuards(t *testing.T) {
	for _, source := range []string{"secret", "configmap", "custody-secret"} {
		value := "-----BEGIN ENCRYPTED PRIVATE KEY-----\nnever-export-this-key\n"
		ref := &fabricopsv1alpha1.ParticipantArtifactKeyRef{}
		var object ctrlclient.Object
		if source == "configmap" {
			ref.ConfigMapKeyRef = &corev1.ConfigMapKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "roots"}, Key: "root"}
			object = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "roots", Namespace: "default"},
				Data: map[string]string{"root": value}}
		} else {
			ref.SecretKeyRef = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "roots"}, Key: "root"}
			secret := joinBundleSecret("default", "roots", map[string]string{"root": value})
			if source == "custody-secret" {
				secret.Labels = map[string]string{"fabricops.io/identity-kind": "peer-pkcs11-msp"}
				secret.Data["root"] = []byte("PUBLIC_CA")
			}
			object = secret
		}
		c := fake.NewClientBuilder().WithScheme(cliScheme).WithObjects(object).Build()
		root, err := loadJoinBundleParticipantTLSRoot(t.Context(), c, "default", ref)
		if err == nil || root != nil || strings.Contains(err.Error(), "never-export-this-key") {
			t.Fatal("custom TLS root bypassed custody/public artifact guards")
		}
	}
}
