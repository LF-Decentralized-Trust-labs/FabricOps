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
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

func TestBuildJoinBundleExportsPublicMembershipArtifacts(t *testing.T) {
	network := joinBundleTestNetwork()
	client := fake.NewClientBuilder().
		WithScheme(cliScheme).
		WithObjects(joinBundleTestSecrets()...).
		Build()

	bundle, err := buildJoinBundle(context.Background(), client, network, "BankA")
	if err != nil {
		t.Fatalf("buildJoinBundle() error = %v", err)
	}

	if bundle.APIVersion != joinBundleAPIVersion {
		t.Fatalf("APIVersion = %q, want %q", bundle.APIVersion, joinBundleAPIVersion)
	}
	if bundle.Kind != joinBundleKind {
		t.Fatalf("Kind = %q, want %q", bundle.Kind, joinBundleKind)
	}
	if bundle.Exported.Name != "BankA" {
		t.Fatalf("Exported.Name = %q, want BankA", bundle.Exported.Name)
	}
	if bundle.Exported.MSPID != "BankAMSP" {
		t.Fatalf("Exported.MSPID = %q, want BankAMSP", bundle.Exported.MSPID)
	}
	if bundle.Exported.AdminMSP.SecretRef.Name != "banka-admin-msp" {
		t.Fatalf("AdminMSP.SecretRef.Name = %q, want banka-admin-msp", bundle.Exported.AdminMSP.SecretRef.Name)
	}
	if bundle.Exported.AdminMSP.CACertPEM != "BANKA_MSP_CA" {
		t.Fatalf("AdminMSP.CACertPEM = %q, want BANKA_MSP_CA", bundle.Exported.AdminMSP.CACertPEM)
	}
	if bundle.Exported.AdminMSP.ConfigYAML != joinBundleTestMSPConfigYAML {
		t.Fatalf("AdminMSP.ConfigYAML = %q, want NodeOUs config", bundle.Exported.AdminMSP.ConfigYAML)
	}
	if bundle.Exported.AdminMSP.TLSCACertPEM != "BANKA_MSP_TLS_CA" {
		t.Fatalf("AdminMSP.TLSCACertPEM = %q, want BANKA_MSP_TLS_CA", bundle.Exported.AdminMSP.TLSCACertPEM)
	}
	if bundle.Exported.AdminTLS == nil || bundle.Exported.AdminTLS.CACertPEM != "BANKA_ADMIN_TLS_CA" {
		t.Fatalf("AdminTLS = %#v, want BANKA_ADMIN_TLS_CA", bundle.Exported.AdminTLS)
	}
	if len(bundle.Exported.Peers) != 2 {
		t.Fatalf("len(Exported.Peers) = %d, want 2", len(bundle.Exported.Peers))
	}
	if bundle.Exported.Peers[0].TLSRoot == nil || bundle.Exported.Peers[0].TLSRoot.CACertPEM != "BANKA_PEER0_TLS_CA" {
		t.Fatalf("Exported.Peers[0].TLSRoot = %#v, want BANKA_PEER0_TLS_CA", bundle.Exported.Peers[0].TLSRoot)
	}
	if len(bundle.Orderers) != 1 {
		t.Fatalf("len(Orderers) = %d, want 1", len(bundle.Orderers))
	}
	if bundle.Orderers[0].TLSRoot == nil ||
		bundle.Orderers[0].TLSRoot.SecretRef == nil ||
		bundle.Orderers[0].TLSRoot.SecretRef.Namespace != "fo-test-orderer" {
		t.Fatalf("Orderers[0].TLSRoot = %#v, want namespace fo-test-orderer", bundle.Orderers[0].TLSRoot)
	}
	if len(bundle.Channels) != 1 {
		t.Fatalf("len(Channels) = %d, want 1", len(bundle.Channels))
	}
	if bundle.Channels[0].Name != "settlement" {
		t.Fatalf("Channels[0].Name = %q, want settlement", bundle.Channels[0].Name)
	}
	if len(bundle.Channels[0].AnchorPeers) != 1 {
		t.Fatalf("len(Channels[0].AnchorPeers) = %d, want 1", len(bundle.Channels[0].AnchorPeers))
	}
	anchor := bundle.Channels[0].AnchorPeers[0]
	if anchor.Host != "peer0.fo-test-banka.svc.cluster.local" || anchor.Port != 7051 {
		t.Fatalf("anchor = %#v, want peer0.fo-test-banka.svc.cluster.local:7051", anchor)
	}
	if len(bundle.Chaincodes) != 1 {
		t.Fatalf("len(Chaincodes) = %d, want 1", len(bundle.Chaincodes))
	}
	if bundle.Chaincodes[0].PackageLabel != "settlement_settlement_2.0" {
		t.Fatalf("Chaincodes[0].PackageLabel = %q", bundle.Chaincodes[0].PackageLabel)
	}
	if bundle.Chaincodes[0].Sequence != 2 {
		t.Fatalf("Chaincodes[0].Sequence = %d, want 2", bundle.Chaincodes[0].Sequence)
	}

	encoded, err := json.Marshal(bundle)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	for _, forbidden := range []string{
		"PRIVATE_KEY_SHOULD_NOT_EXPORT",
		"SIGN_CERT_SHOULD_NOT_EXPORT",
		"TLS_CLIENT_CERT_SHOULD_NOT_EXPORT",
		"TLS_CLIENT_KEY_SHOULD_NOT_EXPORT",
		"keystore.pem",
		"signcert.pem",
		"client.crt",
		"client.key",
	} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("join bundle leaked %q in %s", forbidden, string(encoded))
		}
	}

	result, err := validateJoinBundle(bundle)
	if err != nil {
		t.Fatalf("validateJoinBundle() error = %v", err)
	}
	if !result.Valid {
		t.Fatal("validateJoinBundle().Valid = false, want true")
	}
	if result.Network != "sample" || result.Org != "BankA" {
		t.Fatalf("validation result = %#v, want sample/BankA", result)
	}
	if result.Peers != 2 || result.Orderers != 1 || result.Channels != 1 || result.Chaincodes != 1 {
		t.Fatalf("validation result counts = %#v", result)
	}
}

func TestBuildParticipantJoinBundleExportsParticipantOwnedOrg(t *testing.T) {
	participant := joinBundleTestParticipant()
	client := fake.NewClientBuilder().
		WithScheme(cliScheme).
		WithObjects(joinBundleTestParticipantObjects()...).
		Build()

	bundle, err := buildParticipantJoinBundle(context.Background(), client, participant)
	if err != nil {
		t.Fatalf("buildParticipantJoinBundle() error = %v", err)
	}

	if bundle.Network.Name != "settlement-network" || bundle.Network.Namespace != "default" {
		t.Fatalf("Network = %#v, want default/settlement-network", bundle.Network)
	}
	if bundle.Exported.Name != "BankB" || bundle.Exported.MSPID != "BankBMSP" {
		t.Fatalf("Exported = %#v, want BankB/BankBMSP", bundle.Exported)
	}
	if bundle.Exported.AdminMSP.CACertPEM != "BANKB_MSP_CA" {
		t.Fatalf("AdminMSP.CACertPEM = %q, want BANKB_MSP_CA", bundle.Exported.AdminMSP.CACertPEM)
	}
	if bundle.Exported.AdminTLS == nil || bundle.Exported.AdminTLS.CACertPEM != "BANKB_ADMIN_TLS_CA" {
		t.Fatalf("AdminTLS = %#v, want BANKB_ADMIN_TLS_CA", bundle.Exported.AdminTLS)
	}
	if len(bundle.Exported.Peers) != 1 {
		t.Fatalf("len(Exported.Peers) = %d, want 1", len(bundle.Exported.Peers))
	}
	peer := bundle.Exported.Peers[0]
	if peer.Address != "host.docker.internal:9051" || peer.TLSHostnameOverride != "localhost" {
		t.Fatalf("Exported.Peers[0] = %#v, want host-forwarded peer endpoint", peer)
	}
	if peer.TLSRoot == nil || peer.TLSRoot.CACertPEM != "BANKB_PEER0_TLS_CA" {
		t.Fatalf("Exported.Peers[0].TLSRoot = %#v, want BANKB_PEER0_TLS_CA", peer.TLSRoot)
	}
	if len(bundle.Orderers) != 1 {
		t.Fatalf("len(Orderers) = %d, want 1", len(bundle.Orderers))
	}
	orderer := bundle.Orderers[0]
	if orderer.ClientAddress != "host.docker.internal:8050" || orderer.TLSHostnameOverride != "localhost" {
		t.Fatalf("Orderers[0] = %#v, want imported orderer endpoint", orderer)
	}
	if orderer.TLSRoot == nil ||
		orderer.TLSRoot.ConfigMapRef == nil ||
		orderer.TLSRoot.ConfigMapRef.Name != "orderer0-artifacts" ||
		orderer.TLSRoot.CACertPEM != "ORDERER0_TLS_CA" {
		t.Fatalf("Orderers[0].TLSRoot = %#v, want ConfigMap-backed ORDERER0_TLS_CA", orderer.TLSRoot)
	}
	if len(bundle.Channels) != 1 || bundle.Channels[0].Name != "settlement" {
		t.Fatalf("Channels = %#v, want settlement", bundle.Channels)
	}
	if bundle.Channels[0].Membership == nil {
		t.Fatalf("Channels[0].Membership = nil, want membership hints")
	}
	if bundle.Channels[0].Membership.ApplicationPolicy != "/Channel/Application/Admins" {
		t.Fatalf("Channels[0].Membership.ApplicationPolicy = %q", bundle.Channels[0].Membership.ApplicationPolicy)
	}
	if !slices.Equal(bundle.Channels[0].Membership.RequiredSignerMSPIDs, []string{"BankAMSP", "BankCMSP"}) {
		t.Fatalf(
			"Channels[0].Membership.RequiredSignerMSPIDs = %#v, want BankAMSP/BankCMSP",
			bundle.Channels[0].Membership.RequiredSignerMSPIDs,
		)
	}
	if len(bundle.Channels[0].AnchorPeers) != 1 {
		t.Fatalf("len(Channels[0].AnchorPeers) = %d, want 1", len(bundle.Channels[0].AnchorPeers))
	}
	anchor := bundle.Channels[0].AnchorPeers[0]
	if anchor.Host != "peer0.bankb.fabricops.io" || anchor.Port != 7051 {
		t.Fatalf("Anchor peer = %#v, want peer0.bankb.fabricops.io:7051", anchor)
	}
	if len(bundle.Chaincodes) != 1 {
		t.Fatalf("len(Chaincodes) = %d, want 1", len(bundle.Chaincodes))
	}
	chaincode := bundle.Chaincodes[0]
	if chaincode.PackageLabel != "settlement_settlement_1.0" || chaincode.Sequence != 1 || !chaincode.Ready {
		t.Fatalf("Chaincodes[0] = %#v, want ready settlement sequence 1", chaincode)
	}

	encoded, err := json.Marshal(bundle)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	for _, forbidden := range []string{
		"PRIVATE_KEY_SHOULD_NOT_EXPORT",
		"SIGN_CERT_SHOULD_NOT_EXPORT",
		"TLS_CLIENT_CERT_SHOULD_NOT_EXPORT",
		"TLS_CLIENT_KEY_SHOULD_NOT_EXPORT",
	} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("participant join bundle leaked %q in %s", forbidden, string(encoded))
		}
	}

	result, err := validateJoinBundle(bundle)
	if err != nil {
		t.Fatalf("validateJoinBundle() error = %v", err)
	}
	if result.Org != "BankB" || result.Peers != 1 || result.Orderers != 1 || result.Channels != 1 {
		t.Fatalf("validation result = %#v, want BankB counts", result)
	}
}

func TestSelectParticipantJoinBundleOrgRequiresReadyIdentity(t *testing.T) {
	participant := joinBundleTestParticipant()
	participant.Status.LocalOrgStatus.IdentityReady = false

	_, _, err := selectParticipantJoinBundleOrg(participant)
	if err == nil {
		t.Fatal("selectParticipantJoinBundleOrg() error = nil, want identity readiness error")
	}
	if !strings.Contains(err.Error(), "identity material is not ready") {
		t.Fatalf("selectParticipantJoinBundleOrg() error = %v, want identity readiness error", err)
	}
}

func TestSelectJoinBundleOrgRequiresReadyIdentity(t *testing.T) {
	network := joinBundleTestNetwork()
	network.Status.OrgStatus[1].IdentityReady = false

	_, _, err := selectJoinBundleOrg(network, "BankA")
	if err == nil {
		t.Fatal("selectJoinBundleOrg() error = nil, want identity readiness error")
	}
	if !strings.Contains(err.Error(), "identity material is not ready") {
		t.Fatalf("selectJoinBundleOrg() error = %v, want identity readiness error", err)
	}
}

func TestBuildJoinBundleRejectsMissingChannelPeerStatus(t *testing.T) {
	network := joinBundleTestNetwork()
	network.Status.OrgStatus[1].PeerEndpoints = network.Status.OrgStatus[1].PeerEndpoints[:1]
	client := fake.NewClientBuilder().
		WithScheme(cliScheme).
		WithObjects(joinBundleTestSecrets()...).
		Build()

	_, err := buildJoinBundle(context.Background(), client, network, "BankA")
	if err == nil {
		t.Fatal("buildJoinBundle() error = nil, want missing peer status error")
	}
	if !strings.Contains(err.Error(), "peer \"peer1\"") {
		t.Fatalf("buildJoinBundle() error = %v, want peer1 status error", err)
	}
}

func TestDecodeJoinBundleRejectsUnknownFields(t *testing.T) {
	raw := []byte(`{
  "apiVersion": "fabricops.io/v1alpha1",
  "kind": "FabricNetworkJoinBundle",
  "network": {
    "name": "sample",
    "namespace": "default",
    "fabricVersion": "3.1.0",
    "tls": true
  },
  "exported": {
    "name": "BankA",
    "mspID": "BankAMSP",
    "namespace": "fo-test-banka",
    "adminMSP": {
      "secretRef": {"namespace": "fo-test-banka", "name": "banka-admin-msp"},
      "caCertPEM": "BANKA_MSP_CA",
      "tlsCACertPEM": "BANKA_MSP_TLS_CA",
      "keystorePEM": "PRIVATE_KEY_SHOULD_NOT_IMPORT"
    }
  },
  "orderers": [{
    "org": "OrdererOrg",
    "name": "orderer0",
    "clientAddress": "orderer0.fo-test-orderer.svc.cluster.local:7050",
    "tlsRoot": {
      "secretRef": {"namespace": "fo-test-orderer", "name": "orderer0-tls"},
      "caCertPEM": "ORDERER0_TLS_CA"
    }
  }]
}`)

	_, err := decodeJoinBundle(raw)
	if err == nil {
		t.Fatal("decodeJoinBundle() error = nil, want unknown field rejection")
	}
	if !strings.Contains(err.Error(), "keystorePEM") {
		t.Fatalf("decodeJoinBundle() error = %v, want keystorePEM rejection", err)
	}
}

func TestValidateJoinBundleRequiresTLSMaterial(t *testing.T) {
	bundle := joinBundle{
		APIVersion: joinBundleAPIVersion,
		Kind:       joinBundleKind,
		Network: joinBundleNetwork{
			Name:          "sample",
			Namespace:     "default",
			FabricVersion: "3.1.0",
			TLS:           true,
		},
		Exported: joinBundleExportedOrg{
			Name:      "BankA",
			MSPID:     "BankAMSP",
			Namespace: "fo-test-banka",
			AdminMSP: joinBundlePublicMSP{
				SecretRef: joinBundleObjectRef{Namespace: "fo-test-banka", Name: "banka-admin-msp"},
				CACertPEM: "BANKA_MSP_CA",
			},
			Peers: []joinBundlePeer{{
				Name:    "peer0",
				Address: "peer0.fo-test-banka.svc.cluster.local:7051",
			}},
		},
		Orderers: []joinBundleOrderer{{
			Org:           "OrdererOrg",
			Name:          "orderer0",
			ClientAddress: "orderer0.fo-test-orderer.svc.cluster.local:7050",
		}},
	}

	_, err := validateJoinBundle(bundle)
	if err == nil {
		t.Fatal("validateJoinBundle() error = nil, want TLS material validation error")
	}
	for _, want := range []string{
		"exported.adminMSP.tlsCACertPEM",
		"exported.adminTLS",
		"exported.peers[0].tlsRoot",
		"orderers[0].tlsRoot",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("validateJoinBundle() error = %v, want %q", err, want)
		}
	}
}

func TestBuildJoinBundlePlanSummarizesFounderAndParticipantActions(t *testing.T) {
	bundle := joinBundleTestBundle(t)

	plan, err := buildJoinBundlePlan(bundle, []string{"settlement"})
	if err != nil {
		t.Fatalf("buildJoinBundlePlan() error = %v", err)
	}

	if plan.Kind != joinBundlePlanKind {
		t.Fatalf("Kind = %q, want %q", plan.Kind, joinBundlePlanKind)
	}
	if plan.Org.Name != "BankA" || plan.Org.MSPID != "BankAMSP" {
		t.Fatalf("Org = %#v, want BankA/BankAMSP", plan.Org)
	}
	if !plan.Org.MSP.ConfigYAML || !plan.Org.MSP.CACertPEM || !plan.Org.MSP.TLSCACertPEM {
		t.Fatalf("Org.MSP = %#v, want all public MSP material present", plan.Org.MSP)
	}
	if len(plan.Channels) != 1 {
		t.Fatalf("len(Channels) = %d, want 1", len(plan.Channels))
	}
	channel := plan.Channels[0]
	if channel.Name != "settlement" {
		t.Fatalf("channel.Name = %q, want settlement", channel.Name)
	}
	if len(channel.Peers) != 2 {
		t.Fatalf("len(channel.Peers) = %d, want 2", len(channel.Peers))
	}
	if len(channel.AnchorPeers) != 1 {
		t.Fatalf("len(channel.AnchorPeers) = %d, want 1", len(channel.AnchorPeers))
	}
	if len(channel.Chaincodes) != 1 {
		t.Fatalf("len(channel.Chaincodes) = %d, want 1", len(channel.Chaincodes))
	}
	if channel.Chaincodes[0].PackageLabel != "settlement_settlement_2.0" {
		t.Fatalf("channel.Chaincodes[0].PackageLabel = %q", channel.Chaincodes[0].PackageLabel)
	}

	founderActionNames := joinBundlePlanActionNames(channel.FounderActions)
	for _, want := range []string{"add-org-msp", "update-anchor-peers", "submit-channel-config-update"} {
		if !founderActionNames[want] {
			t.Fatalf("founder actions = %#v, missing %s", channel.FounderActions, want)
		}
	}
	participantActionNames := joinBundlePlanActionNames(channel.ParticipantActions)
	for _, want := range []string{"receive-channel-block", "join-peers", "approve-chaincode-definition"} {
		if !participantActionNames[want] {
			t.Fatalf("participant actions = %#v, missing %s", channel.ParticipantActions, want)
		}
	}

	text := renderJoinBundlePlanText(plan)
	for _, want := range []string{
		"Join plan: default/sample org BankA (BankAMSP)",
		"Channel settlement",
		"add-org-msp",
		"approve-chaincode-definition",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("renderJoinBundlePlanText() = %q, want %q", text, want)
		}
	}
}

func TestBuildJoinBundlePlanPreservesMembershipHints(t *testing.T) {
	participant := joinBundleTestParticipant()
	client := fake.NewClientBuilder().
		WithScheme(cliScheme).
		WithObjects(joinBundleTestParticipantObjects()...).
		Build()
	bundle, err := buildParticipantJoinBundle(context.Background(), client, participant)
	if err != nil {
		t.Fatalf("buildParticipantJoinBundle() error = %v", err)
	}

	plan, err := buildJoinBundlePlan(bundle, []string{"settlement"})
	if err != nil {
		t.Fatalf("buildJoinBundlePlan() error = %v", err)
	}

	if len(plan.Channels) != 1 {
		t.Fatalf("len(Channels) = %d, want 1", len(plan.Channels))
	}
	membership := plan.Channels[0].Membership
	if membership == nil {
		t.Fatal("plan channel membership = nil, want exported membership hints")
	}
	if membership.ApplicationPolicy != "/Channel/Application/Admins" {
		t.Fatalf("ApplicationPolicy = %q, want /Channel/Application/Admins", membership.ApplicationPolicy)
	}
	if !slices.Equal(membership.RequiredSignerMSPIDs, []string{"BankAMSP", "BankCMSP"}) {
		t.Fatalf("RequiredSignerMSPIDs = %#v, want BankAMSP/BankCMSP", membership.RequiredSignerMSPIDs)
	}

	text := renderJoinBundlePlanText(plan)
	for _, want := range []string{
		"Membership policy: /Channel/Application/Admins",
		"Required founder signer MSP IDs: BankAMSP, BankCMSP",
		"Collect founder signatures from BankAMSP, BankCMSP",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("renderJoinBundlePlanText() = %q, want %q", text, want)
		}
	}
}

func TestBuildJoinBundlePlanRejectsUnknownChannelFilter(t *testing.T) {
	bundle := joinBundleTestBundle(t)

	_, err := buildJoinBundlePlan(bundle, []string{"missing"})
	if err == nil {
		t.Fatal("buildJoinBundlePlan() error = nil, want missing channel error")
	}
	if !strings.Contains(err.Error(), "does not contain channel \"missing\"") {
		t.Fatalf("buildJoinBundlePlan() error = %v, want missing channel error", err)
	}
}

func TestBuildJoinBundleApplicationOrgGroupRendersConfigtxOrgJSON(t *testing.T) {
	bundle := joinBundleTestBundle(t)

	rendered, err := buildJoinBundleApplicationOrgGroup(bundle, bundle.Channels[0])
	if err != nil {
		t.Fatalf("buildJoinBundleApplicationOrgGroup() error = %v", err)
	}

	if rendered["mod_policy"] != "Admins" {
		t.Fatalf("mod_policy = %q, want Admins", rendered["mod_policy"])
	}
	policies := rendered["policies"].(map[string]any)
	if len(policies) != 4 {
		t.Fatalf("len(policies) = %d, want 4", len(policies))
	}
	adminsPolicy := policies["Admins"].(map[string]any)
	adminsPolicyValue := adminsPolicy["policy"].(map[string]any)["value"].(map[string]any)
	adminsIdentity := adminsPolicyValue["identities"].([]map[string]any)[0]
	adminsPrincipal := adminsIdentity["principal"].(map[string]any)
	if adminsPrincipal["msp_identifier"] != "BankAMSP" || adminsPrincipal["role"] != "ADMIN" {
		t.Fatalf("Admins policy principal = %#v, want BankAMSP ADMIN", adminsPrincipal)
	}

	values := rendered["values"].(map[string]any)
	mspValue := values["MSP"].(map[string]any)["value"].(map[string]any)
	mspConfig := mspValue["config"].(map[string]any)
	if mspConfig["name"] != "BankAMSP" {
		t.Fatalf("MSP name = %q, want BankAMSP", mspConfig["name"])
	}
	rootCerts := mspConfig["root_certs"].([]string)
	if rootCerts[0] != joinBundleBase64PEM("BANKA_MSP_CA") {
		t.Fatalf("root_certs[0] = %q, want base64 MSP CA", rootCerts[0])
	}
	tlsRootCerts := mspConfig["tls_root_certs"].([]string)
	if tlsRootCerts[0] != joinBundleBase64PEM("BANKA_MSP_TLS_CA") {
		t.Fatalf("tls_root_certs[0] = %q, want base64 MSP TLS CA", tlsRootCerts[0])
	}
	nodeOUs := mspConfig["fabric_node_ous"].(map[string]any)
	if nodeOUs["enable"] != true {
		t.Fatalf("fabric_node_ous.enable = %#v, want true", nodeOUs["enable"])
	}
	adminOU := nodeOUs["admin_ou_identifier"].(map[string]any)
	if adminOU["organizational_unit_identifier"] != "admin" {
		t.Fatalf("admin OU = %#v, want admin", adminOU)
	}
	if adminOU["certificate"] != joinBundleBase64PEM("BANKA_MSP_CA") {
		t.Fatalf("admin OU certificate = %q, want base64 MSP CA", adminOU["certificate"])
	}

	anchorValue := values["AnchorPeers"].(map[string]any)["value"].(map[string]any)
	anchors := anchorValue["anchor_peers"].([]map[string]any)
	if anchors[0]["host"] != "peer0.fo-test-banka.svc.cluster.local" || anchors[0]["port"] != int32(7051) {
		t.Fatalf("anchor peer = %#v", anchors[0])
	}
}

func TestSelectJoinBundleRenderOrgChannelRequiresChannelForAmbiguousBundle(t *testing.T) {
	bundle := joinBundleTestBundle(t)
	bundle.Channels = append(bundle.Channels, joinBundleChannel{Name: "treasury"})

	_, err := selectJoinBundleRenderOrgChannel(bundle, "")
	if err == nil {
		t.Fatal("selectJoinBundleRenderOrgChannel() error = nil, want required channel error")
	}
	if !strings.Contains(err.Error(), "--channel is required") {
		t.Fatalf("selectJoinBundleRenderOrgChannel() error = %v, want --channel requirement", err)
	}
}

func TestBuildJoinBundleConfigUpdateRecipeRendersUnsignedEnvelopeScript(t *testing.T) {
	bundle := joinBundleTestBundle(t)

	recipe, err := buildJoinBundleConfigUpdateRecipe(bundle, joinBundleRenderUpdateOptions{
		channel: "settlement",
		orderer: "OrdererOrg/orderer0",
		workDir: "join-workdir",
	})
	if err != nil {
		t.Fatalf("buildJoinBundleConfigUpdateRecipe() error = %v", err)
	}

	if recipe.Kind != joinBundleUpdateRecipeKind {
		t.Fatalf("Kind = %q, want %q", recipe.Kind, joinBundleUpdateRecipeKind)
	}
	if recipe.Channel != "settlement" {
		t.Fatalf("Channel = %q, want settlement", recipe.Channel)
	}
	if recipe.Org.Name != "BankA" || recipe.Org.MSPID != "BankAMSP" {
		t.Fatalf("Org = %#v, want BankA/BankAMSP", recipe.Org)
	}
	if recipe.Orderer.Name != "orderer0" {
		t.Fatalf("Orderer.Name = %q, want orderer0", recipe.Orderer.Name)
	}
	if recipe.Orderer.TLSHostnameOverride != "localhost" {
		t.Fatalf("Orderer.TLSHostnameOverride = %q", recipe.Orderer.TLSHostnameOverride)
	}
	if recipe.OrdererTLSCACertPEM != "ORDERER0_TLS_CA" {
		t.Fatalf("OrdererTLSCACertPEM = %q, want ORDERER0_TLS_CA", recipe.OrdererTLSCACertPEM)
	}
	if recipe.Files.ApplicationOrgJSON != "settlement-application-org.json" {
		t.Fatalf("ApplicationOrgJSON = %q", recipe.Files.ApplicationOrgJSON)
	}
	if recipe.Files.ConfigUpdateEnvelopePB != "settlement-join-update-envelope.pb" {
		t.Fatalf("ConfigUpdateEnvelopePB = %q", recipe.Files.ConfigUpdateEnvelopePB)
	}
	if _, ok := recipe.ApplicationOrg["values"]; !ok {
		t.Fatalf("ApplicationOrg does not contain values: %#v", recipe.ApplicationOrg)
	}

	script, err := renderJoinBundleConfigUpdateScript(recipe)
	if err != nil {
		t.Fatalf("renderJoinBundleConfigUpdateScript() error = %v", err)
	}
	for _, want := range []string{
		"peer channel fetch config \"$CONFIG_BLOCK_PB\"",
		"--ordererTLSHostnameOverride \"$ORDERER_TLS_HOSTNAME_OVERRIDE\"",
		"jq --slurpfile org \"$APPLICATION_ORG_JSON\" --arg msp \"$JOIN_MSP_ID\"",
		"configtxlator compute_update",
		"--type common.Envelope",
		"Created unsigned channel update envelope",
		"peer channel signconfigtx -f \"$CONFIG_UPDATE_ENVELOPE_PB\"",
		"peer channel update -f \"$CONFIG_UPDATE_ENVELOPE_PB\"",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("script does not contain %q\nscript:\n%s", want, script)
		}
	}
	if !strings.Contains(script, joinBundleBase64PEM("BANKA_MSP_CA")) {
		t.Fatalf("script does not contain base64 MSP CA\nscript:\n%s", script)
	}
	if !strings.Contains(script, "ORDERER0_TLS_CA") {
		t.Fatalf("script does not contain orderer TLS CA\nscript:\n%s", script)
	}
}

func TestBuildJoinBundleConfigUpdateRecipePreservesMembershipHints(t *testing.T) {
	bundle := joinBundleTestBundle(t)
	bundle.Channels[0].Membership = &joinBundleChannelMembership{
		ApplicationPolicy:    "/Channel/Application/Admins",
		RequiredSignerMSPIDs: []string{"BankAMSP", "BankCMSP"},
	}

	recipe, err := buildJoinBundleConfigUpdateRecipe(bundle, joinBundleRenderUpdateOptions{
		channel: "settlement",
		orderer: "OrdererOrg/orderer0",
	})
	if err != nil {
		t.Fatalf("buildJoinBundleConfigUpdateRecipe() error = %v", err)
	}
	if recipe.Membership == nil {
		t.Fatal("recipe.Membership = nil, want membership hints")
	}
	if recipe.Membership.ApplicationPolicy != "/Channel/Application/Admins" {
		t.Fatalf("ApplicationPolicy = %q, want /Channel/Application/Admins", recipe.Membership.ApplicationPolicy)
	}
	if !slices.Equal(recipe.Membership.RequiredSignerMSPIDs, []string{"BankAMSP", "BankCMSP"}) {
		t.Fatalf("RequiredSignerMSPIDs = %#v, want BankAMSP/BankCMSP", recipe.Membership.RequiredSignerMSPIDs)
	}

	script, err := renderJoinBundleConfigUpdateScript(recipe)
	if err != nil {
		t.Fatalf("renderJoinBundleConfigUpdateScript() error = %v", err)
	}
	for _, want := range []string{
		"Application policy hint: /Channel/Application/Admins",
		"Expected founder signer MSP IDs: BankAMSP, BankCMSP",
		"Collect required founder-admin signatures according to the channel policy",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("script does not contain %q\nscript:\n%s", want, script)
		}
	}
}

func TestValidateJoinBundleRejectsInvalidMembershipHints(t *testing.T) {
	bundle := joinBundleTestBundle(t)
	bundle.Channels[0].Membership = &joinBundleChannelMembership{
		RequiredSignerMSPIDs: []string{"BankAMSP", " ", "BankAMSP"},
	}

	_, err := validateJoinBundle(bundle)
	if err == nil {
		t.Fatal("validateJoinBundle() error = nil, want membership validation error")
	}
	for _, want := range []string{
		"channels[0].membership.requiredSignerMSPIDs[1] is required",
		`channels[0].membership required signer MSP "BankAMSP" is declared more than once`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("validateJoinBundle() error = %v, want %q", err, want)
		}
	}
}

func TestRunJoinBundleRenderUpdateOutputsJSONRecipe(t *testing.T) {
	bundle := joinBundleTestBundle(t)
	contents, err := marshalJoinBundle(bundle)
	if err != nil {
		t.Fatalf("marshalJoinBundle() error = %v", err)
	}

	bundlePath := t.TempDir() + "/join-bundle.json"
	if err := os.WriteFile(bundlePath, []byte(contents), 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err = runJoinBundleRenderUpdate(
		[]string{"--channel", "settlement", "--orderer", "orderer0", "-o", "json", bundlePath},
		&stdout,
		&stderr,
	)
	if err != nil {
		t.Fatalf("runJoinBundleRenderUpdate() error = %v\nstderr:\n%s", err, stderr.String())
	}

	var recipe joinBundleConfigUpdateRecipe
	if err := json.Unmarshal(stdout.Bytes(), &recipe); err != nil {
		t.Fatalf("json.Unmarshal() error = %v\nstdout:\n%s", err, stdout.String())
	}
	if recipe.Kind != joinBundleUpdateRecipeKind {
		t.Fatalf("Kind = %q, want %q", recipe.Kind, joinBundleUpdateRecipeKind)
	}
	if recipe.Channel != "settlement" || recipe.Org.MSPID != "BankAMSP" {
		t.Fatalf("recipe target = %#v/%q, want BankAMSP settlement", recipe.Org, recipe.Channel)
	}
	if !slices.Contains(recipe.RequiredTools, "peer") ||
		!slices.Contains(recipe.RequiredTools, "configtxlator") ||
		!slices.Contains(recipe.RequiredTools, "jq") {
		t.Fatalf("RequiredTools = %#v, want peer/configtxlator/jq", recipe.RequiredTools)
	}
	if strings.Contains(stdout.String(), "peer channel fetch config") {
		t.Fatalf("JSON output unexpectedly contains shell script text:\n%s", stdout.String())
	}
}

func TestRenderJoinBundleConfigUpdateScriptIsShellParseable(t *testing.T) {
	bashPath, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is not available")
	}
	bundle := joinBundleTestBundle(t)

	recipe, err := buildJoinBundleConfigUpdateRecipe(bundle, joinBundleRenderUpdateOptions{
		channel: "settlement",
		orderer: "OrdererOrg/orderer0",
		workDir: "join-workdir",
	})
	if err != nil {
		t.Fatalf("buildJoinBundleConfigUpdateRecipe() error = %v", err)
	}
	script, err := renderJoinBundleConfigUpdateScript(recipe)
	if err != nil {
		t.Fatalf("renderJoinBundleConfigUpdateScript() error = %v", err)
	}

	cmd := exec.Command(bashPath, "-n")
	cmd.Stdin = strings.NewReader(script)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("bash -n failed: %v\noutput:\n%s\nscript:\n%s", err, string(output), script)
	}
}

func TestBuildJoinBundleConfigUpdateRecipeWithoutTLSOmitsTLSRequirements(t *testing.T) {
	bundle := joinBundleTestBundle(t)
	bundle.Network.TLS = false
	bundle.Exported.AdminMSP.TLSCACertPEM = ""
	bundle.Exported.AdminTLS = nil
	for i := range bundle.Exported.Peers {
		bundle.Exported.Peers[i].TLSRoot = nil
	}
	for i := range bundle.Orderers {
		bundle.Orderers[i].TLSRoot = nil
	}

	recipe, err := buildJoinBundleConfigUpdateRecipe(bundle, joinBundleRenderUpdateOptions{
		channel: "settlement",
		orderer: "OrdererOrg/orderer0",
	})
	if err != nil {
		t.Fatalf("buildJoinBundleConfigUpdateRecipe() error = %v", err)
	}
	if recipe.OrdererTLSCACertPEM != "" {
		t.Fatalf("OrdererTLSCACertPEM = %q, want empty", recipe.OrdererTLSCACertPEM)
	}
	if recipe.Files.OrdererTLSCACert != "" {
		t.Fatalf("Files.OrdererTLSCACert = %q, want empty", recipe.Files.OrdererTLSCACert)
	}
	if slices.Contains(recipe.FounderAdminEnv, "CORE_PEER_TLS_ROOTCERT_FILE") {
		t.Fatalf("FounderAdminEnv = %#v, want no TLS root env", recipe.FounderAdminEnv)
	}

	script, err := renderJoinBundleConfigUpdateScript(recipe)
	if err != nil {
		t.Fatalf("renderJoinBundleConfigUpdateScript() error = %v", err)
	}
	for _, notWant := range []string{"ORDERER_TLS_CA", "--tls", "--cafile", "--ordererTLSHostnameOverride"} {
		if strings.Contains(script, notWant) {
			t.Fatalf("non-TLS script contains %q\nscript:\n%s", notWant, script)
		}
	}
	if !strings.Contains(script, "export CORE_PEER_TLS_ENABLED=${CORE_PEER_TLS_ENABLED:-false}") {
		t.Fatalf("non-TLS script does not set default CORE_PEER_TLS_ENABLED=false\nscript:\n%s", script)
	}
}

func TestBuildJoinBundleConfigUpdateRecipeRejectsUnknownOrderer(t *testing.T) {
	bundle := joinBundleTestBundle(t)

	_, err := buildJoinBundleConfigUpdateRecipe(bundle, joinBundleRenderUpdateOptions{
		channel: "settlement",
		orderer: "missing",
	})
	if err == nil {
		t.Fatal("buildJoinBundleConfigUpdateRecipe() error = nil, want unknown orderer error")
	}
	if !strings.Contains(err.Error(), "does not contain orderer \"missing\"") {
		t.Fatalf("buildJoinBundleConfigUpdateRecipe() error = %v, want unknown orderer error", err)
	}
}

func joinBundleTestBundle(t *testing.T) joinBundle {
	t.Helper()

	network := joinBundleTestNetwork()
	client := fake.NewClientBuilder().
		WithScheme(cliScheme).
		WithObjects(joinBundleTestSecrets()...).
		Build()
	bundle, err := buildJoinBundle(context.Background(), client, network, "BankA")
	if err != nil {
		t.Fatalf("buildJoinBundle() error = %v", err)
	}
	return bundle
}

const joinBundleTestMSPConfigYAML = `NodeOUs:
  Enable: true
  ClientOUIdentifier:
    Certificate: cacerts/ca.pem
    OrganizationalUnitIdentifier: client
  PeerOUIdentifier:
    Certificate: cacerts/ca.pem
    OrganizationalUnitIdentifier: peer
  AdminOUIdentifier:
    Certificate: cacerts/ca.pem
    OrganizationalUnitIdentifier: admin
  OrdererOUIdentifier:
    Certificate: cacerts/ca.pem
    OrganizationalUnitIdentifier: orderer`

func joinBundlePlanActionNames(actions []joinBundlePlanAction) map[string]bool {
	names := map[string]bool{}
	for _, action := range actions {
		names[action.Name] = true
	}
	return names
}

func joinBundleTestNetwork() *fabricopsv1alpha1.FabricNetwork {
	network := operationTestNetwork()
	network.Spec.Orgs[0].Organization.Domain = "orderer.example.com"
	network.Spec.Orgs[1].Organization.Domain = "banka.example.com"
	network.Spec.Orgs[2].Organization.Domain = "bankb.example.com"
	network.Spec.Channels = []fabricopsv1alpha1.Channel{
		{
			Name: "settlement",
			Orgs: []fabricopsv1alpha1.ChannelOrg{
				{Name: "BankA", Peers: []string{"peer0", "peer1"}},
				{Name: "BankB", Peers: []string{"peer0"}},
			},
		},
		{
			Name: "audit",
			Orgs: []fabricopsv1alpha1.ChannelOrg{
				{Name: "BankB", Peers: []string{"peer0"}},
			},
		},
	}
	network.Spec.Chaincodes = []fabricopsv1alpha1.Chaincode{
		{
			Name:              "settlement",
			Channel:           "settlement",
			Version:           "2.0",
			Image:             "ghcr.io/lf-decentralized-trust-labs/fabricops/sample-node:2.0",
			Sequence:          2,
			EndorsementPolicy: "AND('BankAMSP.member','BankBMSP.member')",
		},
		{
			Name:    "audit",
			Channel: "audit",
			Version: "1.0",
			Image:   "ghcr.io/lf-decentralized-trust-labs/fabricops/sample-go:1.0",
		},
	}
	network.Status.OrgStatus = []fabricopsv1alpha1.OrgStatus{
		{
			Name:          "OrdererOrg",
			Namespace:     "fo-test-orderer",
			IdentityReady: true,
			CAReady:       true,
			OrdererEndpoints: []fabricopsv1alpha1.OrdererEndpointStatus{
				{
					Name:                "orderer0",
					Namespace:           "fo-test-orderer",
					ClientAddress:       "host.docker.internal:8050",
					TLSHostnameOverride: "localhost",
					AdminAddress:        "orderer0.fo-test-orderer.svc.cluster.local:7053",
					OperationsAddress:   "orderer0.fo-test-orderer.svc.cluster.local:9443",
				},
			},
		},
		{
			Name:                           "BankA",
			Namespace:                      "fo-test-banka",
			IdentityReady:                  true,
			CAReady:                        true,
			CAEndpoint:                     "ca.fo-test-banka.svc.cluster.local:7054",
			ConnectionProfileConfigMapName: "sample-connection-profile",
			PeerEndpoints: []fabricopsv1alpha1.PeerEndpointStatus{
				{
					Name:              "peer0",
					Address:           "peer0.fo-test-banka.svc.cluster.local:7051",
					ChaincodeAddress:  "peer0.fo-test-banka.svc.cluster.local:7052",
					OperationsAddress: "peer0.fo-test-banka.svc.cluster.local:9443",
				},
				{
					Name:              "peer1",
					Address:           "peer1.fo-test-banka.svc.cluster.local:7051",
					ChaincodeAddress:  "peer1.fo-test-banka.svc.cluster.local:7052",
					OperationsAddress: "peer1.fo-test-banka.svc.cluster.local:9443",
				},
			},
		},
		{
			Name:          "BankB",
			Namespace:     "fo-test-bankb",
			IdentityReady: true,
			CAReady:       true,
			PeerEndpoints: []fabricopsv1alpha1.PeerEndpointStatus{
				{Name: "peer0", Address: "peer0.fo-test-bankb.svc.cluster.local:7051"},
			},
		},
	}
	network.Status.ChaincodeStatus = []fabricopsv1alpha1.ChaincodeStatus{
		{
			Name:         "settlement",
			Channel:      "settlement",
			Version:      "2.0",
			PackageLabel: "settlement_settlement_2.0",
			Sequence:     2,
			Committed:    true,
			Ready:        true,
		},
		{
			Name:         "audit",
			Channel:      "audit",
			Version:      "1.0",
			PackageLabel: "audit_audit_1.0",
			Sequence:     1,
			Committed:    true,
			Ready:        true,
		},
	}
	return network
}

func joinBundleTestParticipant() *fabricopsv1alpha1.FabricParticipant {
	return &fabricopsv1alpha1.FabricParticipant{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "bankb-participant",
		},
		Spec: fabricopsv1alpha1.FabricParticipantSpec{
			Global: fabricopsv1alpha1.GlobalConfig{
				FabricVersion: "3.1.0",
				TLS:           true,
			},
			Org: fabricopsv1alpha1.Org{
				Organization: fabricopsv1alpha1.OrgMeta{
					Name:    "BankB",
					Domain:  "bankb.fabricops.io",
					MSPName: "BankBMSP",
				},
				Peer: &fabricopsv1alpha1.PeerConfig{
					Prefix:    "peer",
					Instances: 1,
				},
			},
			Network: fabricopsv1alpha1.ParticipantNetwork{
				Name:         "settlement-network",
				FounderMSPID: "BankAMSP",
				Orderers: []fabricopsv1alpha1.ParticipantOrdererEndpoint{
					{
						Org:                 "OrdererOrg",
						Name:                "orderer0",
						ClientAddress:       "host.docker.internal:8050",
						TLSHostnameOverride: "localhost",
						TLSRootCARef: &fabricopsv1alpha1.ParticipantArtifactKeyRef{
							ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: "orderer0-artifacts"},
								Key:                  "tls-ca.pem",
							},
						},
					},
				},
			},
			Channels: []fabricopsv1alpha1.ParticipantChannel{
				{
					Name: "settlement",
					BlockRef: fabricopsv1alpha1.ParticipantArtifactKeyRef{
						ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "settlement-artifacts"},
							Key:                  "channel.block",
						},
					},
					Peers: []string{"peer0"},
					AnchorPeers: []fabricopsv1alpha1.ParticipantAnchorPeer{
						{Name: "peer0", Host: "peer0.bankb.fabricops.io", Port: 7051},
					},
					Membership: &fabricopsv1alpha1.ParticipantChannelMembership{
						ApplicationPolicy:    "/Channel/Application/Admins",
						RequiredSignerMSPIDs: []string{"BankAMSP", "BankCMSP"},
					},
				},
			},
			Chaincodes: []fabricopsv1alpha1.ParticipantChaincode{
				{
					Name:              "settlement",
					Channel:           "settlement",
					Version:           "1.0",
					PackageLabel:      "settlement_settlement_1.0",
					Sequence:          1,
					EndorsementPolicy: "AND('BankAMSP.member','BankBMSP.member')",
					Image:             "ghcr.io/lf-decentralized-trust-labs/fabricops-node-settlement:0.2.0",
				},
			},
		},
		Status: fabricopsv1alpha1.FabricParticipantStatus{
			LocalOrgStatus: fabricopsv1alpha1.OrgStatus{
				Name:          "BankB",
				Namespace:     "fo-test-bankb",
				IdentityReady: true,
				CAReady:       true,
				CAEndpoint:    "ca.fo-test-bankb.svc.cluster.local:7054",
				PeerEndpoints: []fabricopsv1alpha1.PeerEndpointStatus{
					{
						Name:                "peer0",
						Address:             "host.docker.internal:9051",
						TLSHostnameOverride: "localhost",
						ChaincodeAddress:    "peer0.fo-test-bankb.svc.cluster.local:7052",
						OperationsAddress:   "peer0.fo-test-bankb.svc.cluster.local:9443",
					},
				},
			},
			ChaincodeStatus: []fabricopsv1alpha1.ParticipantChaincodeStatus{
				{
					Name:      "settlement",
					Channel:   "settlement",
					Installed: true,
					Approved:  true,
					Ready:     true,
				},
			},
		},
	}
}

func joinBundleTestParticipantObjects() []ctrlclient.Object {
	return []ctrlclient.Object{
		joinBundleSecret("fo-test-bankb", "bankb-admin-msp", map[string]string{
			mspConfigKey:     joinBundleTestMSPConfigYAML,
			mspCACertKey:     "BANKB_MSP_CA",
			mspTLSCACertKey:  "BANKB_MSP_TLS_CA",
			mspSignCertKey:   "SIGN_CERT_SHOULD_NOT_EXPORT",
			mspKeyStoreKey:   "PRIVATE_KEY_SHOULD_NOT_EXPORT",
			tlsClientCertKey: "TLS_CLIENT_CERT_SHOULD_NOT_EXPORT",
			tlsClientKeyKey:  "TLS_CLIENT_KEY_SHOULD_NOT_EXPORT",
		}),
		joinBundleSecret("fo-test-bankb", "bankb-admin-tls", map[string]string{
			tlsCACertKey:     "BANKB_ADMIN_TLS_CA",
			tlsClientCertKey: "TLS_CLIENT_CERT_SHOULD_NOT_EXPORT",
			tlsClientKeyKey:  "TLS_CLIENT_KEY_SHOULD_NOT_EXPORT",
		}),
		joinBundleSecret("fo-test-bankb", "peer0-tls", map[string]string{
			tlsCACertKey:     "BANKB_PEER0_TLS_CA",
			tlsClientCertKey: "TLS_CLIENT_CERT_SHOULD_NOT_EXPORT",
			tlsClientKeyKey:  "TLS_CLIENT_KEY_SHOULD_NOT_EXPORT",
		}),
		joinBundleConfigMap("default", "orderer0-artifacts", map[string]string{
			"tls-ca.pem": "ORDERER0_TLS_CA",
		}),
		joinBundleConfigMap("default", "settlement-artifacts", map[string]string{
			"channel.block": "CHANNEL_BLOCK",
		}),
	}
}

func joinBundleTestSecrets() []ctrlclient.Object {
	return []ctrlclient.Object{
		joinBundleSecret("fo-test-banka", "banka-admin-msp", map[string]string{
			mspConfigKey:     joinBundleTestMSPConfigYAML,
			mspCACertKey:     "BANKA_MSP_CA",
			mspTLSCACertKey:  "BANKA_MSP_TLS_CA",
			mspSignCertKey:   "SIGN_CERT_SHOULD_NOT_EXPORT",
			mspKeyStoreKey:   "PRIVATE_KEY_SHOULD_NOT_EXPORT",
			tlsClientCertKey: "TLS_CLIENT_CERT_SHOULD_NOT_EXPORT",
			tlsClientKeyKey:  "TLS_CLIENT_KEY_SHOULD_NOT_EXPORT",
		}),
		joinBundleSecret("fo-test-banka", "banka-admin-tls", map[string]string{
			tlsCACertKey:     "BANKA_ADMIN_TLS_CA",
			tlsClientCertKey: "TLS_CLIENT_CERT_SHOULD_NOT_EXPORT",
			tlsClientKeyKey:  "TLS_CLIENT_KEY_SHOULD_NOT_EXPORT",
		}),
		joinBundleSecret("fo-test-banka", "peer0-tls", map[string]string{
			tlsCACertKey:     "BANKA_PEER0_TLS_CA",
			tlsClientCertKey: "TLS_CLIENT_CERT_SHOULD_NOT_EXPORT",
			tlsClientKeyKey:  "TLS_CLIENT_KEY_SHOULD_NOT_EXPORT",
		}),
		joinBundleSecret("fo-test-banka", "peer1-tls", map[string]string{
			tlsCACertKey: "BANKA_PEER1_TLS_CA",
		}),
		joinBundleSecret("fo-test-orderer", "orderer0-tls", map[string]string{
			tlsCACertKey: "ORDERER0_TLS_CA",
		}),
	}
}

func joinBundleConfigMap(namespace string, name string, data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Data: data,
	}
}

func joinBundleSecret(namespace string, name string, data map[string]string) *corev1.Secret {
	secretData := map[string][]byte{}
	for key, value := range data {
		secretData[key] = []byte(value)
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Data: secretData,
	}
}
