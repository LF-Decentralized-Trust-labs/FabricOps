// Copyright contributors to FabricOps. SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCustodyStatusExplainsGateWithoutProviderDetails(t *testing.T) {
	for _, participant := range []bool{false, true} {
		org := backupCustodyOrg()
		org.Peer.MSPSigning.PKCS11.Library = "/sensitive-provider-path/module.so"
		org.Peer.MSPSigning.PKCS11.TokenLabel = "sensitive-token"
		var out bytes.Buffer
		if participant {
			p := &fabricopsv1alpha1.FabricParticipant{}
			p.Spec.Org = org
			before := p.DeepCopy()
			printParticipantStatus(&out, p)
			if !reflect.DeepEqual(p, before) {
				t.Fatal("diagnostics mutated participant")
			}
		} else {
			n := &fabricopsv1alpha1.FabricNetwork{}
			n.Spec.Orgs = []fabricopsv1alpha1.Org{org}
			// Old Ready evidence must not be presented as provider verification.
			n.Status.Conditions = []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}}
			before := n.DeepCopy()
			printStatus(&out, n)
			if !reflect.DeepEqual(n, before) {
				t.Fatal("diagnostics mutated network")
			}
		}
		for _, want := range []string{"Peer MSP custody (requested configuration)", "provider=PKCS11",
			"state=Gated", "software fallback is disabled", "Do not delete token claims",
			"not runtime health or hardware attestation"} {
			if !strings.Contains(out.String(), want) {
				t.Fatalf("missing %q in custody diagnostics", want)
			}
		}
		for _, private := range []string{"sensitive-provider-path", "sensitive-token", "provider-pin",
			"provider-config", "token-peer0"} {
			if strings.Contains(out.String(), private) {
				t.Fatal("diagnostics exposed provider details")
			}
		}
	}
}

func TestCustodyStatusRejectsUnknownAndContradictoryProviders(t *testing.T) {
	for _, provider := range []string{strings.Repeat("sensitive-provider\n", 1000), "", "Secret"} {
		org := backupCustodyOrg()
		org.Peer.MSPSigning.Provider = provider
		org.Organization.Name = strings.Repeat("x", 1000) + "\nextra-output"
		n := &fabricopsv1alpha1.FabricNetwork{}
		n.Spec.Orgs = []fabricopsv1alpha1.Org{org}
		var out bytes.Buffer
		printStatus(&out, n)
		if !strings.Contains(out.String(), "provider=Invalid state=Blocked") ||
			strings.Contains(out.String(), "sensitive-provider") || strings.Contains(out.String(), "extra-output") ||
			out.Len() > 1500 {
			t.Fatal("invalid custody diagnostics were unbounded, misleading or unredacted")
		}
	}
}

func TestCustodyStatusLeavesSoftwareOutputUnchanged(t *testing.T) {
	for _, org := range []fabricopsv1alpha1.Org{{}, {Peer: &fabricopsv1alpha1.PeerConfig{}},
		{Peer: &fabricopsv1alpha1.PeerConfig{MSPSigning: &fabricopsv1alpha1.PeerMSPSigningConfig{Provider: "Secret"}}}} {
		n := &fabricopsv1alpha1.FabricNetwork{}
		n.Spec.Orgs = []fabricopsv1alpha1.Org{org}
		var out bytes.Buffer
		printStatus(&out, n)
		if out.String() != "FabricNetwork: /\nPhase: Unknown\n" {
			t.Fatal("software-only status changed")
		}
	}
}

func TestCustodyStatusAppearsInWaitFailureDiagnostics(t *testing.T) {
	for _, participant := range []bool{false, true} {
		var getter waitResourceGetter
		if participant {
			p := &fabricopsv1alpha1.FabricParticipant{}
			p.Spec.Org = backupCustodyOrg()
			getter = fabricParticipantWaitState(func(context.Context, kubeOptions, string) (
				*fabricopsv1alpha1.FabricParticipant, error) {
				return p, nil
			})
		} else {
			n := &fabricopsv1alpha1.FabricNetwork{}
			n.Spec.Orgs = []fabricopsv1alpha1.Org{backupCustodyOrg()}
			getter = fabricNetworkWaitState(func(context.Context, kubeOptions, string) (
				*fabricopsv1alpha1.FabricNetwork, error) {
				return n, nil
			})
		}
		state, err := getter(t.Context(), kubeOptions{}, "sample", "Ready")
		if err != nil || state.conditionIsTrue {
			t.Fatal("unreconciled custody request was treated as ready")
		}
		var out bytes.Buffer
		printResourceWaitDiagnostics(&out, &state, nil)
		if !strings.Contains(out.String(), "state=Gated") || strings.Contains(out.String(), "provider-pin") {
			t.Fatal("wait diagnostics omitted custody guidance or exposed provider details")
		}
	}
}
