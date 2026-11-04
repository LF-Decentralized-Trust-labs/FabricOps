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
	"encoding/json"
	"strings"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

func TestChaincodeDiagnosticsShowRecordedArtifactLocations(t *testing.T) {
	var out bytes.Buffer
	printChaincodeDiagnostics(&out, fabricopsv1alpha1.ChaincodeStatus{
		CommitJobName: "commit-v2", CommitResultNamespace: "bank-b", CommitResultConfigMapName: "commit-v2-result",
		Targets: []fabricopsv1alpha1.ChaincodeTargetStatus{
			{OrgName: "BankA", PeerName: "peer0", Namespace: "bank-a", InstallJobName: "install-v2",
				PackageIDConfigMapName: "install-v2-result", Message: "Chaincode install Job failed"},
			{OrgName: "BankB", PeerName: "peer1", Namespace: "bank-b", Installed: true, ApproveJobName: "approve-v2"},
		},
	})
	for _, want := range []string{
		"BankA/peer0: installed=false", "Chaincode install Job failed", "Install Job: bank-a/install-v2",
		"Install evidence ConfigMap: bank-a/install-v2-result", "BankB/peer1: installed=true",
		"Approve Job: bank-b/approve-v2", "Commit Job: bank-b/commit-v2",
		"Commit evidence ConfigMap: bank-b/commit-v2-result",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q in diagnostics:\n%s", want, out.String())
		}
	}
}

func TestParticipantInspectionSelectsChannelAndName(t *testing.T) {
	statuses := []fabricopsv1alpha1.ParticipantChaincodeStatus{
		{Name: "settlement", Channel: "audit"},
		{Name: "settlement", Channel: "payments", Installed: true, Approved: true},
	}
	var out bytes.Buffer
	if err := printParticipantChaincodeInspection(&out, statuses, "payments", "settlement", "json", true); err != nil {
		t.Fatal(err)
	}
	var result struct {
		Status fabricopsv1alpha1.ParticipantChaincodeStatus `json:"recordedStatus"`
	}
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != statuses[1] {
		t.Fatalf("unexpected recorded status: %+v", result.Status)
	}
	if err := printParticipantChaincodeInspection(&out, statuses, "missing", "settlement", "json", true); err == nil {
		t.Fatal("expected missing recorded definition to fail inspection")
	}
}

func TestReadRecordedInstallsUsesPeerArtifactReferences(t *testing.T) {
	const raw = `{"installed_chaincodes":[{"package_id":"settlement:abc"}]}`
	client := fake.NewClientBuilder().WithScheme(cliScheme).
		WithObjects(joinBundleConfigMap("bank-a", "install-result", map[string]string{"queryinstalled.json": raw})).
		Build()
	targets := []fabricopsv1alpha1.ChaincodeTargetStatus{
		{OrgName: "BankA", PeerName: "peer0", Namespace: "bank-a", PackageIDConfigMapName: "install-result"},
		{OrgName: "BankB", PeerName: "peer0", Namespace: "bank-b", PackageIDConfigMapName: "install-result"},
	}
	results, err := readRecordedInstalls(t.Context(), client, targets)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || string(results[0].QueryInstalled) != raw ||
		results[0].ConfigMap != "bank-a/install-result" || len(results[1].QueryInstalled) != 0 {
		t.Fatalf("unexpected install evidence: %+v", results)
	}
}

func TestReadRecordedCommittedEvidence(t *testing.T) {
	const raw = `{"sequence":2,"version":"0.0.2"}`
	client := fake.NewClientBuilder().WithScheme(cliScheme).
		WithObjects(joinBundleConfigMap("submitter", "commit-result", map[string]string{"querycommitted.json": raw})).
		Build()
	for _, tc := range []struct{ namespace, name, want string }{
		{"submitter", "commit-result", raw},
		{"other-peer", "commit-result", ""},
		{"", "", ""},
	} {
		got, err := readRecordedChaincodeJSON(t.Context(), client, tc.namespace, tc.name, "querycommitted.json")
		if err != nil || string(got) != tc.want {
			t.Fatalf("evidence for %s/%s: got %s, err %v; want %s", tc.namespace, tc.name, got, err, tc.want)
		}
	}
}
