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
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

func TestLocalAdmissionDoesNotTrustPeerJoinAlone(t *testing.T) {
	ctx := context.Background()
	net, channel, adminOrg, adminPeer, _ := channelConfigOperationFixture()
	joiningOrg := adminOrg
	joiningOrg.Organization = fabricopsv1alpha1.OrgMeta{Name: "BankC", MSPName: "BankCMSP", Domain: "bankc.example.com"}
	net.Spec.Orgs = append(net.Spec.Orgs, joiningOrg)
	channel.Orgs = append(channel.Orgs, fabricopsv1alpha1.ChannelOrg{Name: "BankC", Peers: []string{"peer0"}})
	joiningPeer := peerInstance{org: joiningOrg, name: "peer0", namespace: orgNamespaceName(net, joiningOrg)}
	anchorReceipt := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: channelAnchorPeerUpdateResultConfigMapName(channel.Name, adminOrg), Namespace: adminPeer.namespace},
		Data:       map[string]string{channelAnchorPeerResultKey: `{"channel":"settlement","mspID":"BankAMSP","anchorPeers":[{"host":"` + channelPeerHost(adminPeer) + `","port":7051}]}`},
	}
	peerReceipt := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: channelPeerJoinResultConfigMapName(channel.Name, joiningPeer), Namespace: joiningPeer.namespace},
		Data:       map[string]string{channelPeerJoinResultKey: channel.Name},
	}
	staged := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: channelLocalOrgAddResultConfigMapName(channel.Name, joiningOrg), Namespace: adminPeer.namespace},
		Data:       map[string]string{localOrgAddResultKey: `{"channel":"settlement","mspID":"BankCMSP","outcome":"staged"}`},
		BinaryData: map[string][]byte{localOrgAddUpdateArtifactKey: []byte("proposal"), localOrgAddBlockArtifactKey: []byte("old-block")},
	}
	r := &FabricNetworkReconciler{Client: fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(anchorReceipt, peerReceipt, staged).Build(), Scheme: scheme.Scheme}
	status := fabricopsv1alpha1.ChannelStatus{Orgs: []fabricopsv1alpha1.ChannelOrgStatus{
		{Name: "BankA", PeerNames: []string{"peer0"}}, {Name: "BankC", PeerNames: []string{"peer0"}},
	}}
	message, err := r.reconcileLocalOrgAdditions(ctx, net, channel, &status)
	if err != nil || !strings.Contains(message, "Waiting for channel config signatures") {
		t.Fatalf("peer receipt bypassed membership approval: %s (%v)", message, err)
	}
}

func TestExternalOrgSignedSubmissionStatus(t *testing.T) {
	ctx := context.Background()
	net, channel, _, adminPeer, _ := channelConfigOperationFixture()
	external := fabricopsv1alpha1.ChannelExternalOrg{
		Name: "BankC", MSPID: "BankCMSP", RequiredSignerMSPIDs: []string{"BankAMSP", "BankBMSP"},
		SignedUpdateRef: &fabricopsv1alpha1.ChannelArtifactKeyRef{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "signed"}, Key: "envelope.pb",
		}},
	}
	channel.ExternalOrgs = []fabricopsv1alpha1.ChannelExternalOrg{external}
	staged := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: channelExternalOrgUpdateResultConfigMapName(channel.Name, external), Namespace: adminPeer.namespace},
		Data:       map[string]string{externalOrgUpdateResultKey: `{"channel":"settlement","mspID":"BankCMSP","state":"Present","outcome":"staged"}`},
		BinaryData: map[string][]byte{externalOrgUpdateArtifactKey: []byte("proposal")},
	}
	signed := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "signed", Namespace: net.Namespace},
		BinaryData: map[string][]byte{"envelope.pb": []byte("signed-proposal")},
	}
	r := &FabricNetworkReconciler{Client: fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(staged, signed).Build(), Scheme: scheme.Scheme}
	status, err := r.reconcileExternalOrgUpdate(ctx, net, channel, external)
	if err != nil || status.Ready || status.Phase != fabricopsv1alpha1.ChannelConfigUpdatePendingSignatures {
		t.Fatalf("unexpected pending status: %+v (%v)", status, err)
	}
	var job batchv1.Job
	if err := r.Get(ctx, client.ObjectKey{Namespace: adminPeer.namespace, Name: status.UpdateJobName}, &job); err != nil {
		t.Fatal(err)
	}
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
	if err := r.Status().Update(ctx, &job); err != nil {
		t.Fatal(err)
	}
	status, err = r.reconcileExternalOrgUpdate(ctx, net, channel, external)
	if err != nil || status.Ready || status.Phase != fabricopsv1alpha1.ChannelConfigUpdateFailed {
		t.Fatalf("failed signature submission was not reported: %+v (%v)", status, err)
	}
	result := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: status.UpdateJobName + "-result", Namespace: adminPeer.namespace},
		Data:       map[string]string{externalOrgUpdateResultKey: `{"channel":"settlement","mspID":"BankCMSP","state":"Present","outcome":"submitted"}`},
	}
	if err := r.Create(ctx, result); err != nil {
		t.Fatal(err)
	}
	status, err = r.reconcileExternalOrgUpdate(ctx, net, channel, external)
	if err != nil || !status.Ready || status.Phase != fabricopsv1alpha1.ChannelConfigUpdateSubmitted || status.ArtifactConfigMapName != result.Name {
		t.Fatalf("completed handoff did not become ready: %+v (%v)", status, err)
	}
}

func TestLocalAdmissionReceiptPreservesGenesisBlock(t *testing.T) {
	ctx := context.Background()
	net, channel, adminOrg, adminPeer, orderer := channelConfigOperationFixture()
	block := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: channelBlockConfigMapName(channel.Name), Namespace: orderer.namespace},
		BinaryData: map[string][]byte{channelBlockFileName(channel.Name): []byte("original")},
	}
	joiningOrg := fabricopsv1alpha1.Org{Organization: fabricopsv1alpha1.OrgMeta{Name: "BankC", MSPName: "BankCMSP"}}
	receipt := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: channelLocalOrgAddResultConfigMapName(channel.Name, joiningOrg), Namespace: adminPeer.namespace},
		Data:       map[string]string{localOrgAddResultKey: `{"channel":"settlement","mspID":"BankCMSP","outcome":"submitted"}`},
		BinaryData: map[string][]byte{localOrgAddBlockArtifactKey: []byte("latest-config-block")},
	}
	r := &FabricNetworkReconciler{Client: fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(block, receipt).Build(), Scheme: scheme.Scheme}
	ready, message, err := r.reconcileLocalOrgAdd(ctx, net, channel, adminOrg, adminPeer, orderer, joiningOrg)
	if err != nil || !ready || message != "" {
		t.Fatalf("admission did not accept its receipt: %s (%v)", message, err)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(block), block); err != nil {
		t.Fatal(err)
	}
	if string(block.BinaryData[channelBlockFileName(channel.Name)]) != "original" {
		t.Fatal("admission replaced the genesis block needed by peer channel join")
	}
}

func TestSignedChannelUpdateSnapshotAndCleanup(t *testing.T) {
	ctx := context.Background()
	net, channel, adminOrg, adminPeer, orderer := channelConfigOperationFixture()
	joiningOrg := fabricopsv1alpha1.Org{Organization: fabricopsv1alpha1.OrgMeta{Name: "BankC", MSPName: "BankCMSP"}}
	base := buildLocalOrgAddJob(net, channel, adminOrg, adminPeer.namespace, adminPeer, orderer, joiningOrg)
	staged := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "staged", Namespace: adminPeer.namespace},
		Data:       map[string]string{localOrgAddResultKey: `{"channel":"settlement","mspID":"BankCMSP","outcome":"staged"}`},
		BinaryData: map[string][]byte{localOrgAddUpdateArtifactKey: []byte("proposal")},
	}
	signed := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "signed", Namespace: net.Namespace},
		Data:       map[string][]byte{"envelope.pb": []byte("signed-proposal")},
	}
	ref := &fabricopsv1alpha1.ChannelArtifactKeyRef{SecretKeyRef: &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: signed.Name}, Key: "envelope.pb",
	}}
	r := &FabricNetworkReconciler{Client: fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(staged, signed).Build(), Scheme: scheme.Scheme}
	reconcile := func() string {
		t.Helper()
		name, _, err := r.reconcileSignedChannelUpdate(ctx, net, channel, adminOrg, adminPeer, orderer, ref, base,
			staged.Name, localOrgAddResultKey, localOrgAddUpdateArtifactKey, localOrgAddUpdateFile, "BankCMSP", true)
		if err != nil || name == "" {
			t.Fatalf("submission failed: %s (%v)", name, err)
		}
		return name
	}
	first := reconcile()
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs); err != nil || len(jobs.Items) != 1 {
		t.Fatalf("expected one submission job: %v", err)
	}
	var input corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKeyFromObject(&jobs.Items[0]), &input); err != nil {
		t.Fatal(err)
	}
	if string(input.BinaryData["staged.pb"]) != "proposal" || string(input.BinaryData["signed.pb"]) != "signed-proposal" {
		t.Fatal("submission did not snapshot both artifacts")
	}
	result := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: first, Namespace: adminPeer.namespace}}
	if err := r.Create(ctx, result); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(ctx, &jobs.Items[0]); err != nil {
		t.Fatal(err)
	}
	if reconcile() != first {
		t.Fatal("same handoff changed operation identity")
	}
	if err := r.List(ctx, &jobs); err != nil || len(jobs.Items) != 0 {
		t.Fatal("durable evidence did not prevent recreating a cleaned-up Job")
	}
	signed.Data["envelope.pb"] = []byte("corrected-signatures")
	if err := r.Update(ctx, signed); err != nil {
		t.Fatal(err)
	}
	if reconcile() == first {
		t.Fatal("corrected signatures reused the previous submission")
	}
}

func TestSignedChannelUpdateValidation(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is required to execute the Fabric tools script")
	}
	const proposal = `{"payload":{"header":{"channel_header":{"type":2,"channel_id":"settlement"}},"data":{"config_update":{"channel_id":"settlement","read_set":{"version":"1"},"write_set":{"version":"2"}}}}}`
	const signed = `{"payload":{"header":{"channel_header":{"type":2,"channel_id":"settlement"}},"data":{"config_update":{"channel_id":"settlement","read_set":{"version":"1"},"write_set":{"version":"2"}},"signatures":[{"signature":"signed-by-bankb"}]}}}`
	for _, tc := range []struct {
		name     string
		envelope string
		wantOK   bool
	}{
		{"same update with signatures", signed, true},
		{"no signatures", proposal, false},
		{"different channel", strings.ReplaceAll(signed, "settlement", "audit"), false},
		{"different write set", strings.ReplaceAll(signed, `"version":"2"`, `"version":"3"`), false},
		{"different read set", strings.ReplaceAll(signed, `"version":"1"`, `"version":"0"`), false},
		{"not a config update", strings.ReplaceAll(signed, `"type":2`, `"type":3`), false},
		{"malformed envelope", "not-protobuf", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			input := filepath.Join(dir, "input")
			output := filepath.Join(dir, "output")
			bin := filepath.Join(dir, "bin")
			for _, path := range []string{input, output, bin} {
				if err := os.MkdirAll(path, 0700); err != nil {
					t.Fatal(err)
				}
			}
			files := map[string]string{
				filepath.Join(input, "staged.pb"):   proposal,
				filepath.Join(input, "signed.pb"):   tc.envelope,
				filepath.Join(input, "result.json"): `{"channel":"settlement","mspID":"BankCMSP","state":"Present","operationID":"add-c","outcome":"staged"}`,
				filepath.Join(dir, "before.json"):   `{"data":{"data":[{"payload":{"data":{"config":{"channel_group":{"groups":{"Application":{"groups":{}}}}}}}}]}}`,
				filepath.Join(dir, "after.json"):    `{"data":{"data":[{"payload":{"data":{"config":{"channel_group":{"groups":{"Application":{"groups":{"BankCMSP":{}}}}}}}}}]}}`,
				filepath.Join(bin, "configtxlator"): `#!/bin/sh
set -eu
shift
while [ "$#" -gt 0 ]; do
  case "$1" in
    --input) input="$2"; shift 2 ;;
    --output) output="$2"; shift 2 ;;
    *) shift ;;
  esac
done
jq . "$input" > "$output"
`,
				filepath.Join(bin, "peer"): `#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$SUBMISSION_TEST_DIR/peer.log"
if [ "$2" = update ]; then
  touch "$SUBMISSION_TEST_DIR/committed"
else
  block=before.json
  [ ! -f "$SUBMISSION_TEST_DIR/committed" ] || block=after.json
  cp "$SUBMISSION_TEST_DIR/$block" "$4"
fi
`,
			}
			for path, content := range files {
				if err := os.WriteFile(path, []byte(content), 0700); err != nil {
					t.Fatal(err)
				}
			}
			script := signedChannelUpdateScript("settlement", "BankAMSP", "BankCMSP", "peer:7051", "orderer:7050", "/msp", "/tls", "/orderer-tls", "config-update.pb", true)
			script = strings.ReplaceAll(script, channelOutputDir, output)
			script = strings.ReplaceAll(script, channelSubmissionDir, input)
			cmd := exec.Command("sh", "-ec", script)
			cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "SUBMISSION_TEST_DIR="+dir)
			logs, err := cmd.CombinedOutput()
			if (err == nil) != tc.wantOK {
				t.Fatalf("success=%v, want %v: %s", err == nil, tc.wantOK, logs)
			}
			if !tc.wantOK {
				if _, err := os.Stat(filepath.Join(dir, "peer.log")); !os.IsNotExist(err) {
					t.Fatal("invalid handoff reached the Fabric peer CLI")
				}
				return
			}
			receipt, err := os.ReadFile(filepath.Join(output, "external-org.json"))
			if err != nil || !strings.Contains(string(receipt), `"outcome": "submitted"`) || !strings.Contains(string(receipt), `"operationID": "add-c"`) {
				t.Fatalf("missing submission receipt: %s (%v)", receipt, err)
			}
			block, err := os.ReadFile(filepath.Join(output, localOrgAddBlockFile))
			if err != nil || !strings.Contains(string(block), "BankCMSP") {
				t.Fatalf("missing post-commit block: %s (%v)", block, err)
			}
		})
	}
}
