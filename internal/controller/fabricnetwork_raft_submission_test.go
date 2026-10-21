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
	"encoding/json"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
	"github.com/LF-Decentralized-Trust-labs/FabricOps/internal/channelconfig"
)

func TestRaftSignedSubmissionSnapshotsAndDurableStatus(t *testing.T) {
	net, channel, raw := raftStagingFixture(t)
	var plan channelconfig.RaftUpdatePlan
	if err := json.Unmarshal(raw, &plan); err != nil {
		t.Fatal(err)
	}
	desired, err := channelconfig.RaftConsentersFromConfig(plan.UpdatedConfig)
	if err != nil {
		t.Fatal(err)
	}
	group := &net.Spec.Orgs[0].Orderers[0]
	group.Instances = 2
	objects := []client.Object{&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "reviewed-plan", Namespace: net.Namespace}, Data: map[string]string{"plan.json": string(raw)}}}
	instances := desiredOrdererInstances(net)
	for index, consenter := range desired {
		group.ExternalEndpoints = append(group.ExternalEndpoints, fabricopsv1alpha1.ExternalEndpoint{
			Name: instances[index].name, Address: consenter.Host + ":7050"})
		objects = append(objects, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
			Name: identitySecretName(instances[index].name, secretKindTLS), Namespace: instances[index].namespace},
			Data: map[string][]byte{tlsServerCertKey: consenter.ServerTLSCert, tlsCACertKey: []byte("public-ca")}})
	}
	signed := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "signed-raft", Namespace: net.Namespace},
		Data: map[string][]byte{"envelope.pb": []byte("signed-envelope")}}
	objects = append(objects, signed)
	r := &FabricNetworkReconciler{Client: fake.NewClientBuilder().WithScheme(scheme.Scheme).
		WithObjects(objects...).Build(), Scheme: scheme.Scheme}
	ctx := context.Background()
	status, err := r.reconcileRaftUpdate(ctx, net, channel)
	if err != nil {
		t.Fatal(err)
	}
	receipt := raftUpdateReceipt{Channel: channel.Name, OperationID: channel.RaftUpdate.OperationID,
		PlanHash: status.PlanHash, ConfigSequence: plan.ConfigSequence, Action: plan.Action, Outcome: "staged"}
	receiptRaw, _ := json.Marshal(receipt)
	staged := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: status.ArtifactConfigMapName, Namespace: status.Namespace},
		Data:       map[string]string{"raft-update.json": string(receiptRaw)},
		BinaryData: map[string][]byte{"channel-block.pb": []byte("source-config"), "config-update.pb": []byte("staged-envelope")}}
	if err := r.Create(ctx, staged); err != nil {
		t.Fatal(err)
	}
	channel.RaftUpdate.SignedUpdateRef = &fabricopsv1alpha1.ChannelArtifactKeyRef{SecretKeyRef: &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: signed.Name}, Key: "envelope.pb"}}
	status, err = r.reconcileRaftUpdate(ctx, net, channel)
	if err != nil || status.Ready || status.Phase != fabricopsv1alpha1.ChannelConfigUpdatePendingSignatures {
		t.Fatalf("unexpected in-flight status: %+v (%v)", status, err)
	}
	firstJob := status.UpdateJobName
	signed.Data["envelope.pb"] = []byte("corrected-envelope")
	if err := r.Update(ctx, signed); err != nil {
		t.Fatal(err)
	}
	status, err = r.reconcileRaftUpdate(ctx, net, channel)
	if err != nil || status.UpdateJobName == firstJob {
		t.Fatalf("corrected handoff reused immutable Job: %+v (%v)", status, err)
	}
	var input corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKey{Namespace: status.Namespace, Name: status.UpdateJobName + "-input"}, &input); err != nil {
		t.Fatal(err)
	}
	if input.Immutable == nil || !*input.Immutable || string(input.BinaryData["signed.pb"]) != "corrected-envelope" ||
		string(input.BinaryData["staged.pb"]) != "staged-envelope" {
		t.Fatal("signed and staged inputs were not snapshotted together")
	}
	if err := json.Unmarshal([]byte(input.Data["request.json"]), &receipt); err != nil {
		t.Fatal(err)
	}
	receipt.Outcome = "submitted"
	receiptRaw, _ = json.Marshal(receipt)
	result := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: status.ArtifactConfigMapName, Namespace: status.Namespace},
		Data:       map[string]string{"raft-update.json": string(receiptRaw)},
		BinaryData: map[string][]byte{"channel-block.pb": []byte("committed-config"), "config-update.pb": signed.Data["envelope.pb"]}}
	if err := r.Create(ctx, result); err != nil {
		t.Fatal(err)
	}
	var job batchv1.Job
	if err := r.Get(ctx, client.ObjectKey{Namespace: status.Namespace, Name: status.UpdateJobName}, &job); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(ctx, &job); err != nil {
		t.Fatal(err)
	}
	status, err = r.reconcileRaftUpdate(ctx, net, channel)
	if err != nil || !status.Ready || status.Phase != fabricopsv1alpha1.ChannelConfigUpdateSubmitted {
		t.Fatalf("committed receipt lost after cleanup: %+v (%v)", status, err)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(&job), &job); err == nil {
		t.Fatal("completed submission Job was recreated")
	}
}

func TestRaftSubmissionRequiresManagedPinnedConsenters(t *testing.T) {
	net, channel, raw := raftStagingFixture(t)
	var plan channelconfig.RaftUpdatePlan
	if err := json.Unmarshal(raw, &plan); err != nil {
		t.Fatal(err)
	}
	current, err := channelconfig.RaftConsentersFromConfig(plan.OriginalConfig)
	if err != nil {
		t.Fatal(err)
	}
	desired, err := channelconfig.RaftConsentersFromConfig(plan.UpdatedConfig)
	if err != nil {
		t.Fatal(err)
	}
	net.Spec.Orgs[0].Orderers[0].Instances = 2
	instances := desiredOrdererInstances(net)
	current[0].Host, desired[0].Host = channelOrdererHost(instances[0]), channelOrdererHost(instances[0])
	desired[1].Host = channelOrdererHost(instances[1])
	objects := []client.Object{}
	for index, consenter := range desired {
		secret := &corev1.Secret{}
		secret.Name = identitySecretName(instances[index].name, secretKindTLS)
		secret.Namespace = instances[index].namespace
		secret.Data = map[string][]byte{tlsServerCertKey: consenter.ServerTLSCert, tlsCACertKey: []byte("public-ca")}
		objects = append(objects, secret)
	}
	r := &FabricNetworkReconciler{Client: fake.NewClientBuilder().WithScheme(scheme.Scheme).
		WithObjects(objects...).Build(), Scheme: scheme.Scheme}
	nodes, err := r.raftSubmissionNodes(context.Background(), net, channel.RaftUpdate, current, desired)
	if err != nil || len(nodes) != 2 || !nodes[0].Current || nodes[1].Current || !nodes[1].Desired {
		t.Fatalf("incorrect preflight cohort: %+v (%v)", nodes, err)
	}
	if _, err := r.raftSubmissionNodes(context.Background(), net, channel.RaftUpdate, current, desired[1:]); err == nil {
		t.Fatal("submission allowed removal of its own config-fetch source")
	}
	desired[1].Host = "unmanaged.example.com"
	if _, err := r.raftSubmissionNodes(context.Background(), net, channel.RaftUpdate, current, desired); err == nil {
		t.Fatal("unmanaged consenter accepted")
	}
	desired[1].Host = channelOrdererHost(instances[1])
	desired[1].ServerTLSCert = current[0].ServerTLSCert
	if _, err := r.raftSubmissionNodes(context.Background(), net, channel.RaftUpdate, current, desired); err == nil {
		t.Fatal("unpinned certificate accepted")
	}
}
