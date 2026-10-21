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
	"strings"
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

func TestRaftStagingReconciliation(t *testing.T) {
	net, channel, plan := raftStagingFixture(t)
	input := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "reviewed-plan", Namespace: net.Namespace},
		Data: map[string]string{"plan.json": string(plan)}}
	r := &FabricNetworkReconciler{Client: fake.NewClientBuilder().WithScheme(scheme.Scheme).
		WithObjects(input).Build(), Scheme: scheme.Scheme}
	ctx := context.Background()
	status, err := r.reconcileRaftUpdate(ctx, net, channel)
	if err != nil || status.Ready || status.UpdateJobName == "" {
		t.Fatalf("missing staging Job: %+v (%v)", status, err)
	}
	var job batchv1.Job
	key := client.ObjectKey{Name: status.UpdateJobName, Namespace: status.Namespace}
	if err := r.Get(ctx, key, &job); err != nil {
		t.Fatal(err)
	}
	script := strings.Join(job.Spec.Template.Spec.InitContainers[0].Command, " ")
	for _, forbidden := range []string{"peer channel update", "osnadmin channel remove", "kubectl delete"} {
		if strings.Contains(script, forbidden) {
			t.Fatalf("staging performs a mutation: %s", forbidden)
		}
	}
	if !strings.Contains(script, "OrdererMSP") || strings.Contains(script, "BankAMSP") {
		t.Fatal("staging must use the ordering org admin")
	}
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.Secret != nil && strings.Contains(volume.Secret.SecretName, "banka") {
			t.Fatal("application admin private material crossed into the orderer staging Job")
		}
	}
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
	if err := r.Status().Update(ctx, &job); err != nil {
		t.Fatal(err)
	}
	status, err = r.reconcileRaftUpdate(ctx, net, channel)
	if err != nil || status.Phase != fabricopsv1alpha1.ChannelConfigUpdateFailed {
		t.Fatalf("failed staging not reported: %+v (%v)", status, err)
	}
	result := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: status.ArtifactConfigMapName, Namespace: status.Namespace},
		Data: map[string]string{"raft-update.json": `{"channel":"settlement","operationID":"add-orderer1","outcome":"staged","planHash":"` +
			status.PlanHash + `","configSequence":"3","action":"Add"}`},
		BinaryData: map[string][]byte{"config-update.pb": []byte("unsigned-envelope"), "channel-block.pb": []byte("live-config")}}
	if err := r.Create(ctx, result); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(ctx, &job); err != nil {
		t.Fatal(err)
	}
	status, err = r.reconcileRaftUpdate(ctx, net, channel)
	if err != nil || status.Ready || status.Phase != fabricopsv1alpha1.ChannelConfigUpdatePendingSignatures {
		t.Fatalf("durable staging lost after Job cleanup: %+v (%v)", status, err)
	}
	if err := r.Get(ctx, key, &job); err == nil {
		t.Fatal("completed staging Job was recreated")
	}
	result.Data["raft-update.json"] = strings.ReplaceAll(result.Data["raft-update.json"], "add-orderer1", "wrong-operation")
	if err := r.Update(ctx, result); err != nil {
		t.Fatal(err)
	}
	status, err = r.reconcileRaftUpdate(ctx, net, channel)
	if err != nil || status.Phase != fabricopsv1alpha1.ChannelConfigUpdateFailed {
		t.Fatalf("mismatched evidence accepted: %+v (%v)", status, err)
	}
}

func TestRaftStagingRejectsInvalidRequests(t *testing.T) {
	net, channel, plan := raftStagingFixture(t)
	for _, tc := range []struct {
		name   string
		mutate func(*fabricopsv1alpha1.ChannelRaftUpdate)
	}{
		{"application admin", func(op *fabricopsv1alpha1.ChannelRaftUpdate) { op.AdminOrg = "BankA" }},
		{"missing source", func(op *fabricopsv1alpha1.ChannelRaftUpdate) { op.Orderer.Name = "" }},
		{"unknown source", func(op *fabricopsv1alpha1.ChannelRaftUpdate) { op.Orderer.Name = "unknown" }},
		{"different org", func(op *fabricopsv1alpha1.ChannelRaftUpdate) { op.Orderer.Org = "BankA" }},
		{"empty ID", func(op *fabricopsv1alpha1.ChannelRaftUpdate) { op.OperationID = "" }},
		{"missing plan", func(op *fabricopsv1alpha1.ChannelRaftUpdate) { op.PlanRef = fabricopsv1alpha1.ChannelArtifactKeyRef{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			copy := channel
			op := *channel.RaftUpdate
			copy.RaftUpdate = &op
			tc.mutate(&op)
			if len(validateChannelRaftUpdate(net, copy)) == 0 {
				t.Fatal("invalid request passed topology validation")
			}
		})
	}
	input := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "reviewed-plan", Namespace: net.Namespace},
		Data: map[string]string{"plan.json": string(plan)}}
	input.Data["plan.json"] = strings.ReplaceAll(string(plan), `"channel":"settlement"`, `"channel":"audit"`)
	r := &FabricNetworkReconciler{Client: fake.NewClientBuilder().WithScheme(scheme.Scheme).
		WithObjects(input).Build(), Scheme: scheme.Scheme}
	status, err := r.reconcileRaftUpdate(context.Background(), net, channel)
	if err != nil || status.Phase != fabricopsv1alpha1.ChannelConfigUpdateFailed {
		t.Fatalf("wrong-channel plan accepted: %+v (%v)", status, err)
	}
	var jobs batchv1.JobList
	if err := r.List(context.Background(), &jobs); err != nil || len(jobs.Items) != 0 {
		t.Fatal("invalid plan created a Job", err)
	}
}

func raftStagingFixture(t *testing.T) (*fabricopsv1alpha1.FabricNetwork, fabricopsv1alpha1.Channel, []byte) {
	t.Helper()
	net, channel, _, _, orderer := channelConfigOperationFixture()
	authority, err := generateTestIdentityAuthority(orderer.org)
	if err != nil {
		t.Fatal(err)
	}
	consenters := []channelconfig.RaftConsenter{}
	for _, name := range []string{"orderer0", "orderer1"} {
		secret, err := buildTestWorkloadTLSSecret(net, orderer.org, orderer.namespace, name,
			componentOrderer, []string{name + ".example.com"}, authority)
		if err != nil {
			t.Fatal(err)
		}
		consenters = append(consenters, channelconfig.RaftConsenter{Host: name + ".example.com", Port: 7050,
			ClientTLSCert: secret.Data[tlsServerCertKey], ServerTLSCert: secret.Data[tlsServerCertKey]})
	}
	current, _ := json.Marshal(consenters[:1])
	config := `{"sequence":"3","channel_group":{"groups":{"Orderer":{"groups":{"OrdererMSP":{}},` +
		`"values":{"ConsensusType":{"version":"1","value":{"type":"etcdraft","state":"STATE_NORMAL",` +
		`"metadata":{"consenters":` + string(current) + `}}}}}}}}`
	block := `{"data":{"data":[{"payload":{"header":{"channel_header":{"channel_id":"settlement","type":1}},` +
		`"data":{"config":` + config + `}}}]}}`
	plan, err := channelconfig.PlanRaftUpdate([]byte(block), "settlement", consenters)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	channel.RaftUpdate = &fabricopsv1alpha1.ChannelRaftUpdate{OperationID: "add-orderer1", AdminOrg: "Orderer",
		Orderer: fabricopsv1alpha1.ChannelOrdererRef{Org: "Orderer", Name: "orderer0"},
		PlanRef: fabricopsv1alpha1.ChannelArtifactKeyRef{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "reviewed-plan"}, Key: "plan.json"}}}
	net.Spec.Channels = []fabricopsv1alpha1.Channel{channel}
	return net, channel, raw
}
