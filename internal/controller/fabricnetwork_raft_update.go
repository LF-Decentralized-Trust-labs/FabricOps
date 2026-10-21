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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
	"github.com/LF-Decentralized-Trust-labs/FabricOps/internal/channelconfig"
)

const raftStagingInputDir = channelWorkDir + "/raft-input"

type raftUpdateReceipt struct {
	Channel        string `json:"channel"`
	OperationID    string `json:"operationID"`
	PlanHash       string `json:"planHash"`
	ConfigSequence string `json:"configSequence"`
	Action         string `json:"action"`
	Outcome        string `json:"outcome,omitempty"`
	SignedHash     string `json:"signedHash,omitempty"`
}

func validateChannelRaftUpdate(net *fabricopsv1alpha1.FabricNetwork, channel fabricopsv1alpha1.Channel) []string {
	op := channel.RaftUpdate
	if op == nil {
		return nil
	}
	path := "channel " + channel.Name + " raftUpdate"
	problems := validateChannelArtifactRef(path+".planRef", &op.PlanRef)
	if op.SignedUpdateRef != nil {
		problems = append(problems, validateChannelArtifactRef(path+".signedUpdateRef", op.SignedUpdateRef)...)
	}
	if len(validation.IsDNS1123Label(op.OperationID)) != 0 {
		problems = append(problems, path+" operationID must be a non-empty DNS label")
	}
	org, ok := orgsByName(net)[op.AdminOrg]
	if !ok || len(org.Orderers) == 0 {
		problems = append(problems, path+" adminOrg must be a local ordering organization")
	}
	if op.Orderer.Org != op.AdminOrg || op.Orderer.Name == "" || !channelOrdererRefMatchesAny(net, op.Orderer) {
		problems = append(problems, path+" orderer must name an instance within adminOrg")
	}
	if networkUsesBFTOrderers(net) {
		problems = append(problems, path+" does not support BFT reconfiguration")
	}
	return problems
}

func (r *FabricNetworkReconciler) reconcileRaftUpdate(
	ctx context.Context, net *fabricopsv1alpha1.FabricNetwork, channel fabricopsv1alpha1.Channel,
) (*fabricopsv1alpha1.ChannelRaftUpdateStatus, error) {
	op := channel.RaftUpdate
	if op == nil {
		return nil, nil
	}
	status := &fabricopsv1alpha1.ChannelRaftUpdateStatus{
		OperationID: op.OperationID, AdminOrg: op.AdminOrg, Orderer: op.Orderer.Org + "/" + op.Orderer.Name,
		Phase: fabricopsv1alpha1.ChannelConfigUpdatePending,
	}
	if problems := validateChannelRaftUpdate(net, channel); len(problems) != 0 {
		status.Phase, status.Message = fabricopsv1alpha1.ChannelConfigUpdateFailed, strings.Join(problems, "; ")
		return status, nil
	}
	admin := orgsByName(net)[op.AdminOrg]
	status.Namespace = orgNamespaceName(net, admin)
	raw, ready, message, err := r.channelArtifactBytes(ctx, net.Namespace, &op.PlanRef)
	if err != nil || !ready {
		status.Message = "Waiting for Raft plan: " + message
		return status, err
	}
	plan, err := channelconfig.ValidateRaftPlan(raw, channel.Name)
	if err != nil {
		status.Phase, status.Message = fabricopsv1alpha1.ChannelConfigUpdateFailed, "Invalid Raft plan: "+err.Error()
		return status, nil
	}
	canonical, err := json.Marshal(plan)
	if err != nil {
		return status, err
	}
	digest := sha256.Sum256(append([]byte(op.OperationID+"\x00"+op.AdminOrg+"\x00"+status.Orderer+"\x00"), canonical...))
	status.PlanHash, status.ConfigSequence, status.Action = hex.EncodeToString(digest[:]), plan.ConfigSequence, plan.Action
	name := fmt.Sprintf("%s-raft-%s-%x", channel.Name[:min(len(channel.Name), 16)],
		op.OperationID[:min(len(op.OperationID), 12)], digest[:8])
	status.UpdateJobName, status.ArtifactConfigMapName = name, name+"-result"
	expected := raftUpdateReceipt{Channel: channel.Name, OperationID: op.OperationID, PlanHash: status.PlanHash,
		ConfigSequence: plan.ConfigSequence, Action: plan.Action}
	var result corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKey{Namespace: status.Namespace, Name: status.ArtifactConfigMapName}, &result); err == nil {
		var receipt raftUpdateReceipt
		err := json.Unmarshal([]byte(result.Data["raft-update.json"]), &receipt)
		outcome := receipt.Outcome
		receipt.Outcome = ""
		block := append([]byte(result.Data["channel-block.pb"]), result.BinaryData["channel-block.pb"]...)
		envelope := append([]byte(result.Data["config-update.pb"]), result.BinaryData["config-update.pb"]...)
		if err != nil || receipt != expected || len(block) == 0 ||
			(outcome == "staged" && (len(envelope) == 0 || plan.Action == "AlreadyApplied")) ||
			(outcome == "alreadyApplied" && plan.Action != "AlreadyApplied") ||
			(outcome != "staged" && outcome != "alreadyApplied") {
			status.Phase = fabricopsv1alpha1.ChannelConfigUpdateFailed
			status.Message = "Raft staging evidence does not match the reviewed operation"
		} else if outcome == "alreadyApplied" {
			status.Phase, status.Ready = fabricopsv1alpha1.ChannelConfigUpdateAlreadyApplied, true
		} else {
			status.Phase = fabricopsv1alpha1.ChannelConfigUpdatePendingSignatures
			status.Message = "Waiting for orderer-admin signatures; unsigned envelope is in ConfigMap " +
				status.Namespace + "/" + status.ArtifactConfigMapName
			if op.SignedUpdateRef != nil {
				return r.reconcileRaftSubmission(ctx, net, channel, admin, plan, status, &result, expected)
			}
		}
		return status, nil
	} else if !apierrors.IsNotFound(err) {
		return status, err
	}
	if err := r.ensureChannelRBAC(ctx, net, admin, channel.Name, status.Namespace); err != nil {
		return status, err
	}
	receiptJSON, err := json.Marshal(expected)
	if err != nil {
		return status, err
	}
	immutable := true
	input := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name + "-input", Namespace: status.Namespace,
		Labels: channelLabels(net, admin, channel.Name), Annotations: resourceAnnotations(net, admin)},
		Immutable: &immutable, Data: map[string]string{"plan.json": string(canonical), "request.json": string(receiptJSON)}}
	if err := r.ensureConfigMap(ctx, input); err != nil {
		return status, err
	}
	orderer := ordererInstance{org: admin, name: op.Orderer.Name, namespace: status.Namespace}
	job := buildRaftStagingJob(net, channel, admin, orderer, name)
	if err := r.ensureJob(ctx, job); err != nil {
		return status, err
	}
	var actual batchv1.Job
	if err := r.Get(ctx, client.ObjectKeyFromObject(job), &actual); err != nil {
		return status, err
	}
	status.Message = "Waiting for Raft staging Job " + name
	if jobFailed(actual) || jobSucceeded(actual) {
		status.Phase = fabricopsv1alpha1.ChannelConfigUpdateFailed
		status.Message = "Raft staging Job failed or completed without durable evidence: " + name
	}
	return status, nil
}

func buildRaftStagingJob(net *fabricopsv1alpha1.FabricNetwork, channel fabricopsv1alpha1.Channel,
	admin fabricopsv1alpha1.Org, orderer ordererInstance, name string,
) *batchv1.Job {
	labels, annotations := channelLabels(net, admin, channel.Name), resourceAnnotations(net, admin)
	backoff := int32(0)
	return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: orderer.namespace, Labels: labels,
		Annotations: succeededJobCleanupAnnotations(annotations)}, Spec: batchv1.JobSpec{BackoffLimit: &backoff,
		Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{
			ServiceAccountName: channelServiceAccountName(channel.Name), RestartPolicy: corev1.RestartPolicyNever,
			Volumes: []corev1.Volume{
				{Name: "output", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
				{Name: "input", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: name + "-input"}}}},
				{Name: "msp", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
					SecretName: identitySecretName(adminIdentityName(admin), secretKindMSP), Items: mspSecretItems(true)}}},
				{Name: "admin-tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
					SecretName: identitySecretName(adminIdentityName(admin), secretKindTLS), Items: adminTLSSecretItems()}}},
				{Name: "orderer-ca", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
					SecretName: identitySecretName(orderer.name, secretKindTLS),
					Items:      []corev1.KeyToPath{{Key: tlsCACertKey, Path: "ca.crt"}}}}},
			},
			InitContainers: []corev1.Container{{Name: "stage-raft-update", Image: fabricToolsImage(net.Spec.Global),
				Command: []string{"sh", "-ec", raftStagingScript(channel.Name, admin.Organization.MSPName,
					ordererClientAddress(orderer))}, Resources: componentResourceRequirements(componentFabricCLI),
				VolumeMounts: []corev1.VolumeMount{{Name: "output", MountPath: channelOutputDir},
					{Name: "input", MountPath: raftStagingInputDir, ReadOnly: true},
					{Name: "msp", MountPath: "/msp", ReadOnly: true},
					{Name: "admin-tls", MountPath: "/admin-tls", ReadOnly: true},
					{Name: "orderer-ca", MountPath: "/orderer-ca", ReadOnly: true}},
			}},
			Containers: []corev1.Container{{Name: "publish-raft-update", Image: kubectlImage(),
				Resources: componentResourceRequirements(componentKubectl),
				Command:   []string{"sh", "-ec", publishRaftStagingScript()},
				Env: []corev1.EnvVar{{Name: "FABRICOPS_RAFT_RESULT", Value: name + "-result"},
					{Name: "FABRICOPS_CHANNEL", Value: channel.Name},
					{Name: envPodNamespace, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.namespace"}}}},
				VolumeMounts: []corev1.VolumeMount{{Name: "output", MountPath: channelOutputDir}},
			}},
		}},
	}}
}

func raftStagingScript(channel, adminMSP, orderer string) string {
	return fmt.Sprintf(`set -eu
CHANNEL_ID=%q
export CORE_PEER_LOCALMSPID=%q
export CORE_PEER_MSPCONFIGPATH=/msp
export CORE_PEER_TLS_ENABLED=true
export CORE_PEER_TLS_CERT_FILE=/admin-tls/client.crt
export CORE_PEER_TLS_KEY_FILE=/admin-tls/client.key
OUTPUT_DIR=%q
INPUT_DIR=%q
mkdir -p "$OUTPUT_DIR"
peer channel fetch config "$OUTPUT_DIR/channel-block.pb" -c "$CHANNEL_ID" -o %q --tls --cafile /orderer-ca/ca.crt
configtxlator proto_decode --input "$OUTPUT_DIR/channel-block.pb" --type common.Block --output "$OUTPUT_DIR/block.json"
jq -e --arg channel "$CHANNEL_ID" '
  .data.data[0].payload.header.channel_header.type == 1 and
  .data.data[0].payload.header.channel_header.channel_id == $channel' "$OUTPUT_DIR/block.json" >/dev/null
jq '.data.data[0].payload.data.config' "$OUTPUT_DIR/block.json" > "$OUTPUT_DIR/live.json"
jq -e --arg msp "$CORE_PEER_LOCALMSPID" '.channel_group.groups.Orderer.groups[$msp] != null' "$OUTPUT_DIR/live.json" >/dev/null
jq -S -c '.' "$OUTPUT_DIR/live.json" > "$OUTPUT_DIR/live-canonical.json"
jq -S -c '.originalConfig' "$INPUT_DIR/plan.json" > "$OUTPUT_DIR/reviewed-canonical.json"
cmp "$OUTPUT_DIR/live-canonical.json" "$OUTPUT_DIR/reviewed-canonical.json" || {
  echo "Reviewed Raft plan is stale; fetch live config and create a new plan" >&2
  exit 1
}
outcome=alreadyApplied
if [ "$(jq -r '.action' "$INPUT_DIR/plan.json")" != AlreadyApplied ]; then
  jq '.updatedConfig' "$INPUT_DIR/plan.json" > "$OUTPUT_DIR/updated.json"
  configtxlator proto_encode --input "$OUTPUT_DIR/live.json" --type common.Config --output "$OUTPUT_DIR/original.pb"
  configtxlator proto_encode --input "$OUTPUT_DIR/updated.json" --type common.Config --output "$OUTPUT_DIR/updated.pb"
  configtxlator compute_update --channel_id "$CHANNEL_ID" --original "$OUTPUT_DIR/original.pb" --updated "$OUTPUT_DIR/updated.pb" --output "$OUTPUT_DIR/update.pb"
  configtxlator proto_decode --input "$OUTPUT_DIR/update.pb" --type common.ConfigUpdate --output "$OUTPUT_DIR/update.json"
  jq --arg channel "$CHANNEL_ID" '{payload:{header:{channel_header:{channel_id:$channel,type:2}},data:{config_update:.}}}' \
    "$OUTPUT_DIR/update.json" > "$OUTPUT_DIR/envelope.json"
  configtxlator proto_encode --input "$OUTPUT_DIR/envelope.json" --type common.Envelope --output "$OUTPUT_DIR/config-update.pb"
  outcome=staged
fi
jq --arg outcome "$outcome" '. + {outcome:$outcome}' "$INPUT_DIR/request.json" > "$OUTPUT_DIR/raft-update.json"
chmod 0644 "$OUTPUT_DIR"/*
`, channel, adminMSP, channelOutputDir, raftStagingInputDir, orderer)
}

func publishRaftStagingScript() string {
	return `set -eu
set -- kubectl -n "$POD_NAMESPACE" create configmap "$FABRICOPS_RAFT_RESULT" \
  --from-file=raft-update.json=` + channelOutputDir + `/raft-update.json \
  --from-file=channel-block.pb=` + channelOutputDir + `/channel-block.pb
if [ -f ` + channelOutputDir + `/config-update.pb ]; then
  set -- "$@" --from-file=config-update.pb=` + channelOutputDir + `/config-update.pb
fi
"$@" --dry-run=client -o yaml | kubectl -n "$POD_NAMESPACE" apply -f -
kubectl -n "$POD_NAMESPACE" label configmap "$FABRICOPS_RAFT_RESULT" \
  fabricops.io/component=channel fabricops.io/channel="$FABRICOPS_CHANNEL" --overwrite
`
}
