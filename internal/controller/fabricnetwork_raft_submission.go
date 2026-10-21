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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
	"github.com/LF-Decentralized-Trust-labs/FabricOps/internal/channelconfig"
)

type raftSubmissionNode struct {
	Name         string `json:"name"`
	AdminAddress string `json:"adminAddress"`
	Operations   string `json:"operations"`
	CAFile       string `json:"caFile"`
	Current      bool   `json:"current"`
	Desired      bool   `json:"desired"`
}

func (r *FabricNetworkReconciler) raftSubmissionNodes(ctx context.Context, net *fabricopsv1alpha1.FabricNetwork,
	op *fabricopsv1alpha1.ChannelRaftUpdate, current, desired []channelconfig.RaftConsenter,
) ([]raftSubmissionNode, error) {
	nodes := []raftSubmissionNode{}
	byName := map[string]int{}
	for setIndex, set := range [][]channelconfig.RaftConsenter{current, desired} {
		for _, consenter := range set {
			found := false
			for _, instance := range desiredOrdererInstances(net) {
				if instance.org.Organization.Name != op.AdminOrg || consenter.Host != channelOrdererHost(instance) ||
					consenter.Port != uint32(channelOrdererPort(instance)) {
					continue
				}
				var secret corev1.Secret
				if err := r.Get(ctx, client.ObjectKey{Namespace: instance.namespace,
					Name: identitySecretName(instance.name, secretKindTLS)}, &secret); err != nil {
					return nil, fmt.Errorf("read %s TLS material: %w", instance.name, err)
				}
				if !bytes.Equal(secret.Data[tlsServerCertKey], consenter.ServerTLSCert) ||
					!bytes.Equal(secret.Data[tlsServerCertKey], consenter.ClientTLSCert) || len(secret.Data[tlsCACertKey]) == 0 {
					return nil, fmt.Errorf("consenter %s does not match its managed TLS certificate", instance.name)
				}
				cert, err := parsePEMCertificate(consenter.ServerTLSCert)
				if err != nil || time.Now().Before(cert.NotBefore) || !time.Now().Before(cert.NotAfter) {
					return nil, fmt.Errorf("consenter %s TLS certificate is invalid or outside its validity period", instance.name)
				}
				index, exists := byName[instance.name]
				if !exists {
					index, byName[instance.name] = len(nodes), len(nodes)
					nodes = append(nodes, raftSubmissionNode{Name: instance.name, AdminAddress: ordererAdminAddress(instance),
						Operations: "http://" + serviceDNS(operationsServiceName(instance.name), instance.namespace, ordererOpsPort),
						CAFile:     "/raft-ca/" + instance.name + "/ca.crt"})
				}
				if setIndex == 0 {
					nodes[index].Current = true
				} else {
					nodes[index].Desired = true
				}
				found = true
				break
			}
			if !found {
				return nil, fmt.Errorf("automatic Raft submission requires every consenter to be managed by adminOrg %s", op.AdminOrg)
			}
		}
	}
	sourceRetained := false
	for _, node := range nodes {
		if node.Name == op.Orderer.Name && node.Current && node.Desired {
			sourceRetained = true
		}
	}
	if !sourceRetained {
		return nil, fmt.Errorf("select a current consenter that remains in the desired set as the config-fetch source")
	}
	return nodes, nil
}

func (r *FabricNetworkReconciler) reconcileRaftSubmission(ctx context.Context, net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel, admin fabricopsv1alpha1.Org, plan *channelconfig.RaftUpdatePlan,
	status *fabricopsv1alpha1.ChannelRaftUpdateStatus, staged *corev1.ConfigMap, expected raftUpdateReceipt,
) (*fabricopsv1alpha1.ChannelRaftUpdateStatus, error) {
	signed, ready, message, err := r.channelArtifactBytes(ctx, net.Namespace, channel.RaftUpdate.SignedUpdateRef)
	if err != nil || !ready {
		status.Message = "Waiting for signed Raft envelope: " + message
		return status, err
	}
	unsigned := staged.BinaryData["config-update.pb"]
	if len(unsigned) == 0 {
		unsigned = []byte(staged.Data["config-update.pb"])
	}
	canonical, err := json.Marshal(plan)
	if err != nil {
		return status, err
	}
	signedDigest := sha256.Sum256(signed)
	expected.SignedHash = hex.EncodeToString(signedDigest[:])
	digest := sha256.Sum256(append([]byte(status.PlanHash), signed...))
	name := fmt.Sprintf("%s-raft-submit-%x", channel.Name[:min(len(channel.Name), 16)], digest[:8])
	status.UpdateJobName, status.ArtifactConfigMapName = name, name+"-result"
	var result corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKey{Namespace: status.Namespace, Name: status.ArtifactConfigMapName}, &result); err == nil {
		var receipt raftUpdateReceipt
		decodeErr := json.Unmarshal([]byte(result.Data["raft-update.json"]), &receipt)
		outcome := receipt.Outcome
		receipt.Outcome = ""
		block := append([]byte(result.Data["channel-block.pb"]), result.BinaryData["channel-block.pb"]...)
		envelope := append([]byte(result.Data["config-update.pb"]), result.BinaryData["config-update.pb"]...)
		if decodeErr != nil || receipt != expected || len(block) == 0 || !bytes.Equal(envelope, signed) ||
			(outcome != "submitted" && outcome != "alreadyApplied") {
			status.Phase, status.Message = fabricopsv1alpha1.ChannelConfigUpdateFailed, "Signed Raft evidence does not match the operation"
		} else {
			status.Phase, status.Ready, status.Message = fabricopsv1alpha1.ChannelConfigUpdateSubmitted, true, ""
			if outcome == "alreadyApplied" {
				status.Phase = fabricopsv1alpha1.ChannelConfigUpdateAlreadyApplied
			}
		}
		return status, nil
	} else if !apierrors.IsNotFound(err) {
		return status, err
	}
	current, err := channelconfig.RaftConsentersFromConfig(plan.OriginalConfig)
	if err != nil {
		return status, err
	}
	desired, err := channelconfig.RaftConsentersFromConfig(plan.UpdatedConfig)
	if err != nil {
		return status, err
	}
	nodes, err := r.raftSubmissionNodes(ctx, net, channel.RaftUpdate, current, desired)
	if err != nil {
		status.Phase, status.Message = fabricopsv1alpha1.ChannelConfigUpdateFailed, "Raft submission blocked: "+err.Error()
		return status, nil
	}
	nodesJSON, err := json.Marshal(nodes)
	if err != nil {
		return status, err
	}
	receiptJSON, err := json.Marshal(expected)
	if err != nil {
		return status, err
	}
	immutable := true
	input := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name + "-input", Namespace: status.Namespace,
		Labels: channelLabels(net, admin, channel.Name), Annotations: resourceAnnotations(net, admin)}, Immutable: &immutable,
		Data:       map[string]string{"plan.json": string(canonical), "request.json": string(receiptJSON), "nodes.json": string(nodesJSON)},
		BinaryData: map[string][]byte{"staged.pb": unsigned, "signed.pb": signed}}
	if err := r.ensureChannelRBAC(ctx, net, admin, channel.Name, status.Namespace); err != nil {
		return status, err
	}
	if err := r.ensureConfigMap(ctx, input); err != nil {
		return status, err
	}
	orderer := ordererInstance{org: admin, name: channel.RaftUpdate.Orderer.Name, namespace: status.Namespace}
	job := buildRaftStagingJob(net, channel, admin, orderer, name)
	job.Spec.Template.Spec.InitContainers[0].Command = []string{"sh", "-ec",
		raftSubmissionScript(channel.Name, admin.Organization.MSPName, ordererClientAddress(orderer))}
	for _, node := range nodes {
		volumeName := "ca-" + node.Name
		job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{Name: volumeName,
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName: identitySecretName(node.Name, secretKindTLS),
				Items:      []corev1.KeyToPath{{Key: tlsCACertKey, Path: "ca.crt"}}}}})
		container := &job.Spec.Template.Spec.InitContainers[0]
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name: volumeName, MountPath: "/raft-ca/" + node.Name, ReadOnly: true})
	}
	if err := r.ensureJob(ctx, job); err != nil {
		return status, err
	}
	var actual batchv1.Job
	if err := r.Get(ctx, client.ObjectKeyFromObject(job), &actual); err != nil {
		return status, err
	}
	status.Message = "Waiting for Raft submission health/catch-up checks and committed membership: " + name
	if jobFailed(actual) || jobSucceeded(actual) {
		status.Phase = fabricopsv1alpha1.ChannelConfigUpdateFailed
		status.Message = "Raft submission Job failed or completed without durable evidence: " + name
	}
	return status, nil
}

func raftSubmissionScript(channel, adminMSP, orderer string) string {
	return fmt.Sprintf(`set -eu
CHANNEL_ID=%q
export CORE_PEER_LOCALMSPID=%q
export CORE_PEER_MSPCONFIGPATH=/msp
export CORE_PEER_TLS_ENABLED=true
export CORE_PEER_TLS_CERT_FILE=/admin-tls/client.crt
export CORE_PEER_TLS_KEY_FILE=/admin-tls/client.key
ORDERER_ADDRESS=%q
OUTPUT_DIR=%q
INPUT_DIR=%q
mkdir -p "$OUTPUT_DIR"
configtxlator proto_decode --input "$INPUT_DIR/staged.pb" --type common.Envelope --output "$OUTPUT_DIR/staged.json"
configtxlator proto_decode --input "$INPUT_DIR/signed.pb" --type common.Envelope --output "$OUTPUT_DIR/signed.json"
jq -e --arg channel "$CHANNEL_ID" '
  .payload.header.channel_header.type == 2 and .payload.header.channel_header.channel_id == $channel and
  .payload.data.config_update.channel_id == $channel and (.payload.data.signatures | length) > 0' "$OUTPUT_DIR/signed.json" >/dev/null
jq -S -c '.payload.data.config_update' "$OUTPUT_DIR/staged.json" > "$OUTPUT_DIR/staged-update.json"
jq -S -c '.payload.data.config_update' "$OUTPUT_DIR/signed.json" > "$OUTPUT_DIR/signed-update.json"
cmp "$OUTPUT_DIR/staged-update.json" "$OUTPUT_DIR/signed-update.json"
jq '.originalConfig' "$INPUT_DIR/plan.json" > "$OUTPUT_DIR/reviewed-original.json"
jq '.updatedConfig' "$INPUT_DIR/plan.json" > "$OUTPUT_DIR/reviewed-updated.json"
configtxlator proto_encode --input "$OUTPUT_DIR/reviewed-original.json" --type common.Config --output "$OUTPUT_DIR/reviewed-original.pb"
configtxlator proto_encode --input "$OUTPUT_DIR/reviewed-updated.json" --type common.Config --output "$OUTPUT_DIR/reviewed-updated.pb"
configtxlator compute_update --channel_id "$CHANNEL_ID" --original "$OUTPUT_DIR/reviewed-original.pb" --updated "$OUTPUT_DIR/reviewed-updated.pb" --output "$OUTPUT_DIR/expected-update.pb"
configtxlator proto_decode --input "$OUTPUT_DIR/expected-update.pb" --type common.ConfigUpdate --output "$OUTPUT_DIR/expected-update.json"
jq -S -c '.' "$OUTPUT_DIR/expected-update.json" > "$OUTPUT_DIR/expected-canonical.json"
cmp "$OUTPUT_DIR/staged-update.json" "$OUTPUT_DIR/expected-canonical.json"
cp "$INPUT_DIR/signed.pb" "$OUTPUT_DIR/config-update.pb"
jq -r '.[] | [.name,.adminAddress,.operations,.caFile,.current,.desired] | @tsv' "$INPUT_DIR/nodes.json" > "$OUTPUT_DIR/nodes.tsv"
jq -S -c '.updatedConfig.channel_group.groups.Orderer.values.ConsensusType.value.metadata.consenters' \
  "$INPUT_DIR/plan.json" > "$OUTPUT_DIR/desired.json"
fetch_config() {
  peer channel fetch config "$OUTPUT_DIR/channel-block.pb" -c "$CHANNEL_ID" -o "$ORDERER_ADDRESS" --tls --cafile /orderer-ca/ca.crt || return 1
  configtxlator proto_decode --input "$OUTPUT_DIR/channel-block.pb" --type common.Block --output "$OUTPUT_DIR/block.json" || return 1
  jq -e --arg channel "$CHANNEL_ID" '.data.data[0].payload.header.channel_header.channel_id == $channel and
    .data.data[0].payload.header.channel_header.type == 1' "$OUTPUT_DIR/block.json" >/dev/null || return 1
  jq '.data.data[0].payload.data.config' "$OUTPUT_DIR/block.json" > "$OUTPUT_DIR/live.json"
}
membership_matches() {
  jq -e '.channel_group.groups.Orderer.values.ConsensusType.value | .type == "etcdraft" and .state == "STATE_NORMAL"' "$OUTPUT_DIR/live.json" >/dev/null &&
  jq -S -c '.channel_group.groups.Orderer.values.ConsensusType.value.metadata.consenters' "$OUTPUT_DIR/live.json" > "$OUTPUT_DIR/current.json" &&
  cmp -s "$OUTPUT_DIR/current.json" "$OUTPUT_DIR/desired.json" &&
  jq -e -s '.[0].sequence as $a | .[1].configSequence as $b |
    ($a|length) > ($b|length) or (($a|length) == ($b|length) and $a > $b)' \
    "$OUTPUT_DIR/live.json" "$INPUT_DIR/plan.json" >/dev/null
}
channel_info() {
  osnadmin channel list --no-status --channelID "$CHANNEL_ID" -o "$addr" --ca-file "$ca" \
    --client-cert /admin-tls/client.crt --client-key /admin-tls/client.key
}
check_nodes() {
  mode="$1"
  while IFS="$(printf '\t')" read -r name addr ops ca current desired; do
    [ "$mode" != after ] || [ "$desired" = true ] || continue
    curl -fsS --max-time 10 "$ops/healthz" > "$OUTPUT_DIR/health.json" || {
      echo "Orderer $name health endpoint is unavailable" >&2; return 1;
    }
    jq -e '.status == "OK"' "$OUTPUT_DIR/health.json" >/dev/null || {
      echo "Orderer $name health check failed" >&2; return 1;
    }
    channel_info > "$OUTPUT_DIR/info.json" || return 1
    relation=consenter
    if [ "$mode" = before ] && [ "$current" != true ]; then relation=follower; fi
    jq -e --arg channel "$CHANNEL_ID" --arg relation "$relation" --argjson height "$reference_height" '
      .name == $channel and .status == "active" and .consensusRelation == $relation and
      (.height | type) == "number" and .height >= $height' "$OUTPUT_DIR/info.json" >/dev/null || {
        echo "Orderer $name is unhealthy, has the wrong consensus role, or has not caught up" >&2
        return 1
      }
  done < "$OUTPUT_DIR/nodes.tsv"
}
fetch_config
outcome=alreadyApplied
if ! membership_matches; then
  jq -S -c '.' "$OUTPUT_DIR/live.json" > "$OUTPUT_DIR/live-canonical.json"
  jq -S -c '.originalConfig' "$INPUT_DIR/plan.json" > "$OUTPUT_DIR/reviewed-canonical.json"
  cmp "$OUTPUT_DIR/live-canonical.json" "$OUTPUT_DIR/reviewed-canonical.json" || {
    echo "Signed Raft proposal is stale" >&2; exit 1;
  }
  peer channel fetch newest "$OUTPUT_DIR/tip.pb" -c "$CHANNEL_ID" -o "$ORDERER_ADDRESS" --tls --cafile /orderer-ca/ca.crt
  configtxlator proto_decode --input "$OUTPUT_DIR/tip.pb" --type common.Block --output "$OUTPUT_DIR/tip.json"
  reference_height=$(jq '.header.number | tonumber + 1' "$OUTPUT_DIR/tip.json")
  check_nodes before
  fetch_config
  jq -S -c '.' "$OUTPUT_DIR/live.json" > "$OUTPUT_DIR/live-canonical.json"
  cmp "$OUTPUT_DIR/live-canonical.json" "$OUTPUT_DIR/reviewed-canonical.json" || {
    echo "Config changed during Raft preflight" >&2; exit 1;
  }
  peer channel update -c "$CHANNEL_ID" -o "$ORDERER_ADDRESS" -f "$OUTPUT_DIR/config-update.pb" --tls --cafile /orderer-ca/ca.crt
  outcome=submitted
fi
n=0
until fetch_config && membership_matches; do
  n=$((n + 1)); [ "$n" -lt 30 ] || { echo "Raft membership did not commit" >&2; exit 1; }
  sleep 5
done
reference_height=$(jq '.header.number | tonumber + 1' "$OUTPUT_DIR/block.json")
n=0
until check_nodes after; do
  n=$((n + 1)); [ "$n" -lt 30 ] || { echo "Desired consenters did not become active" >&2; exit 1; }
  sleep 5
done
jq --arg outcome "$outcome" '. + {outcome:$outcome}' "$INPUT_DIR/request.json" > "$OUTPUT_DIR/raft-update.json"
chmod 0644 "$OUTPUT_DIR"/*
`, channel, adminMSP, orderer, channelOutputDir, raftStagingInputDir)
}
