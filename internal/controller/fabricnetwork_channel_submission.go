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
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

const channelSubmissionDir = channelWorkDir + "/submission"

// Keep the signed handoff and staged proposal together in an immutable input
// snapshot. A corrected handoff gets a different Job without replacing a Job spec.
func (r *FabricNetworkReconciler) reconcileSignedChannelUpdate(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	adminOrg fabricopsv1alpha1.Org,
	adminPeer peerInstance,
	orderer ordererInstance,
	ref *fabricopsv1alpha1.ChannelArtifactKeyRef,
	baseJob *batchv1.Job,
	stagedConfigMapName, resultKey, envelopeKey, outputFile, mspID string,
	present bool,
) (string, string, error) {
	raw, ready, message, err := r.channelArtifactBytes(ctx, net.Namespace, ref)
	if err != nil || !ready {
		return "", message, err
	}
	var staged corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKey{Namespace: baseJob.Namespace, Name: stagedConfigMapName}, &staged); err != nil {
		return "", "", err
	}
	unsigned := staged.BinaryData[envelopeKey]
	if len(unsigned) == 0 {
		return "", "Waiting for staged config update envelope", nil
	}
	digest := sha256.New()
	_, _ = digest.Write([]byte(baseJob.Name))
	_, _ = digest.Write(unsigned)
	_, _ = digest.Write(raw)
	suffix := fmt.Sprintf("-%x", digest.Sum(nil)[:8])
	name := sanitizeName(baseJob.Name[:min(len(baseJob.Name), 32)] + "-submit" + suffix)
	input := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: baseJob.Namespace,
			Labels: channelLabels(net, adminOrg, channel.Name), Annotations: resourceAnnotations(net, adminOrg),
		},
		BinaryData: map[string][]byte{"staged.pb": unsigned, "signed.pb": raw},
		Data:       map[string]string{"result.json": staged.Data[resultKey]},
	}
	if err := r.ensureConfigMap(ctx, input); err != nil {
		return "", "", err
	}
	job := baseJob.DeepCopy()
	job.Name = name
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: "signed-update", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: name},
		}},
	})
	container := &job.Spec.Template.Spec.InitContainers[0]
	container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
		Name: "signed-update", MountPath: channelSubmissionDir, ReadOnly: true,
	})
	container.Command = []string{"sh", "-ec", signedChannelUpdateScript(
		channel.Name, adminOrg.Organization.MSPName, mspID, peerAddress(adminPeer),
		ordererClientAddress(orderer), channelOrgMSPPath(adminOrg), channelOrdererAdminTLSPath(adminOrg),
		channelOrdererTLSPath(orderer.name), outputFile, present,
	)}
	resultName := sanitizeName(name + "-result")
	var existingResult corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKey{Namespace: job.Namespace, Name: resultName}, &existingResult); err == nil {
		return resultName, "Waiting for signed channel update evidence", nil
	} else if !apierrors.IsNotFound(err) {
		return "", "", err
	}
	for i := range job.Spec.Template.Spec.Containers[0].Env {
		env := &job.Spec.Template.Spec.Containers[0].Env[i]
		if env.Name == envExternalOrgResultConfigMap || env.Name == envLocalOrgResultConfigMap {
			env.Value = resultName
		}
	}
	if err := r.ensureJob(ctx, job); err != nil {
		return "", "", err
	}
	var actual batchv1.Job
	if err := r.Get(ctx, client.ObjectKeyFromObject(job), &actual); err != nil {
		return "", "", err
	}
	if jobFailed(actual) {
		return resultName, "Signed channel update Job failed: " + name, nil
	}
	return resultName, "Waiting for signed channel update Job " + name, nil
}

func signedChannelUpdateScript(channel, adminMSP, joiningMSP, peer, orderer, mspPath, adminTLS, ordererTLS, outputFile string, present bool) string {
	return fmt.Sprintf(`set -eu
CHANNEL_ID=%q
MSP_ID=%q
DESIRED_PRESENT=%t
ORDERER_ADDRESS=%q
OUTPUT_DIR=%q
INPUT_DIR=%q
export CORE_PEER_LOCALMSPID=%q
export CORE_PEER_ADDRESS=%q
export CORE_PEER_MSPCONFIGPATH=%q
export CORE_PEER_TLS_ENABLED=true
export CORE_PEER_TLS_CERT_FILE=%q/client.crt
export CORE_PEER_TLS_KEY_FILE=%q/client.key
export CORE_PEER_TLS_ROOTCERT_FILE=%q/ca.crt
ORDERER_CA=%q/ca.crt
mkdir -p "$OUTPUT_DIR"
configtxlator proto_decode --input "$INPUT_DIR/staged.pb" --type common.Envelope --output "$OUTPUT_DIR/staged.json"
configtxlator proto_decode --input "$INPUT_DIR/signed.pb" --type common.Envelope --output "$OUTPUT_DIR/signed.json"
jq -e --arg channel "$CHANNEL_ID" '
  .payload.header.channel_header.type == 2 and
  .payload.header.channel_header.channel_id == $channel and
  .payload.data.config_update.channel_id == $channel and
  (.payload.data.signatures | length) > 0' "$OUTPUT_DIR/signed.json" >/dev/null
jq -S -c '.payload.data.config_update' "$OUTPUT_DIR/staged.json" > "$OUTPUT_DIR/staged-update.json"
jq -S -c '.payload.data.config_update' "$OUTPUT_DIR/signed.json" > "$OUTPUT_DIR/signed-update.json"
cmp "$OUTPUT_DIR/staged-update.json" "$OUTPUT_DIR/signed-update.json"
cp "$INPUT_DIR/signed.pb" "$OUTPUT_DIR/%s"

fetch_config() {
  peer channel fetch config "$OUTPUT_DIR/channel-block.pb" -c "$CHANNEL_ID" -o "$ORDERER_ADDRESS" --tls --cafile "$ORDERER_CA"
  configtxlator proto_decode --input "$OUTPUT_DIR/channel-block.pb" --type common.Block --output "$OUTPUT_DIR/block.json"
}
membership_matches() {
  jq -e --arg msp "$MSP_ID" --argjson present "$DESIRED_PRESENT" \
    '(.data.data[0].payload.data.config.channel_group.groups.Application.groups[$msp] != null) == $present' \
    "$OUTPUT_DIR/block.json" >/dev/null
}
fetch_config
outcome=alreadyApplied
if ! membership_matches; then
  peer channel update -c "$CHANNEL_ID" -o "$ORDERER_ADDRESS" -f "$OUTPUT_DIR/%s" --tls --cafile "$ORDERER_CA"
  outcome=submitted
  n=0
  until fetch_config && membership_matches; do
    n=$((n + 1))
    [ "$n" -lt 30 ] || { echo "Channel membership update has not committed" >&2; exit 1; }
    sleep 5
  done
fi
jq --arg outcome "$outcome" '.outcome = $outcome' "$INPUT_DIR/result.json" > "$OUTPUT_DIR/local-org.json"
cp "$OUTPUT_DIR/local-org.json" "$OUTPUT_DIR/external-org.json"
`, channel, joiningMSP, present, orderer, channelOutputDir, channelSubmissionDir,
		adminMSP, peer, mspPath, adminTLS, adminTLS, adminTLS, ordererTLS, outputFile, outputFile)
}
