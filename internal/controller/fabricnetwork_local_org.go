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
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

const (
	addLocalOrgContainer         = "add-local-org"
	publishLocalOrgContainer     = "publish-local-org-update"
	localOrgAddResultKey         = "local-org.json"
	localOrgAddResultFile        = "local-org.json"
	localOrgAddUpdateArtifactKey = "config-update.pb"
	localOrgAddBlockArtifactKey  = "channel-block.pb"
	localOrgAddUpdateFile        = "config-update.pb"
	localOrgAddBlockFile         = "channel-block.pb"
	localOrgAddOutcomeSubmitted  = "submitted"
	localOrgAddOutcomeApplied    = "alreadyApplied"
	localOrgAddOutcomeStaged     = "staged"
	envLocalOrgResultConfigMap   = "FABRICOPS_LOCAL_ORG_RESULT_CONFIGMAP"
	envLocalOrgResultKey         = "FABRICOPS_LOCAL_ORG_RESULT_KEY"
	envLocalOrgResultFile        = "FABRICOPS_LOCAL_ORG_RESULT_FILE"
	envLocalOrgUpdateKey         = "FABRICOPS_LOCAL_ORG_UPDATE_KEY"
	envLocalOrgUpdateFile        = "FABRICOPS_LOCAL_ORG_UPDATE_FILE"
	envLocalOrgBlockKey          = "FABRICOPS_LOCAL_ORG_BLOCK_KEY"
	envLocalOrgBlockFile         = "FABRICOPS_LOCAL_ORG_BLOCK_FILE"
	envLocalOrgResultChannel     = "FABRICOPS_LOCAL_ORG_RESULT_CHANNEL"
	envLocalOrgResultOrg         = "FABRICOPS_LOCAL_ORG_RESULT_ORG"
)

type localOrgAddEvidence struct {
	Channel      string `json:"channel"`
	MSPID        string `json:"mspID"`
	Outcome      string `json:"outcome"`
	ChannelBlock []byte `json:"-"`
}

func (r *FabricNetworkReconciler) reconcileLocalOrgAdd(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	adminOrg fabricopsv1alpha1.Org,
	adminPeer peerInstance,
	orderer ordererInstance,
	joiningOrg fabricopsv1alpha1.Org,
) (bool, string, error) {
	namespace := orgNamespaceName(net, adminOrg)
	evidence, evidenceMessage, err := r.localOrgAddEvidence(ctx, namespace, channel.Name, joiningOrg)
	if err != nil {
		return false, "", err
	}
	if evidence != nil {
		if evidence.Outcome == localOrgAddOutcomeStaged {
			message := fmt.Sprintf("%s: Waiting for channel config signatures; envelope is in ConfigMap %s/%s", joiningOrg.Organization.Name, namespace, channelLocalOrgAddResultConfigMapName(channel.Name, joiningOrg))
			var ref *fabricopsv1alpha1.ChannelArtifactKeyRef
			for _, org := range channel.Orgs {
				if org.Name == joiningOrg.Organization.Name {
					ref = org.SignedUpdateRef
				}
			}
			if ref == nil {
				return false, message, nil
			}
			baseJob := buildLocalOrgAddJob(net, channel, adminOrg, namespace, adminPeer, orderer, joiningOrg)
			resultName, message, err := r.reconcileSignedChannelUpdate(
				ctx, net, channel, adminOrg, adminPeer, orderer, ref, baseJob,
				channelLocalOrgAddResultConfigMapName(channel.Name, joiningOrg), localOrgAddResultKey,
				localOrgAddUpdateArtifactKey, localOrgAddUpdateFile, joiningOrg.Organization.MSPName, true,
			)
			if err != nil || resultName == "" {
				return false, message, err
			}
			submitted, _, err := r.localOrgAddEvidenceNamed(ctx, namespace, channel.Name, joiningOrg, resultName)
			if err != nil || submitted == nil {
				return false, message, err
			}
			if submitted.Outcome != localOrgAddOutcomeSubmitted && submitted.Outcome != localOrgAddOutcomeApplied {
				return false, message, nil
			}
		}
		return true, "", nil
	}
	if evidenceMessage != "" {
		return false, evidenceMessage, nil
	}

	if err := r.ensureChannelRBAC(ctx, net, adminOrg, channel.Name, namespace); err != nil {
		return false, "", err
	}
	if err := r.ensureChannelCryptoSecrets(ctx, net, channel, adminOrg, namespace); err != nil {
		return false, "", err
	}
	configMap, err := buildChannelConfigMap(net, channel, adminOrg, namespace)
	if err != nil {
		return false, "", err
	}
	if err := r.ensureConfigMap(ctx, configMap); err != nil {
		return false, "", err
	}
	if err := r.ensureJob(ctx, buildLocalOrgAddJob(net, channel, adminOrg, namespace, adminPeer, orderer, joiningOrg)); err != nil {
		return false, "", err
	}

	return r.localOrgAddReadiness(ctx, namespace, channel.Name, joiningOrg)
}

func (r *FabricNetworkReconciler) reconcileLocalOrgAdditions(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	status *fabricopsv1alpha1.ChannelStatus,
) (string, error) {
	orderers := desiredOrdererInstances(net)
	if len(orderers) == 0 {
		return "", nil
	}
	orgs := orgsByName(net)
	var adminOrg fabricopsv1alpha1.Org
	var adminPeer peerInstance
	candidates := make([]int, 0)

	for i := range status.Orgs {
		orgStatus := &status.Orgs[i]
		org, ok := orgs[orgStatus.Name]
		if !ok {
			continue
		}
		namespace := orgNamespaceName(net, org)
		joinedPeerFound := false
		for _, peerName := range orgStatus.PeerNames {
			peer := peerInstance{org: org, name: peerName, namespace: namespace}
			joined, _, err := r.anchorPeerUpdateResultReadiness(ctx, namespace, channel.Name, org, peer)
			if err != nil {
				return "", err
			}
			if !joined {
				continue
			}
			joinedPeerFound = true
			if adminPeer.name == "" {
				adminOrg = org
				adminPeer = peer
			}
			break
		}
		if !joinedPeerFound {
			candidates = append(candidates, i)
		}
	}

	if adminPeer.name == "" || len(candidates) == 0 {
		return "", nil
	}

	for _, index := range candidates {
		orgStatus := &status.Orgs[index]
		joiningOrg, ok := orgs[orgStatus.Name]
		if !ok {
			continue
		}
		ready, message, err := r.reconcileLocalOrgAdd(
			ctx,
			net,
			channel,
			adminOrg,
			adminPeer,
			orderers[0],
			joiningOrg,
		)
		if err != nil {
			return "", err
		}
		if !ready {
			orgStatus.Message = message
			orgStatus.Ready = false
			return message, nil
		}
	}

	return "", nil
}

func (r *FabricNetworkReconciler) localOrgAddReadiness(
	ctx context.Context,
	namespace string,
	channelName string,
	joiningOrg fabricopsv1alpha1.Org,
) (bool, string, error) {
	var job batchv1.Job
	err := r.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      channelLocalOrgAddJobName(channelName, joiningOrg),
	}, &job)
	if apierrors.IsNotFound(err) {
		return false, "Waiting for local org channel update Job", nil
	}
	if err != nil {
		return false, "", err
	}
	if jobFailed(job) {
		return false, fmt.Sprintf("%s: local org channel update Job failed", joiningOrg.Organization.Name), nil
	}
	if jobSucceeded(job) {
		return false, fmt.Sprintf("Waiting for %s local org update result ConfigMap", joiningOrg.Organization.Name), nil
	}

	return false, fmt.Sprintf("Waiting for %s local org channel update Job", joiningOrg.Organization.Name), nil
}

func (r *FabricNetworkReconciler) localOrgAddEvidence(
	ctx context.Context,
	namespace string,
	channelName string,
	joiningOrg fabricopsv1alpha1.Org,
) (*localOrgAddEvidence, string, error) {
	return r.localOrgAddEvidenceNamed(ctx, namespace, channelName, joiningOrg, channelLocalOrgAddResultConfigMapName(channelName, joiningOrg))
}

func (r *FabricNetworkReconciler) localOrgAddEvidenceNamed(
	ctx context.Context, namespace, channelName string, joiningOrg fabricopsv1alpha1.Org, name string,
) (*localOrgAddEvidence, string, error) {
	var result corev1.ConfigMap
	err := r.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      name,
	}, &result)
	if apierrors.IsNotFound(err) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}

	raw := strings.TrimSpace(result.Data[localOrgAddResultKey])
	if raw == "" {
		return nil, fmt.Sprintf("Waiting for %s local org update result ConfigMap data", joiningOrg.Organization.Name), nil
	}
	evidence := &localOrgAddEvidence{}
	if err := json.Unmarshal([]byte(jsonPayload(raw)), evidence); err != nil {
		return nil, fmt.Sprintf("Waiting for %s local org update result for channel %s", joiningOrg.Organization.Name, channelName), nil
	}
	if evidence.Channel != channelName || evidence.MSPID != joiningOrg.Organization.MSPName {
		return nil, fmt.Sprintf("Waiting for %s local org update result for channel %s", joiningOrg.Organization.Name, channelName), nil
	}
	if evidence.Outcome != localOrgAddOutcomeSubmitted && evidence.Outcome != localOrgAddOutcomeApplied && evidence.Outcome != localOrgAddOutcomeStaged {
		return nil, fmt.Sprintf("Waiting for %s local org update result", joiningOrg.Organization.Name), nil
	}
	evidence.ChannelBlock = result.BinaryData[localOrgAddBlockArtifactKey]
	if len(evidence.ChannelBlock) == 0 {
		return nil, fmt.Sprintf("Waiting for %s refreshed channel block", joiningOrg.Organization.Name), nil
	}

	return evidence, "", nil
}

func buildLocalOrgAddJob(
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	adminOrg fabricopsv1alpha1.Org,
	namespace string,
	adminPeer peerInstance,
	orderer ordererInstance,
	joiningOrg fabricopsv1alpha1.Org,
) *batchv1.Job {
	labels := channelLabels(net, adminOrg, channel.Name)
	labels[labelInstance] = sanitizeName(joiningOrg.Organization.Name)
	labels[labelWorkload] = sanitizeName(joiningOrg.Organization.Name)
	annotations := resourceAnnotations(net, adminOrg)
	backoffLimit := int32(4)
	adminMSPVolumeName := channelOrgMSPVolumeName(adminOrg)
	adminTLSVolumeName := channelOrdererAdminTLSVolumeName(adminOrg)
	ordererTLSVolumeName := channelOrdererTLSVolumeName(orderer.name)
	volumes := []corev1.Volume{
		{
			Name:         channelOutputVolumeName,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
		{
			Name: channelConfigVolumeName,
			VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: channelConfigMapName(channel.Name)},
			}},
		},
		{
			Name: adminMSPVolumeName,
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName:  identitySecretName(adminIdentityName(adminOrg), secretKindMSP),
				Items:       mspSecretItems(net.Spec.Global.TLS),
				DefaultMode: secretVolumeDefaultMode(),
			}},
		},
		{
			Name: adminTLSVolumeName,
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName:  identitySecretName(adminIdentityName(adminOrg), secretKindTLS),
				Items:       adminTLSSecretItems(),
				DefaultMode: secretVolumeDefaultMode(),
			}},
		},
		{
			Name: ordererTLSVolumeName,
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName:  channelOrdererTLSSecretName(channel.Name, orderer.name),
				Items:       tlsSecretItems(),
				DefaultMode: secretVolumeDefaultMode(),
			}},
		},
	}
	volumeMounts := []corev1.VolumeMount{
		{Name: channelOutputVolumeName, MountPath: channelOutputDir},
		{Name: channelConfigVolumeName, MountPath: channelConfigDir, ReadOnly: true},
		{Name: adminMSPVolumeName, MountPath: channelOrgMSPPath(adminOrg), ReadOnly: true},
		{Name: adminTLSVolumeName, MountPath: channelOrdererAdminTLSPath(adminOrg), ReadOnly: true},
		{Name: ordererTLSVolumeName, MountPath: channelOrdererTLSPath(orderer.name), ReadOnly: true},
	}
	for _, org := range channelConfigOrganizations(net, channel) {
		volumeName := channelOrgMSPVolumeName(org)
		if volumeName == adminMSPVolumeName {
			continue
		}
		volumes = append(volumes, corev1.Volume{
			Name: volumeName,
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName:  channelOrgMSPSecretName(channel.Name, org),
				Items:       mspSecretItems(net.Spec.Global.TLS),
				DefaultMode: secretVolumeDefaultMode(),
			}},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name: volumeName, MountPath: channelOrgMSPPath(org), ReadOnly: true,
		})
	}
	if networkUsesBFTOrderers(net) {
		for _, ordererInstance := range desiredOrdererInstances(net) {
			volumeName := channelOrdererMSPVolumeName(ordererInstance.name)
			volumes = append(volumes, corev1.Volume{
				Name: volumeName,
				VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
					SecretName:  channelOrdererMSPSecretName(channel.Name, ordererInstance.name),
					Items:       mspSecretItems(net.Spec.Global.TLS),
					DefaultMode: secretVolumeDefaultMode(),
				}},
			})
			volumeMounts = append(volumeMounts, corev1.VolumeMount{
				Name: volumeName, MountPath: channelOrdererMSPPath(ordererInstance.name), ReadOnly: true,
			})
		}
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        channelLocalOrgAddJobName(channel.Name, joiningOrg),
			Namespace:   namespace,
			Labels:      labels,
			Annotations: succeededJobCleanupAnnotations(annotations),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: resourceAnnotations(net, adminOrg)},
				Spec: corev1.PodSpec{
					ServiceAccountName: channelServiceAccountName(channel.Name),
					RestartPolicy:      corev1.RestartPolicyNever,
					Volumes:            volumes,
					InitContainers: []corev1.Container{{
						Name:  addLocalOrgContainer,
						Image: fabricToolsImage(net.Spec.Global),
						Command: []string{"sh", "-ec", addLocalOrgScript(
							channel.Name,
							adminOrg.Organization.MSPName,
							joiningOrg.Organization.MSPName,
							peerAddress(adminPeer),
							ordererClientAddress(orderer),
							channelOrgMSPPath(adminOrg),
							channelOrdererAdminTLSPath(adminOrg),
							channelOrdererTLSPath(orderer.name),
						)},
						Resources:    componentResourceRequirements(componentFabricCLI),
						VolumeMounts: volumeMounts,
					}},
					Containers: []corev1.Container{{
						Name:      publishLocalOrgContainer,
						Image:     kubectlImage(),
						Command:   []string{"sh", "-ec", publishLocalOrgAddResultScript()},
						Env:       publishLocalOrgAddResultEnv(channel.Name, joiningOrg),
						Resources: componentResourceRequirements(componentKubectl),
						VolumeMounts: []corev1.VolumeMount{{
							Name: channelOutputVolumeName, MountPath: channelOutputDir,
						}},
					}},
				},
			},
		},
	}
}

func addLocalOrgScript(
	channelName string,
	adminMSPID string,
	joiningMSPID string,
	peerAddress string,
	ordererAddress string,
	mspPath string,
	adminTLSPath string,
	ordererTLSPath string,
) string {
	return fmt.Sprintf(`set -eu

CHANNEL_ID=%q
ADMIN_MSP_ID=%q
JOIN_MSP_ID=%q
ORDERER_ADDRESS=%q
CONFIG_DIR=%q
ADMIN_TLS_DIR=%q
ORDERER_TLS_DIR=%q
OUTPUT_DIR=%q
RESULT_FILE="$OUTPUT_DIR/%s"
APPLICATION_ORG_JSON="$OUTPUT_DIR/application-org.json"
CURRENT_BLOCK_PB="$OUTPUT_DIR/%s"
CONFIG_BLOCK_JSON="$OUTPUT_DIR/config_block.json"
CONFIG_JSON="$OUTPUT_DIR/config.json"
MODIFIED_CONFIG_JSON="$OUTPUT_DIR/modified_config.json"
CONFIG_PB="$OUTPUT_DIR/config.pb"
MODIFIED_CONFIG_PB="$OUTPUT_DIR/modified_config.pb"
CONFIG_UPDATE_PB="$OUTPUT_DIR/%s"
CONFIG_UPDATE_JSON="$OUTPUT_DIR/config_update.json"
ENVELOPE_JSON="$OUTPUT_DIR/config_update_envelope.json"

export CORE_PEER_LOCALMSPID="$ADMIN_MSP_ID"
export CORE_PEER_ADDRESS=%q
export CORE_PEER_MSPCONFIGPATH=%q
export CORE_PEER_TLS_ENABLED=true
export CORE_PEER_TLS_CERT_FILE="$ADMIN_TLS_DIR/client.crt"
export CORE_PEER_TLS_KEY_FILE="$ADMIN_TLS_DIR/client.key"
export CORE_PEER_TLS_ROOTCERT_FILE="$ADMIN_TLS_DIR/ca.crt"

mkdir -p "$OUTPUT_DIR"

write_local_org_result() {
  jq -n --arg channel "$CHANNEL_ID" --arg msp "$JOIN_MSP_ID" --arg outcome "$1" \
    '{"channel":$channel,"mspID":$msp,"outcome":$outcome}' > "$RESULT_FILE"
}

retry() {
  attempts="$1"
  delay="$2"
  shift 2

  n=1
  until "$@"; do
    if [ "$n" -ge "$attempts" ]; then
      return 1
    fi
    echo "Command failed on attempt $n/$attempts. Retrying in ${delay}s: $*"
    n=$((n + 1))
    sleep "$delay"
  done
}

configtxgen -configPath "$CONFIG_DIR" -printOrg "$JOIN_MSP_ID" > "$APPLICATION_ORG_JSON"
# printOrg includes orderer endpoint metadata from Fabric 3 configtx input.
# Application org groups cannot contain the orderer-only Endpoints value.
jq 'del(.values.Endpoints)' "$APPLICATION_ORG_JSON" > "$OUTPUT_DIR/application-org-filtered.json"
mv "$OUTPUT_DIR/application-org-filtered.json" "$APPLICATION_ORG_JSON"

retry 30 5 peer channel fetch config "$CURRENT_BLOCK_PB" \
  -c "$CHANNEL_ID" -o "$ORDERER_ADDRESS" --tls --cafile "$ORDERER_TLS_DIR/ca.crt"

configtxlator proto_decode --input "$CURRENT_BLOCK_PB" --type common.Block --output "$CONFIG_BLOCK_JSON"
jq '.data.data[0].payload.data.config' "$CONFIG_BLOCK_JSON" > "$CONFIG_JSON"

if jq -e --arg msp "$JOIN_MSP_ID" \
  '.channel_group.groups.Application.groups[$msp] != null' "$CONFIG_JSON" >/dev/null; then
  write_local_org_result "alreadyApplied"
  exit 0
fi

jq --slurpfile org "$APPLICATION_ORG_JSON" --arg msp "$JOIN_MSP_ID" \
  '.channel_group.groups.Application.groups[$msp] = $org[0]' \
  "$CONFIG_JSON" > "$MODIFIED_CONFIG_JSON"

configtxlator proto_encode --input "$CONFIG_JSON" --type common.Config --output "$CONFIG_PB"
configtxlator proto_encode --input "$MODIFIED_CONFIG_JSON" --type common.Config --output "$MODIFIED_CONFIG_PB"
configtxlator compute_update --channel_id "$CHANNEL_ID" --original "$CONFIG_PB" --updated "$MODIFIED_CONFIG_PB" --output "$CONFIG_UPDATE_PB"
configtxlator proto_decode --input "$CONFIG_UPDATE_PB" --type common.ConfigUpdate --output "$CONFIG_UPDATE_JSON"
jq -n --arg channel "$CHANNEL_ID" --slurpfile update "$CONFIG_UPDATE_JSON" \
  '{"payload":{"header":{"channel_header":{"channel_id":$channel,"type":2}},"data":{"config_update":$update[0]}}}' \
  > "$ENVELOPE_JSON"
configtxlator proto_encode --input "$ENVELOPE_JSON" --type common.Envelope --output "$OUTPUT_DIR/%s"
chmod 0644 "$OUTPUT_DIR/%s"

write_local_org_result "staged"
`, channelName, adminMSPID, joiningMSPID, ordererAddress, channelConfigDir, adminTLSPath, ordererTLSPath,
		channelOutputDir, localOrgAddResultFile, localOrgAddBlockFile, localOrgAddUpdateFile,
		peerAddress, mspPath, localOrgAddUpdateFile, localOrgAddUpdateFile)
}

func publishLocalOrgAddResultEnv(channelName string, joiningOrg fabricopsv1alpha1.Org) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: envLocalOrgResultConfigMap, Value: channelLocalOrgAddResultConfigMapName(channelName, joiningOrg)},
		{Name: envLocalOrgResultKey, Value: localOrgAddResultKey},
		{Name: envLocalOrgResultFile, Value: localOrgAddResultFile},
		{Name: envLocalOrgUpdateKey, Value: localOrgAddUpdateArtifactKey},
		{Name: envLocalOrgUpdateFile, Value: localOrgAddUpdateFile},
		{Name: envLocalOrgBlockKey, Value: localOrgAddBlockArtifactKey},
		{Name: envLocalOrgBlockFile, Value: localOrgAddBlockFile},
		{Name: envLocalOrgResultChannel, Value: sanitizeName(channelName)},
		{Name: envLocalOrgResultOrg, Value: sanitizeName(joiningOrg.Organization.Name)},
		{
			Name:      envPodNamespace,
			ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}},
		},
	}
}

func publishLocalOrgAddResultScript() string {
	return `set -eu

set -- kubectl -n "$POD_NAMESPACE" create configmap "$FABRICOPS_LOCAL_ORG_RESULT_CONFIGMAP" \
  --from-file="$FABRICOPS_LOCAL_ORG_RESULT_KEY=` + channelOutputDir + `/$FABRICOPS_LOCAL_ORG_RESULT_FILE" \
  --from-file="$FABRICOPS_LOCAL_ORG_BLOCK_KEY=` + channelOutputDir + `/$FABRICOPS_LOCAL_ORG_BLOCK_FILE"

if [ -f "` + channelOutputDir + `/$FABRICOPS_LOCAL_ORG_UPDATE_FILE" ]; then
  set -- "$@" --from-file="$FABRICOPS_LOCAL_ORG_UPDATE_KEY=` + channelOutputDir + `/$FABRICOPS_LOCAL_ORG_UPDATE_FILE"
fi

"$@" --dry-run=client -o yaml | kubectl -n "$POD_NAMESPACE" apply -f -

kubectl -n "$POD_NAMESPACE" label configmap "$FABRICOPS_LOCAL_ORG_RESULT_CONFIGMAP" \
  fabricops.io/component=channel \
  fabricops.io/channel="$FABRICOPS_LOCAL_ORG_RESULT_CHANNEL" \
  fabricops.io/org="$FABRICOPS_LOCAL_ORG_RESULT_ORG" \
  app.kubernetes.io/component=channel \
  --overwrite
`
}

func channelLocalOrgAddJobName(channelName string, org fabricopsv1alpha1.Org) string {
	return sanitizeName(channelName + "-" + org.Organization.Name + "-local-org-add")
}

func channelLocalOrgAddResultConfigMapName(channelName string, org fabricopsv1alpha1.Org) string {
	return sanitizeName(channelLocalOrgAddJobName(channelName, org) + "-result")
}
