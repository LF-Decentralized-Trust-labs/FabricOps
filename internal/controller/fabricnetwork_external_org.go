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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

const (
	addExternalOrgContainer       = "add-external-org"
	publishExternalOrgContainer   = "publish-external-org-update"
	externalOrgConfigVolumeName   = "external-org-config"
	channelExternalOrgConfigDir   = channelWorkDir + "/external-org"
	externalOrgApplicationKey     = "application-org.json"
	externalOrgUpdateResultKey    = "external-org.json"
	externalOrgUpdateResultFile   = "external-org.json"
	envExternalOrgResultConfigMap = "FABRICOPS_EXTERNAL_ORG_RESULT_CONFIGMAP"
	envExternalOrgResultKey       = "FABRICOPS_EXTERNAL_ORG_RESULT_KEY"
	envExternalOrgResultFile      = "FABRICOPS_EXTERNAL_ORG_RESULT_FILE"
	envExternalOrgResultChannel   = "FABRICOPS_EXTERNAL_ORG_RESULT_CHANNEL"
	envExternalOrgResultOrg       = "FABRICOPS_EXTERNAL_ORG_RESULT_ORG"
)

func (r *FabricNetworkReconciler) reconcileExternalOrgUpdates(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	status *fabricopsv1alpha1.ChannelStatus,
) (string, error) {
	if len(channel.ExternalOrgs) == 0 {
		status.ExternalOrgs = nil
		return "", nil
	}

	status.ExternalOrgs = make([]fabricopsv1alpha1.ChannelExternalOrgStatus, 0, len(channel.ExternalOrgs))
	allReady := true
	pendingMessage := ""
	for _, externalOrg := range channel.ExternalOrgs {
		externalStatus, err := r.reconcileExternalOrgUpdate(ctx, net, channel, externalOrg)
		if err != nil {
			return "", err
		}
		status.ExternalOrgs = append(status.ExternalOrgs, externalStatus)
		if !externalStatus.Ready {
			allReady = false
			if pendingMessage == "" && externalStatus.Message != "" {
				pendingMessage = externalStatus.Message
			}
		}
	}
	if allReady {
		return "", nil
	}
	if pendingMessage != "" {
		return pendingMessage, nil
	}
	return "Waiting for external org channel update Jobs", nil
}

func channelExternalOrgStatuses(
	channel fabricopsv1alpha1.Channel,
	message string,
) []fabricopsv1alpha1.ChannelExternalOrgStatus {
	if len(channel.ExternalOrgs) == 0 {
		return nil
	}
	statuses := make([]fabricopsv1alpha1.ChannelExternalOrgStatus, 0, len(channel.ExternalOrgs))
	for _, externalOrg := range channel.ExternalOrgs {
		statuses = append(statuses, channelExternalOrgStatus(channel, externalOrg, nil, ordererInstance{}, message))
	}
	return statuses
}

func channelExternalOrgsReady(statuses []fabricopsv1alpha1.ChannelExternalOrgStatus) bool {
	for _, status := range statuses {
		if !status.Ready {
			return false
		}
	}
	return true
}

func (r *FabricNetworkReconciler) reconcileExternalOrgUpdate(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	externalOrg fabricopsv1alpha1.ChannelExternalOrg,
) (fabricopsv1alpha1.ChannelExternalOrgStatus, error) {
	adminOrg, adminPeer, ok := channelExternalOrgAdminPeer(net, channel, externalOrg)
	if !ok {
		return channelExternalOrgStatus(channel, externalOrg, nil, ordererInstance{}, "Waiting for a local founder admin org"), nil
	}
	orderer, ok := channelExternalOrgOrderer(net, externalOrg)
	if !ok {
		return channelExternalOrgStatus(channel, externalOrg, &adminOrg, ordererInstance{}, "Waiting for a local orderer"), nil
	}

	namespace := orgNamespaceName(net, adminOrg)
	externalStatus := channelExternalOrgStatus(channel, externalOrg, &adminOrg, orderer, "")

	resultReady, _, err := r.externalOrgUpdateResultReadiness(ctx, namespace, channel.Name, externalOrg)
	if err != nil {
		return externalStatus, err
	}
	if resultReady {
		externalStatus.Ready = true
		return externalStatus, nil
	}

	applicationOrgJSON, artifactMessage, err := r.externalOrgApplicationOrgJSON(ctx, net.Namespace, externalOrg)
	if err != nil {
		return externalStatus, err
	}
	if artifactMessage != "" {
		externalStatus.Message = externalOrg.Name + ": " + artifactMessage
		return externalStatus, nil
	}

	if err := r.ensureChannelRBAC(ctx, net, adminOrg, channel.Name, namespace); err != nil {
		return externalStatus, err
	}
	if err := r.ensureExternalOrgUpdateInputs(ctx, net, channel, adminOrg, namespace, orderer, externalOrg, applicationOrgJSON); err != nil {
		return externalStatus, err
	}

	if requiredSigners := externalOrgManualSignatureMSPIDs(adminOrg, externalOrg); len(requiredSigners) > 0 {
		externalStatus.Message = fmt.Sprintf(
			"%s: Waiting for manual channel config update signatures from %s",
			externalOrg.Name,
			strings.Join(requiredSigners, ", "),
		)
		return externalStatus, nil
	}

	if err := r.ensureJob(ctx, buildExternalOrgUpdateJob(net, channel, adminOrg, namespace, adminPeer, orderer, externalOrg)); err != nil {
		return externalStatus, err
	}

	updated, message, err := r.externalOrgUpdateReadiness(ctx, namespace, channel.Name, externalOrg)
	if err != nil {
		return externalStatus, err
	}
	if updated {
		externalStatus.Ready = true
		return externalStatus, nil
	}
	if message != "" {
		externalStatus.Message = message
		return externalStatus, nil
	}

	externalStatus.Message = externalOrg.Name + ": Waiting for external org channel update Job"
	return externalStatus, nil
}

func channelExternalOrgStatus(
	channel fabricopsv1alpha1.Channel,
	externalOrg fabricopsv1alpha1.ChannelExternalOrg,
	adminOrg *fabricopsv1alpha1.Org,
	orderer ordererInstance,
	message string,
) fabricopsv1alpha1.ChannelExternalOrgStatus {
	status := fabricopsv1alpha1.ChannelExternalOrgStatus{
		Name:                        externalOrg.Name,
		MSPID:                       externalOrg.MSPID,
		ApplicationOrgConfigMapName: channelExternalOrgApplicationConfigMapName(channel.Name, externalOrg),
		UpdateJobName:               channelExternalOrgUpdateJobName(channel.Name, externalOrg),
		AnchorPeers:                 append([]fabricopsv1alpha1.ChannelExternalAnchorPeer(nil), externalOrg.AnchorPeers...),
		RequiredSignerMSPIDs:        externalOrgRequiredSignerMSPIDs(externalOrg),
		Message:                     message,
	}
	if adminOrg != nil {
		status.AdminOrg = adminOrg.Organization.Name
	}
	if orderer.name != "" {
		status.Orderer = orderer.org.Organization.Name + "/" + orderer.name
	}
	return status
}

func externalOrgRequiredSignerMSPIDs(externalOrg fabricopsv1alpha1.ChannelExternalOrg) []string {
	signers := make([]string, 0, len(externalOrg.RequiredSignerMSPIDs))
	seen := map[string]struct{}{}
	for _, signer := range externalOrg.RequiredSignerMSPIDs {
		signer = strings.TrimSpace(signer)
		if signer == "" {
			continue
		}
		if _, ok := seen[signer]; ok {
			continue
		}
		seen[signer] = struct{}{}
		signers = append(signers, signer)
	}
	return signers
}

func externalOrgManualSignatureMSPIDs(
	adminOrg fabricopsv1alpha1.Org,
	externalOrg fabricopsv1alpha1.ChannelExternalOrg,
) []string {
	requiredSigners := externalOrgRequiredSignerMSPIDs(externalOrg)
	if len(requiredSigners) == 0 {
		return nil
	}

	adminMSPID := strings.TrimSpace(adminOrg.Organization.MSPName)
	if len(requiredSigners) == 1 && requiredSigners[0] == adminMSPID {
		return nil
	}
	return requiredSigners
}

func channelExternalOrgAdminPeer(
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	externalOrg fabricopsv1alpha1.ChannelExternalOrg,
) (fabricopsv1alpha1.Org, peerInstance, bool) {
	adminOrgName := strings.TrimSpace(externalOrg.AdminOrg)
	if adminOrgName == "" && len(channel.Orgs) > 0 {
		adminOrgName = channel.Orgs[0].Name
	}
	orgs := orgsByName(net)
	adminOrg, ok := orgs[adminOrgName]
	if !ok {
		return fabricopsv1alpha1.Org{}, peerInstance{}, false
	}
	for _, channelOrg := range channel.Orgs {
		if channelOrg.Name != adminOrgName || len(channelOrg.Peers) == 0 {
			continue
		}
		peer := peerInstance{
			org:       adminOrg,
			name:      sanitizeName(channelOrg.Peers[0]),
			namespace: orgNamespaceName(net, adminOrg),
		}
		return adminOrg, peer, true
	}
	return fabricopsv1alpha1.Org{}, peerInstance{}, false
}

func channelExternalOrgOrderer(
	net *fabricopsv1alpha1.FabricNetwork,
	externalOrg fabricopsv1alpha1.ChannelExternalOrg,
) (ordererInstance, bool) {
	orderers := desiredOrdererInstances(net)
	if len(orderers) == 0 {
		return ordererInstance{}, false
	}
	if externalOrg.Orderer == nil {
		return orderers[0], true
	}
	ref := *externalOrg.Orderer
	for _, orderer := range orderers {
		if strings.TrimSpace(ref.Org) != "" && ref.Org != orderer.org.Organization.Name {
			continue
		}
		if strings.TrimSpace(ref.Name) != "" && ref.Name != orderer.name {
			continue
		}
		return orderer, true
	}
	return ordererInstance{}, false
}

func (r *FabricNetworkReconciler) ensureExternalOrgUpdateInputs(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	adminOrg fabricopsv1alpha1.Org,
	namespace string,
	orderer ordererInstance,
	externalOrg fabricopsv1alpha1.ChannelExternalOrg,
	applicationOrgJSON string,
) error {
	labels := channelLabels(net, adminOrg, channel.Name)
	annotations := resourceAnnotations(net, adminOrg)
	source := client.ObjectKey{
		Namespace: orderer.namespace,
		Name:      identitySecretName(orderer.name, secretKindTLS),
	}
	if err := r.ensureCopiedSecret(
		ctx,
		source,
		namespace,
		channelOrdererTLSSecretName(channel.Name, orderer.name),
		labels,
		annotations,
	); err != nil {
		return err
	}

	return r.ensureConfigMap(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:        channelExternalOrgApplicationConfigMapName(channel.Name, externalOrg),
			Namespace:   namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Data: map[string]string{
			externalOrgApplicationKey: applicationOrgJSON,
		},
	})
}

func (r *FabricNetworkReconciler) externalOrgApplicationOrgJSON(
	ctx context.Context,
	namespace string,
	externalOrg fabricopsv1alpha1.ChannelExternalOrg,
) (string, string, error) {
	raw, ready, message, err := r.channelArtifactBytes(ctx, namespace, &externalOrg.ApplicationOrgRef)
	if err != nil || !ready {
		return "", message, err
	}
	if !json.Valid(raw) {
		return "", "applicationOrgRef contains invalid JSON", nil
	}

	mspID, anchorPeers, err := parseApplicationOrgJSON(raw)
	if err != nil {
		return "", "applicationOrgRef is not a Fabric Application org JSON: " + err.Error(), nil
	}
	if mspID != externalOrg.MSPID {
		return "", fmt.Sprintf("applicationOrgRef MSP ID %q does not match %q", mspID, externalOrg.MSPID), nil
	}
	if len(externalOrg.AnchorPeers) > 0 && !externalAnchorPeersEqual(externalOrg.AnchorPeers, anchorPeers) {
		return "", "applicationOrgRef anchor peers do not match spec anchorPeers", nil
	}

	return string(raw), "", nil
}

func (r *FabricNetworkReconciler) channelArtifactBytes(
	ctx context.Context,
	namespace string,
	ref *fabricopsv1alpha1.ChannelArtifactKeyRef,
) ([]byte, bool, string, error) {
	if ref == nil {
		return nil, false, "applicationOrgRef is required", nil
	}
	if ref.ConfigMapKeyRef != nil {
		return r.channelConfigMapArtifactBytes(ctx, namespace, *ref.ConfigMapKeyRef)
	}
	if ref.SecretKeyRef != nil {
		return r.channelSecretArtifactBytes(ctx, namespace, *ref.SecretKeyRef)
	}
	return nil, false, "applicationOrgRef must set configMapKeyRef or secretKeyRef", nil
}

func (r *FabricNetworkReconciler) channelConfigMapArtifactBytes(
	ctx context.Context,
	namespace string,
	ref corev1.ConfigMapKeySelector,
) ([]byte, bool, string, error) {
	var configMap corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: ref.Name}, &configMap); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, fmt.Sprintf("ConfigMap %s/%s is missing", namespace, ref.Name), nil
		}
		return nil, false, "", err
	}
	if value, ok := configMap.BinaryData[ref.Key]; ok {
		return append([]byte(nil), value...), true, "", nil
	}
	if value, ok := configMap.Data[ref.Key]; ok {
		return []byte(value), true, "", nil
	}
	return nil, false, fmt.Sprintf("ConfigMap %s/%s is missing key %q", namespace, ref.Name, ref.Key), nil
}

func (r *FabricNetworkReconciler) channelSecretArtifactBytes(
	ctx context.Context,
	namespace string,
	ref corev1.SecretKeySelector,
) ([]byte, bool, string, error) {
	var secret corev1.Secret
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: ref.Name}, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, fmt.Sprintf("Secret %s/%s is missing", namespace, ref.Name), nil
		}
		return nil, false, "", err
	}
	if value, ok := secret.Data[ref.Key]; ok {
		return append([]byte(nil), value...), true, "", nil
	}
	if value, ok := secret.StringData[ref.Key]; ok {
		return []byte(value), true, "", nil
	}
	return nil, false, fmt.Sprintf("Secret %s/%s is missing key %q", namespace, ref.Name, ref.Key), nil
}

type applicationOrgGroupJSON struct {
	Values struct {
		MSP struct {
			Value struct {
				Config struct {
					Name         string   `json:"name"`
					TLSRootCerts []string `json:"tls_root_certs"`
				} `json:"config"`
			} `json:"value"`
		} `json:"MSP"`
		AnchorPeers *struct {
			Value struct {
				AnchorPeers []fabricopsv1alpha1.ChannelExternalAnchorPeer `json:"anchor_peers"`
			} `json:"value"`
		} `json:"AnchorPeers,omitempty"`
	} `json:"values"`
}

func parseApplicationOrgJSON(raw []byte) (string, []fabricopsv1alpha1.ChannelExternalAnchorPeer, error) {
	var group applicationOrgGroupJSON
	if err := json.Unmarshal(raw, &group); err != nil {
		return "", nil, err
	}
	mspID := strings.TrimSpace(group.Values.MSP.Value.Config.Name)
	if mspID == "" {
		return "", nil, fmt.Errorf("missing values.MSP.value.config.name")
	}
	anchorPeers := []fabricopsv1alpha1.ChannelExternalAnchorPeer{}
	if group.Values.AnchorPeers != nil {
		anchorPeers = append(anchorPeers, group.Values.AnchorPeers.Value.AnchorPeers...)
	}
	return mspID, anchorPeers, nil
}

func applicationOrgTLSRootCA(raw []byte) ([]byte, error) {
	var group applicationOrgGroupJSON
	if err := json.Unmarshal(raw, &group); err != nil {
		return nil, err
	}
	for _, encoded := range group.Values.MSP.Value.Config.TLSRootCerts {
		encoded = strings.TrimSpace(encoded)
		if encoded == "" {
			continue
		}
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, err
		}
		if len(decoded) > 0 {
			return decoded, nil
		}
	}
	return nil, nil
}

func externalAnchorPeersEqual(
	expected []fabricopsv1alpha1.ChannelExternalAnchorPeer,
	actual []fabricopsv1alpha1.ChannelExternalAnchorPeer,
) bool {
	return reflect.DeepEqual(expected, actual)
}

func (r *FabricNetworkReconciler) externalOrgUpdateReadiness(
	ctx context.Context,
	namespace string,
	channelName string,
	externalOrg fabricopsv1alpha1.ChannelExternalOrg,
) (bool, string, error) {
	resultReady, resultMessage, err := r.externalOrgUpdateResultReadiness(ctx, namespace, channelName, externalOrg)
	if err != nil || resultReady {
		return resultReady, resultMessage, err
	}

	var job batchv1.Job
	err = r.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      channelExternalOrgUpdateJobName(channelName, externalOrg),
	}, &job)
	if apierrors.IsNotFound(err) {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}
	if jobFailed(job) {
		return false, fmt.Sprintf("%s: external org channel update Job failed", externalOrg.Name), nil
	}
	if jobSucceeded(job) {
		if resultMessage != "" {
			return false, resultMessage, nil
		}
		if succeededJobCleanupEligible(&job) {
			return false, "Waiting for external org channel update result ConfigMap", nil
		}
		return true, "", nil
	}

	return false, "", nil
}

func (r *FabricNetworkReconciler) externalOrgUpdateResultReadiness(
	ctx context.Context,
	namespace string,
	channelName string,
	externalOrg fabricopsv1alpha1.ChannelExternalOrg,
) (bool, string, error) {
	var result corev1.ConfigMap
	err := r.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      channelExternalOrgUpdateResultConfigMapName(channelName, externalOrg),
	}, &result)
	if apierrors.IsNotFound(err) {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}

	raw := strings.TrimSpace(result.Data[externalOrgUpdateResultKey])
	if raw == "" {
		return false, fmt.Sprintf("Waiting for %s external org update result ConfigMap data", externalOrg.Name), nil
	}
	if !externalOrgUpdateResultMatches(raw, channelName, externalOrg.MSPID, externalOrg.AnchorPeers) {
		return false, fmt.Sprintf("Waiting for %s external org update result for channel %s", externalOrg.Name, channelName), nil
	}

	return true, "", nil
}

func externalOrgUpdateResultMatches(
	raw string,
	channelName string,
	mspID string,
	anchorPeers []fabricopsv1alpha1.ChannelExternalAnchorPeer,
) bool {
	var result struct {
		Channel     string                                        `json:"channel"`
		MSPID       string                                        `json:"mspID"`
		AnchorPeers []fabricopsv1alpha1.ChannelExternalAnchorPeer `json:"anchorPeers,omitempty"`
	}
	if err := json.Unmarshal([]byte(jsonPayload(raw)), &result); err != nil {
		return false
	}
	if result.Channel != channelName || result.MSPID != mspID {
		return false
	}
	return len(anchorPeers) == 0 || externalAnchorPeersEqual(anchorPeers, result.AnchorPeers)
}

func buildExternalOrgUpdateJob(
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	adminOrg fabricopsv1alpha1.Org,
	namespace string,
	adminPeer peerInstance,
	orderer ordererInstance,
	externalOrg fabricopsv1alpha1.ChannelExternalOrg,
) *batchv1.Job {
	labels := channelLabels(net, adminOrg, channel.Name)
	labels[labelInstance] = sanitizeName(externalOrg.Name)
	labels[labelWorkload] = sanitizeName(externalOrg.Name)
	annotations := resourceAnnotations(net, adminOrg)
	backoffLimit := int32(4)
	adminMSPVolumeName := channelOrgMSPVolumeName(adminOrg)
	adminTLSVolumeName := channelOrdererAdminTLSVolumeName(adminOrg)
	ordererTLSVolumeName := channelOrdererTLSVolumeName(orderer.name)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        channelExternalOrgUpdateJobName(channel.Name, externalOrg),
			Namespace:   namespace,
			Labels:      labels,
			Annotations: succeededJobCleanupAnnotations(annotations),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: resourceAnnotations(net, adminOrg),
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: channelServiceAccountName(channel.Name),
					RestartPolicy:      corev1.RestartPolicyNever,
					Volumes: []corev1.Volume{
						{
							Name: channelOutputVolumeName,
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
						{
							Name: externalOrgConfigVolumeName,
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: channelExternalOrgApplicationConfigMapName(channel.Name, externalOrg),
									},
									Items: []corev1.KeyToPath{
										{
											Key:  externalOrgApplicationKey,
											Path: externalOrgApplicationKey,
										},
									},
								},
							},
						},
						{
							Name: adminMSPVolumeName,
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName:  identitySecretName(adminIdentityName(adminOrg), secretKindMSP),
									Items:       mspSecretItems(net.Spec.Global.TLS),
									DefaultMode: secretVolumeDefaultMode(),
								},
							},
						},
						{
							Name: adminTLSVolumeName,
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName:  identitySecretName(adminIdentityName(adminOrg), secretKindTLS),
									Items:       adminTLSSecretItems(),
									DefaultMode: secretVolumeDefaultMode(),
								},
							},
						},
						{
							Name: ordererTLSVolumeName,
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName:  channelOrdererTLSSecretName(channel.Name, orderer.name),
									Items:       tlsSecretItems(),
									DefaultMode: secretVolumeDefaultMode(),
								},
							},
						},
					},
					InitContainers: []corev1.Container{
						{
							Name:  addExternalOrgContainer,
							Image: fabricToolsImage(net.Spec.Global.FabricVersion),
							Command: []string{"sh", "-ec", addExternalOrgScript(
								channel.Name,
								adminOrg.Organization.MSPName,
								externalOrg.MSPID,
								peerAddress(adminPeer),
								ordererClientAddress(orderer),
								channelOrgMSPPath(adminOrg),
								channelOrdererAdminTLSPath(adminOrg),
								channelOrdererTLSPath(orderer.name),
								channelExternalOrgApplicationFilePath(),
								externalOrgUpdateFilePath(channel.Name, externalOrg),
							)},
							Resources: componentResourceRequirements(componentFabricCLI),
							VolumeMounts: []corev1.VolumeMount{
								{Name: channelOutputVolumeName, MountPath: channelOutputDir},
								{Name: externalOrgConfigVolumeName, MountPath: channelExternalOrgConfigDir, ReadOnly: true},
								{Name: adminMSPVolumeName, MountPath: channelOrgMSPPath(adminOrg), ReadOnly: true},
								{Name: adminTLSVolumeName, MountPath: channelOrdererAdminTLSPath(adminOrg), ReadOnly: true},
								{Name: ordererTLSVolumeName, MountPath: channelOrdererTLSPath(orderer.name), ReadOnly: true},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:      publishExternalOrgContainer,
							Image:     kubectlImage(),
							Command:   []string{"sh", "-ec", publishExternalOrgUpdateResultScript()},
							Env:       publishExternalOrgUpdateResultEnv(channel.Name, externalOrg),
							Resources: componentResourceRequirements(componentKubectl),
							VolumeMounts: []corev1.VolumeMount{
								{Name: channelOutputVolumeName, MountPath: channelOutputDir},
							},
						},
					},
				},
			},
		},
	}
}

func addExternalOrgScript(
	channelName string,
	adminMSPID string,
	externalMSPID string,
	peerAddress string,
	ordererAddress string,
	mspPath string,
	adminTLSPath string,
	ordererTLSPath string,
	applicationOrgPath string,
	updateFile string,
) string {
	return fmt.Sprintf(`set -eu

CHANNEL_ID=%q
ADMIN_MSP_ID=%q
JOIN_MSP_ID=%q
ORDERER_ADDRESS=%q
ADMIN_TLS_DIR=%q
ORDERER_TLS_DIR=%q
APPLICATION_ORG_JSON=%q
CONFIG_UPDATE_ENVELOPE_PB=%q
OUTPUT_DIR=%q
RESULT_FILE="$OUTPUT_DIR/%s"

export CORE_PEER_LOCALMSPID="$ADMIN_MSP_ID"
export CORE_PEER_ADDRESS=%q
export CORE_PEER_MSPCONFIGPATH=%q
export CORE_PEER_TLS_ENABLED=true
export CORE_PEER_TLS_CERT_FILE="$ADMIN_TLS_DIR/client.crt"
export CORE_PEER_TLS_KEY_FILE="$ADMIN_TLS_DIR/client.key"
export CORE_PEER_TLS_ROOTCERT_FILE="$ADMIN_TLS_DIR/ca.crt"

CONFIG_BLOCK_PB="$OUTPUT_DIR/external_org_config_block.pb"
CONFIG_BLOCK_JSON="$OUTPUT_DIR/external_org_config_block.json"
CONFIG_JSON="$OUTPUT_DIR/external_org_config.json"
MODIFIED_CONFIG_JSON="$OUTPUT_DIR/external_org_modified_config.json"
CONFIG_PB="$OUTPUT_DIR/external_org_config.pb"
MODIFIED_CONFIG_PB="$OUTPUT_DIR/external_org_modified_config.pb"
CONFIG_UPDATE_PB="$OUTPUT_DIR/external_org_config_update.pb"
CONFIG_UPDATE_JSON="$OUTPUT_DIR/external_org_config_update.json"
CONFIG_UPDATE_ENVELOPE_JSON="$OUTPUT_DIR/external_org_config_update_envelope.json"

mkdir -p "$OUTPUT_DIR"

write_external_org_result() {
  jq -n \
    --slurpfile org "$APPLICATION_ORG_JSON" \
    --arg channel "$CHANNEL_ID" \
    --arg msp "$JOIN_MSP_ID" \
    '{"channel":$channel,"mspID":$msp,"anchorPeers":($org[0].values.AnchorPeers.value.anchor_peers // [])}' \
    > "$RESULT_FILE"
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

jq -e --arg msp "$JOIN_MSP_ID" \
  '.values.MSP.value.config.name == $msp' \
  "$APPLICATION_ORG_JSON" >/dev/null

retry 30 5 peer channel fetch config "$CONFIG_BLOCK_PB" \
  -c "$CHANNEL_ID" \
  -o "$ORDERER_ADDRESS" \
  --tls \
  --cafile "$ORDERER_TLS_DIR/ca.crt"

configtxlator proto_decode \
  --input "$CONFIG_BLOCK_PB" \
  --type common.Block \
  --output "$CONFIG_BLOCK_JSON"

jq '.data.data[0].payload.data.config' "$CONFIG_BLOCK_JSON" > "$CONFIG_JSON"

if jq -e --arg msp "$JOIN_MSP_ID" \
  '.channel_group.groups.Application.groups[$msp] != null' \
  "$CONFIG_JSON" >/dev/null; then
  echo "Application org $JOIN_MSP_ID already exists on channel $CHANNEL_ID"
  write_external_org_result
  exit 0
fi

jq --slurpfile org "$APPLICATION_ORG_JSON" --arg msp "$JOIN_MSP_ID" \
  '.channel_group.groups.Application.groups[$msp] = $org[0]' \
  "$CONFIG_JSON" > "$MODIFIED_CONFIG_JSON"

configtxlator proto_encode \
  --input "$CONFIG_JSON" \
  --type common.Config \
  --output "$CONFIG_PB"

configtxlator proto_encode \
  --input "$MODIFIED_CONFIG_JSON" \
  --type common.Config \
  --output "$MODIFIED_CONFIG_PB"

configtxlator compute_update \
  --channel_id "$CHANNEL_ID" \
  --original "$CONFIG_PB" \
  --updated "$MODIFIED_CONFIG_PB" \
  --output "$CONFIG_UPDATE_PB"

configtxlator proto_decode \
  --input "$CONFIG_UPDATE_PB" \
  --type common.ConfigUpdate \
  --output "$CONFIG_UPDATE_JSON"

jq --arg channel "$CHANNEL_ID" \
  '{"payload":{"header":{"channel_header":{"channel_id":$channel,"type":2}},"data":{"config_update":.}}}' \
  "$CONFIG_UPDATE_JSON" > "$CONFIG_UPDATE_ENVELOPE_JSON"

configtxlator proto_encode \
  --input "$CONFIG_UPDATE_ENVELOPE_JSON" \
  --type common.Envelope \
  --output "$CONFIG_UPDATE_ENVELOPE_PB"

retry 30 5 peer channel update \
  -c "$CHANNEL_ID" \
  -o "$ORDERER_ADDRESS" \
  -f "$CONFIG_UPDATE_ENVELOPE_PB" \
  --tls \
  --cafile "$ORDERER_TLS_DIR/ca.crt"

write_external_org_result
`, channelName, adminMSPID, externalMSPID, ordererAddress, adminTLSPath, ordererTLSPath, applicationOrgPath, updateFile, channelOutputDir, externalOrgUpdateResultFile, peerAddress, mspPath)
}

func publishExternalOrgUpdateResultEnv(
	channelName string,
	externalOrg fabricopsv1alpha1.ChannelExternalOrg,
) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: envExternalOrgResultConfigMap, Value: channelExternalOrgUpdateResultConfigMapName(channelName, externalOrg)},
		{Name: envExternalOrgResultKey, Value: externalOrgUpdateResultKey},
		{Name: envExternalOrgResultFile, Value: externalOrgUpdateResultFile},
		{Name: envExternalOrgResultChannel, Value: sanitizeName(channelName)},
		{Name: envExternalOrgResultOrg, Value: sanitizeName(externalOrg.Name)},
		{
			Name: envPodNamespace,
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.namespace",
				},
			},
		},
	}
}

func publishExternalOrgUpdateResultScript() string {
	return `set -eu

kubectl -n "$POD_NAMESPACE" create configmap "$FABRICOPS_EXTERNAL_ORG_RESULT_CONFIGMAP" \
  --from-file="$FABRICOPS_EXTERNAL_ORG_RESULT_KEY=` + channelOutputDir + `/$FABRICOPS_EXTERNAL_ORG_RESULT_FILE" \
  --dry-run=client -o yaml | kubectl -n "$POD_NAMESPACE" apply -f -

kubectl -n "$POD_NAMESPACE" label configmap "$FABRICOPS_EXTERNAL_ORG_RESULT_CONFIGMAP" \
  fabricops.io/component=channel \
  fabricops.io/channel="$FABRICOPS_EXTERNAL_ORG_RESULT_CHANNEL" \
  fabricops.io/org="$FABRICOPS_EXTERNAL_ORG_RESULT_ORG" \
  app.kubernetes.io/component=channel \
  --overwrite
`
}

func channelExternalOrgApplicationConfigMapName(
	channelName string,
	externalOrg fabricopsv1alpha1.ChannelExternalOrg,
) string {
	return sanitizeName(channelName + "-" + externalOrg.Name + "-application-org")
}

func channelExternalOrgUpdateJobName(channelName string, externalOrg fabricopsv1alpha1.ChannelExternalOrg) string {
	return sanitizeName(channelName + "-" + externalOrg.Name + "-external-org-update")
}

func channelExternalOrgUpdateResultConfigMapName(
	channelName string,
	externalOrg fabricopsv1alpha1.ChannelExternalOrg,
) string {
	return sanitizeName(channelExternalOrgUpdateJobName(channelName, externalOrg) + "-result")
}

func externalOrgUpdateFileName(channelName string, externalOrg fabricopsv1alpha1.ChannelExternalOrg) string {
	return sanitizeName(externalOrg.MSPID+"-"+channelName+"-org-update") + ".tx"
}

func externalOrgUpdateFilePath(channelName string, externalOrg fabricopsv1alpha1.ChannelExternalOrg) string {
	return channelOutputDir + "/" + externalOrgUpdateFileName(channelName, externalOrg)
}

func channelExternalOrgApplicationFilePath() string {
	return channelExternalOrgConfigDir + "/" + externalOrgApplicationKey
}
