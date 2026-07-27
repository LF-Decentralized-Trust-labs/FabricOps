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
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	fabricopsv1alpha1 "github.com/dpereowei/fabricops/api/v1alpha1"
)

func (r *FabricParticipantReconciler) reconcileParticipantChaincodes(
	ctx context.Context,
	participant *fabricopsv1alpha1.FabricParticipant,
	localOrgStatus fabricopsv1alpha1.OrgStatus,
	channelStatuses []fabricopsv1alpha1.ParticipantChannelStatus,
	artifacts participantArtifactStatus,
) ([]fabricopsv1alpha1.ParticipantChaincodeStatus, bool, string, error) {
	if len(participant.Spec.Chaincodes) == 0 {
		return nil, true, "No participant chaincodes declared", nil
	}

	networkReconciler := &FabricNetworkReconciler{
		Client: r.Client,
		Scheme: r.Scheme,
	}
	net := participantLocalFabricNetwork(participant)
	org := participant.Spec.Org
	namespace := localOrgStatus.Namespace
	if namespace == "" {
		namespace = orgNamespaceName(net, org)
	}
	channels := participantChannelsByName(participant.Spec.Channels)
	readyChannels := participantReadyChannelsByName(channelStatuses)
	orderer := participant.Spec.Network.Orderers[0]

	statuses := make([]fabricopsv1alpha1.ParticipantChaincodeStatus, 0, len(participant.Spec.Chaincodes))
	allReady := true
	pendingMessage := ""

	for _, chaincode := range participant.Spec.Chaincodes {
		status, err := r.reconcileParticipantChaincode(
			ctx,
			networkReconciler,
			net,
			org,
			namespace,
			channels[chaincode.Channel],
			readyChannels[chaincode.Channel],
			orderer,
			chaincode,
			artifacts.chaincodePackages[participantChaincodeKey(chaincode.Channel, chaincode.Name)],
		)
		if err != nil {
			return statuses, false, "", err
		}

		statuses = append(statuses, status)
		if !status.Ready {
			allReady = false
			if status.Message != "" && pendingMessage == "" {
				pendingMessage = status.Message
			}
		}
	}

	message := "Participant chaincode lifecycle is ready"
	if !allReady && pendingMessage != "" {
		message = pendingMessage
	}
	return statuses, allReady, message, nil
}

func (r *FabricParticipantReconciler) reconcileParticipantChaincode(
	ctx context.Context,
	networkReconciler *FabricNetworkReconciler,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	channel fabricopsv1alpha1.ParticipantChannel,
	channelReady bool,
	orderer fabricopsv1alpha1.ParticipantOrdererEndpoint,
	participantChaincode fabricopsv1alpha1.ParticipantChaincode,
	importedPackageReady bool,
) (fabricopsv1alpha1.ParticipantChaincodeStatus, error) {
	chaincode := participantChaincodeAsNetworkChaincode(participantChaincode)
	status := fabricopsv1alpha1.ParticipantChaincodeStatus{
		Name:    participantChaincode.Name,
		Channel: participantChaincode.Channel,
	}

	if participantChaincode.PackageRef != nil && !importedPackageReady {
		status.Message = "Waiting for imported chaincode package"
		return status, nil
	}
	if !channelReady {
		status.PackageReady = participantChaincode.PackageRef == nil || importedPackageReady
		status.Message = "Waiting for participant channel join before chaincode lifecycle"
		return status, nil
	}

	if err := r.ensureParticipantChaincodePackageConfigMap(
		ctx,
		networkReconciler,
		net,
		org,
		namespace,
		participantChaincode,
		chaincode,
	); err != nil {
		return status, err
	}
	status.PackageReady = true

	allInstalled := len(channel.Peers) > 0
	workloadsReady := true
	firstPackageID := ""
	firstChaincodeID := ""

	for _, peerName := range channel.Peers {
		if err := networkReconciler.ensureChaincodeInstallRBAC(ctx, net, org, chaincode, peerName); err != nil {
			return status, err
		}

		installed, packageID, chaincodeID, message, err := networkReconciler.chaincodeInstallReadiness(
			ctx,
			namespace,
			chaincodePackageIDConfigMapName(chaincode, org, peerName),
			chaincodeInstallJobName(chaincode, org, peerName),
		)
		if err != nil {
			return status, err
		}
		if packageID != "" && firstPackageID == "" {
			firstPackageID = packageID
			firstChaincodeID = chaincodeID
		}
		if !installed {
			allInstalled = false
			if err := networkReconciler.ensureJob(ctx, buildChaincodeInstallJob(net, chaincode, org, peerName)); err != nil {
				return status, err
			}
			if status.Message == "" {
				status.Message = message
			}
			continue
		}

		if strings.TrimSpace(chaincode.Image) == "" {
			continue
		}
		if err := networkReconciler.ensureChaincodeWorkload(ctx, net, chaincode, org, peerName, chaincodeID); err != nil {
			return status, err
		}
		_, ready, message, err := networkReconciler.chaincodeWorkloadReadiness(
			ctx,
			namespace,
			chaincodeServiceName(chaincode, org, peerName),
		)
		if err != nil {
			return status, err
		}
		if !ready {
			workloadsReady = false
			if status.Message == "" && message != "" {
				status.Message = message
			}
		}
	}

	status.Installed = allInstalled
	if !allInstalled {
		if status.Message == "" {
			status.Message = "Waiting for chaincode install Jobs"
		}
		return status, nil
	}
	if !workloadsReady {
		if status.Message == "" {
			status.Message = "Waiting for chaincode workload Deployment"
		}
	}
	if firstPackageID == "" {
		status.Message = "Waiting for chaincode package ID before lifecycle approval"
		return status, nil
	}

	approved, message, err := r.reconcileParticipantChaincodeApproval(
		ctx,
		networkReconciler,
		net,
		org,
		namespace,
		channel,
		participantChaincode,
		chaincode,
		firstPackageID,
		orderer,
	)
	if err != nil {
		return status, err
	}
	status.Approved = approved
	if !approved {
		if message != "" {
			status.Message = message
		} else if status.Message == "" {
			status.Message = "Waiting for chaincode approve Job"
		}
		return status, nil
	}

	status.Ready = status.PackageReady && status.Installed && status.Approved && workloadsReady
	if !status.Ready && status.Message == "" {
		status.Message = "Waiting for chaincode workload Deployment"
	}
	if status.Ready {
		status.Message = "Participant chaincode package installed, workload ready, and definition approved"
		if strings.TrimSpace(firstChaincodeID) == "" {
			status.Message = "Participant chaincode package installed and definition approved"
		}
	}

	return status, nil
}

func (r *FabricParticipantReconciler) ensureParticipantChaincodePackageConfigMap(
	ctx context.Context,
	networkReconciler *FabricNetworkReconciler,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	participantChaincode fabricopsv1alpha1.ParticipantChaincode,
	chaincode fabricopsv1alpha1.Chaincode,
) error {
	if participantChaincode.PackageRef == nil {
		configMap, err := buildChaincodePackageConfigMap(net, chaincode, org)
		if err != nil {
			return err
		}
		return networkReconciler.ensureConfigMap(ctx, configMap)
	}

	packageArchive, err := r.participantArtifactBytes(ctx, net.Namespace, participantChaincode.PackageRef)
	if err != nil {
		return err
	}
	packageFile := chaincodePackageLabel(chaincode) + ".tar.gz"
	metadataJSON, err := marshalChaincodeJSON(chaincodePackageMetadata{
		Type:  "ccaas",
		Label: chaincodePackageLabel(chaincode),
	})
	if err != nil {
		return err
	}
	connectionJSON := "{}\n"
	connectionAddress := ""
	if strings.TrimSpace(chaincode.Image) != "" {
		connectionAddress, err = chaincodeConnectionAddressTemplate(chaincode, org, namespace)
		if err != nil {
			return err
		}
		connectionJSON, err = marshalChaincodeJSON(chaincodeConnection{
			Address:     connectionAddress,
			DialTimeout: chaincodeDialTimeout(chaincode),
			TLSRequired: false,
		})
		if err != nil {
			return err
		}
	}

	desired := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:        chaincodePackageConfigMapName(chaincode, org),
			Namespace:   namespace,
			Labels:      chaincodeLabels(net, org, chaincode.Channel, chaincode.Name),
			Annotations: resourceAnnotations(net, org),
		},
		Data: map[string]string{
			chaincodeMetadataKey:       metadataJSON,
			chaincodeConnectionKey:     connectionJSON,
			chaincodePackageLabelKey:   chaincodePackageLabel(chaincode),
			chaincodePackageFileKey:    packageFile,
			chaincodeConnectionAddrKey: connectionAddress,
		},
		BinaryData: map[string][]byte{
			packageFile: packageArchive,
		},
	}

	return networkReconciler.ensureConfigMap(ctx, desired)
}

func (r *FabricParticipantReconciler) reconcileParticipantChaincodeApproval(
	ctx context.Context,
	networkReconciler *FabricNetworkReconciler,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	channel fabricopsv1alpha1.ParticipantChannel,
	participantChaincode fabricopsv1alpha1.ParticipantChaincode,
	chaincode fabricopsv1alpha1.Chaincode,
	packageID string,
	orderer fabricopsv1alpha1.ParticipantOrdererEndpoint,
) (bool, string, error) {
	if len(channel.Peers) == 0 {
		return false, "Waiting for participant approval peer", nil
	}
	if len(net.Spec.Global.FabricVersion) == 0 {
		return false, "Waiting for participant Fabric version", nil
	}
	if len(net.Spec.Orgs) == 0 {
		return false, "Waiting for participant org", nil
	}

	if strings.TrimSpace(orderer.ClientAddress) == "" {
		return false, "Waiting for imported orderer before lifecycle approval", nil
	}
	peerName := channel.Peers[0]
	resultName := chaincodeApproveResultConfigMapName(chaincode, org, packageID)
	jobName := chaincodeApproveJobName(chaincode, org, packageID)
	approved, message, err := networkReconciler.chaincodeApproveReadiness(
		ctx,
		namespace,
		resultName,
		jobName,
		org,
		chaincodeSequence(chaincode),
	)
	if err != nil || approved {
		return approved, message, err
	}

	if err := r.ensureParticipantOrdererTLSCASecret(ctx, networkReconciler, net, org, namespace, channel.Name, chaincode.Name, orderer); err != nil {
		return false, "", err
	}
	if err := networkReconciler.ensureJob(ctx, buildParticipantChaincodeApproveJob(
		net,
		participantChannelAsNetworkChannel(channel, org),
		participantChaincode,
		chaincode,
		org,
		namespace,
		peerName,
		packageID,
		orderer,
	)); err != nil {
		return false, "", err
	}

	return false, message, nil
}

func (r *FabricParticipantReconciler) ensureParticipantOrdererTLSCASecret(
	ctx context.Context,
	networkReconciler *FabricNetworkReconciler,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	channelName string,
	chaincodeName string,
	orderer fabricopsv1alpha1.ParticipantOrdererEndpoint,
) error {
	if !net.Spec.Global.TLS {
		return nil
	}

	tlsRootCA, err := r.participantArtifactBytes(ctx, net.Namespace, orderer.TLSRootCARef)
	if err != nil {
		return err
	}
	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:        channelOrdererTLSSecretName(channelName, orderer.Name),
			Namespace:   namespace,
			Labels:      chaincodeLabels(net, org, channelName, chaincodeName),
			Annotations: resourceAnnotations(net, org),
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			tlsCACertKey: tlsRootCA,
		},
	}

	return networkReconciler.ensureReplicatedSecret(ctx, desired)
}

func buildParticipantChaincodeApproveJob(
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	participantChaincode fabricopsv1alpha1.ParticipantChaincode,
	chaincode fabricopsv1alpha1.Chaincode,
	org fabricopsv1alpha1.Org,
	namespace string,
	peerName string,
	packageID string,
	orderer fabricopsv1alpha1.ParticipantOrdererEndpoint,
) *batchv1.Job {
	labels := chaincodePeerLabels(net, org, chaincode, peerName)
	labels[labelWorkload] = sanitizeName(adminIdentityName(org))
	annotations := resourceAnnotations(net, org)
	backoffLimit := int32(4)
	volumeMounts := []corev1.VolumeMount{
		{Name: chaincodeAdminMSPVolume, MountPath: chaincodeAdminMSPPath, ReadOnly: true},
		{Name: chaincodeOutputVolumeName, MountPath: chaincodeOutputDir},
	}
	volumes := []corev1.Volume{
		{
			Name: chaincodeOutputVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
		{
			Name: chaincodeAdminMSPVolume,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  identitySecretName(adminIdentityName(org), secretKindMSP),
					Items:       mspSecretItems(net.Spec.Global.TLS),
					DefaultMode: secretVolumeDefaultMode(),
				},
			},
		},
	}

	if net.Spec.Global.TLS {
		volumes = append(volumes,
			corev1.Volume{
				Name: chaincodeAdminTLSVolume,
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName:  identitySecretName(adminIdentityName(org), secretKindTLS),
						Items:       adminTLSSecretItems(),
						DefaultMode: secretVolumeDefaultMode(),
					},
				},
			},
			corev1.Volume{
				Name: channelOrdererTLSVolumeName(orderer.Name),
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: channelOrdererTLSSecretName(channel.Name, orderer.Name),
						Items: []corev1.KeyToPath{{
							Key:  tlsCACertKey,
							Path: "ca.crt",
						}},
						DefaultMode: secretVolumeDefaultMode(),
					},
				},
			},
		)
		volumeMounts = append(volumeMounts,
			corev1.VolumeMount{Name: chaincodeAdminTLSVolume, MountPath: chaincodeAdminTLSPath, ReadOnly: true},
			corev1.VolumeMount{Name: channelOrdererTLSVolumeName(orderer.Name), MountPath: chaincodeOrdererTLSPath(orderer.Name), ReadOnly: true},
		)
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        chaincodeApproveJobName(chaincode, org, packageID),
			Namespace:   namespace,
			Labels:      labels,
			Annotations: succeededJobCleanupAnnotations(annotations),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: resourceAnnotations(net, org),
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: chaincodeInstallerName(chaincode, org, peerName),
					RestartPolicy:      corev1.RestartPolicyNever,
					Volumes:            volumes,
					InitContainers: []corev1.Container{
						{
							Name:  approveChaincodeContainer,
							Image: fabricToolsImage(net.Spec.Global.FabricVersion),
							Command: []string{"sh", "-ec", approveChaincodeDefinitionScriptForOrderer(
								net,
								channel,
								chaincode,
								org,
								peerName,
								namespace,
								packageID,
								orderer.ClientAddress,
								chaincodeOrdererTLSPath(orderer.Name)+"/ca.crt",
								orderer.TLSHostnameOverride,
								strings.TrimSpace(participantChaincode.EndorsementPolicy),
								"",
							)},
							Resources:    componentResourceRequirements(componentPeer),
							VolumeMounts: volumeMounts,
						},
					},
					Containers: []corev1.Container{
						{
							Name:      publishChaincodeLifecycleResultContainerName(approveChaincodeContainer),
							Image:     kubectlImage(),
							Command:   []string{"sh", "-ec", publishChaincodeLifecycleResultScript()},
							Env:       publishChaincodeLifecycleResultEnv(chaincode, peerName, chaincodeApproveResultConfigMapName(chaincode, org, packageID), chaincodeQueryApprovedKey, chaincodeQueryApprovedFile),
							Resources: componentResourceRequirements(componentKubectl),
							VolumeMounts: []corev1.VolumeMount{
								{Name: chaincodeOutputVolumeName, MountPath: chaincodeOutputDir},
							},
						},
					},
				},
			},
		},
	}
}

func participantChaincodeAsNetworkChaincode(
	chaincode fabricopsv1alpha1.ParticipantChaincode,
) fabricopsv1alpha1.Chaincode {
	return fabricopsv1alpha1.Chaincode{
		Name:              chaincode.Name,
		Version:           chaincode.Version,
		Channel:           chaincode.Channel,
		Image:             chaincode.Image,
		Sequence:          chaincode.Sequence,
		PackageLabel:      chaincode.PackageLabel,
		EndorsementPolicy: chaincode.EndorsementPolicy,
		InitRequired:      chaincode.InitRequired,
		CCAAS:             chaincode.CCAAS,
	}
}

func participantChannelsByName(
	channels []fabricopsv1alpha1.ParticipantChannel,
) map[string]fabricopsv1alpha1.ParticipantChannel {
	out := map[string]fabricopsv1alpha1.ParticipantChannel{}
	for _, channel := range channels {
		out[channel.Name] = channel
	}
	return out
}

func participantReadyChannelsByName(
	statuses []fabricopsv1alpha1.ParticipantChannelStatus,
) map[string]bool {
	out := map[string]bool{}
	for _, status := range statuses {
		out[status.Name] = status.Ready
	}
	return out
}
