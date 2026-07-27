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
	"maps"
	"reflect"
	"slices"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	fabricopsv1alpha1 "github.com/dpereowei/fabricops/api/v1alpha1"
)

const (
	channelWorkDir                = "/fabricops/channel"
	channelBlockDir               = channelWorkDir + "/block"
	channelConfigDir              = channelWorkDir + "/config"
	channelOutputDir              = channelWorkDir + "/output"
	channelCryptoDir              = channelWorkDir + "/crypto"
	channelAdminTLSDir            = channelCryptoDir + "/admin-tls"
	channelBlockVolumeName        = "channel-block"
	channelConfigVolumeName       = "channel-config"
	channelOutputVolumeName       = "channel-output"
	generateChannelBlockContainer = "generate-channel-block"
	updateAnchorPeerContainer     = "update-anchor-peer"
	joinOrdererContainer          = "join-orderer"
	joinPeerContainer             = "join-peer"
	publishChannelBlockContainer  = "publish-channel-block"
	publishOrdererJoinContainer   = "publish-orderer-join"
	publishPeerJoinContainer      = "publish-peer-join"
	publishAnchorPeerContainer    = "publish-anchor-peer-update"

	channelOrdererJoinResultKey  = "channels.json"
	channelOrdererJoinResultFile = "orderer-channels.json"
	channelPeerJoinResultKey     = "channels.txt"
	channelPeerJoinResultFile    = "peer-channels.txt"
	channelAnchorPeerResultKey   = "anchor-peer.json"
	channelAnchorPeerResultFile  = "anchor-peer.json"

	envChannelBlockConfigMap      = "FABRICOPS_CHANNEL_BLOCK_CONFIGMAP"
	envChannelBlockFile           = "FABRICOPS_CHANNEL_BLOCK_FILE"
	envOrdererJoinResultConfigMap = "FABRICOPS_ORDERER_JOIN_RESULT_CONFIGMAP"
	envOrdererJoinResultKey       = "FABRICOPS_ORDERER_JOIN_RESULT_KEY"
	envOrdererJoinResultFile      = "FABRICOPS_ORDERER_JOIN_RESULT_FILE"
	envOrdererJoinResultChannel   = "FABRICOPS_ORDERER_JOIN_RESULT_CHANNEL"
	envOrdererJoinResultOrderer   = "FABRICOPS_ORDERER_JOIN_RESULT_ORDERER"
	envPeerJoinResultConfigMap    = "FABRICOPS_PEER_JOIN_RESULT_CONFIGMAP"
	envPeerJoinResultKey          = "FABRICOPS_PEER_JOIN_RESULT_KEY"
	envPeerJoinResultFile         = "FABRICOPS_PEER_JOIN_RESULT_FILE"
	envPeerJoinResultChannel      = "FABRICOPS_PEER_JOIN_RESULT_CHANNEL"
	envPeerJoinResultPeer         = "FABRICOPS_PEER_JOIN_RESULT_PEER"
	envAnchorPeerResultConfigMap  = "FABRICOPS_ANCHOR_PEER_RESULT_CONFIGMAP"
	envAnchorPeerResultKey        = "FABRICOPS_ANCHOR_PEER_RESULT_KEY"
	envAnchorPeerResultFile       = "FABRICOPS_ANCHOR_PEER_RESULT_FILE"
	envAnchorPeerResultChannel    = "FABRICOPS_ANCHOR_PEER_RESULT_CHANNEL"
	envAnchorPeerResultOrg        = "FABRICOPS_ANCHOR_PEER_RESULT_ORG"

	channelServiceAccount = "channel-bootstrapper"
)

func (r *FabricNetworkReconciler) reconcileChannels(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	orgStatuses []fabricopsv1alpha1.OrgStatus,
) ([]fabricopsv1alpha1.ChannelStatus, error) {
	statuses := make([]fabricopsv1alpha1.ChannelStatus, 0, len(net.Spec.Channels))
	orderers := desiredNetworkOrdererStatus(net)
	ordererInstances := desiredOrdererInstances(net)
	orgs := orgsByName(net)
	readyOrgs := readyOrgsByName(orgStatuses)
	componentsReady := allOrgsReady(orgStatuses)

	for _, channel := range net.Spec.Channels {
		status := fabricopsv1alpha1.ChannelStatus{
			Name:               channel.Name,
			ConfigMapName:      channelConfigMapName(channel.Name),
			BlockConfigMapName: channelBlockConfigMapName(channel.Name),
			Orderers:           orderers,
			Orgs:               make([]fabricopsv1alpha1.ChannelOrgStatus, 0, len(channel.Orgs)),
			ExternalOrgs:       channelExternalOrgStatuses(channel, "Waiting for local channel bootstrap"),
		}
		messages := []string{}

		if strings.TrimSpace(channel.Name) == "" {
			messages = append(messages, "Channel name is required")
		}
		if len(channel.Orgs) == 0 {
			messages = append(messages, "At least one peer org is required")
		}
		if orderers.Desired == 0 {
			messages = append(messages, "At least one orderer is required")
		}
		if !net.Spec.Global.TLS {
			messages = append(messages, "Channel block generation requires spec.global.tls=true")
		}

		for _, channelOrg := range channel.Orgs {
			orgStatus := fabricopsv1alpha1.ChannelOrgStatus{
				Name:      channelOrg.Name,
				PeerNames: append([]string(nil), channelOrg.Peers...),
				Peers: fabricopsv1alpha1.WorkloadStatus{
					Desired: int32(len(channelOrg.Peers)),
				},
			}
			status.Peers.Desired += orgStatus.Peers.Desired

			org, ok := orgs[channelOrg.Name]
			if !ok {
				orgStatus.Message = "Unknown org in channel"
				messages = append(messages, fmt.Sprintf("%s: unknown org", channelOrg.Name))
				status.Orgs = append(status.Orgs, orgStatus)
				continue
			}

			orgStatus.Namespace = orgNamespaceName(net, org)
			orgStatus.MSPName = org.Organization.MSPName

			unknownPeers := unknownChannelPeers(org, channelOrg.Peers)
			if len(channelOrg.Peers) == 0 {
				orgStatus.Message = "At least one peer is required"
				messages = append(messages, fmt.Sprintf("%s: at least one peer is required", channelOrg.Name))
			} else if len(unknownPeers) > 0 {
				orgStatus.Message = "Unknown peers: " + strings.Join(unknownPeers, ", ")
				messages = append(messages, fmt.Sprintf("%s: unknown peers %s", channelOrg.Name, strings.Join(unknownPeers, ", ")))
			} else if !readyOrgs[channelOrg.Name] {
				orgStatus.Message = "Waiting for org components to become ready"
			} else {
				orgStatus.Message = "Waiting for peer join Jobs"
			}

			orgStatus.Ready = orgStatus.Peers.Ready >= orgStatus.Peers.Desired &&
				orgStatus.Peers.Desired > 0 &&
				orgStatus.Message == ""
			status.Orgs = append(status.Orgs, orgStatus)
		}

		if len(messages) > 0 {
			status.Message = strings.Join(messages, "; ")
		} else if !componentsReady {
			status.Message = "Waiting for Fabric components before channel bootstrap"
		} else {
			hostOrg := ordererInstances[0].org
			hostNamespace := orgNamespaceName(net, hostOrg)
			if err := r.reconcileChannelBlockGeneration(ctx, net, channel, hostOrg, hostNamespace); err != nil {
				return statuses, err
			}

			status.ConfigReady = true
			blockReady, blockMessage, err := r.channelBlockReadiness(ctx, hostNamespace, channel.Name)
			if err != nil {
				return statuses, err
			}
			status.BlockReady = blockReady
			if blockReady {
				orderersJoined, joinMessage, err := r.reconcileOrdererJoins(ctx, net, channel, hostOrg, hostNamespace)
				if err != nil {
					return statuses, err
				}
				status.Orderers.Ready = orderersJoined
				if joinMessage != "" {
					status.Message = joinMessage
					setChannelOrgMessages(&status, joinMessage)
				} else if status.Orderers.Ready < status.Orderers.Desired {
					status.Message = "Waiting for orderer join Jobs"
					setChannelOrgMessages(&status, status.Message)
				} else {
					peersJoined, peerMessage, err := r.reconcilePeerJoins(ctx, net, channel, hostNamespace, &status)
					if err != nil {
						return statuses, err
					}
					status.Peers.Ready = peersJoined
					status.Message = peerMessage
					if peerMessage == "" {
						anchorMessage, err := r.reconcileAnchorPeerUpdates(ctx, net, channel, &status)
						if err != nil {
							return statuses, err
						}
						status.Message = anchorMessage
						if anchorMessage == "" {
							externalOrgMessage, err := r.reconcileExternalOrgUpdates(ctx, net, channel, &status)
							if err != nil {
								return statuses, err
							}
							status.Message = externalOrgMessage
						}
					}
				}
			} else {
				status.Message = blockMessage
			}
		}
		status.Ready = status.Orderers.Ready >= status.Orderers.Desired &&
			status.Peers.Ready >= status.Peers.Desired &&
			status.Orderers.Desired > 0 &&
			status.Peers.Desired > 0 &&
			channelExternalOrgsReady(status.ExternalOrgs) &&
			status.Message == ""
		statuses = append(statuses, status)
	}

	return statuses, nil
}

func (r *FabricNetworkReconciler) reconcileChannelBlockGeneration(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	hostOrg fabricopsv1alpha1.Org,
	hostNamespace string,
) error {
	if err := r.ensureChannelRBAC(ctx, net, hostOrg, channel.Name, hostNamespace); err != nil {
		return err
	}
	if err := r.ensureChannelCryptoSecrets(ctx, net, channel, hostOrg, hostNamespace); err != nil {
		return err
	}

	configMap, err := buildChannelConfigMap(net, channel, hostOrg, hostNamespace)
	if err != nil {
		return err
	}
	if err := r.ensureConfigMap(ctx, configMap); err != nil {
		return err
	}

	blockReady, _, err := r.channelBlockReadiness(ctx, hostNamespace, channel.Name)
	if err != nil || blockReady {
		return err
	}

	return r.ensureJob(ctx, buildChannelBlockJob(net, channel, hostOrg, hostNamespace))
}

func (r *FabricNetworkReconciler) ensureChannelRBAC(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	hostOrg fabricopsv1alpha1.Org,
	channelName string,
	namespace string,
) error {
	if err := r.ensureServiceAccount(ctx, buildChannelServiceAccount(net, hostOrg, channelName, namespace)); err != nil {
		return err
	}
	if err := r.ensureRole(ctx, buildChannelRole(net, hostOrg, channelName, namespace)); err != nil {
		return err
	}
	return r.ensureRoleBinding(ctx, buildChannelRoleBinding(net, hostOrg, channelName, namespace))
}

func (r *FabricNetworkReconciler) ensureChannelCryptoSecrets(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	hostOrg fabricopsv1alpha1.Org,
	namespace string,
) error {
	labels := channelLabels(net, hostOrg, channel.Name)
	annotations := resourceAnnotations(net, hostOrg)

	for _, org := range channelConfigOrganizations(net, channel) {
		source := client.ObjectKey{
			Namespace: orgNamespaceName(net, org),
			Name:      identitySecretName(adminIdentityName(org), secretKindMSP),
		}
		if err := r.ensureCopiedSecret(ctx, source, namespace, channelOrgMSPSecretName(channel.Name, org), labels, annotations); err != nil {
			return err
		}
	}

	ordererOrgAdminTLSSecrets := map[string]struct{}{}
	for _, orderer := range desiredOrdererInstances(net) {
		orgName := orderer.org.Organization.Name
		if _, ok := ordererOrgAdminTLSSecrets[orgName]; ok {
			continue
		}
		ordererOrgAdminTLSSecrets[orgName] = struct{}{}

		source := client.ObjectKey{
			Namespace: orderer.namespace,
			Name:      identitySecretName(adminIdentityName(orderer.org), secretKindTLS),
		}
		if err := r.ensureCopiedSecret(ctx, source, namespace, channelOrdererAdminTLSSecretName(channel.Name, orderer.org), labels, annotations); err != nil {
			return err
		}
	}

	for _, orderer := range desiredOrdererInstances(net) {
		source := client.ObjectKey{
			Namespace: orderer.namespace,
			Name:      identitySecretName(orderer.name, secretKindTLS),
		}
		if err := r.ensureCopiedSecret(ctx, source, namespace, channelOrdererTLSSecretName(channel.Name, orderer.name), labels, annotations); err != nil {
			return err
		}
	}

	return nil
}

func (r *FabricNetworkReconciler) reconcileOrdererJoins(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	hostOrg fabricopsv1alpha1.Org,
	namespace string,
) (int32, string, error) {
	var ready int32
	for _, orderer := range desiredOrdererInstances(net) {
		resultReady, _, err := r.ordererJoinResultReadiness(ctx, namespace, channel.Name, orderer)
		if err != nil {
			return ready, "", err
		}
		if resultReady {
			ready++
			continue
		}

		if err := r.ensureJob(ctx, buildOrdererJoinJob(net, channel, hostOrg, namespace, orderer)); err != nil {
			return ready, "", err
		}

		joined, message, err := r.ordererJoinReadiness(ctx, namespace, channel.Name, orderer)
		if err != nil {
			return ready, "", err
		}
		if message != "" {
			return ready, message, nil
		}
		if joined {
			ready++
		}
	}

	return ready, "", nil
}

func (r *FabricNetworkReconciler) ordererJoinReadiness(
	ctx context.Context,
	namespace string,
	channelName string,
	orderer ordererInstance,
) (bool, string, error) {
	resultReady, resultMessage, err := r.ordererJoinResultReadiness(ctx, namespace, channelName, orderer)
	if err != nil || resultReady {
		return resultReady, resultMessage, err
	}

	var job batchv1.Job
	err = r.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      channelOrdererJoinJobName(channelName, orderer),
	}, &job)
	if apierrors.IsNotFound(err) {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}
	if jobFailed(job) {
		return false, fmt.Sprintf("%s: orderer join Job failed", orderer.name), nil
	}
	if jobSucceeded(job) {
		if resultMessage != "" {
			return false, resultMessage, nil
		}
		if succeededJobCleanupEligible(&job) {
			return false, "Waiting for orderer join result ConfigMap", nil
		}
		return true, "", nil
	}

	return false, "", nil
}

func (r *FabricNetworkReconciler) ordererJoinResultReadiness(
	ctx context.Context,
	namespace string,
	channelName string,
	orderer ordererInstance,
) (bool, string, error) {
	var result corev1.ConfigMap
	err := r.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      channelOrdererJoinResultConfigMapName(channelName, orderer),
	}, &result)
	if apierrors.IsNotFound(err) {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}

	raw := strings.TrimSpace(result.Data[channelOrdererJoinResultKey])
	if raw == "" {
		return false, fmt.Sprintf("Waiting for %s orderer join result ConfigMap data", orderer.name), nil
	}
	found, status := ordererJoinResultChannelStatus(raw, channelName)
	if !found {
		return false, fmt.Sprintf("Waiting for %s orderer join result for channel %s", orderer.name, channelName), nil
	}
	if status != "" && status != "active" {
		return false, fmt.Sprintf("Waiting for %s orderer channel %s to become active (status: %s)", orderer.name, channelName, status), nil
	}

	return true, "", nil
}

func ordererJoinResultChannelStatus(raw string, channelName string) (bool, string) {
	var payload any
	if err := json.Unmarshal([]byte(jsonPayload(raw)), &payload); err != nil {
		return false, ""
	}

	return jsonObjectChannelStatus(payload, channelName)
}

func jsonPayload(raw string) string {
	trimmed := strings.TrimSpace(raw)
	objectStart := strings.Index(trimmed, "{")
	arrayStart := strings.Index(trimmed, "[")

	start := objectStart
	if start < 0 || (arrayStart >= 0 && arrayStart < start) {
		start = arrayStart
	}
	if start < 0 {
		return ""
	}

	return strings.TrimSpace(trimmed[start:])
}

func jsonObjectChannelStatus(value any, name string) (bool, string) {
	switch typed := value.(type) {
	case map[string]any:
		if typedName, ok := typed["name"].(string); ok && typedName == name {
			status, _ := typed["status"].(string)
			return true, strings.ToLower(strings.TrimSpace(status))
		}
		for _, child := range typed {
			if found, status := jsonObjectChannelStatus(child, name); found {
				return true, status
			}
		}
	case []any:
		for _, child := range typed {
			if found, status := jsonObjectChannelStatus(child, name); found {
				return true, status
			}
		}
	}

	return false, ""
}

func (r *FabricNetworkReconciler) reconcilePeerJoins(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	hostNamespace string,
	status *fabricopsv1alpha1.ChannelStatus,
) (int32, string, error) {
	var ready int32
	orgs := orgsByName(net)

	for i := range status.Orgs {
		orgStatus := &status.Orgs[i]
		org, ok := orgs[orgStatus.Name]
		if !ok {
			continue
		}

		namespace := orgNamespaceName(net, org)
		if err := r.ensureChannelRBAC(ctx, net, org, channel.Name, namespace); err != nil {
			return ready, "", err
		}
		if err := r.ensureChannelBlockConfigMapCopy(ctx, net, channel, hostNamespace, org, namespace); err != nil {
			return ready, "", err
		}

		var orgReady int32
		for _, peerName := range orgStatus.PeerNames {
			peer := peerInstance{
				org:       org,
				name:      peerName,
				namespace: namespace,
			}
			resultReady, _, err := r.peerJoinResultReadiness(ctx, namespace, channel.Name, peer)
			if err != nil {
				return ready, "", err
			}
			if resultReady {
				orgReady++
				continue
			}

			if err := r.ensureJob(ctx, buildPeerJoinJob(net, channel, org, namespace, peer)); err != nil {
				return ready, "", err
			}

			joined, message, err := r.peerJoinReadiness(ctx, namespace, channel.Name, peer)
			if err != nil {
				return ready, "", err
			}
			if message != "" {
				orgStatus.Peers.Ready = orgReady
				orgStatus.Message = message
				orgStatus.Ready = false
				return ready + orgReady, message, nil
			}
			if joined {
				orgReady++
			}
		}

		orgStatus.Peers.Ready = orgReady
		ready += orgReady
		if orgReady < orgStatus.Peers.Desired {
			orgStatus.Message = "Waiting for peer join Jobs"
			orgStatus.Ready = false
		} else {
			orgStatus.Message = ""
			orgStatus.Ready = orgStatus.Peers.Desired > 0
		}
	}

	if ready < status.Peers.Desired {
		return ready, "Waiting for peer join Jobs", nil
	}

	return ready, "", nil
}

func (r *FabricNetworkReconciler) reconcileAnchorPeerUpdates(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	status *fabricopsv1alpha1.ChannelStatus,
) (string, error) {
	orderers := desiredOrdererInstances(net)
	if len(orderers) == 0 {
		return "", nil
	}

	targetOrderer := orderers[0]
	orgs := orgsByName(net)

	for i := range status.Orgs {
		orgStatus := &status.Orgs[i]
		if orgStatus.Peers.Ready < orgStatus.Peers.Desired || orgStatus.Peers.Desired == 0 {
			continue
		}

		org, ok := orgs[orgStatus.Name]
		if !ok {
			continue
		}

		anchorPeer, ok := channelAnchorPeerForOrg(net, channel, org)
		if !ok {
			continue
		}

		namespace := orgNamespaceName(net, org)
		if err := r.ensureAnchorPeerInputs(ctx, net, channel, org, namespace, targetOrderer); err != nil {
			return "", err
		}
		resultReady, _, err := r.anchorPeerUpdateResultReadiness(ctx, namespace, channel.Name, org, anchorPeer)
		if err != nil {
			return "", err
		}
		if resultReady {
			orgStatus.Message = ""
			orgStatus.Ready = true
			continue
		}

		if err := r.ensureJob(ctx, buildAnchorPeerUpdateJob(net, channel, org, namespace, anchorPeer, targetOrderer)); err != nil {
			return "", err
		}

		updated, message, err := r.anchorPeerUpdateReadiness(ctx, namespace, channel.Name, org, anchorPeer)
		if err != nil {
			return "", err
		}
		if message != "" {
			orgStatus.Message = message
			orgStatus.Ready = false
			return message, nil
		}
		if !updated {
			orgStatus.Message = "Waiting for anchor peer update Job"
			orgStatus.Ready = false
			return "Waiting for anchor peer update Jobs", nil
		}

		orgStatus.Message = ""
		orgStatus.Ready = true
	}

	return "", nil
}

func (r *FabricNetworkReconciler) ensureAnchorPeerInputs(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	org fabricopsv1alpha1.Org,
	namespace string,
	orderer ordererInstance,
) error {
	source := client.ObjectKey{
		Namespace: orderer.namespace,
		Name:      identitySecretName(orderer.name, secretKindTLS),
	}
	return r.ensureCopiedSecret(
		ctx,
		source,
		namespace,
		channelOrdererTLSSecretName(channel.Name, orderer.name),
		channelLabels(net, org, channel.Name),
		resourceAnnotations(net, org),
	)
}

func (r *FabricNetworkReconciler) anchorPeerUpdateReadiness(
	ctx context.Context,
	namespace string,
	channelName string,
	org fabricopsv1alpha1.Org,
	anchorPeer peerInstance,
) (bool, string, error) {
	resultReady, resultMessage, err := r.anchorPeerUpdateResultReadiness(ctx, namespace, channelName, org, anchorPeer)
	if err != nil || resultReady {
		return resultReady, resultMessage, err
	}

	var job batchv1.Job
	err = r.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      channelAnchorPeerUpdateJobName(channelName, org),
	}, &job)
	if apierrors.IsNotFound(err) {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}
	if jobFailed(job) {
		return false, fmt.Sprintf("%s: anchor peer update Job failed", org.Organization.Name), nil
	}
	if jobSucceeded(job) {
		if resultMessage != "" {
			return false, resultMessage, nil
		}
		if succeededJobCleanupEligible(&job) {
			return false, "Waiting for anchor peer update result ConfigMap", nil
		}
		return true, "", nil
	}

	return false, "", nil
}

func (r *FabricNetworkReconciler) anchorPeerUpdateResultReadiness(
	ctx context.Context,
	namespace string,
	channelName string,
	org fabricopsv1alpha1.Org,
	anchorPeer peerInstance,
) (bool, string, error) {
	var result corev1.ConfigMap
	err := r.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      channelAnchorPeerUpdateResultConfigMapName(channelName, org),
	}, &result)
	if apierrors.IsNotFound(err) {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}

	raw := strings.TrimSpace(result.Data[channelAnchorPeerResultKey])
	if raw == "" {
		return false, fmt.Sprintf("Waiting for %s anchor peer update result ConfigMap data", org.Organization.Name), nil
	}
	if !anchorPeerUpdateResultMatches(raw, channelName, org.Organization.MSPName, channelPeerHost(anchorPeer), channelPeerPort(anchorPeer)) {
		return false, fmt.Sprintf("Waiting for %s anchor peer update result for channel %s", org.Organization.Name, channelName), nil
	}

	return true, "", nil
}

func anchorPeerUpdateResultMatches(raw string, channelName string, mspID string, host string, port int32) bool {
	var result struct {
		Channel     string `json:"channel"`
		MSPID       string `json:"mspID"`
		AnchorPeers []struct {
			Host string `json:"host"`
			Port int32  `json:"port"`
		} `json:"anchorPeers"`
	}
	if err := json.Unmarshal([]byte(jsonPayload(raw)), &result); err != nil {
		return false
	}
	if result.Channel != channelName || result.MSPID != mspID || len(result.AnchorPeers) != 1 {
		return false
	}

	return result.AnchorPeers[0].Host == host && result.AnchorPeers[0].Port == port
}

func (r *FabricNetworkReconciler) ensureChannelBlockConfigMapCopy(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	sourceNamespace string,
	org fabricopsv1alpha1.Org,
	namespace string,
) error {
	return r.ensureChannelConfigMapNamedCopy(ctx, net, channel.Name, channelBlockConfigMapName(channel.Name), sourceNamespace, org, namespace)
}

func (r *FabricNetworkReconciler) ensureChannelConfigMapNamedCopy(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	channelName string,
	configMapName string,
	sourceNamespace string,
	org fabricopsv1alpha1.Org,
	namespace string,
) error {
	var source corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: sourceNamespace,
		Name:      configMapName,
	}, &source); err != nil {
		return err
	}

	desired := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:        configMapName,
			Namespace:   namespace,
			Labels:      channelLabels(net, org, channelName),
			Annotations: resourceAnnotations(net, org),
		},
		Data:       copyStringData(source.Data),
		BinaryData: copySecretData(source.BinaryData),
	}

	return r.ensureConfigMap(ctx, desired)
}

func (r *FabricNetworkReconciler) peerJoinReadiness(
	ctx context.Context,
	namespace string,
	channelName string,
	peer peerInstance,
) (bool, string, error) {
	resultReady, resultMessage, err := r.peerJoinResultReadiness(ctx, namespace, channelName, peer)
	if err != nil || resultReady {
		return resultReady, resultMessage, err
	}

	var job batchv1.Job
	err = r.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      channelPeerJoinJobName(channelName, peer),
	}, &job)
	if apierrors.IsNotFound(err) {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}
	if jobFailed(job) {
		return false, fmt.Sprintf("%s: peer join Job failed", peer.name), nil
	}
	if jobSucceeded(job) {
		if resultMessage != "" {
			return false, resultMessage, nil
		}
		if succeededJobCleanupEligible(&job) {
			return false, "Waiting for peer join result ConfigMap", nil
		}
		return true, "", nil
	}

	return false, "", nil
}

func (r *FabricNetworkReconciler) peerJoinResultReadiness(
	ctx context.Context,
	namespace string,
	channelName string,
	peer peerInstance,
) (bool, string, error) {
	var result corev1.ConfigMap
	err := r.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      channelPeerJoinResultConfigMapName(channelName, peer),
	}, &result)
	if apierrors.IsNotFound(err) {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}

	raw := strings.TrimSpace(result.Data[channelPeerJoinResultKey])
	if raw == "" {
		return false, fmt.Sprintf("Waiting for %s peer join result ConfigMap data", peer.name), nil
	}
	if !fieldsContain(raw, channelName) {
		return false, fmt.Sprintf("Waiting for %s peer join result for channel %s", peer.name, channelName), nil
	}

	return true, "", nil
}

func fieldsContain(raw string, value string) bool {
	return slices.Contains(strings.Fields(raw), value)
}

func (r *FabricNetworkReconciler) ensureCopiedSecret(
	ctx context.Context,
	source client.ObjectKey,
	namespace string,
	name string,
	labels map[string]string,
	annotations map[string]string,
) error {
	var sourceSecret corev1.Secret
	if err := r.Get(ctx, source, &sourceSecret); err != nil {
		return err
	}

	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Type: sourceSecret.Type,
		Data: copySecretData(sourceSecret.Data),
	}

	return r.ensureReplicatedSecret(ctx, desired)
}

func (r *FabricNetworkReconciler) ensureReplicatedSecret(ctx context.Context, desired *corev1.Secret) error {
	var existing corev1.Secret
	key := client.ObjectKeyFromObject(desired)

	err := r.Get(ctx, key, &existing)
	if apierrors.IsNotFound(err) {
		log := logf.FromContext(ctx)
		log.Info("Creating Secret", "name", desired.Name, "namespace", desired.Namespace)
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	return r.updateObjectWithRetry(ctx, desired, func(object client.Object) (bool, error) {
		existing := object.(*corev1.Secret)
		changed := mergeLabels(&existing.Labels, desired.Labels)
		if mergeAnnotations(&existing.Annotations, desired.Annotations) {
			changed = true
		}
		if existing.Type != desired.Type {
			existing.Type = desired.Type
			changed = true
		}
		if !reflect.DeepEqual(existing.Data, desired.Data) {
			existing.Data = desired.Data
			changed = true
		}
		if !changed {
			return false, nil
		}

		log := logf.FromContext(ctx)
		log.Info("Updating Secret", "name", desired.Name, "namespace", desired.Namespace)
		return true, nil
	})
}

func copySecretData(data map[string][]byte) map[string][]byte {
	if data == nil {
		return nil
	}

	out := make(map[string][]byte, len(data))
	for key, value := range data {
		out[key] = append([]byte(nil), value...)
	}

	return out
}

func copyStringData(data map[string]string) map[string]string {
	if data == nil {
		return nil
	}

	out := make(map[string]string, len(data))
	maps.Copy(out, data)

	return out
}

func (r *FabricNetworkReconciler) channelBlockReadiness(
	ctx context.Context,
	namespace string,
	channelName string,
) (bool, string, error) {
	var blockConfigMap corev1.ConfigMap
	err := r.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      channelBlockConfigMapName(channelName),
	}, &blockConfigMap)
	if err == nil {
		if _, ok := blockConfigMap.BinaryData[channelBlockFileName(channelName)]; ok {
			return true, "", nil
		}
		if _, ok := blockConfigMap.Data[channelBlockFileName(channelName)]; ok {
			return true, "", nil
		}
		return false, "Channel block ConfigMap is missing the block file", nil
	}
	if !apierrors.IsNotFound(err) {
		return false, "", err
	}

	var job batchv1.Job
	err = r.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      channelBlockJobName(channelName),
	}, &job)
	if apierrors.IsNotFound(err) {
		return false, "Waiting for channel block generation Job", nil
	}
	if err != nil {
		return false, "", err
	}
	if jobFailed(job) {
		return false, "Channel block generation Job failed", nil
	}
	if job.Status.Succeeded > 0 {
		return false, "Waiting for channel block ConfigMap to be published", nil
	}

	return false, "Waiting for channel block generation Job", nil
}

func channelStatusesEqual(a, b []fabricopsv1alpha1.ChannelStatus) bool {
	return reflect.DeepEqual(a, b)
}

func desiredNetworkOrdererStatus(net *fabricopsv1alpha1.FabricNetwork) fabricopsv1alpha1.WorkloadStatus {
	status := fabricopsv1alpha1.WorkloadStatus{}
	for _, org := range net.Spec.Orgs {
		status.Desired += desiredOrdererStatus(org).Desired
	}
	return status
}

type ordererInstance struct {
	org       fabricopsv1alpha1.Org
	group     fabricopsv1alpha1.OrdererGroup
	name      string
	namespace string
}

type peerInstance struct {
	org       fabricopsv1alpha1.Org
	name      string
	namespace string
}

func desiredOrdererInstances(net *fabricopsv1alpha1.FabricNetwork) []ordererInstance {
	instances := []ordererInstance{}
	for _, org := range net.Spec.Orgs {
		namespace := orgNamespaceName(net, org)
		for _, group := range org.Orderers {
			for i := 0; i < group.Instances; i++ {
				instances = append(instances, ordererInstance{
					org:       org,
					group:     group,
					name:      sanitizeName(fmt.Sprintf("%s%d", group.Prefix, i)),
					namespace: namespace,
				})
			}
		}
	}

	return instances
}

func desiredOrdererOrgs(net *fabricopsv1alpha1.FabricNetwork) []fabricopsv1alpha1.Org {
	orgs := []fabricopsv1alpha1.Org{}
	for _, org := range net.Spec.Orgs {
		if desiredOrdererStatus(org).Desired > 0 {
			orgs = append(orgs, org)
		}
	}

	return orgs
}

func orgsByName(net *fabricopsv1alpha1.FabricNetwork) map[string]fabricopsv1alpha1.Org {
	orgs := map[string]fabricopsv1alpha1.Org{}
	for _, org := range net.Spec.Orgs {
		orgs[org.Organization.Name] = org
	}
	return orgs
}

func readyOrgsByName(statuses []fabricopsv1alpha1.OrgStatus) map[string]bool {
	orgs := map[string]bool{}
	for _, status := range statuses {
		orgs[status.Name] = status.Ready
	}
	return orgs
}

func unknownChannelPeers(org fabricopsv1alpha1.Org, peers []string) []string {
	desiredPeers := desiredPeerNames(org)
	unknown := []string{}

	for _, peer := range peers {
		if _, ok := desiredPeers[peer]; !ok {
			unknown = append(unknown, peer)
		}
	}

	return unknown
}

func desiredPeerNames(org fabricopsv1alpha1.Org) map[string]struct{} {
	peers := map[string]struct{}{}
	if org.Peer == nil {
		return peers
	}

	for i := 0; i < org.Peer.Instances; i++ {
		name := sanitizeName(fmt.Sprintf("%s%d", org.Peer.Prefix, i))
		peers[name] = struct{}{}
	}

	return peers
}

func allChannelsReady(statuses []fabricopsv1alpha1.ChannelStatus) bool {
	for _, status := range statuses {
		if !status.Ready {
			return false
		}
	}

	return true
}

func channelStatusMessage(statuses []fabricopsv1alpha1.ChannelStatus) string {
	if len(statuses) == 0 {
		return "No channels declared"
	}

	messages := []string{}
	for _, status := range statuses {
		if !status.Ready && status.Message != "" {
			messages = append(messages, status.Name+": "+status.Message)
		}
	}
	if len(messages) == 0 {
		return "All declared channels are ready"
	}

	return strings.Join(messages, "; ")
}

func buildChannelConfigMap(
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	hostOrg fabricopsv1alpha1.Org,
	namespace string,
) (*corev1.ConfigMap, error) {
	configtx, err := buildConfigtxYAML(net, channel)
	if err != nil {
		return nil, err
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:        channelConfigMapName(channel.Name),
			Namespace:   namespace,
			Labels:      channelLabels(net, hostOrg, channel.Name),
			Annotations: resourceAnnotations(net, hostOrg),
		},
		Data: map[string]string{
			"configtx.yaml": configtx,
		},
	}, nil
}

func buildChannelServiceAccount(
	net *fabricopsv1alpha1.FabricNetwork,
	hostOrg fabricopsv1alpha1.Org,
	channelName string,
	namespace string,
) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:        channelServiceAccountName(channelName),
			Namespace:   namespace,
			Labels:      channelLabels(net, hostOrg, channelName),
			Annotations: resourceAnnotations(net, hostOrg),
		},
	}
}

func buildChannelRole(
	net *fabricopsv1alpha1.FabricNetwork,
	hostOrg fabricopsv1alpha1.Org,
	channelName string,
	namespace string,
) *rbacv1.Role {
	return &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:        channelServiceAccountName(channelName),
			Namespace:   namespace,
			Labels:      channelLabels(net, hostOrg, channelName),
			Annotations: resourceAnnotations(net, hostOrg),
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"configmaps"},
				Verbs:     []string{"get", "create", "update", "patch"},
			},
		},
	}
}

func buildChannelRoleBinding(
	net *fabricopsv1alpha1.FabricNetwork,
	hostOrg fabricopsv1alpha1.Org,
	channelName string,
	namespace string,
) *rbacv1.RoleBinding {
	name := channelServiceAccountName(channelName)

	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      channelLabels(net, hostOrg, channelName),
			Annotations: resourceAnnotations(net, hostOrg),
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      name,
				Namespace: namespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     name,
		},
	}
}

func buildChannelBlockJob(
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	hostOrg fabricopsv1alpha1.Org,
	namespace string,
) *batchv1.Job {
	labels := channelLabels(net, hostOrg, channel.Name)
	annotations := resourceAnnotations(net, hostOrg)
	backoffLimit := int32(4)
	volumes := []corev1.Volume{
		{
			Name: channelConfigVolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: channelConfigMapName(channel.Name),
					},
				},
			},
		},
		{
			Name: channelOutputVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	}
	generateMounts := []corev1.VolumeMount{
		{Name: channelConfigVolumeName, MountPath: channelConfigDir, ReadOnly: true},
		{Name: channelOutputVolumeName, MountPath: channelOutputDir},
	}

	for _, org := range channelConfigOrganizations(net, channel) {
		volumeName := channelOrgMSPVolumeName(org)
		volumes = append(volumes, corev1.Volume{
			Name: volumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  channelOrgMSPSecretName(channel.Name, org),
					Items:       mspSecretItems(net.Spec.Global.TLS),
					DefaultMode: secretVolumeDefaultMode(),
				},
			},
		})
		generateMounts = append(generateMounts, corev1.VolumeMount{
			Name:      volumeName,
			MountPath: channelOrgMSPPath(org),
			ReadOnly:  true,
		})
	}

	for _, orderer := range desiredOrdererInstances(net) {
		volumeName := channelOrdererTLSVolumeName(orderer.name)
		volumes = append(volumes, corev1.Volume{
			Name: volumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  channelOrdererTLSSecretName(channel.Name, orderer.name),
					Items:       tlsSecretItems(),
					DefaultMode: secretVolumeDefaultMode(),
				},
			},
		})
		generateMounts = append(generateMounts, corev1.VolumeMount{
			Name:      volumeName,
			MountPath: channelOrdererTLSPath(orderer.name),
			ReadOnly:  true,
		})
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        channelBlockJobName(channel.Name),
			Namespace:   namespace,
			Labels:      labels,
			Annotations: succeededJobCleanupAnnotations(annotations),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: resourceAnnotations(net, hostOrg),
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: channelServiceAccountName(channel.Name),
					RestartPolicy:      corev1.RestartPolicyNever,
					Volumes:            volumes,
					InitContainers: []corev1.Container{
						{
							Name:         generateChannelBlockContainer,
							Image:        fabricToolsImage(net.Spec.Global.FabricVersion),
							Command:      []string{"sh", "-ec", generateChannelBlockScript(channel.Name)},
							Resources:    componentResourceRequirements(componentOrderer),
							VolumeMounts: generateMounts,
						},
					},
					Containers: []corev1.Container{
						{
							Name:      publishChannelBlockContainer,
							Image:     kubectlImage(),
							Command:   []string{"sh", "-ec", publishChannelBlockScript()},
							Env:       publishChannelBlockEnv(channel.Name),
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

func buildOrdererJoinJob(
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	hostOrg fabricopsv1alpha1.Org,
	namespace string,
	orderer ordererInstance,
) *batchv1.Job {
	labels := channelLabels(net, hostOrg, channel.Name)
	labels[labelInstance] = orderer.name
	labels[labelWorkload] = orderer.name
	annotations := resourceAnnotations(net, hostOrg)
	backoffLimit := int32(4)
	adminTLSVolumeName := channelOrdererAdminTLSVolumeName(orderer.org)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        channelOrdererJoinJobName(channel.Name, orderer),
			Namespace:   namespace,
			Labels:      labels,
			Annotations: succeededJobCleanupAnnotations(annotations),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: resourceAnnotations(net, hostOrg),
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
							Name: channelBlockVolumeName,
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: channelBlockConfigMapName(channel.Name),
									},
									Items: []corev1.KeyToPath{
										{
											Key:  channelBlockFileName(channel.Name),
											Path: channelBlockFileName(channel.Name),
										},
									},
								},
							},
						},
						{
							Name: adminTLSVolumeName,
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName:  channelOrdererAdminTLSSecretName(channel.Name, orderer.org),
									Items:       adminTLSSecretItems(),
									DefaultMode: secretVolumeDefaultMode(),
								},
							},
						},
					},
					InitContainers: []corev1.Container{
						{
							Name:      joinOrdererContainer,
							Image:     fabricToolsImage(net.Spec.Global.FabricVersion),
							Command:   []string{"sh", "-ec", joinOrdererScript(channel.Name, ordererAdminAddress(orderer), channelOrdererAdminTLSPath(orderer.org))},
							Resources: componentResourceRequirements(componentOrderer),
							VolumeMounts: []corev1.VolumeMount{
								{Name: channelOutputVolumeName, MountPath: channelOutputDir},
								{Name: channelBlockVolumeName, MountPath: channelBlockDir, ReadOnly: true},
								{Name: adminTLSVolumeName, MountPath: channelOrdererAdminTLSPath(orderer.org), ReadOnly: true},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:      publishOrdererJoinContainer,
							Image:     kubectlImage(),
							Command:   []string{"sh", "-ec", publishOrdererJoinResultScript()},
							Env:       publishOrdererJoinResultEnv(channel.Name, orderer),
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

func buildPeerJoinJob(
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	org fabricopsv1alpha1.Org,
	namespace string,
	peer peerInstance,
) *batchv1.Job {
	labels := channelLabels(net, org, channel.Name)
	labels[labelInstance] = peer.name
	labels[labelWorkload] = peer.name
	annotations := resourceAnnotations(net, org)
	backoffLimit := int32(4)
	adminMSPVolumeName := channelOrgMSPVolumeName(org)
	adminTLSVolumeName := channelOrdererAdminTLSVolumeName(org)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        channelPeerJoinJobName(channel.Name, peer),
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
							Name: channelBlockVolumeName,
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: channelBlockConfigMapName(channel.Name),
									},
									Items: []corev1.KeyToPath{
										{
											Key:  channelBlockFileName(channel.Name),
											Path: channelBlockFileName(channel.Name),
										},
									},
								},
							},
						},
						{
							Name: adminMSPVolumeName,
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName:  identitySecretName(adminIdentityName(org), secretKindMSP),
									Items:       mspSecretItems(net.Spec.Global.TLS),
									DefaultMode: secretVolumeDefaultMode(),
								},
							},
						},
						{
							Name: adminTLSVolumeName,
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName:  identitySecretName(adminIdentityName(org), secretKindTLS),
									Items:       adminTLSSecretItems(),
									DefaultMode: secretVolumeDefaultMode(),
								},
							},
						},
					},
					InitContainers: []corev1.Container{
						{
							Name:      joinPeerContainer,
							Image:     fabricToolsImage(net.Spec.Global.FabricVersion),
							Command:   []string{"sh", "-ec", joinPeerScript(channel.Name, org.Organization.MSPName, peerAddress(peer), channelOrgMSPPath(org), channelOrdererAdminTLSPath(org))},
							Resources: componentResourceRequirements(componentPeer),
							VolumeMounts: []corev1.VolumeMount{
								{Name: channelOutputVolumeName, MountPath: channelOutputDir},
								{Name: channelBlockVolumeName, MountPath: channelBlockDir, ReadOnly: true},
								{Name: adminMSPVolumeName, MountPath: channelOrgMSPPath(org), ReadOnly: true},
								{Name: adminTLSVolumeName, MountPath: channelOrdererAdminTLSPath(org), ReadOnly: true},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:      publishPeerJoinContainer,
							Image:     kubectlImage(),
							Command:   []string{"sh", "-ec", publishPeerJoinResultScript()},
							Env:       publishPeerJoinResultEnv(channel.Name, peer),
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

func buildAnchorPeerUpdateJob(
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	org fabricopsv1alpha1.Org,
	namespace string,
	anchorPeer peerInstance,
	orderer ordererInstance,
) *batchv1.Job {
	labels := channelLabels(net, org, channel.Name)
	labels[labelInstance] = anchorPeer.name
	labels[labelWorkload] = anchorPeer.name
	annotations := resourceAnnotations(net, org)
	backoffLimit := int32(4)
	adminMSPVolumeName := channelOrgMSPVolumeName(org)
	adminTLSVolumeName := channelOrdererAdminTLSVolumeName(org)
	ordererTLSVolumeName := channelOrdererTLSVolumeName(orderer.name)
	volumes := []corev1.Volume{
		{
			Name: channelOutputVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
		{
			Name: adminMSPVolumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  identitySecretName(adminIdentityName(org), secretKindMSP),
					Items:       mspSecretItems(net.Spec.Global.TLS),
					DefaultMode: secretVolumeDefaultMode(),
				},
			},
		},
		{
			Name: adminTLSVolumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  identitySecretName(adminIdentityName(org), secretKindTLS),
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
	}
	volumeMounts := []corev1.VolumeMount{
		{Name: channelOutputVolumeName, MountPath: channelOutputDir},
		{Name: adminMSPVolumeName, MountPath: channelOrgMSPPath(org), ReadOnly: true},
		{Name: adminTLSVolumeName, MountPath: channelOrdererAdminTLSPath(org), ReadOnly: true},
		{Name: ordererTLSVolumeName, MountPath: channelOrdererTLSPath(orderer.name), ReadOnly: true},
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        channelAnchorPeerUpdateJobName(channel.Name, org),
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
					ServiceAccountName: channelServiceAccountName(channel.Name),
					RestartPolicy:      corev1.RestartPolicyNever,
					Volumes:            volumes,
					InitContainers: []corev1.Container{
						{
							Name:  updateAnchorPeerContainer,
							Image: fabricToolsImage(net.Spec.Global.FabricVersion),
							Command: []string{"sh", "-ec", updateAnchorPeerScript(
								channel.Name,
								org.Organization.MSPName,
								peerAddress(anchorPeer),
								channelPeerHost(anchorPeer),
								channelPeerPort(anchorPeer),
								ordererClientAddress(orderer),
								channelOrgMSPPath(org),
								channelOrdererAdminTLSPath(org),
								channelOrdererTLSPath(orderer.name),
								anchorPeerUpdateFilePath(channel.Name, org),
							)},
							Resources:    componentResourceRequirements(componentPeer),
							VolumeMounts: volumeMounts,
						},
					},
					Containers: []corev1.Container{
						{
							Name:      publishAnchorPeerContainer,
							Image:     kubectlImage(),
							Command:   []string{"sh", "-ec", publishAnchorPeerUpdateResultScript()},
							Env:       publishAnchorPeerUpdateResultEnv(channel.Name, org),
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

func (r *FabricNetworkReconciler) ensureConfigMap(ctx context.Context, desired *corev1.ConfigMap) error {
	var existing corev1.ConfigMap
	key := client.ObjectKeyFromObject(desired)

	err := r.Get(ctx, key, &existing)
	if apierrors.IsNotFound(err) {
		log := logf.FromContext(ctx)
		log.Info("Creating ConfigMap", "name", desired.Name, "namespace", desired.Namespace)
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	return r.updateObjectWithRetry(ctx, desired, func(object client.Object) (bool, error) {
		existing := object.(*corev1.ConfigMap)
		changed := mergeLabels(&existing.Labels, desired.Labels)
		if mergeAnnotations(&existing.Annotations, desired.Annotations) {
			changed = true
		}
		if !reflect.DeepEqual(existing.Data, desired.Data) {
			existing.Data = desired.Data
			changed = true
		}
		if !reflect.DeepEqual(existing.BinaryData, desired.BinaryData) {
			existing.BinaryData = desired.BinaryData
			changed = true
		}
		if !changed {
			return false, nil
		}

		log := logf.FromContext(ctx)
		log.Info("Updating ConfigMap", "name", desired.Name, "namespace", desired.Namespace)
		return true, nil
	})
}

func buildConfigtxYAML(net *fabricopsv1alpha1.FabricNetwork, channel fabricopsv1alpha1.Channel) (string, error) {
	orderers := desiredOrdererInstances(net)
	if len(orderers) == 0 {
		return "", fmt.Errorf("channel %q requires at least one orderer", channel.Name)
	}

	capabilities := fabricCapabilities(net.Spec.Global.FabricVersion)
	ordererOrgs := desiredOrdererOrgs(net)
	channelOrgs := channelPeerOrganizations(net, channel)
	configOrgs := channelConfigOrganizations(net, channel)
	ordererEndpoints := channelOrdererEndpoints(orderers)
	anchorPeers := channelAnchorPeersByOrg(net, channel)
	consensus := ordererConsensusType(orderers[0].group.Type)

	var b strings.Builder
	b.WriteString("Capabilities:\n")
	fmt.Fprintf(&b, "  Channel: &ChannelCapabilities\n    %s: true\n", capabilities.channel)
	fmt.Fprintf(&b, "  Orderer: &OrdererCapabilities\n    %s: true\n", capabilities.orderer)
	fmt.Fprintf(&b, "  Application: &ApplicationCapabilities\n    %s: true\n\n", capabilities.application)

	b.WriteString("Channel: &ChannelDefaults\n")
	b.WriteString("  Policies:\n")
	b.WriteString("    Readers:\n      Type: ImplicitMeta\n      Rule: \"ANY Readers\"\n")
	b.WriteString("    Writers:\n      Type: ImplicitMeta\n      Rule: \"ANY Writers\"\n")
	b.WriteString("    Admins:\n      Type: ImplicitMeta\n      Rule: \"MAJORITY Admins\"\n")
	b.WriteString("  Capabilities:\n    <<: *ChannelCapabilities\n\n")

	b.WriteString("Organizations:\n")
	for _, org := range configOrgs {
		fmt.Fprintf(&b, "  - &%s\n", configtxOrgAnchorName(org))
		fmt.Fprintf(&b, "    Name: %s\n", org.Organization.MSPName)
		fmt.Fprintf(&b, "    ID: %s\n", org.Organization.MSPName)
		fmt.Fprintf(&b, "    MSPDir: %s\n", channelOrgMSPPath(org))
		if capabilities.isV3 {
			b.WriteString("    OrdererEndpoints:\n")
			for _, endpoint := range ordererEndpoints {
				fmt.Fprintf(&b, "      - %s\n", endpoint)
			}
		}
		if peers := anchorPeers[org.Organization.Name]; len(peers) > 0 {
			b.WriteString("    AnchorPeers:\n")
			for _, peer := range peers {
				fmt.Fprintf(&b, "      - Host: %s\n", channelPeerHost(peer))
				fmt.Fprintf(&b, "        Port: %d\n", channelPeerPort(peer))
			}
		}
		b.WriteString("    Policies:\n")
		fmt.Fprintf(&b, "      Readers:\n        Type: Signature\n        Rule: \"OR('%s.member')\"\n", org.Organization.MSPName)
		fmt.Fprintf(&b, "      Writers:\n        Type: Signature\n        Rule: \"OR('%s.member')\"\n", org.Organization.MSPName)
		fmt.Fprintf(&b, "      Admins:\n        Type: Signature\n        Rule: \"OR('%s.admin')\"\n", org.Organization.MSPName)
		fmt.Fprintf(&b, "      Endorsement:\n        Type: Signature\n        Rule: \"OR('%s.member')\"\n\n", org.Organization.MSPName)
	}

	b.WriteString("Application: &ApplicationDefaults\n")
	b.WriteString("  Organizations:\n")
	b.WriteString("  Policies:\n")
	b.WriteString("    Readers:\n      Type: ImplicitMeta\n      Rule: \"ANY Readers\"\n")
	b.WriteString("    Writers:\n      Type: ImplicitMeta\n      Rule: \"ANY Writers\"\n")
	b.WriteString("    Admins:\n      Type: ImplicitMeta\n      Rule: \"MAJORITY Admins\"\n")
	b.WriteString("    Endorsement:\n      Type: ImplicitMeta\n      Rule: \"MAJORITY Endorsement\"\n")
	b.WriteString("  Capabilities:\n    <<: *ApplicationCapabilities\n\n")

	b.WriteString("Orderer: &OrdererDefaults\n")
	fmt.Fprintf(&b, "  OrdererType: %s\n", consensus)
	if !capabilities.isV3 {
		b.WriteString("  Addresses:\n")
		for _, endpoint := range ordererEndpoints {
			fmt.Fprintf(&b, "    - %s\n", endpoint)
		}
	}
	if consensus == "etcdraft" {
		b.WriteString("  EtcdRaft:\n    Consenters:\n")
		for _, orderer := range orderers {
			fmt.Fprintf(&b, "      - Host: %s\n", channelOrdererHost(orderer))
			fmt.Fprintf(&b, "        Port: %d\n", channelOrdererPort(orderer))
			fmt.Fprintf(&b, "        ClientTLSCert: %s/server.crt\n", channelOrdererTLSPath(orderer.name))
			fmt.Fprintf(&b, "        ServerTLSCert: %s/server.crt\n", channelOrdererTLSPath(orderer.name))
		}
	}
	b.WriteString("  BatchTimeout: 2s\n")
	b.WriteString("  BatchSize:\n    MaxMessageCount: 10\n    AbsoluteMaxBytes: 99 MB\n    PreferredMaxBytes: 512 KB\n")
	b.WriteString("  Organizations:\n")
	b.WriteString("  Policies:\n")
	b.WriteString("    Readers:\n      Type: ImplicitMeta\n      Rule: \"ANY Readers\"\n")
	b.WriteString("    Writers:\n      Type: ImplicitMeta\n      Rule: \"ANY Writers\"\n")
	b.WriteString("    Admins:\n      Type: ImplicitMeta\n      Rule: \"MAJORITY Admins\"\n")
	b.WriteString("    BlockValidation:\n      Type: ImplicitMeta\n      Rule: \"ANY Writers\"\n")
	b.WriteString("  Capabilities:\n    <<: *OrdererCapabilities\n\n")

	b.WriteString("Profiles:\n")
	fmt.Fprintf(&b, "  %s:\n", channelProfileName(channel.Name))
	b.WriteString("    <<: *ChannelDefaults\n")
	b.WriteString("    Orderer:\n")
	b.WriteString("      <<: *OrdererDefaults\n")
	b.WriteString("      Organizations:\n")
	for _, org := range ordererOrgs {
		fmt.Fprintf(&b, "        - *%s\n", configtxOrgAnchorName(org))
	}
	b.WriteString("      Capabilities:\n        <<: *OrdererCapabilities\n")
	b.WriteString("    Application:\n")
	b.WriteString("      <<: *ApplicationDefaults\n")
	b.WriteString("      Organizations:\n")
	for _, org := range channelOrgs {
		fmt.Fprintf(&b, "        - *%s\n", configtxOrgAnchorName(org))
	}

	return b.String(), nil
}

type fabricCapabilitySet struct {
	channel     string
	orderer     string
	application string
	isV3        bool
}

func fabricCapabilities(version string) fabricCapabilitySet {
	v := strings.TrimPrefix(strings.TrimSpace(version), "v")
	if strings.HasPrefix(v, "3.") {
		return fabricCapabilitySet{channel: "V2_0", orderer: "V2_0", application: "V2_5", isV3: true}
	}
	if strings.HasPrefix(v, "2.5.") || v == "2.5" {
		return fabricCapabilitySet{channel: "V2_0", orderer: "V2_0", application: "V2_5"}
	}

	return fabricCapabilitySet{channel: "V2_0", orderer: "V2_0", application: "V2_0"}
}

func channelConfigOrganizations(
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
) []fabricopsv1alpha1.Org {
	orgs := []fabricopsv1alpha1.Org{}
	seen := map[string]struct{}{}
	for _, org := range desiredOrdererOrgs(net) {
		orgs = append(orgs, org)
		seen[org.Organization.Name] = struct{}{}
	}
	for _, org := range channelPeerOrganizations(net, channel) {
		if _, ok := seen[org.Organization.Name]; ok {
			continue
		}
		orgs = append(orgs, org)
		seen[org.Organization.Name] = struct{}{}
	}

	return orgs
}

func channelPeerOrganizations(
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
) []fabricopsv1alpha1.Org {
	orgs := []fabricopsv1alpha1.Org{}
	allOrgs := orgsByName(net)
	for _, channelOrg := range channel.Orgs {
		if org, ok := allOrgs[channelOrg.Name]; ok {
			orgs = append(orgs, org)
		}
	}

	return orgs
}

func channelAnchorPeersByOrg(
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
) map[string][]peerInstance {
	anchors := map[string][]peerInstance{}
	allOrgs := orgsByName(net)

	for _, channelOrg := range channel.Orgs {
		org, ok := allOrgs[channelOrg.Name]
		if !ok || len(channelOrg.Peers) == 0 {
			continue
		}

		anchorPeer := peerInstance{
			org:       org,
			name:      channelOrg.Peers[0],
			namespace: orgNamespaceName(net, org),
		}
		anchors[org.Organization.Name] = []peerInstance{anchorPeer}
	}

	return anchors
}

func channelAnchorPeerForOrg(
	net *fabricopsv1alpha1.FabricNetwork,
	channel fabricopsv1alpha1.Channel,
	org fabricopsv1alpha1.Org,
) (peerInstance, bool) {
	anchors := channelAnchorPeersByOrg(net, channel)
	peers := anchors[org.Organization.Name]
	if len(peers) == 0 {
		return peerInstance{}, false
	}

	return peers[0], true
}

func channelOrdererEndpoints(orderers []ordererInstance) []string {
	endpoints := make([]string, 0, len(orderers))
	for _, orderer := range orderers {
		endpoints = append(endpoints, fmt.Sprintf("%s:%d", channelOrdererHost(orderer), channelOrdererPort(orderer)))
	}

	return endpoints
}

func channelOrdererHost(orderer ordererInstance) string {
	return ordererAdvertisedHost(orderer)
}

func channelOrdererPort(orderer ordererInstance) int32 {
	return ordererAdvertisedPort(orderer)
}

func channelPeerHost(peer peerInstance) string {
	return peerAdvertisedHost(peer)
}

func channelPeerPort(peer peerInstance) int32 {
	return peerAdvertisedPort(peer)
}

func ordererConsensusType(groupType string) string {
	switch strings.ToLower(strings.TrimSpace(groupType)) {
	case "raft", "etcdraft", "":
		return "etcdraft"
	default:
		return groupType
	}
}

func channelLabels(
	net *fabricopsv1alpha1.FabricNetwork,
	hostOrg fabricopsv1alpha1.Org,
	channelName string,
) map[string]string {
	labels := orgLabels(net, hostOrg, componentChannel)
	labels[labelChannel] = sanitizeName(channelName)

	return labels
}

func channelConfigMapName(channelName string) string {
	return sanitizeName(channelName + "-configtx")
}

func channelBlockConfigMapName(channelName string) string {
	return sanitizeName(channelName + "-channel-block")
}

func channelBlockJobName(channelName string) string {
	return sanitizeName(channelName + "-channel-block")
}

func channelOrdererJoinJobName(channelName string, orderer ordererInstance) string {
	return sanitizeName(channelName + "-" + orderer.org.Organization.Name + "-" + orderer.name + "-orderer-join")
}

func channelOrdererJoinResultConfigMapName(channelName string, orderer ordererInstance) string {
	return sanitizeName(channelOrdererJoinJobName(channelName, orderer) + "-result")
}

func channelPeerJoinJobName(channelName string, peer peerInstance) string {
	return sanitizeName(channelName + "-" + peer.org.Organization.Name + "-" + peer.name + "-peer-join")
}

func channelPeerJoinResultConfigMapName(channelName string, peer peerInstance) string {
	return sanitizeName(channelPeerJoinJobName(channelName, peer) + "-result")
}

func channelAnchorPeerUpdateJobName(channelName string, org fabricopsv1alpha1.Org) string {
	return sanitizeName(channelName + "-" + org.Organization.Name + "-anchor-peer-update")
}

func channelAnchorPeerUpdateResultConfigMapName(channelName string, org fabricopsv1alpha1.Org) string {
	return sanitizeName(channelAnchorPeerUpdateJobName(channelName, org) + "-result")
}

func channelServiceAccountName(channelName string) string {
	return sanitizeName(channelName + "-" + channelServiceAccount)
}

func channelBlockFileName(channelName string) string {
	return sanitizeName(channelName) + ".block"
}

func channelBlockFilePath(channelName string) string {
	return channelBlockDir + "/" + channelBlockFileName(channelName)
}

func anchorPeerUpdateFileName(channelName string, org fabricopsv1alpha1.Org) string {
	return fmt.Sprintf("%sanchors-%s.tx", org.Organization.MSPName, sanitizeName(channelName))
}

func anchorPeerUpdateFilePath(channelName string, org fabricopsv1alpha1.Org) string {
	return channelOutputDir + "/" + anchorPeerUpdateFileName(channelName, org)
}

func channelProfileName(channelName string) string {
	return configtxIdentifier(channelName)
}

func configtxOrgAnchorName(org fabricopsv1alpha1.Org) string {
	return configtxIdentifier(org.Organization.Name)
}

func configtxIdentifier(value string) string {
	sanitized := sanitizeName(value)
	parts := strings.Split(sanitized, "-")
	var b strings.Builder
	for _, part := range parts {
		if part == "" {
			continue
		}
		b.WriteString(strings.ToUpper(part[:1]))
		if len(part) > 1 {
			b.WriteString(part[1:])
		}
	}
	if b.Len() == 0 {
		return "Resource"
	}

	out := b.String()
	if out[0] >= '0' && out[0] <= '9' {
		return "X" + out
	}

	return out
}

func channelOrgMSPVolumeName(org fabricopsv1alpha1.Org) string {
	return sanitizeName("msp-" + org.Organization.Name)
}

func channelOrdererTLSVolumeName(ordererName string) string {
	return sanitizeName("tls-" + ordererName)
}

func channelOrdererAdminTLSVolumeName(org fabricopsv1alpha1.Org) string {
	return sanitizeName("admin-tls-" + org.Organization.Name)
}

func channelOrgMSPSecretName(channelName string, org fabricopsv1alpha1.Org) string {
	return sanitizeName(channelName + "-" + org.Organization.Name + "-admin-msp")
}

func channelOrdererAdminTLSSecretName(channelName string, org fabricopsv1alpha1.Org) string {
	return sanitizeName(channelName + "-" + org.Organization.Name + "-admin-tls")
}

func channelOrdererTLSSecretName(channelName string, ordererName string) string {
	return sanitizeName(channelName + "-" + ordererName + "-tls")
}

func channelOrgMSPPath(org fabricopsv1alpha1.Org) string {
	return channelCryptoDir + "/orgs/" + sanitizeName(org.Organization.Name) + "/msp"
}

func channelOrdererTLSPath(ordererName string) string {
	return channelCryptoDir + "/orderers/" + sanitizeName(ordererName) + "/tls"
}

func channelOrdererAdminTLSPath(org fabricopsv1alpha1.Org) string {
	return channelAdminTLSDir + "/" + sanitizeName(org.Organization.Name)
}

func ordererAdminAddress(orderer ordererInstance) string {
	return serviceDNS(orderer.name, orderer.namespace, ordererAdminPort)
}

func ordererClientAddress(orderer ordererInstance) string {
	return serviceDNS(orderer.name, orderer.namespace, ordererPort)
}

func peerAddress(peer peerInstance) string {
	return serviceDNS(peer.name, peer.namespace, peerPort)
}

func adminTLSSecretItems() []corev1.KeyToPath {
	return []corev1.KeyToPath{
		{Key: tlsCACertKey, Path: "ca.crt"},
		{Key: tlsClientCertKey, Path: "client.crt"},
		{Key: tlsClientKeyKey, Path: "client.key"},
	}
}

func fabricToolsImage(version string) string {
	if version == "" {
		version = "2.5.12"
	}
	if strings.HasPrefix(strings.TrimPrefix(strings.TrimSpace(version), "v"), "3.") {
		version = "2.5.14"
	}

	return fmt.Sprintf("hyperledger/fabric-tools:%s", version)
}

func generateChannelBlockScript(channelName string) string {
	return fmt.Sprintf(`set -eu

mkdir -p %s
configtxgen \
  --configPath %s \
  -profile %s \
  -outputBlock %s/%s \
  -channelID %s
`, channelOutputDir, channelConfigDir, channelProfileName(channelName), channelOutputDir, channelBlockFileName(channelName), channelName)
}

func joinOrdererScript(channelName string, ordererAddress string, adminTLSPath string) string {
	return fmt.Sprintf(`set -eu

CHANNEL_ID=%q
ORDERER_ADMIN_ADDRESS=%q
BLOCK_FILE=%q
ADMIN_TLS_DIR=%q
OUTPUT_DIR=%q
CHANNELS_FILE="$OUTPUT_DIR/%s"

mkdir -p "$OUTPUT_DIR"

channel_status() {
  osnadmin channel list \
    -o "$ORDERER_ADMIN_ADDRESS" \
    --channelID "$CHANNEL_ID" \
    --ca-file "$ADMIN_TLS_DIR/ca.crt" \
    --client-cert "$ADMIN_TLS_DIR/client.crt" \
    --client-key "$ADMIN_TLS_DIR/client.key" \
    --no-status > "$CHANNELS_FILE"

  sed -n '/^[[:space:]]*[{[]/,$p' "$CHANNELS_FILE" | jq -r --arg channel "$CHANNEL_ID" '
    if (.name? == $channel) then
      (.status // "")
    else
      (.channels // [] | map(select(.name == $channel)) | .[0].status // "")
    end
  '
}

status=""
if status="$(channel_status 2>/tmp/fabricops-osnadmin-list.err)" && [ "$status" = "active" ]; then
  echo "Orderer already joined active channel $CHANNEL_ID"
  exit 0
fi

if [ -z "$status" ]; then
  osnadmin channel join \
    --channelID "$CHANNEL_ID" \
    --config-block "$BLOCK_FILE" \
    -o "$ORDERER_ADMIN_ADDRESS" \
    --client-cert "$ADMIN_TLS_DIR/client.crt" \
    --client-key "$ADMIN_TLS_DIR/client.key" \
    --ca-file "$ADMIN_TLS_DIR/ca.crt"
fi

n=1
until status="$(channel_status)" && [ "$status" = "active" ]; do
  if [ "$n" -ge 60 ]; then
    echo "Orderer channel $CHANNEL_ID did not become active; last status: ${status:-missing}"
    exit 1
  fi
  echo "Waiting for orderer channel $CHANNEL_ID to become active; last status: ${status:-missing}"
  n=$((n + 1))
  sleep 5
done
`, channelName, ordererAddress, channelBlockFilePath(channelName), adminTLSPath, channelOutputDir, channelOrdererJoinResultFile)
}

func joinPeerScript(channelName string, mspID string, peerAddress string, mspPath string, adminTLSPath string) string {
	return fmt.Sprintf(`set -eu

CHANNEL_ID=%q
BLOCK_FILE=%q
ADMIN_TLS_DIR=%q
OUTPUT_DIR=%q
CHANNELS_FILE="$OUTPUT_DIR/%s"

mkdir -p "$OUTPUT_DIR"

export CORE_PEER_LOCALMSPID=%q
export CORE_PEER_ADDRESS=%q
export CORE_PEER_MSPCONFIGPATH=%q
export CORE_PEER_TLS_ENABLED=true
export CORE_PEER_TLS_CERT_FILE="$ADMIN_TLS_DIR/client.crt"
export CORE_PEER_TLS_KEY_FILE="$ADMIN_TLS_DIR/client.key"
export CORE_PEER_TLS_ROOTCERT_FILE="$ADMIN_TLS_DIR/ca.crt"

if peer channel list --tls --cafile "$CORE_PEER_TLS_ROOTCERT_FILE" > "$CHANNELS_FILE" 2>/tmp/fabricops-peer-channel-list.err; then
  if grep -Eq '(^|[[:space:]])'"$CHANNEL_ID"'($|[[:space:]])' "$CHANNELS_FILE"; then
    echo "Peer already joined channel $CHANNEL_ID"
    exit 0
  fi
fi

peer channel join \
  -b "$BLOCK_FILE" \
  --tls \
  --cafile "$CORE_PEER_TLS_ROOTCERT_FILE"

peer channel list \
  --tls \
  --cafile "$CORE_PEER_TLS_ROOTCERT_FILE" > "$CHANNELS_FILE"
`, channelName, channelBlockFilePath(channelName), adminTLSPath, channelOutputDir, channelPeerJoinResultFile, mspID, peerAddress, mspPath)
}

func updateAnchorPeerScript(
	channelName string,
	mspID string,
	peerAddress string,
	anchorHost string,
	anchorPort int32,
	ordererAddress string,
	mspPath string,
	adminTLSPath string,
	ordererTLSPath string,
	anchorUpdateFile string,
) string {
	return fmt.Sprintf(`set -eu

CHANNEL_ID=%q
MSP_ID=%q
ANCHOR_HOST=%q
ANCHOR_PORT=%d
ORDERER_ADDRESS=%q
ADMIN_TLS_DIR=%q
ORDERER_TLS_DIR=%q
ANCHOR_UPDATE_FILE=%q
OUTPUT_DIR=%q
RESULT_FILE="$OUTPUT_DIR/%s"

export CORE_PEER_LOCALMSPID="$MSP_ID"
export CORE_PEER_ADDRESS=%q
export CORE_PEER_MSPCONFIGPATH=%q
export CORE_PEER_TLS_ENABLED=true
export CORE_PEER_TLS_CERT_FILE="$ADMIN_TLS_DIR/client.crt"
export CORE_PEER_TLS_KEY_FILE="$ADMIN_TLS_DIR/client.key"
export CORE_PEER_TLS_ROOTCERT_FILE="$ADMIN_TLS_DIR/ca.crt"

CONFIG_BLOCK="$OUTPUT_DIR/config_block.pb"
CONFIG_BLOCK_JSON="$OUTPUT_DIR/config_block.json"
CONFIG_JSON="$OUTPUT_DIR/config.json"
MODIFIED_CONFIG_JSON="$OUTPUT_DIR/modified_config.json"
CONFIG_PB="$OUTPUT_DIR/config.pb"
MODIFIED_CONFIG_PB="$OUTPUT_DIR/modified_config.pb"
CONFIG_UPDATE_PB="$OUTPUT_DIR/config_update.pb"
CONFIG_UPDATE_JSON="$OUTPUT_DIR/config_update.json"
ENVELOPE_JSON="$OUTPUT_DIR/anchor_update_envelope.json"

mkdir -p "$OUTPUT_DIR"

write_anchor_result() {
  jq -n \
    --arg channel "$CHANNEL_ID" \
    --arg msp "$MSP_ID" \
    --arg host "$ANCHOR_HOST" \
    --argjson port "$ANCHOR_PORT" \
    '{"channel":$channel,"mspID":$msp,"anchorPeers":[{"host":$host,"port":$port}]}' \
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

retry 30 5 peer channel fetch config "$CONFIG_BLOCK" \
  -c "$CHANNEL_ID" \
  -o "$ORDERER_ADDRESS" \
  --tls \
  --cafile "$ORDERER_TLS_DIR/ca.crt"

configtxlator proto_decode \
  --input "$CONFIG_BLOCK" \
  --type common.Block \
  --output "$CONFIG_BLOCK_JSON"

jq '.data.data[0].payload.data.config' "$CONFIG_BLOCK_JSON" > "$CONFIG_JSON"

if jq -e \
  --arg msp "$MSP_ID" \
  --arg host "$ANCHOR_HOST" \
  --argjson port "$ANCHOR_PORT" \
  '(.channel_group.groups.Application.groups[$msp].values.AnchorPeers.value.anchor_peers // []) == [{"host": $host, "port": $port}]' \
  "$CONFIG_JSON" >/dev/null; then
  echo "Anchor peers already configured for $MSP_ID on channel $CHANNEL_ID"
  write_anchor_result
  exit 0
fi

jq \
  --arg msp "$MSP_ID" \
  --arg host "$ANCHOR_HOST" \
  --argjson port "$ANCHOR_PORT" \
  '.channel_group.groups.Application.groups[$msp].values.AnchorPeers =
    ((.channel_group.groups.Application.groups[$msp].values.AnchorPeers // {"mod_policy":"Admins","version":"0","value":{}})
    | .mod_policy = "Admins"
    | .value.anchor_peers = [{"host": $host, "port": $port}])' \
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

jq -n \
  --arg channel "$CHANNEL_ID" \
  --slurpfile update "$CONFIG_UPDATE_JSON" \
  '{"payload":{"header":{"channel_header":{"channel_id":$channel,"type":2}},"data":{"config_update":$update[0]}}}' \
  > "$ENVELOPE_JSON"

configtxlator proto_encode \
  --input "$ENVELOPE_JSON" \
  --type common.Envelope \
  --output "$ANCHOR_UPDATE_FILE"

retry 30 5 peer channel update \
  -c "$CHANNEL_ID" \
  -o "$ORDERER_ADDRESS" \
  -f "$ANCHOR_UPDATE_FILE" \
  --tls \
  --cafile "$ORDERER_TLS_DIR/ca.crt"

write_anchor_result
`, channelName, mspID, anchorHost, anchorPort, ordererAddress, adminTLSPath, ordererTLSPath, anchorUpdateFile, channelOutputDir, channelAnchorPeerResultFile, peerAddress, mspPath)
}

func publishChannelBlockEnv(channelName string) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: envChannelBlockConfigMap, Value: channelBlockConfigMapName(channelName)},
		{Name: envChannelBlockFile, Value: channelBlockFileName(channelName)},
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

func publishChannelBlockScript() string {
	return `set -eu

kubectl -n "$POD_NAMESPACE" create configmap "$FABRICOPS_CHANNEL_BLOCK_CONFIGMAP" \
  --from-file="$FABRICOPS_CHANNEL_BLOCK_FILE=` + channelOutputDir + `/$FABRICOPS_CHANNEL_BLOCK_FILE" \
  --dry-run=client -o yaml | kubectl -n "$POD_NAMESPACE" apply -f -

kubectl -n "$POD_NAMESPACE" label configmap "$FABRICOPS_CHANNEL_BLOCK_CONFIGMAP" \
  fabricops.io/component=channel \
  fabricops.io/channel="${FABRICOPS_CHANNEL_BLOCK_FILE%.block}" \
  app.kubernetes.io/component=channel \
  --overwrite
`
}

func publishOrdererJoinResultEnv(channelName string, orderer ordererInstance) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: envOrdererJoinResultConfigMap, Value: channelOrdererJoinResultConfigMapName(channelName, orderer)},
		{Name: envOrdererJoinResultKey, Value: channelOrdererJoinResultKey},
		{Name: envOrdererJoinResultFile, Value: channelOrdererJoinResultFile},
		{Name: envOrdererJoinResultChannel, Value: sanitizeName(channelName)},
		{Name: envOrdererJoinResultOrderer, Value: sanitizeName(orderer.name)},
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

func publishOrdererJoinResultScript() string {
	return `set -eu

kubectl -n "$POD_NAMESPACE" create configmap "$FABRICOPS_ORDERER_JOIN_RESULT_CONFIGMAP" \
  --from-file="$FABRICOPS_ORDERER_JOIN_RESULT_KEY=` + channelOutputDir + `/$FABRICOPS_ORDERER_JOIN_RESULT_FILE" \
  --dry-run=client -o yaml | kubectl -n "$POD_NAMESPACE" apply -f -

kubectl -n "$POD_NAMESPACE" label configmap "$FABRICOPS_ORDERER_JOIN_RESULT_CONFIGMAP" \
  fabricops.io/component=channel \
  fabricops.io/channel="$FABRICOPS_ORDERER_JOIN_RESULT_CHANNEL" \
  fabricops.io/workload="$FABRICOPS_ORDERER_JOIN_RESULT_ORDERER" \
  app.kubernetes.io/component=channel \
  --overwrite
`
}

func publishPeerJoinResultEnv(channelName string, peer peerInstance) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: envPeerJoinResultConfigMap, Value: channelPeerJoinResultConfigMapName(channelName, peer)},
		{Name: envPeerJoinResultKey, Value: channelPeerJoinResultKey},
		{Name: envPeerJoinResultFile, Value: channelPeerJoinResultFile},
		{Name: envPeerJoinResultChannel, Value: sanitizeName(channelName)},
		{Name: envPeerJoinResultPeer, Value: sanitizeName(peer.name)},
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

func publishPeerJoinResultScript() string {
	return `set -eu

kubectl -n "$POD_NAMESPACE" create configmap "$FABRICOPS_PEER_JOIN_RESULT_CONFIGMAP" \
  --from-file="$FABRICOPS_PEER_JOIN_RESULT_KEY=` + channelOutputDir + `/$FABRICOPS_PEER_JOIN_RESULT_FILE" \
  --dry-run=client -o yaml | kubectl -n "$POD_NAMESPACE" apply -f -

kubectl -n "$POD_NAMESPACE" label configmap "$FABRICOPS_PEER_JOIN_RESULT_CONFIGMAP" \
  fabricops.io/component=channel \
  fabricops.io/channel="$FABRICOPS_PEER_JOIN_RESULT_CHANNEL" \
  fabricops.io/workload="$FABRICOPS_PEER_JOIN_RESULT_PEER" \
  app.kubernetes.io/component=channel \
  --overwrite
`
}

func publishAnchorPeerUpdateResultEnv(channelName string, org fabricopsv1alpha1.Org) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: envAnchorPeerResultConfigMap, Value: channelAnchorPeerUpdateResultConfigMapName(channelName, org)},
		{Name: envAnchorPeerResultKey, Value: channelAnchorPeerResultKey},
		{Name: envAnchorPeerResultFile, Value: channelAnchorPeerResultFile},
		{Name: envAnchorPeerResultChannel, Value: sanitizeName(channelName)},
		{Name: envAnchorPeerResultOrg, Value: sanitizeName(org.Organization.Name)},
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

func publishAnchorPeerUpdateResultScript() string {
	return `set -eu

kubectl -n "$POD_NAMESPACE" create configmap "$FABRICOPS_ANCHOR_PEER_RESULT_CONFIGMAP" \
  --from-file="$FABRICOPS_ANCHOR_PEER_RESULT_KEY=` + channelOutputDir + `/$FABRICOPS_ANCHOR_PEER_RESULT_FILE" \
  --dry-run=client -o yaml | kubectl -n "$POD_NAMESPACE" apply -f -

kubectl -n "$POD_NAMESPACE" label configmap "$FABRICOPS_ANCHOR_PEER_RESULT_CONFIGMAP" \
  fabricops.io/component=channel \
  fabricops.io/channel="$FABRICOPS_ANCHOR_PEER_RESULT_CHANNEL" \
  fabricops.io/org="$FABRICOPS_ANCHOR_PEER_RESULT_ORG" \
  app.kubernetes.io/component=channel \
  --overwrite
`
}

func jobFailed(job batchv1.Job) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Status == corev1.ConditionTrue &&
			(condition.Type == batchv1.JobFailed || condition.Type == batchv1.JobFailureTarget) {
			return true
		}
	}

	return false
}

func jobSucceeded(job batchv1.Job) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Status == corev1.ConditionTrue && condition.Type == batchv1.JobComplete {
			return true
		}
	}

	return job.Status.Succeeded > 0
}

func setChannelOrgMessages(status *fabricopsv1alpha1.ChannelStatus, message string) {
	for i := range status.Orgs {
		if status.Orgs[i].Message != "" {
			status.Orgs[i].Message = message
		}
	}
}
