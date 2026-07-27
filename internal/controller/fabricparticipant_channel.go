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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	fabricopsv1alpha1 "github.com/dpereowei/fabricops/api/v1alpha1"
)

func (r *FabricParticipantReconciler) reconcileParticipantChannelJoins(
	ctx context.Context,
	participant *fabricopsv1alpha1.FabricParticipant,
	localOrgStatus fabricopsv1alpha1.OrgStatus,
	artifacts participantArtifactStatus,
) ([]fabricopsv1alpha1.ParticipantChannelStatus, bool, string, error) {
	if len(participant.Spec.Channels) == 0 {
		return nil, true, "No participant channels declared", nil
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

	statuses := make([]fabricopsv1alpha1.ParticipantChannelStatus, 0, len(participant.Spec.Channels))
	allReady := true
	pendingMessage := ""

	for _, channel := range participant.Spec.Channels {
		status, err := r.reconcileParticipantChannelJoin(
			ctx,
			networkReconciler,
			net,
			org,
			namespace,
			channel,
			artifacts.channelBlocks[participantChannelKey(channel.Name)],
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

	message := "Participant peers joined declared channels"
	if !allReady && pendingMessage != "" {
		message = pendingMessage
	}
	return statuses, allReady, message, nil
}

func (r *FabricParticipantReconciler) reconcileParticipantChannelJoin(
	ctx context.Context,
	networkReconciler *FabricNetworkReconciler,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	channel fabricopsv1alpha1.ParticipantChannel,
	blockReady bool,
) (fabricopsv1alpha1.ParticipantChannelStatus, error) {
	status := fabricopsv1alpha1.ParticipantChannelStatus{
		Name:       channel.Name,
		BlockReady: blockReady,
		Peers: fabricopsv1alpha1.WorkloadStatus{
			Desired: int32(len(channel.Peers)),
		},
	}

	if !blockReady {
		status.Message = "Waiting for imported channel block"
		return status, nil
	}

	if err := networkReconciler.ensureChannelRBAC(ctx, net, org, channel.Name, namespace); err != nil {
		return status, err
	}
	if err := r.ensureParticipantChannelBlockConfigMap(ctx, networkReconciler, net, org, namespace, channel); err != nil {
		return status, err
	}

	for _, peerName := range channel.Peers {
		peer := peerInstance{
			org:       org,
			name:      sanitizeName(peerName),
			namespace: namespace,
		}

		resultReady, _, err := networkReconciler.peerJoinResultReadiness(ctx, namespace, channel.Name, peer)
		if err != nil {
			return status, err
		}
		if resultReady {
			status.Peers.Ready++
			continue
		}

		if err := networkReconciler.ensureJob(
			ctx,
			buildPeerJoinJob(net, participantChannelAsNetworkChannel(channel, org), org, namespace, peer),
		); err != nil {
			return status, err
		}

		joined, message, err := networkReconciler.peerJoinReadiness(ctx, namespace, channel.Name, peer)
		if err != nil {
			return status, err
		}
		if joined {
			status.Peers.Ready++
			continue
		}
		if message != "" {
			status.Message = message
			return status, nil
		}
	}

	status.Joined = status.Peers.Ready >= status.Peers.Desired && status.Peers.Desired > 0
	status.Ready = status.BlockReady && status.Joined
	if !status.Ready {
		status.Message = "Waiting for peer join Jobs"
	}

	return status, nil
}

func (r *FabricParticipantReconciler) ensureParticipantChannelBlockConfigMap(
	ctx context.Context,
	networkReconciler *FabricNetworkReconciler,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	channel fabricopsv1alpha1.ParticipantChannel,
) error {
	block, err := r.participantArtifactBytes(ctx, net.Namespace, &channel.BlockRef)
	if err != nil {
		return err
	}

	desired := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:        channelBlockConfigMapName(channel.Name),
			Namespace:   namespace,
			Labels:      channelLabels(net, org, channel.Name),
			Annotations: resourceAnnotations(net, org),
		},
		BinaryData: map[string][]byte{
			channelBlockFileName(channel.Name): block,
		},
	}

	return networkReconciler.ensureConfigMap(ctx, desired)
}

func (r *FabricParticipantReconciler) participantArtifactBytes(
	ctx context.Context,
	namespace string,
	ref *fabricopsv1alpha1.ParticipantArtifactKeyRef,
) ([]byte, error) {
	if ref == nil {
		return nil, fmt.Errorf("artifact ref is required")
	}
	if ref.ConfigMapKeyRef != nil {
		return r.participantConfigMapBytes(ctx, namespace, *ref.ConfigMapKeyRef)
	}
	if ref.SecretKeyRef != nil {
		return r.participantSecretBytes(ctx, namespace, *ref.SecretKeyRef)
	}
	return nil, fmt.Errorf("artifact ref must set configMapKeyRef or secretKeyRef")
}

func (r *FabricParticipantReconciler) participantConfigMapBytes(
	ctx context.Context,
	namespace string,
	ref corev1.ConfigMapKeySelector,
) ([]byte, error) {
	var configMap corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: ref.Name}, &configMap); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("configmap %s/%s is missing", namespace, ref.Name)
		}
		return nil, err
	}
	if value, ok := configMap.BinaryData[ref.Key]; ok {
		return append([]byte(nil), value...), nil
	}
	if value, ok := configMap.Data[ref.Key]; ok {
		return []byte(value), nil
	}
	return nil, fmt.Errorf("configmap %s/%s is missing key %q", namespace, ref.Name, ref.Key)
}

func (r *FabricParticipantReconciler) participantSecretBytes(
	ctx context.Context,
	namespace string,
	ref corev1.SecretKeySelector,
) ([]byte, error) {
	var secret corev1.Secret
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: ref.Name}, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("secret %s/%s is missing", namespace, ref.Name)
		}
		return nil, err
	}
	if value, ok := secret.Data[ref.Key]; ok {
		return append([]byte(nil), value...), nil
	}
	if value, ok := secret.StringData[ref.Key]; ok {
		return []byte(value), nil
	}
	return nil, fmt.Errorf("secret %s/%s is missing key %q", namespace, ref.Name, ref.Key)
}

func participantChannelAsNetworkChannel(
	channel fabricopsv1alpha1.ParticipantChannel,
	org fabricopsv1alpha1.Org,
) fabricopsv1alpha1.Channel {
	return fabricopsv1alpha1.Channel{
		Name: channel.Name,
		Orgs: []fabricopsv1alpha1.ChannelOrg{{
			Name:  org.Organization.Name,
			Peers: channel.Peers,
		}},
	}
}
