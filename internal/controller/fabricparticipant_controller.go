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
	"reflect"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	fabricopsv1alpha1 "github.com/dpereowei/fabricops/api/v1alpha1"
)

// FabricParticipantReconciler reconciles a FabricParticipant object
type FabricParticipantReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

const (
	conditionLocalInfrastructureReady = "LocalInfrastructureReady"
	conditionRemoteArtifactsReady     = "RemoteArtifactsReady"
	conditionChaincodeLifecycleReady  = "ChaincodeLifecycleReady"
)

// +kubebuilder:rbac:groups=fabricops.io,resources=fabricparticipants,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=fabricops.io,resources=fabricparticipants/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=fabricops.io,resources=fabricparticipants/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch

// Reconcile validates the participant join contract, reconciles the local org
// infrastructure, and advances participant-side channel joins when imported
// artifacts are available.
func (r *FabricParticipantReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var participant fabricopsv1alpha1.FabricParticipant
	if err := r.Get(ctx, req.NamespacedName, &participant); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	status, reconcileErr := r.fabricParticipantStatus(ctx, &participant)
	result := participantReconcileResult(status, reconcileErr)
	if reflect.DeepEqual(participant.Status, status) {
		return result, reconcileErr
	}

	participant.Status = status
	log.Info(
		"Updating FabricParticipant status",
		"name", participant.Name,
		"namespace", participant.Namespace,
		"phase", status.Phase,
	)
	if err := r.Status().Update(ctx, &participant); err != nil {
		return ctrl.Result{}, err
	}
	return result, reconcileErr
}

// SetupWithManager sets up the controller with the Manager.
func (r *FabricParticipantReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fabricopsv1alpha1.FabricParticipant{}).
		Named("fabricparticipant").
		Complete(r)
}

func (r *FabricParticipantReconciler) fabricParticipantStatus(
	ctx context.Context,
	participant *fabricopsv1alpha1.FabricParticipant,
) (fabricopsv1alpha1.FabricParticipantStatus, error) {
	problems := validateFabricParticipantTopology(participant)
	if len(problems) > 0 {
		message := strings.Join(problems, "; ")
		return fabricopsv1alpha1.FabricParticipantStatus{
			Phase:           fabricopsv1alpha1.PhaseFailed,
			Message:         message,
			ChannelStatus:   participantChannelStatuses(participant, nil, "Topology validation failed"),
			ChaincodeStatus: participantChaincodeStatuses(participant, nil, "Topology validation failed"),
			Conditions: participantConditions(
				participant,
				metav1.ConditionFalse,
				"TopologyInvalid",
				message,
				metav1.ConditionUnknown,
				"TopologyInvalid",
				"Topology validation failed before local infrastructure reconciliation",
				metav1.ConditionUnknown,
				"TopologyInvalid",
				"Topology validation failed before imported artifact checks",
				metav1.ConditionUnknown,
				"TopologyInvalid",
				"Topology validation failed before channel join reconciliation",
				metav1.ConditionUnknown,
				"TopologyInvalid",
				"Topology validation failed before chaincode lifecycle reconciliation",
			),
		}, nil
	}

	localOrgStatus, err := r.reconcileParticipantLocalOrg(ctx, participant)
	if err != nil {
		message := "Failed to reconcile participant local infrastructure: " + err.Error()
		return fabricopsv1alpha1.FabricParticipantStatus{
			Phase:          fabricopsv1alpha1.PhaseFailed,
			Message:        message,
			LocalOrgStatus: localOrgStatus,
			Conditions: participantConditions(
				participant,
				metav1.ConditionFalse,
				"ReconcileError",
				message,
				metav1.ConditionFalse,
				"ReconcileError",
				message,
				metav1.ConditionUnknown,
				"ReconcileError",
				"Skipped imported artifact checks because local infrastructure reconciliation failed",
				metav1.ConditionUnknown,
				"ReconcileError",
				"Skipped channel join reconciliation because local infrastructure reconciliation failed",
				metav1.ConditionUnknown,
				"ReconcileError",
				"Skipped chaincode lifecycle reconciliation because local infrastructure reconciliation failed",
			),
		}, err
	}

	artifacts := r.participantRemoteArtifactStatus(ctx, participant)
	localReady := localOrgStatus.Ready
	channelStatuses := participantChannelStatuses(
		participant,
		artifacts.channelBlocks,
		"Waiting for participant channel join prerequisites",
	)
	channelsReady := len(participant.Spec.Channels) == 0
	channelsReason := "ParticipantChannelJoinPending"
	channelsMessage := "Waiting for participant channel join prerequisites"
	if len(participant.Spec.Channels) == 0 {
		channelsReason = "NoParticipantChannels"
		channelsMessage = "No participant channels declared"
	}
	if localReady && artifacts.ready {
		var err error
		channelStatuses, channelsReady, channelsMessage, err = r.reconcileParticipantChannelJoins(
			ctx,
			participant,
			localOrgStatus,
			artifacts,
		)
		if err != nil {
			message := "Failed to reconcile participant channel joins: " + err.Error()
			return fabricopsv1alpha1.FabricParticipantStatus{
				Phase:                    fabricopsv1alpha1.PhaseFailed,
				Message:                  message,
				LocalInfrastructureReady: localReady,
				LocalOrgStatus:           localOrgStatus,
				RemoteArtifactsReady:     artifacts.ready,
				ChannelStatus:            channelStatuses,
				ChaincodeStatus: participantChaincodeStatuses(
					participant,
					artifacts.chaincodePackages,
					"Skipped participant chaincode lifecycle reconciliation because channel join reconciliation failed",
				),
				Conditions: participantConditions(
					participant,
					metav1.ConditionFalse,
					"ReconcileError",
					message,
					metav1.ConditionTrue,
					"LocalInfrastructureReady",
					"Participant-local CA, peer, and admin identity material are ready",
					metav1.ConditionTrue,
					"RemoteArtifactsReady",
					artifacts.message,
					metav1.ConditionFalse,
					"ReconcileError",
					message,
					metav1.ConditionUnknown,
					"ReconcileError",
					"Skipped participant chaincode lifecycle reconciliation because channel join reconciliation failed",
				),
			}, err
		}
		if channelsReady && len(participant.Spec.Channels) > 0 {
			channelsReason = "ParticipantChannelsJoined"
		}
	}

	chaincodeStatuses := participantChaincodeStatuses(
		participant,
		artifacts.chaincodePackages,
		"Waiting for participant chaincode lifecycle prerequisites",
	)
	chaincodesReady := channelsReady && len(participant.Spec.Chaincodes) == 0
	chaincodeReason := "ParticipantChaincodeLifecyclePending"
	chaincodeMessage := "Waiting for participant chaincode lifecycle prerequisites"
	if localReady && artifacts.ready && channelsReady {
		var err error
		chaincodeStatuses, chaincodesReady, chaincodeMessage, err = r.reconcileParticipantChaincodes(
			ctx,
			participant,
			localOrgStatus,
			channelStatuses,
			artifacts,
		)
		if err != nil {
			message := "Failed to reconcile participant chaincode lifecycle: " + err.Error()
			return fabricopsv1alpha1.FabricParticipantStatus{
				Phase:                    fabricopsv1alpha1.PhaseFailed,
				Message:                  message,
				LocalInfrastructureReady: localReady,
				LocalOrgStatus:           localOrgStatus,
				RemoteArtifactsReady:     artifacts.ready,
				ChannelStatus:            channelStatuses,
				ChaincodeStatus:          chaincodeStatuses,
				ChannelsReady:            channelsReady,
				Conditions: participantConditions(
					participant,
					metav1.ConditionFalse,
					"ReconcileError",
					message,
					metav1.ConditionTrue,
					"LocalInfrastructureReady",
					"Participant-local CA, peer, and admin identity material are ready",
					metav1.ConditionTrue,
					"RemoteArtifactsReady",
					artifacts.message,
					metav1.ConditionTrue,
					channelsReason,
					channelsMessage,
					metav1.ConditionFalse,
					"ReconcileError",
					message,
				),
			}, err
		}
		if len(participant.Spec.Chaincodes) == 0 {
			chaincodeReason = "NoParticipantChaincodes"
		} else if chaincodesReady {
			chaincodeReason = "ParticipantChaincodesReady"
		}
	}

	readyReason := "ParticipantWorkflowPending"
	message := channelsMessage
	phase := fabricopsv1alpha1.PhaseCreating
	readyStatus := metav1.ConditionFalse
	if !localReady {
		readyReason = "LocalInfrastructureNotReady"
		message = participantLocalInfrastructureMessage(localOrgStatus)
	} else if !artifacts.ready {
		readyReason = "RemoteArtifactsMissing"
		message = artifacts.message
	} else if !channelsReady {
		readyReason = "ParticipantChannelJoinPending"
	} else if !chaincodesReady {
		message = chaincodeMessage
	} else {
		readyStatus = metav1.ConditionTrue
		readyReason = "FabricParticipantReady"
		message = "FabricParticipant local infrastructure, imported artifacts, declared channels, and chaincode lifecycle are ready"
		phase = fabricopsv1alpha1.PhaseReady
	}

	localConditionStatus := metav1.ConditionFalse
	localConditionReason := "LocalInfrastructureNotReady"
	localConditionMessage := participantLocalInfrastructureMessage(localOrgStatus)
	if localReady {
		localConditionStatus = metav1.ConditionTrue
		localConditionReason = "LocalInfrastructureReady"
		localConditionMessage = "Participant-local CA, peer, and admin identity material are ready"
	}

	artifactConditionStatus := metav1.ConditionFalse
	artifactConditionReason := "RemoteArtifactsMissing"
	if artifacts.ready {
		artifactConditionStatus = metav1.ConditionTrue
		artifactConditionReason = "RemoteArtifactsReady"
	}

	channelStatus := metav1.ConditionUnknown
	channelReason := readyReason
	channelConditionMessage := "Waiting for participant channel join prerequisites"
	if localReady && artifacts.ready {
		channelReason = channelsReason
		channelConditionMessage = channelsMessage
		if channelsReady {
			channelStatus = metav1.ConditionTrue
		} else {
			channelStatus = metav1.ConditionFalse
		}
	}

	chaincodeStatus := metav1.ConditionUnknown
	if localReady && artifacts.ready && channelsReady {
		if chaincodesReady {
			chaincodeStatus = metav1.ConditionTrue
		} else {
			chaincodeStatus = metav1.ConditionFalse
		}
	}

	return fabricopsv1alpha1.FabricParticipantStatus{
		Phase:                    phase,
		Message:                  message,
		LocalInfrastructureReady: localReady,
		LocalOrgStatus:           localOrgStatus,
		RemoteArtifactsReady:     artifacts.ready,
		ChannelStatus:            channelStatuses,
		ChaincodeStatus:          chaincodeStatuses,
		ChannelsReady:            channelsReady,
		ChaincodeLifecycleReady:  chaincodesReady,
		Conditions: participantConditions(
			participant,
			readyStatus,
			readyReason,
			message,
			localConditionStatus,
			localConditionReason,
			localConditionMessage,
			artifactConditionStatus,
			artifactConditionReason,
			artifacts.message,
			channelStatus,
			channelReason,
			channelConditionMessage,
			chaincodeStatus,
			chaincodeReason,
			chaincodeMessage,
		),
	}, nil
}

func participantReconcileResult(
	status fabricopsv1alpha1.FabricParticipantStatus,
	reconcileErr error,
) ctrl.Result {
	if reconcileErr != nil || status.Phase == fabricopsv1alpha1.PhaseReady || status.Phase == fabricopsv1alpha1.PhaseFailed {
		return ctrl.Result{}
	}

	return ctrl.Result{RequeueAfter: 10 * time.Second}
}

func participantConditions(
	participant *fabricopsv1alpha1.FabricParticipant,
	readyStatus metav1.ConditionStatus,
	readyReason string,
	readyMessage string,
	localStatus metav1.ConditionStatus,
	localReason string,
	localMessage string,
	artifactsStatus metav1.ConditionStatus,
	artifactsReason string,
	artifactsMessage string,
	channelsStatus metav1.ConditionStatus,
	channelsReason string,
	channelsMessage string,
	chaincodesStatus metav1.ConditionStatus,
	chaincodesReason string,
	chaincodesMessage string,
) []metav1.Condition {
	conditions := append([]metav1.Condition(nil), participant.Status.Conditions...)
	apiMeta.SetStatusCondition(&conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             readyStatus,
		Reason:             readyReason,
		Message:            readyMessage,
		ObservedGeneration: participant.Generation,
	})
	apiMeta.SetStatusCondition(&conditions, metav1.Condition{
		Type:               conditionLocalInfrastructureReady,
		Status:             localStatus,
		Reason:             localReason,
		Message:            localMessage,
		ObservedGeneration: participant.Generation,
	})
	apiMeta.SetStatusCondition(&conditions, metav1.Condition{
		Type:               conditionRemoteArtifactsReady,
		Status:             artifactsStatus,
		Reason:             artifactsReason,
		Message:            artifactsMessage,
		ObservedGeneration: participant.Generation,
	})
	apiMeta.SetStatusCondition(&conditions, metav1.Condition{
		Type:               conditionChannelsReady,
		Status:             channelsStatus,
		Reason:             channelsReason,
		Message:            channelsMessage,
		ObservedGeneration: participant.Generation,
	})
	apiMeta.SetStatusCondition(&conditions, metav1.Condition{
		Type:               conditionChaincodeLifecycleReady,
		Status:             chaincodesStatus,
		Reason:             chaincodesReason,
		Message:            chaincodesMessage,
		ObservedGeneration: participant.Generation,
	})
	return conditions
}

func participantChannelStatuses(
	participant *fabricopsv1alpha1.FabricParticipant,
	blocks map[string]bool,
	message string,
) []fabricopsv1alpha1.ParticipantChannelStatus {
	statuses := make([]fabricopsv1alpha1.ParticipantChannelStatus, 0, len(participant.Spec.Channels))
	for _, channel := range participant.Spec.Channels {
		statuses = append(statuses, fabricopsv1alpha1.ParticipantChannelStatus{
			Name:       channel.Name,
			BlockReady: blocks[participantChannelKey(channel.Name)],
			Peers: fabricopsv1alpha1.WorkloadStatus{
				Desired: int32(len(channel.Peers)),
			},
			Message: message,
		})
	}
	return statuses
}

func participantChaincodeStatuses(
	participant *fabricopsv1alpha1.FabricParticipant,
	packages map[string]bool,
	message string,
) []fabricopsv1alpha1.ParticipantChaincodeStatus {
	statuses := make([]fabricopsv1alpha1.ParticipantChaincodeStatus, 0, len(participant.Spec.Chaincodes))
	for _, chaincode := range participant.Spec.Chaincodes {
		statuses = append(statuses, fabricopsv1alpha1.ParticipantChaincodeStatus{
			Name:         chaincode.Name,
			Channel:      chaincode.Channel,
			PackageReady: packages[participantChaincodeKey(chaincode.Channel, chaincode.Name)],
			Message:      message,
		})
	}
	return statuses
}

func (r *FabricParticipantReconciler) reconcileParticipantLocalOrg(
	ctx context.Context,
	participant *fabricopsv1alpha1.FabricParticipant,
) (fabricopsv1alpha1.OrgStatus, error) {
	networkReconciler := &FabricNetworkReconciler{
		Client: r.Client,
		Scheme: r.Scheme,
	}
	net := participantLocalFabricNetwork(participant)
	return networkReconciler.reconcileOrg(ctx, net, participant.Spec.Org)
}

func participantLocalFabricNetwork(
	participant *fabricopsv1alpha1.FabricParticipant,
) *fabricopsv1alpha1.FabricNetwork {
	return &fabricopsv1alpha1.FabricNetwork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      participantLocalNetworkName(participant.Name),
			Namespace: participant.Namespace,
			UID:       participant.UID,
		},
		Spec: fabricopsv1alpha1.FabricNetworkSpec{
			Global: participant.Spec.Global,
			Orgs:   []fabricopsv1alpha1.Org{participant.Spec.Org},
		},
	}
}

func participantLocalNetworkName(name string) string {
	return sanitizeName("fp-" + name)
}

type participantArtifactStatus struct {
	ready             bool
	message           string
	channelBlocks     map[string]bool
	chaincodePackages map[string]bool
}

func (r *FabricParticipantReconciler) participantRemoteArtifactStatus(
	ctx context.Context,
	participant *fabricopsv1alpha1.FabricParticipant,
) participantArtifactStatus {
	missing := []string{}
	status := participantArtifactStatus{
		ready:             true,
		channelBlocks:     map[string]bool{},
		chaincodePackages: map[string]bool{},
	}

	if participant.Spec.Global.TLS {
		for i, orderer := range participant.Spec.Network.Orderers {
			path := fmt.Sprintf("spec.network.orderers[%d].tlsRootCARef", i)
			if !r.participantArtifactRefReady(ctx, participant.Namespace, path, orderer.TLSRootCARef, &missing) {
				status.ready = false
			}
		}
		for i, peer := range participant.Spec.Network.Peers {
			path := fmt.Sprintf("spec.network.peers[%d].tlsRootCARef", i)
			if !r.participantArtifactRefReady(ctx, participant.Namespace, path, peer.TLSRootCARef, &missing) {
				status.ready = false
			}
		}
	}

	for i, channel := range participant.Spec.Channels {
		path := fmt.Sprintf("spec.channels[%d].blockRef", i)
		ready := r.participantArtifactRefReady(ctx, participant.Namespace, path, &channel.BlockRef, &missing)
		status.channelBlocks[participantChannelKey(channel.Name)] = ready
		if !ready {
			status.ready = false
		}
	}

	for i, chaincode := range participant.Spec.Chaincodes {
		if chaincode.PackageRef == nil {
			continue
		}
		path := fmt.Sprintf("spec.chaincodes[%d].packageRef", i)
		ready := r.participantArtifactRefReady(ctx, participant.Namespace, path, chaincode.PackageRef, &missing)
		status.chaincodePackages[participantChaincodeKey(chaincode.Channel, chaincode.Name)] = ready
		if !ready {
			status.ready = false
		}
	}

	if status.ready {
		status.message = "All imported network artifacts are present"
		return status
	}

	status.message = "Missing imported network artifacts: " + strings.Join(missing, "; ")
	return status
}

func (r *FabricParticipantReconciler) participantArtifactRefReady(
	ctx context.Context,
	namespace string,
	path string,
	ref *fabricopsv1alpha1.ParticipantArtifactKeyRef,
	missing *[]string,
) bool {
	if ref == nil {
		*missing = append(*missing, path+" is required")
		return false
	}
	if ref.ConfigMapKeyRef != nil {
		return r.participantConfigMapKeyReady(ctx, namespace, path+".configMapKeyRef", *ref.ConfigMapKeyRef, missing)
	}
	if ref.SecretKeyRef != nil {
		return r.participantSecretKeyReady(ctx, namespace, path+".secretKeyRef", *ref.SecretKeyRef, missing)
	}
	*missing = append(*missing, path+" must set configMapKeyRef or secretKeyRef")
	return false
}

func (r *FabricParticipantReconciler) participantConfigMapKeyReady(
	ctx context.Context,
	namespace string,
	path string,
	ref corev1.ConfigMapKeySelector,
	missing *[]string,
) bool {
	var configMap corev1.ConfigMap
	err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: ref.Name}, &configMap)
	if apierrors.IsNotFound(err) {
		*missing = append(*missing, fmt.Sprintf("%s ConfigMap %s/%s is missing", path, namespace, ref.Name))
		return false
	}
	if err != nil {
		*missing = append(*missing, fmt.Sprintf("%s ConfigMap %s/%s could not be read: %v", path, namespace, ref.Name, err))
		return false
	}
	if _, ok := configMap.Data[ref.Key]; ok {
		return true
	}
	if _, ok := configMap.BinaryData[ref.Key]; ok {
		return true
	}
	*missing = append(*missing, fmt.Sprintf("%s ConfigMap %s/%s is missing key %q", path, namespace, ref.Name, ref.Key))
	return false
}

func (r *FabricParticipantReconciler) participantSecretKeyReady(
	ctx context.Context,
	namespace string,
	path string,
	ref corev1.SecretKeySelector,
	missing *[]string,
) bool {
	var secret corev1.Secret
	err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: ref.Name}, &secret)
	if apierrors.IsNotFound(err) {
		*missing = append(*missing, fmt.Sprintf("%s Secret %s/%s is missing", path, namespace, ref.Name))
		return false
	}
	if err != nil {
		*missing = append(*missing, fmt.Sprintf("%s Secret %s/%s could not be read: %v", path, namespace, ref.Name, err))
		return false
	}
	if _, ok := secret.Data[ref.Key]; ok {
		return true
	}
	if _, ok := secret.StringData[ref.Key]; ok {
		return true
	}
	*missing = append(*missing, fmt.Sprintf("%s Secret %s/%s is missing key %q", path, namespace, ref.Name, ref.Key))
	return false
}

func participantLocalInfrastructureMessage(status fabricopsv1alpha1.OrgStatus) string {
	if !status.CAReady {
		return "Waiting for participant CA to become ready"
	}
	if !status.IdentityReady {
		if status.IdentityError != "" {
			return status.IdentityError
		}
		return "Waiting for participant admin and peer identity material"
	}
	if !status.PeersReady {
		return "Waiting for participant peers to become ready"
	}
	return "Waiting for participant local infrastructure"
}

func participantChannelKey(channelName string) string {
	return strings.ToLower(strings.TrimSpace(channelName))
}

func participantChaincodeKey(channelName string, chaincodeName string) string {
	return participantChannelKey(channelName) + "/" + strings.ToLower(strings.TrimSpace(chaincodeName))
}
