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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// FabricParticipantSpec defines the desired state of FabricParticipant
type FabricParticipantSpec struct {
	Global GlobalConfig `json:"global"`

	// Org is the local organization this participant cluster owns. It should
	// contain the participant CA and peer shape; remote founder/orderer
	// material belongs under Network.
	Org Org `json:"org"`

	Network ParticipantNetwork `json:"network"`

	// Channels declares the existing network channels this participant should
	// join after the founder/coordinator admits its MSP to channel config.
	// +optional
	Channels []ParticipantChannel `json:"channels,omitempty"`

	// Chaincodes declares lifecycle definitions this participant expects to
	// install and approve after joining the relevant channels.
	// +optional
	Chaincodes []ParticipantChaincode `json:"chaincodes,omitempty"`
}

type ParticipantNetwork struct {
	// Name is the human-readable name of the existing Fabric network.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern="^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?$"
	Name string `json:"name"`
	// FounderMSPID identifies the coordinator/founder MSP that is expected to
	// submit channel config updates.
	// +optional
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:Pattern="^[A-Za-z][A-Za-z0-9]*$"
	FounderMSPID string `json:"founderMSPID,omitempty"`
	// Orderers lists reachable ordering endpoints from the existing network.
	// +kubebuilder:validation:MinItems=1
	Orderers []ParticipantOrdererEndpoint `json:"orderers"`
	// Peers lists optional remote peer endpoints from already-admitted orgs.
	// `fabricopsctl invoke/query --participant` can use these endpoints for
	// explicit cross-cluster endorsement target selection. FabricOps does not
	// reconcile these remote peers as Kubernetes workloads.
	// +optional
	Peers []ParticipantPeerEndpoint `json:"peers,omitempty"`
}

type ParticipantOrdererEndpoint struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern="^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?$"
	Org string `json:"org"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern="^[A-Za-z0-9]([A-Za-z0-9_.-]*[A-Za-z0-9])?$"
	Name string `json:"name"`
	// ClientAddress is the host:port endpoint used by peer CLI/Gateway flows.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	ClientAddress string `json:"clientAddress"`
	// TLSHostnameOverride is passed to Fabric CLI calls when ClientAddress
	// differs from the orderer's certificate hostname, for example local
	// port-forward testing. Production manifests should prefer DNS names that
	// match the orderer certificate SANs.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	TLSHostnameOverride string `json:"tlsHostnameOverride,omitempty"`
	// AdminAddress is the optional orderer admin endpoint for participation
	// APIs when the participant is expected to interact with it directly.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	AdminAddress string `json:"adminAddress,omitempty"`
	// TLSRootCARef points at the PEM encoded TLS root CA for this orderer.
	// Required when spec.global.tls is true.
	// +optional
	TLSRootCARef *ParticipantArtifactKeyRef `json:"tlsRootCARef,omitempty"`
}

type ParticipantPeerEndpoint struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern="^[A-Za-z0-9]([A-Za-z0-9_.-]*[A-Za-z0-9])?$"
	Org string `json:"org"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern="^[A-Za-z0-9]([A-Za-z0-9_.-]*[A-Za-z0-9])?$"
	Name string `json:"name"`
	// Address is the host:port endpoint used by Fabric client operations.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Address string `json:"address"`
	// TLSHostnameOverride records local-test override intent. Fabric peer CLI
	// paths do not support a per-peer TLS hostname override, so production and
	// e2e addresses should match the peer certificate SANs.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	TLSHostnameOverride string `json:"tlsHostnameOverride,omitempty"`
	// TLSRootCARef points at the PEM encoded TLS root CA for this peer.
	// Required when spec.global.tls is true.
	// +optional
	TLSRootCARef *ParticipantArtifactKeyRef `json:"tlsRootCARef,omitempty"`
}

type ParticipantArtifactKeyRef struct {
	// ConfigMapKeyRef points at a key in a ConfigMap in the FabricParticipant
	// namespace.
	// +optional
	ConfigMapKeyRef *corev1.ConfigMapKeySelector `json:"configMapKeyRef,omitempty"`
	// SecretKeyRef points at a key in a Secret in the FabricParticipant
	// namespace.
	// +optional
	SecretKeyRef *corev1.SecretKeySelector `json:"secretKeyRef,omitempty"`
}

type ParticipantChannel struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=249
	// +kubebuilder:validation:Pattern="^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$"
	Name string `json:"name"`
	// BlockRef points at the channel block the participant peers should join.
	// The founder/coordinator provides this after the org is admitted.
	BlockRef ParticipantArtifactKeyRef `json:"blockRef"`
	// Peers lists local participant peers that should join this channel.
	// +kubebuilder:validation:MinItems=1
	Peers []string `json:"peers"`
	// AnchorPeers declares the externally reachable anchor peers the founder
	// should put into the channel Application config for this org.
	// +optional
	AnchorPeers []ParticipantAnchorPeer `json:"anchorPeers,omitempty"`
	// Membership carries founder-side policy hints captured with the imported
	// artifacts. FabricOps records this contract before it automates updates.
	// +optional
	Membership *ParticipantChannelMembership `json:"membership,omitempty"`
}

type ParticipantAnchorPeer struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern="^[A-Za-z0-9]([A-Za-z0-9_.-]*[A-Za-z0-9])?$"
	Name string `json:"name"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Host string `json:"host"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port"`
}

type ParticipantChannelMembership struct {
	// ApplicationPolicy describes the channel Application policy expected to
	// authorize membership updates, for example /Channel/Application/Admins.
	// +optional
	// +kubebuilder:validation:MaxLength=256
	ApplicationPolicy string `json:"applicationPolicy,omitempty"`
	// RequiredSignerMSPIDs lists MSP IDs expected to sign the org-add config
	// update before submission.
	// +optional
	RequiredSignerMSPIDs []string `json:"requiredSignerMSPIDs,omitempty"`
}

type ParticipantChaincode struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern="^[A-Za-z0-9]([A-Za-z0-9_.-]*[A-Za-z0-9])?$"
	Name string `json:"name"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=249
	// +kubebuilder:validation:Pattern="^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$"
	Channel string `json:"channel"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	Version string `json:"version"`
	// PackageLabel is the lifecycle package label the participant should
	// install and approve for this definition.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:Pattern="^[A-Za-z0-9]([A-Za-z0-9_.-]*[A-Za-z0-9])?$"
	PackageLabel string `json:"packageLabel"`
	// Sequence is the Fabric lifecycle sequence expected on the channel.
	// +kubebuilder:validation:Minimum=1
	Sequence int32 `json:"sequence"`
	// EndorsementPolicy is the expected Fabric signature policy, if known.
	// +optional
	// +kubebuilder:validation:MaxLength=512
	EndorsementPolicy string `json:"endorsementPolicy,omitempty"`
	// InitRequired records whether the channel definition requires init.
	// +optional
	InitRequired bool `json:"initRequired,omitempty"`
	// CollectionConfigHash records the expected private-data collection config
	// hash for the definition, if collections are used.
	// +optional
	CollectionConfigHash string `json:"collectionConfigHash,omitempty"`
	// PackageRef optionally points at a prebuilt Fabric lifecycle package
	// archive. When omitted, FabricOps generates a CCaaS package from Image
	// and CCAAS settings.
	// +optional
	PackageRef *ParticipantArtifactKeyRef `json:"packageRef,omitempty"`
	// Image is the participant chaincode server image for CCaaS workloads.
	// +optional
	// +kubebuilder:validation:MaxLength=512
	Image string `json:"image,omitempty"`
	// CCAAS describes the participant-local chaincode service workload.
	// +optional
	CCAAS *ChaincodeAsAService `json:"ccaas,omitempty"`
}

// FabricParticipantStatus defines the observed state of FabricParticipant.
type FabricParticipantStatus struct {
	// +optional
	Phase Phase `json:"phase,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// +optional
	LocalInfrastructureReady bool `json:"localInfrastructureReady"`
	// LocalOrgStatus reports the participant-owned org infrastructure status.
	// +optional
	LocalOrgStatus OrgStatus `json:"localOrgStatus,omitempty"`
	// +optional
	RemoteArtifactsReady bool `json:"remoteArtifactsReady"`
	// +optional
	ChannelsReady bool `json:"channelsReady"`
	// +optional
	ChaincodeLifecycleReady bool `json:"chaincodeLifecycleReady"`
	// +optional
	ChannelStatus []ParticipantChannelStatus `json:"channelStatus,omitempty"`
	// +optional
	ChaincodeStatus []ParticipantChaincodeStatus `json:"chaincodeStatus,omitempty"`

	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

type ParticipantChannelStatus struct {
	Name       string         `json:"name"`
	BlockReady bool           `json:"blockReady"`
	Peers      WorkloadStatus `json:"peers,omitempty"`
	Joined     bool           `json:"joined"`
	Ready      bool           `json:"ready"`
	Message    string         `json:"message,omitempty"`
}

type ParticipantChaincodeStatus struct {
	Name         string `json:"name"`
	Channel      string `json:"channel"`
	PackageReady bool   `json:"packageReady"`
	Installed    bool   `json:"installed"`
	Approved     bool   `json:"approved"`
	Ready        bool   `json:"ready"`
	Message      string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// FabricParticipant is the Schema for the fabricparticipants API
type FabricParticipant struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of FabricParticipant
	// +required
	Spec FabricParticipantSpec `json:"spec"`

	// status defines the observed state of FabricParticipant
	// +optional
	Status FabricParticipantStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// FabricParticipantList contains a list of FabricParticipant
type FabricParticipantList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []FabricParticipant `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &FabricParticipant{}, &FabricParticipantList{})
		return nil
	})
}
