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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

var _ = Describe("FabricParticipant Controller", func() {
	const resourceNamespace = "default"

	ctx := context.Background()

	It("reports pending status for a valid participant contract", func() {
		participant := fabricParticipantFixture("participant-valid")
		Expect(k8sClient.Create(ctx, participant)).To(Succeed())
		DeferCleanup(func() {
			Expect(k8sClient.Delete(ctx, participant)).To(Succeed())
		})
		DeferCleanup(cleanupParticipantNamespace, ctx, participant)
		createParticipantRemoteArtifacts(ctx, participant)

		reconciler := &FabricParticipantReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
		}
		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: participant.Name, Namespace: resourceNamespace},
		})
		Expect(err).NotTo(HaveOccurred())

		var updated fabricopsv1alpha1.FabricParticipant
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: participant.Name, Namespace: resourceNamespace}, &updated)).
			To(Succeed())
		Expect(updated.Status.Phase).To(Equal(fabricopsv1alpha1.PhaseCreating))
		Expect(updated.Status.Message).To(ContainSubstring("Waiting for participant CA"))
		Expect(updated.Status.LocalInfrastructureReady).To(BeFalse())
		Expect(updated.Status.RemoteArtifactsReady).To(BeTrue())
		Expect(updated.Status.LocalOrgStatus.Name).To(Equal("BankB"))
		Expect(updated.Status.LocalOrgStatus.Namespace).To(Equal(participantOrgNamespace(participant)))
		Expect(updated.Status.LocalOrgStatus.CAEndpoint).To(Equal("http://bankb-ca." + participantOrgNamespace(participant) + ".svc.cluster.local:7054"))
		Expect(updated.Status.ChannelStatus).To(HaveLen(1))
		Expect(updated.Status.ChannelStatus[0].BlockReady).To(BeTrue())
		Expect(updated.Status.ChannelStatus[0].Peers.Desired).To(Equal(int32(1)))
		Expect(updated.Status.ChaincodeStatus).To(HaveLen(1))

		ready := apiMeta.FindStatusCondition(updated.Status.Conditions, conditionReady)
		Expect(ready).NotTo(BeNil())
		Expect(ready.Status).To(Equal(metav1.ConditionFalse))
		Expect(ready.Reason).To(Equal("LocalInfrastructureNotReady"))

		local := apiMeta.FindStatusCondition(updated.Status.Conditions, conditionLocalInfrastructureReady)
		Expect(local).NotTo(BeNil())
		Expect(local.Status).To(Equal(metav1.ConditionFalse))
		Expect(local.Message).To(ContainSubstring("Waiting for participant CA"))

		remote := apiMeta.FindStatusCondition(updated.Status.Conditions, conditionRemoteArtifactsReady)
		Expect(remote).NotTo(BeNil())
		Expect(remote.Status).To(Equal(metav1.ConditionTrue))

		var namespace corev1.Namespace
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: participantOrgNamespace(participant)}, &namespace)).
			To(Succeed())
		Expect(namespace.Annotations[annotationFabricNetwork]).To(Equal(participantLocalNetworkName(participant.Name)))

		var caDeployment appsv1.Deployment
		Expect(k8sClient.Get(
			ctx,
			types.NamespacedName{Name: "bankb-ca", Namespace: participantOrgNamespace(participant)},
			&caDeployment,
		)).To(Succeed())

		for _, secretName := range []string{"bankb-ca-bootstrap", "bankb-admin-enrollment", "peer0-enrollment"} {
			var secret corev1.Secret
			Expect(k8sClient.Get(
				ctx,
				types.NamespacedName{Name: secretName, Namespace: participantOrgNamespace(participant)},
				&secret,
			)).To(Succeed())
		}

		var ordererDeployment appsv1.Deployment
		err = k8sClient.Get(
			ctx,
			types.NamespacedName{Name: "orderer0", Namespace: participantOrgNamespace(participant)},
			&ordererDeployment,
		)
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
	})

	It("joins participant peers with imported channel blocks once local infrastructure is ready", func() {
		participant := fabricParticipantFixture("participant-join")
		Expect(k8sClient.Create(ctx, participant)).To(Succeed())
		DeferCleanup(func() {
			Expect(k8sClient.Delete(ctx, participant)).To(Succeed())
		})
		DeferCleanup(cleanupParticipantNamespace, ctx, participant)
		createParticipantRemoteArtifacts(ctx, participant)

		reconciler := &FabricParticipantReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
		}
		request := reconcile.Request{
			NamespacedName: types.NamespacedName{Name: participant.Name, Namespace: resourceNamespace},
		}

		_, err := reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())

		localNet := participantLocalFabricNetwork(participant)
		namespace := participantOrgNamespace(participant)
		markDeploymentReady(ctx, namespace, "bankb-ca")
		writeEnrolledOrgIdentitySecrets(ctx, localNet, participant.Spec.Org, namespace)

		_, err = reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		markDeploymentReady(ctx, namespace, "peer0")

		result, err := reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).NotTo(BeZero())

		var block corev1.ConfigMap
		Expect(k8sClient.Get(
			ctx,
			types.NamespacedName{Name: "settlement-channel-block", Namespace: namespace},
			&block,
		)).To(Succeed())
		Expect(block.BinaryData).To(HaveKeyWithValue("settlement.block", []byte("CHANNEL_BLOCK")))
		Expect(block.Labels[labelAppComponent]).To(Equal(componentChannel))
		Expect(block.Labels[labelChannel]).To(Equal("settlement"))

		var serviceAccount corev1.ServiceAccount
		Expect(k8sClient.Get(
			ctx,
			types.NamespacedName{Name: "settlement-channel-bootstrapper", Namespace: namespace},
			&serviceAccount,
		)).To(Succeed())

		var joinJob batchv1.Job
		Expect(k8sClient.Get(
			ctx,
			types.NamespacedName{Name: "settlement-bankb-peer0-peer-join", Namespace: namespace},
			&joinJob,
		)).To(Succeed())
		Expect(joinJob.Annotations[annotationSucceededJobCleanup]).To(Equal("true"))
		Expect(joinJob.Spec.Template.Spec.ServiceAccountName).To(Equal("settlement-channel-bootstrapper"))
		Expect(configMapVolumeNames(joinJob.Spec.Template.Spec)).To(HaveKeyWithValue(channelBlockVolumeName, "settlement-channel-block"))
		Expect(secretVolumeNames(joinJob.Spec.Template.Spec)).To(HaveKeyWithValue("msp-bankb", "bankb-admin-msp"))
		Expect(secretVolumeNames(joinJob.Spec.Template.Spec)).To(HaveKeyWithValue("admin-tls-bankb", "bankb-admin-tls"))
		Expect(joinJob.Spec.Template.Spec.InitContainers).To(HaveLen(1))
		joinContainer := joinJob.Spec.Template.Spec.InitContainers[0]
		Expect(joinContainer.Command[2]).To(ContainSubstring("peer channel join"))
		Expect(joinContainer.Command[2]).To(ContainSubstring("CORE_PEER_LOCALMSPID=\"BankBMSP\""))
		Expect(joinContainer.Command[2]).To(ContainSubstring("peer0." + namespace + ".svc.cluster.local:7051"))

		var pending fabricopsv1alpha1.FabricParticipant
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: participant.Name, Namespace: resourceNamespace}, &pending)).
			To(Succeed())
		Expect(pending.Status.ChannelsReady).To(BeFalse())
		Expect(pending.Status.ChannelStatus).To(HaveLen(1))
		Expect(pending.Status.ChannelStatus[0].Peers.Ready).To(Equal(int32(0)))
		Expect(pending.Status.ChannelStatus[0].Joined).To(BeFalse())
		Expect(pending.Status.ChannelStatus[0].Ready).To(BeFalse())

		markJobComplete(ctx, namespace, "settlement-bankb-peer0-peer-join")
		createPeerJoinResultConfigMap(ctx, namespace, "settlement-bankb-peer0-peer-join-result", "settlement")

		_, err = reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())

		var joined fabricopsv1alpha1.FabricParticipant
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: participant.Name, Namespace: resourceNamespace}, &joined)).
			To(Succeed())
		Expect(joined.Status.Phase).To(Equal(fabricopsv1alpha1.PhaseCreating))
		Expect(joined.Status.Message).To(Equal("Waiting for chaincode install Job"))
		Expect(joined.Status.LocalInfrastructureReady).To(BeTrue())
		Expect(joined.Status.RemoteArtifactsReady).To(BeTrue())
		Expect(joined.Status.ChannelsReady).To(BeTrue())
		Expect(joined.Status.ChaincodeLifecycleReady).To(BeFalse())
		Expect(joined.Status.ChannelStatus).To(HaveLen(1))
		Expect(joined.Status.ChannelStatus[0].Peers.Ready).To(Equal(int32(1)))
		Expect(joined.Status.ChannelStatus[0].Joined).To(BeTrue())
		Expect(joined.Status.ChannelStatus[0].Ready).To(BeTrue())
		Expect(joined.Status.ChaincodeStatus).To(HaveLen(1))
		Expect(joined.Status.ChaincodeStatus[0].PackageReady).To(BeTrue())
		Expect(joined.Status.ChaincodeStatus[0].Installed).To(BeFalse())

		var packageConfig corev1.ConfigMap
		Expect(k8sClient.Get(
			ctx,
			types.NamespacedName{Name: "settlement-settlement-bankb-package", Namespace: namespace},
			&packageConfig,
		)).To(Succeed())
		Expect(packageConfig.Data[chaincodePackageLabelKey]).To(Equal("settlement_settlement_1.0"))
		Expect(packageConfig.Data[chaincodeConnectionAddrKey]).
			To(Equal("settlement-settlement-bankb-{{.peer_hostname}}-ccaas." + namespace + ".svc.cluster.local:7052"))
		Expect(packageConfig.BinaryData).To(HaveKey("settlement_settlement_1.0.tar.gz"))

		var installJob batchv1.Job
		Expect(k8sClient.Get(
			ctx,
			types.NamespacedName{Name: "settlement-settlement-1-0-bankb-peer0-install", Namespace: namespace},
			&installJob,
		)).To(Succeed())
		Expect(installJob.Spec.Template.Spec.ServiceAccountName).To(Equal("settlement-settlement-1-0-bankb-peer0-installer"))
		Expect(configMapVolumeNames(installJob.Spec.Template.Spec)).
			To(HaveKeyWithValue(chaincodePackageVolumeName, "settlement-settlement-bankb-package"))

		channels := apiMeta.FindStatusCondition(joined.Status.Conditions, conditionChannelsReady)
		Expect(channels).NotTo(BeNil())
		Expect(channels.Status).To(Equal(metav1.ConditionTrue))
		Expect(channels.Reason).To(Equal("ParticipantChannelsJoined"))

		chaincodes := apiMeta.FindStatusCondition(joined.Status.Conditions, conditionChaincodeLifecycleReady)
		Expect(chaincodes).NotTo(BeNil())
		Expect(chaincodes.Status).To(Equal(metav1.ConditionFalse))
		Expect(chaincodes.Message).To(Equal("Waiting for chaincode install Job"))
	})

	It("installs and approves participant chaincodes after channel join", func() {
		participant := fabricParticipantFixture("participant-lifecycle")
		Expect(k8sClient.Create(ctx, participant)).To(Succeed())
		DeferCleanup(func() {
			Expect(k8sClient.Delete(ctx, participant)).To(Succeed())
		})
		DeferCleanup(cleanupParticipantNamespace, ctx, participant)
		createParticipantRemoteArtifacts(ctx, participant)

		reconciler := &FabricParticipantReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
		}
		request := reconcile.Request{
			NamespacedName: types.NamespacedName{Name: participant.Name, Namespace: resourceNamespace},
		}

		namespace := prepareParticipantChannelJoined(ctx, reconciler, participant, request)
		chaincode := participantChaincodeAsNetworkChaincode(participant.Spec.Chaincodes[0])
		packageID := "settlement_settlement_1.0:abc123"
		createParticipantChaincodePackageIDConfigMap(ctx, namespace, chaincode, participant.Spec.Org, "peer0", packageID)

		_, err := reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())

		var chaincodeService corev1.Service
		Expect(k8sClient.Get(
			ctx,
			types.NamespacedName{Name: "settlement-settlement-bankb-peer0-ccaas", Namespace: namespace},
			&chaincodeService,
		)).To(Succeed())

		var chaincodeDeployment appsv1.Deployment
		Expect(k8sClient.Get(
			ctx,
			types.NamespacedName{Name: "settlement-settlement-bankb-peer0-ccaas", Namespace: namespace},
			&chaincodeDeployment,
		)).To(Succeed())
		Expect(chaincodeDeployment.Spec.Template.Spec.Containers[0].Image).
			To(Equal("ghcr.io/lf-decentralized-trust-labs/fabricops-node-settlement:0.2.0"))
		Expect(envMap(chaincodeDeployment.Spec.Template.Spec.Containers[0])[envCCAASChaincodeID]).
			To(Equal(packageID))

		var ordererTLS corev1.Secret
		Expect(k8sClient.Get(
			ctx,
			types.NamespacedName{Name: "settlement-orderer0-tls", Namespace: namespace},
			&ordererTLS,
		)).To(Succeed())
		Expect(ordererTLS.Data).To(HaveKeyWithValue(tlsCACertKey, []byte("ORDERER0_TLS_CA")))

		var approveJob batchv1.Job
		Expect(k8sClient.Get(
			ctx,
			types.NamespacedName{Name: chaincodeApproveJobName(chaincode, participant.Spec.Org, packageID), Namespace: namespace},
			&approveJob,
		)).To(Succeed())
		Expect(approveJob.Annotations[annotationSucceededJobCleanup]).To(Equal("true"))
		Expect(approveJob.Spec.Template.Spec.ServiceAccountName).To(Equal("settlement-settlement-1-0-bankb-peer0-installer"))
		Expect(secretVolumeNames(approveJob.Spec.Template.Spec)).To(HaveKeyWithValue(chaincodeAdminMSPVolume, "bankb-admin-msp"))
		Expect(secretVolumeNames(approveJob.Spec.Template.Spec)).To(HaveKeyWithValue(chaincodeAdminTLSVolume, "bankb-admin-tls"))
		Expect(secretVolumeNames(approveJob.Spec.Template.Spec)).To(HaveKeyWithValue(channelOrdererTLSVolumeName("orderer0"), "settlement-orderer0-tls"))
		approveContainer := approveJob.Spec.Template.Spec.InitContainers[0]
		Expect(approveContainer.Command[2]).To(ContainSubstring("peer lifecycle chaincode approveformyorg"))
		Expect(approveContainer.Command[2]).To(ContainSubstring("ORDERER_ADDRESS=\"orderer0.bank-a.fabricops.io:7050\""))
		Expect(approveContainer.Command[2]).To(ContainSubstring("--ordererTLSHostnameOverride \"localhost\""))
		Expect(approveContainer.Command[2]).To(ContainSubstring("ENDORSEMENT_POLICY=\"AND('BankAMSP.member','BankBMSP.member')\""))
		Expect(approveContainer.Command[2]).To(ContainSubstring("--cafile \"/fabricops/chaincode/crypto/orderers/orderer0/tls/ca.crt\""))

		var approvedPending fabricopsv1alpha1.FabricParticipant
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: participant.Name, Namespace: resourceNamespace}, &approvedPending)).
			To(Succeed())
		Expect(approvedPending.Status.ChaincodeStatus[0].Installed).To(BeTrue())
		Expect(approvedPending.Status.ChaincodeStatus[0].Approved).To(BeFalse())
		Expect(approvedPending.Status.ChaincodeStatus[0].Ready).To(BeFalse())

		markDeploymentReady(ctx, namespace, "settlement-settlement-bankb-peer0-ccaas")
		markJobComplete(ctx, namespace, chaincodeApproveJobName(chaincode, participant.Spec.Org, packageID))
		createChaincodeLifecycleResultConfigMap(
			ctx,
			participantLocalFabricNetwork(participant),
			participant.Spec.Org,
			chaincode,
			chaincodeApproveResultConfigMapName(chaincode, participant.Spec.Org, packageID),
			chaincodeQueryApprovedKey,
			chaincodeSequence(chaincode),
		)

		_, err = reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())

		var ready fabricopsv1alpha1.FabricParticipant
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: participant.Name, Namespace: resourceNamespace}, &ready)).
			To(Succeed())
		Expect(ready.Status.Phase).To(Equal(fabricopsv1alpha1.PhaseReady))
		Expect(ready.Status.Message).
			To(Equal("FabricParticipant local infrastructure, imported artifacts, declared channels, and chaincode lifecycle are ready"))
		Expect(ready.Status.ChannelsReady).To(BeTrue())
		Expect(ready.Status.ChaincodeLifecycleReady).To(BeTrue())
		Expect(ready.Status.ChaincodeStatus).To(HaveLen(1))
		Expect(ready.Status.ChaincodeStatus[0].Installed).To(BeTrue())
		Expect(ready.Status.ChaincodeStatus[0].Approved).To(BeTrue())
		Expect(ready.Status.ChaincodeStatus[0].Ready).To(BeTrue())

		readyCondition := apiMeta.FindStatusCondition(ready.Status.Conditions, conditionReady)
		Expect(readyCondition).NotTo(BeNil())
		Expect(readyCondition.Status).To(Equal(metav1.ConditionTrue))

		chaincodes := apiMeta.FindStatusCondition(ready.Status.Conditions, conditionChaincodeLifecycleReady)
		Expect(chaincodes).NotTo(BeNil())
		Expect(chaincodes.Status).To(Equal(metav1.ConditionTrue))
		Expect(chaincodes.Reason).To(Equal("ParticipantChaincodesReady"))
	})

	It("reports failed status for invalid imported artifacts", func() {
		participant := fabricParticipantFixture("participant-invalid")
		participant.Spec.Network.Orderers[0].TLSRootCARef = nil
		participant.Spec.Network.Peers = []fabricopsv1alpha1.ParticipantPeerEndpoint{{
			Org:     "BankA",
			Name:    "peer0",
			Address: "peer0.banka.fabricops.io:7051",
		}}
		participant.Spec.Channels[0].Peers = []string{"peer9"}
		Expect(k8sClient.Create(ctx, participant)).To(Succeed())
		DeferCleanup(func() {
			Expect(k8sClient.Delete(ctx, participant)).To(Succeed())
		})
		DeferCleanup(cleanupParticipantNamespace, ctx, participant)

		reconciler := &FabricParticipantReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
		}
		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: participant.Name, Namespace: resourceNamespace},
		})
		Expect(err).NotTo(HaveOccurred())

		var updated fabricopsv1alpha1.FabricParticipant
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: participant.Name, Namespace: resourceNamespace}, &updated)).
			To(Succeed())
		Expect(updated.Status.Phase).To(Equal(fabricopsv1alpha1.PhaseFailed))
		Expect(updated.Status.Message).To(ContainSubstring("tlsRootCARef is required"))
		Expect(updated.Status.Message).To(ContainSubstring("spec.network.peers[0].tlsRootCARef is required"))
		Expect(updated.Status.Message).To(ContainSubstring("unknown local peers: peer9"))

		ready := apiMeta.FindStatusCondition(updated.Status.Conditions, conditionReady)
		Expect(ready).NotTo(BeNil())
		Expect(ready.Status).To(Equal(metav1.ConditionFalse))
		Expect(ready.Reason).To(Equal("TopologyInvalid"))

		var namespace corev1.Namespace
		err = k8sClient.Get(ctx, types.NamespacedName{Name: participantOrgNamespace(participant)}, &namespace)
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
	})

	It("reports missing remote artifact status for imported peer TLS roots", func() {
		participant := fabricParticipantFixture("participant-peer-artifact")
		participant.Spec.Network.Peers = []fabricopsv1alpha1.ParticipantPeerEndpoint{{
			Org:     "BankA",
			Name:    "peer0",
			Address: "peer0.banka.fabricops.io:7051",
			TLSRootCARef: &fabricopsv1alpha1.ParticipantArtifactKeyRef{
				ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: participant.Name + "-banka-peer0-artifacts"},
					Key:                  "tls-ca.pem",
				},
			},
		}}
		Expect(k8sClient.Create(ctx, participant)).To(Succeed())
		DeferCleanup(func() {
			Expect(k8sClient.Delete(ctx, participant)).To(Succeed())
		})
		DeferCleanup(cleanupParticipantNamespace, ctx, participant)
		createParticipantRemoteArtifacts(ctx, participant)

		reconciler := &FabricParticipantReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
		}
		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: participant.Name, Namespace: resourceNamespace},
		})
		Expect(err).NotTo(HaveOccurred())

		var updated fabricopsv1alpha1.FabricParticipant
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: participant.Name, Namespace: resourceNamespace}, &updated)).
			To(Succeed())
		Expect(updated.Status.RemoteArtifactsReady).To(BeFalse())
		remote := apiMeta.FindStatusCondition(updated.Status.Conditions, conditionRemoteArtifactsReady)
		Expect(remote).NotTo(BeNil())
		Expect(remote.Status).To(Equal(metav1.ConditionFalse))
		Expect(remote.Message).To(ContainSubstring("spec.network.peers[0].tlsRootCARef.configMapKeyRef"))
		Expect(remote.Message).To(ContainSubstring(participant.Name + "-banka-peer0-artifacts"))
	})
})

func fabricParticipantFixture(name string) *fabricopsv1alpha1.FabricParticipant {
	return &fabricopsv1alpha1.FabricParticipant{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: fabricopsv1alpha1.FabricParticipantSpec{
			Global: fabricopsv1alpha1.GlobalConfig{
				FabricVersion: "3.1.0",
				TLS:           true,
			},
			Org: fabricopsv1alpha1.Org{
				Organization: fabricopsv1alpha1.OrgMeta{
					Name:    "BankB",
					Domain:  "bankb.fabricops.io",
					MSPName: "BankBMSP",
				},
				CA: fabricopsv1alpha1.CAConfig{DB: "sqlite"},
				Peer: &fabricopsv1alpha1.PeerConfig{
					Instances: 1,
					DB:        "leveldb",
					Prefix:    "peer",
				},
			},
			Network: fabricopsv1alpha1.ParticipantNetwork{
				Name:         "settlement-network",
				FounderMSPID: "BankAMSP",
				Orderers: []fabricopsv1alpha1.ParticipantOrdererEndpoint{{
					Org:                 "OrdererOrg",
					Name:                "orderer0",
					ClientAddress:       "orderer0.bank-a.fabricops.io:7050",
					TLSHostnameOverride: "localhost",
					TLSRootCARef: &fabricopsv1alpha1.ParticipantArtifactKeyRef{
						ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: name + "-orderer0-artifacts"},
							Key:                  "tls-ca.pem",
						},
					},
				}},
			},
			Channels: []fabricopsv1alpha1.ParticipantChannel{{
				Name: "settlement",
				BlockRef: fabricopsv1alpha1.ParticipantArtifactKeyRef{
					ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: name + "-settlement-artifacts"},
						Key:                  "channel.block",
					},
				},
				Peers: []string{"peer0"},
				AnchorPeers: []fabricopsv1alpha1.ParticipantAnchorPeer{{
					Name: "peer0",
					Host: "peer0.bankb.fabricops.io",
					Port: 7051,
				}},
				Membership: &fabricopsv1alpha1.ParticipantChannelMembership{
					ApplicationPolicy:    "/Channel/Application/Admins",
					RequiredSignerMSPIDs: []string{"BankAMSP"},
				},
			}},
			Chaincodes: []fabricopsv1alpha1.ParticipantChaincode{{
				Name:              "settlement",
				Channel:           "settlement",
				Version:           "1.0",
				PackageLabel:      "settlement_settlement_1.0",
				Sequence:          1,
				EndorsementPolicy: "AND('BankAMSP.member','BankBMSP.member')",
				Image:             "ghcr.io/lf-decentralized-trust-labs/fabricops-node-settlement:0.2.0",
			}},
		},
	}
}

func createParticipantRemoteArtifacts(
	ctx context.Context,
	participant *fabricopsv1alpha1.FabricParticipant,
) {
	ordererArtifacts := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      participant.Spec.Network.Orderers[0].TLSRootCARef.ConfigMapKeyRef.Name,
			Namespace: participant.Namespace,
		},
		Data: map[string]string{
			"tls-ca.pem": "ORDERER0_TLS_CA",
		},
	}
	Expect(k8sClient.Create(ctx, ordererArtifacts)).To(Succeed())
	DeferCleanup(func() {
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, ordererArtifacts))).To(Succeed())
	})

	channelArtifacts := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      participant.Spec.Channels[0].BlockRef.ConfigMapKeyRef.Name,
			Namespace: participant.Namespace,
		},
		BinaryData: map[string][]byte{
			"channel.block": []byte("CHANNEL_BLOCK"),
		},
	}
	Expect(k8sClient.Create(ctx, channelArtifacts)).To(Succeed())
	DeferCleanup(func() {
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, channelArtifacts))).To(Succeed())
	})

	for _, chaincode := range participant.Spec.Chaincodes {
		if chaincode.PackageRef == nil || chaincode.PackageRef.ConfigMapKeyRef == nil {
			continue
		}
		packageArtifacts := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      chaincode.PackageRef.ConfigMapKeyRef.Name,
				Namespace: participant.Namespace,
			},
			BinaryData: map[string][]byte{
				chaincode.PackageRef.ConfigMapKeyRef.Key: []byte("CHAINCODE_PACKAGE"),
			},
		}
		Expect(k8sClient.Create(ctx, packageArtifacts)).To(Succeed())
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, packageArtifacts))).To(Succeed())
		})
	}
}

func prepareParticipantChannelJoined(
	ctx context.Context,
	reconciler *FabricParticipantReconciler,
	participant *fabricopsv1alpha1.FabricParticipant,
	request reconcile.Request,
) string {
	_, err := reconciler.Reconcile(ctx, request)
	Expect(err).NotTo(HaveOccurred())

	localNet := participantLocalFabricNetwork(participant)
	namespace := participantOrgNamespace(participant)
	markDeploymentReady(ctx, namespace, "bankb-ca")
	writeEnrolledOrgIdentitySecrets(ctx, localNet, participant.Spec.Org, namespace)

	_, err = reconciler.Reconcile(ctx, request)
	Expect(err).NotTo(HaveOccurred())
	markDeploymentReady(ctx, namespace, "peer0")

	_, err = reconciler.Reconcile(ctx, request)
	Expect(err).NotTo(HaveOccurred())
	markJobComplete(ctx, namespace, "settlement-bankb-peer0-peer-join")
	createPeerJoinResultConfigMap(ctx, namespace, "settlement-bankb-peer0-peer-join-result", "settlement")

	_, err = reconciler.Reconcile(ctx, request)
	Expect(err).NotTo(HaveOccurred())

	return namespace
}

func createParticipantChaincodePackageIDConfigMap(
	ctx context.Context,
	namespace string,
	chaincode fabricopsv1alpha1.Chaincode,
	org fabricopsv1alpha1.Org,
	peerName string,
	packageID string,
) {
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      chaincodePackageIDConfigMapName(chaincode, org, peerName),
			Namespace: namespace,
		},
		Data: map[string]string{
			chaincodePackageIDKey:      packageID,
			chaincodeChaincodeIDKey:    packageID,
			chaincodePackageHashKey:    chaincodePackageHash(packageID),
			chaincodeQueryInstalledKey: `{"installed_chaincodes":[{"label":"settlement_settlement_1.0","package_id":"settlement_settlement_1.0:abc123"}]}`,
		},
	}
	Expect(k8sClient.Create(ctx, configMap)).To(Succeed())
}

func participantOrgNamespace(participant *fabricopsv1alpha1.FabricParticipant) string {
	return orgNamespaceName(participantLocalFabricNetwork(participant), participant.Spec.Org)
}

func cleanupParticipantNamespace(ctx context.Context, participant *fabricopsv1alpha1.FabricParticipant) {
	namespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: participantOrgNamespace(participant)},
	}
	Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, namespace))).To(Succeed())
}
