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
	"fmt"
	"os/exec"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

var _ = Describe("Channel config operations", func() {
	It("preserves reviewed Raft proposal references through the API server", func() {
		net, channel, _, _, _ := channelConfigOperationFixture()
		net.Name, net.Namespace = "raft-proposal", "raft-proposal-schema"
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: net.Namespace}}
		Expect(k8sClient.Create(ctx, namespace)).To(Succeed())
		DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, namespace))).To(Succeed()) })
		channel.RaftUpdate = &fabricopsv1alpha1.ChannelRaftUpdate{
			OperationID: "add-orderer1", AdminOrg: "Orderer",
			Orderer: fabricopsv1alpha1.ChannelOrdererRef{Org: "Orderer", Name: "orderer0"},
			PlanRef: fabricopsv1alpha1.ChannelArtifactKeyRef{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "reviewed-raft-plan"}, Key: "plan.json"}},
			SignedUpdateRef: &fabricopsv1alpha1.ChannelArtifactKeyRef{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "signed-raft-update"}, Key: "envelope.pb"}},
		}
		net.Spec.Channels = []fabricopsv1alpha1.Channel{channel}
		net.Spec.Chaincodes = []fabricopsv1alpha1.Chaincode{}
		Expect(k8sClient.Create(ctx, net)).To(Succeed())
		DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, net))).To(Succeed()) })
		actual := &fabricopsv1alpha1.FabricNetwork{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(net), actual)).To(Succeed())
		Expect(actual.Spec.Channels[0].RaftUpdate).To(Equal(channel.RaftUpdate))
		copy := actual.DeepCopy()
		copy.Spec.Channels[0].RaftUpdate.PlanRef.ConfigMapKeyRef.Key = "other.json"
		Expect(actual.Spec.Channels[0].RaftUpdate.PlanRef.ConfigMapKeyRef.Key).To(Equal("plan.json"))
		copy.Spec.Channels[0].RaftUpdate.SignedUpdateRef.SecretKeyRef.Key = "other.pb"
		Expect(actual.Spec.Channels[0].RaftUpdate.SignedUpdateRef.SecretKeyRef.Key).To(Equal("envelope.pb"))
	})

	It("runs local org admission before attempting the new peer join", func() {
		net, channel, _, _, _ := channelConfigOperationFixture()
		net.Name = "operations-additions"
		joiningOrg := fabricopsv1alpha1.Org{
			Organization: fabricopsv1alpha1.OrgMeta{Name: "BankB", Domain: "bankb.example.com", MSPName: "BankBMSP"},
			CA:           fabricopsv1alpha1.CAConfig{DB: "sqlite"},
			Peer:         &fabricopsv1alpha1.PeerConfig{Instances: 1, DB: "CouchDB", Prefix: componentPeer},
		}
		net.Spec.Orgs = append(net.Spec.Orgs, joiningOrg)
		channel.Orgs = append(channel.Orgs, fabricopsv1alpha1.ChannelOrg{Name: "BankB", Peers: []string{"peer0"}})
		ordererOrg := net.Spec.Orgs[0]
		adminOrg := net.Spec.Orgs[1]
		hostNamespace := orgNamespaceName(net, ordererOrg)
		adminNamespace := orgNamespaceName(net, adminOrg)
		for _, name := range []string{hostNamespace, adminNamespace} {
			namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
			Expect(k8sClient.Create(ctx, namespace)).To(Succeed())
			DeferCleanup(func() {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, namespace))).To(Succeed())
			})
		}
		bootstrapBlock := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: channelBlockConfigMapName(channel.Name), Namespace: hostNamespace},
			BinaryData: map[string][]byte{channelBlockFileName(channel.Name): []byte("original-channel-block")},
		}
		Expect(k8sClient.Create(ctx, bootstrapBlock)).To(Succeed())
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, bootstrapBlock))).To(Succeed())
		})
		adminPeer := peerInstance{org: adminOrg, name: "peer0", namespace: adminNamespace}
		peerJoinResult := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: channelAnchorPeerUpdateResultConfigMapName(channel.Name, adminOrg), Namespace: adminNamespace},
			Data: map[string]string{channelAnchorPeerResultKey: fmt.Sprintf(
				`{"channel":"settlement","mspID":"BankAMSP","anchorPeers":[{"host":%q,"port":7051}]}`, channelPeerHost(adminPeer),
			)},
		}
		Expect(k8sClient.Create(ctx, peerJoinResult)).To(Succeed())
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, peerJoinResult))).To(Succeed())
		})
		localAddResult := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: channelLocalOrgAddResultConfigMapName(channel.Name, joiningOrg), Namespace: adminNamespace},
			Data:       map[string]string{localOrgAddResultKey: `{"channel":"settlement","mspID":"BankBMSP","outcome":"submitted"}`},
			BinaryData: map[string][]byte{localOrgAddBlockArtifactKey: []byte("updated-channel-block")},
		}
		Expect(k8sClient.Create(ctx, localAddResult)).To(Succeed())
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, localAddResult))).To(Succeed())
		})

		status := fabricopsv1alpha1.ChannelStatus{Orgs: []fabricopsv1alpha1.ChannelOrgStatus{
			{Name: "BankA", PeerNames: []string{"peer0"}},
			{Name: "BankB", PeerNames: []string{"peer0"}},
		}}
		reconciler := &FabricNetworkReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		message, err := reconciler.reconcileLocalOrgAdditions(ctx, net, channel, &status)
		Expect(err).NotTo(HaveOccurred())
		Expect(message).To(BeEmpty())
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: bootstrapBlock.Name, Namespace: hostNamespace}, bootstrapBlock)).To(Succeed())
		Expect(bootstrapBlock.BinaryData[channelBlockFileName(channel.Name)]).To(Equal([]byte("original-channel-block")))
	})

	It("preserves the genesis block after local org admission for the new peer join", func() {
		net, channel, adminOrg, adminPeer, orderer := channelConfigOperationFixture()
		joiningOrg := fabricopsv1alpha1.Org{
			Organization: fabricopsv1alpha1.OrgMeta{Name: "BankB", Domain: "bankb.example.com", MSPName: "BankBMSP"},
			CA:           fabricopsv1alpha1.CAConfig{DB: "sqlite"},
			Peer:         &fabricopsv1alpha1.PeerConfig{Instances: 1, DB: "CouchDB", Prefix: componentPeer},
		}
		net.Spec.Orgs = append(net.Spec.Orgs, joiningOrg)
		channel.Orgs = append(channel.Orgs, fabricopsv1alpha1.ChannelOrg{Name: "BankB", Peers: []string{"peer0"}})
		hostNamespace := orderer.namespace
		for _, name := range []string{hostNamespace, adminPeer.namespace} {
			namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
			Expect(k8sClient.Create(ctx, namespace)).To(Succeed())
			DeferCleanup(func() {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, namespace))).To(Succeed())
			})
		}
		bootstrapBlock := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: channelBlockConfigMapName(channel.Name), Namespace: hostNamespace},
			BinaryData: map[string][]byte{channelBlockFileName(channel.Name): []byte("original-channel-block")},
		}
		Expect(k8sClient.Create(ctx, bootstrapBlock)).To(Succeed())
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, bootstrapBlock))).To(Succeed())
		})
		result := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: channelLocalOrgAddResultConfigMapName(channel.Name, joiningOrg), Namespace: adminPeer.namespace},
			Data:       map[string]string{localOrgAddResultKey: `{"channel":"settlement","mspID":"BankBMSP","outcome":"submitted"}`},
			BinaryData: map[string][]byte{localOrgAddBlockArtifactKey: []byte("updated-channel-block")},
		}
		Expect(k8sClient.Create(ctx, result)).To(Succeed())
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, result))).To(Succeed())
		})

		reconciler := &FabricNetworkReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		ready, message, err := reconciler.reconcileLocalOrgAdd(
			ctx, net, channel, adminOrg, adminPeer, orderer, joiningOrg,
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(ready).To(BeTrue())
		Expect(message).To(BeEmpty())
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: bootstrapBlock.Name, Namespace: hostNamespace}, bootstrapBlock)).To(Succeed())
		Expect(bootstrapBlock.BinaryData[channelBlockFileName(channel.Name)]).To(Equal([]byte("original-channel-block")))
	})

	It("reads a completed local org addition only when it includes the refreshed channel block", func() {
		net, channel, _, adminPeer, _ := channelConfigOperationFixture()
		adminPeer.namespace += "-evidence"
		joiningOrg := fabricopsv1alpha1.Org{Organization: fabricopsv1alpha1.OrgMeta{Name: "BankB", MSPName: "BankBMSP"}}
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: adminPeer.namespace}}
		Expect(k8sClient.Create(ctx, namespace)).To(Succeed())
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, namespace))).To(Succeed())
		})
		result := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      channelLocalOrgAddResultConfigMapName(channel.Name, joiningOrg),
				Namespace: adminPeer.namespace,
			},
			Data: map[string]string{
				localOrgAddResultKey: `{"channel":"settlement","mspID":"BankBMSP","outcome":"submitted"}`,
			},
			BinaryData: map[string][]byte{localOrgAddBlockArtifactKey: []byte("updated-channel-block")},
		}
		Expect(k8sClient.Create(ctx, result)).To(Succeed())
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, result))).To(Succeed())
		})

		reconciler := &FabricNetworkReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		evidence, message, err := reconciler.localOrgAddEvidence(ctx, adminPeer.namespace, channel.Name, joiningOrg)
		Expect(err).NotTo(HaveOccurred())
		Expect(message).To(BeEmpty())
		Expect(evidence).NotTo(BeNil())
		Expect(evidence.Outcome).To(Equal(localOrgAddOutcomeSubmitted))
		Expect(evidence.ChannelBlock).To(Equal([]byte("updated-channel-block")))
		Expect(net.Name).To(Equal("operations"))
	})

	It("builds a local org addition Job that generates the org config before submitting the update", func() {
		net, channel, adminOrg, adminPeer, orderer := channelConfigOperationFixture()
		joiningOrg := fabricopsv1alpha1.Org{
			Organization: fabricopsv1alpha1.OrgMeta{
				Name:    "BankB",
				Domain:  "bankb.example.com",
				MSPName: "BankBMSP",
			},
			CA: fabricopsv1alpha1.CAConfig{DB: "sqlite"},
			Peer: &fabricopsv1alpha1.PeerConfig{
				Instances: 1,
				DB:        "CouchDB",
				Prefix:    componentPeer,
			},
		}
		net.Spec.Orgs = append(net.Spec.Orgs, joiningOrg)
		channel.Orgs = append(channel.Orgs, fabricopsv1alpha1.ChannelOrg{Name: "BankB", Peers: []string{"peer0"}})

		job := buildLocalOrgAddJob(net, channel, adminOrg, adminPeer.namespace, adminPeer, orderer, joiningOrg)

		Expect(job.Name).To(Equal("settlement-bankb-local-org-add"))
		Expect(job.Spec.Template.Spec.InitContainers).To(HaveLen(1))
		script := job.Spec.Template.Spec.InitContainers[0].Command[2]
		syntaxCheck := exec.Command("sh", "-n")
		syntaxCheck.Stdin = strings.NewReader(script)
		Expect(syntaxCheck.Run()).To(Succeed())
		Expect(script).To(ContainSubstring(`JOIN_MSP_ID="BankBMSP"`))
		Expect(script).To(ContainSubstring("configtxgen -configPath \"$CONFIG_DIR\" -printOrg \"$JOIN_MSP_ID\""))
		Expect(script).To(ContainSubstring(`write_local_org_result "staged"`))
		Expect(script).NotTo(ContainSubstring("peer channel update"))
		Expect(script).To(ContainSubstring("peer channel fetch config"))

		publisherEnv := envMap(job.Spec.Template.Spec.Containers[0])
		Expect(publisherEnv[envLocalOrgResultConfigMap]).To(Equal("settlement-bankb-local-org-add-result"))
		Expect(publisherEnv[envLocalOrgBlockKey]).To(Equal(localOrgAddBlockArtifactKey))
		Expect(publishLocalOrgAddResultScript()).To(ContainSubstring(`if [ -f "/fabricops/channel/output/$FABRICOPS_LOCAL_ORG_UPDATE_FILE" ]; then`))
	})

	It("treats omitted external org state as Present", func() {
		externalOrg := fabricopsv1alpha1.ChannelExternalOrg{Name: "BankB", MSPID: "BankBMSP"}

		Expect(channelExternalOrgDesiredState(externalOrg)).To(Equal(fabricopsv1alpha1.ChannelExternalOrgPresent))
	})

	It("builds an application org removal Job without requiring org material", func() {
		net, channel, adminOrg, adminPeer, orderer := channelConfigOperationFixture()
		externalOrg := fabricopsv1alpha1.ChannelExternalOrg{
			Name:        "BankB",
			MSPID:       "BankBMSP",
			State:       fabricopsv1alpha1.ChannelExternalOrgAbsent,
			OperationID: "remove-2026q4",
		}

		job := buildExternalOrgUpdateJob(
			net,
			channel,
			adminOrg,
			adminPeer.namespace,
			adminPeer,
			orderer,
			externalOrg,
			true,
		)

		Expect(job.Name).To(Equal("settlement-bankb-external-org-remove-remove-2026q4"))
		Expect(configMapVolumeNames(job.Spec.Template.Spec)).NotTo(HaveKey(externalOrgConfigVolumeName))
		Expect(job.Spec.Template.Spec.InitContainers).To(HaveLen(1))
		script := job.Spec.Template.Spec.InitContainers[0].Command[2]
		syntaxCheck := exec.Command("sh", "-n")
		syntaxCheck.Stdin = strings.NewReader(script)
		Expect(syntaxCheck.Run()).To(Succeed())
		Expect(script).To(ContainSubstring(`DESIRED_STATE="Absent"`))
		Expect(script).To(ContainSubstring(`OPERATION_ID="remove-2026q4"`))
		Expect(script).To(ContainSubstring("SUBMIT_UPDATE=true"))
		Expect(script).To(ContainSubstring("del(.channel_group.groups.Application.groups[$msp])"))
		Expect(script).To(ContainSubstring(`write_external_org_result "alreadyApplied"`))
		Expect(script).To(ContainSubstring(`write_external_org_result "submitted"`))

		publisherEnv := envMap(job.Spec.Template.Spec.Containers[0])
		Expect(publisherEnv[envExternalOrgResultConfigMap]).To(Equal("settlement-bankb-external-org-remove-remove-2026q4-result"))
		Expect(publisherEnv[envExternalOrgUpdateKey]).To(Equal(externalOrgUpdateArtifactKey))
		Expect(publisherEnv[envExternalOrgUpdateFile]).To(Equal("bankbmsp-settlement-org-remove-remove-2026q4.tx"))
	})

	It("validates removals without an application org artifact", func() {
		net, channel, _, _, _ := channelConfigOperationFixture()
		channel.ExternalOrgs = []fabricopsv1alpha1.ChannelExternalOrg{
			{
				Name:  "BankB",
				MSPID: "BankBMSP",
				State: fabricopsv1alpha1.ChannelExternalOrgAbsent,
			},
		}
		net.Spec.Channels = []fabricopsv1alpha1.Channel{channel}

		Expect(validateFabricNetworkTopology(net)).To(BeEmpty())

		net.Spec.Channels[0].ExternalOrgs[0].AnchorPeers = []fabricopsv1alpha1.ChannelExternalAnchorPeer{
			{Host: "peer0.bankb.example.com", Port: 7051},
		}
		Expect(validateFabricNetworkTopology(net)).To(ContainElement(
			`channel "settlement" externalOrgs[0] anchorPeers must be empty when state is Absent`,
		))
	})
})

func channelConfigOperationFixture() (
	*fabricopsv1alpha1.FabricNetwork,
	fabricopsv1alpha1.Channel,
	fabricopsv1alpha1.Org,
	peerInstance,
	ordererInstance,
) {
	ordererOrg := fabricopsv1alpha1.Org{
		Organization: fabricopsv1alpha1.OrgMeta{
			Name:    "Orderer",
			Domain:  "orderer.example.com",
			MSPName: "OrdererMSP",
		},
		CA: fabricopsv1alpha1.CAConfig{DB: "sqlite"},
		Orderers: []fabricopsv1alpha1.OrdererGroup{
			{GroupName: "group1", Type: "raft", Instances: 1, Prefix: componentOrderer},
		},
	}
	adminOrg := fabricopsv1alpha1.Org{
		Organization: fabricopsv1alpha1.OrgMeta{
			Name:    "BankA",
			Domain:  "banka.example.com",
			MSPName: "BankAMSP",
		},
		CA: fabricopsv1alpha1.CAConfig{DB: "sqlite"},
		Peer: &fabricopsv1alpha1.PeerConfig{
			Instances: 1,
			DB:        "CouchDB",
			Prefix:    componentPeer,
		},
	}
	net := &fabricopsv1alpha1.FabricNetwork{
		ObjectMeta: metav1.ObjectMeta{Name: "operations", Namespace: "default"},
		Spec: fabricopsv1alpha1.FabricNetworkSpec{
			Global: fabricopsv1alpha1.GlobalConfig{FabricVersion: "2.5.14", TLS: true},
			Orgs:   []fabricopsv1alpha1.Org{ordererOrg, adminOrg},
		},
	}
	channel := fabricopsv1alpha1.Channel{
		Name: "settlement",
		Orgs: []fabricopsv1alpha1.ChannelOrg{{Name: "BankA", Peers: []string{"peer0"}}},
	}
	adminPeer := peerInstance{org: adminOrg, name: "peer0", namespace: "fo-operations-banka"}
	orderer := ordererInstance{org: ordererOrg, name: "orderer0", namespace: "fo-operations-orderer"}
	return net, channel, adminOrg, adminPeer, orderer
}
