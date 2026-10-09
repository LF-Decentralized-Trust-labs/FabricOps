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
	"os/exec"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

var _ = Describe("Channel config operations", func() {
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
