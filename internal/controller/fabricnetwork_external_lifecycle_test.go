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
	"encoding/base64"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	fabricopsv1alpha1 "github.com/dpereowei/fabricops/api/v1alpha1"
)

var _ = Describe("External chaincode lifecycle peers", func() {
	It("uses external org anchor peers and public TLS roots for founder commits", func() {
		tlsRoot := []byte("participant peer tls root")
		applicationOrgJSON := fmt.Sprintf(`{
  "values": {
    "MSP": {
      "value": {
        "config": {
          "name": "BankBMSP",
          "tls_root_certs": [%q]
        }
      }
    },
    "AnchorPeers": {
      "value": {
        "anchor_peers": [
          {"host": "peer0.bankb.example.com", "port": 7051}
        ]
      }
    }
  }
}`, base64.StdEncoding.EncodeToString(tlsRoot))

		configMap := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "bankb-application-org-lifecycle",
				Namespace: "default",
			},
			Data: map[string]string{
				"org.json": applicationOrgJSON,
			},
		}
		Expect(k8sClient.Create(ctx, configMap)).To(Succeed())
		DeferCleanup(k8sClient.Delete, ctx, configMap)

		network := &fabricopsv1alpha1.FabricNetwork{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "federated",
				Namespace: "default",
			},
			Spec: fabricopsv1alpha1.FabricNetworkSpec{
				Global: fabricopsv1alpha1.GlobalConfig{TLS: true},
			},
		}
		channel := fabricopsv1alpha1.Channel{
			Name: "settlement",
			ExternalOrgs: []fabricopsv1alpha1.ChannelExternalOrg{
				{
					Name:  "BankB",
					MSPID: "BankBMSP",
					ApplicationOrgRef: fabricopsv1alpha1.ChannelArtifactKeyRef{
						ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: configMap.Name},
							Key:                  "org.json",
						},
					},
				},
			},
		}
		reconciler := &FabricNetworkReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}

		peers, err := reconciler.externalChaincodeLifecyclePeers(ctx, network, channel)
		Expect(err).NotTo(HaveOccurred())
		Expect(peers).To(HaveLen(1))
		Expect(peers[0].org.Organization.Name).To(Equal("BankB"))
		Expect(peers[0].org.Organization.MSPName).To(Equal("BankBMSP"))
		Expect(peers[0].peerName).To(Equal("external-peer-0"))
		Expect(peers[0].tlsRootCA).To(Equal(tlsRoot))
		Expect(chaincodeLifecyclePeerAddress(peers[0])).To(Equal("peer0.bankb.example.com:7051"))

		submitter := chaincodeLifecyclePeer{
			org: fabricopsv1alpha1.Org{
				Organization: fabricopsv1alpha1.OrgMeta{
					Name:    "BankA",
					MSPName: "BankAMSP",
				},
			},
			peerName:  "peer0",
			namespace: "fo-federated-banka",
		}
		chaincode := fabricopsv1alpha1.Chaincode{
			Name:    "settlement",
			Channel: "settlement",
			Version: "1.0",
		}
		orderer := ordererInstance{name: "orderer0"}
		job := buildChaincodeCommitJob(
			network,
			channel,
			chaincode,
			"settlement:abc123",
			submitter,
			orderer,
			[]chaincodeLifecyclePeer{submitter, peers[0]},
		)
		externalPeerTLSVolume := chaincodePeerTLSVolumeName(peers[0].org, peers[0].peerName)
		Expect(secretVolumeItems(job.Spec.Template.Spec, externalPeerTLSVolume)).To(Equal([]corev1.KeyToPath{
			{Key: tlsCACertKey, Path: "ca.crt"},
		}))
	})
})
