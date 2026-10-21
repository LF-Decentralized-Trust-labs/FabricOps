//go:build e2e

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

package e2e

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
	"github.com/LF-Decentralized-Trust-labs/FabricOps/internal/channelconfig"
)

func testRaftConsenterUpdates(existingSettlement string) {
	GinkgoHelper()
	genesis := admissionChannelBlock("settlement")
	dir := GinkgoT().TempDir()
	By("provisioning orderer2 as a follower without changing channel membership")
	runCommand(time.Minute, kubectlBin, "patch", "fabricnetwork", sampleName, "-n", sampleNamespace,
		"--type=json", "-p", `[{"op":"replace","path":"/spec/orgs/0/orderers/0/instances","value":3}]`)
	waitForFabricNetworkReadyGeneration(getFabricNetworkProbe().Metadata.Generation, 20*time.Minute)
	for _, action := range []string{"add", "remove"} {
		By("planning a live Raft consenter " + action + " using fabricopsctl")
		logs := runAdmissionAdminJob(dir, "settlement", "orderer", "raft-"+action+"-fetch", nil, `
peer channel fetch config /tmp/block.pb -c settlement \
  -o orderer0.fo-sample-orderer.svc.cluster.local:7050 --tls --cafile /tls/ca.crt
configtxlator proto_decode --input /tmp/block.pb --type common.Block --output /tmp/block.json
echo FABRICOPS_RAFT_BLOCK
base64 /tmp/block.json | tr -d '\n'
echo`)
		_, encoded, found := strings.Cut(logs, "FABRICOPS_RAFT_BLOCK\n")
		Expect(found).To(BeTrue())
		block, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
		Expect(err).NotTo(HaveOccurred())
		var decoded struct {
			Data struct {
				Data []struct {
					Payload struct {
						Data struct {
							Config json.RawMessage `json:"config"`
						} `json:"data"`
					} `json:"payload"`
				} `json:"data"`
			} `json:"data"`
		}
		Expect(json.Unmarshal(block, &decoded)).To(Succeed())
		Expect(decoded.Data.Data).To(HaveLen(1))
		consenters, err := channelconfig.RaftConsentersFromConfig(decoded.Data.Data[0].Payload.Data.Config)
		Expect(err).NotTo(HaveOccurred())
		host := "orderer2.fo-sample-orderer.svc.cluster.local"
		if action == "add" {
			Expect(consenters).To(HaveLen(2))
			publicCert := runCommandQuiet(30*time.Second, kubectlBin, "get", "secret", "orderer2-tls",
				"-n", "fo-sample-orderer", "-o", `jsonpath={.data.server\.crt}`)
			cert, decodeErr := base64.StdEncoding.DecodeString(strings.TrimSpace(publicCert))
			Expect(decodeErr).NotTo(HaveOccurred())
			Expect(cert).NotTo(BeEmpty())
			consenters = append(consenters, channelconfig.RaftConsenter{
				Host: host, Port: 7050, ClientTLSCert: cert, ServerTLSCert: cert})
		} else {
			Expect(consenters).To(HaveLen(3))
			retained := []channelconfig.RaftConsenter{}
			for _, consenter := range consenters {
				if consenter.Host != host {
					retained = append(retained, consenter)
				}
			}
			Expect(retained).To(HaveLen(2))
			consenters = retained
		}
		desired, err := json.Marshal(consenters)
		Expect(err).NotTo(HaveOccurred())
		blockPath, desiredPath, planPath := filepath.Join(dir, action+"-block.json"),
			filepath.Join(dir, action+"-consenters.json"), filepath.Join(dir, action+"-plan.json")
		Expect(os.WriteFile(blockPath, block, 0600)).To(Succeed())
		Expect(os.WriteFile(desiredPath, desired, 0600)).To(Succeed())
		runFabricOpsctl(time.Minute, "channel", "plan-raft-update", "--channel", "settlement",
			"--block", blockPath, "--consenters", desiredPath, "--out", planPath)
		name := "raft-" + action + "-orderer2"
		runCommand(time.Minute, kubectlBin, "create", "configmap", name, "-n", sampleNamespace,
			"--from-file=plan.json="+planPath)
		operation := fabricopsv1alpha1.ChannelRaftUpdate{OperationID: name, AdminOrg: "Orderer",
			Orderer: fabricopsv1alpha1.ChannelOrdererRef{Org: "Orderer", Name: "orderer0"},
			PlanRef: fabricopsv1alpha1.ChannelArtifactKeyRef{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: name}, Key: "plan.json"}}}
		operationJSON, err := json.Marshal(operation)
		Expect(err).NotTo(HaveOccurred())
		patch := fmt.Sprintf(`[{"op":"add","path":"/spec/channels/0/raftUpdate","value":%s}]`, operationJSON)
		runCommand(time.Minute, kubectlBin, "patch", "fabricnetwork", sampleName,
			"-n", sampleNamespace, "--type=json", "-p", patch)
		var staged fabricopsv1alpha1.ChannelRaftUpdateStatus
		Eventually(func(g Gomega) {
			staged = liveRaftStatus()
			g.Expect(staged.OperationID).To(Equal(name))
			g.Expect(staged.Phase).To(Equal(fabricopsv1alpha1.ChannelConfigUpdatePendingSignatures))
			g.Expect(staged.ArtifactConfigMapName).NotTo(BeEmpty())
		}, 5*time.Minute, 5*time.Second).Should(Succeed())
		output := runCommandQuiet(30*time.Second, kubectlBin, "get", "configmap", staged.ArtifactConfigMapName,
			"-n", staged.Namespace, "-o", "json")
		var artifact corev1.ConfigMap
		Expect(json.Unmarshal([]byte(output), &artifact)).To(Succeed())
		envelope := artifact.BinaryData["config-update.pb"]
		Expect(envelope).NotTo(BeEmpty())
		envelope = signChannelEnvelope(dir, "settlement", "orderer", "raft-"+action+"-sign", envelope)
		envelopePath := filepath.Join(dir, name+"-signed.pb")
		Expect(os.WriteFile(envelopePath, envelope, 0600)).To(Succeed())
		runCommand(time.Minute, kubectlBin, "create", "secret", "generic", name, "-n", sampleNamespace,
			"--from-file=envelope.pb="+envelopePath)
		patch = fmt.Sprintf(`[{"op":"add","path":"/spec/channels/0/raftUpdate/signedUpdateRef",`+
			`"value":{"secretKeyRef":{"name":%q,"key":"envelope.pb"}}}]`, name)
		runCommand(time.Minute, kubectlBin, "patch", "fabricnetwork", sampleName,
			"-n", sampleNamespace, "--type=json", "-p", patch)
		waitForFabricNetworkReadyGeneration(getFabricNetworkProbe().Metadata.Generation, 10*time.Minute)
		status := liveRaftStatus()
		Expect(status.Ready).To(BeTrue())
		Expect(status.Phase).To(Equal(fabricopsv1alpha1.ChannelConfigUpdateSubmitted))
		Expect(admissionChannelBlock("settlement")).To(Equal(genesis))
		invokeAndQuerySettlement(existingSettlement+"-raft-"+action, runtimeConfig.createFunction,
			runtimeConfig.readFunction, "BankA/peer0", "BankB/peer0")
	}
}

func liveRaftStatus() fabricopsv1alpha1.ChannelRaftUpdateStatus {
	GinkgoHelper()
	output := runCommandQuiet(30*time.Second, kubectlBin, "get", "fabricnetwork", sampleName,
		"-n", sampleNamespace, "-o", "json")
	var network fabricopsv1alpha1.FabricNetwork
	Expect(json.Unmarshal([]byte(output), &network)).To(Succeed())
	for _, channel := range network.Status.ChannelStatus {
		if channel.Name == "settlement" && channel.RaftUpdate != nil {
			return *channel.RaftUpdate
		}
	}
	return fabricopsv1alpha1.ChannelRaftUpdateStatus{}
}
