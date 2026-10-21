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
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

func testLocalOrgAdmission(existingSettlement string) {
	GinkgoHelper()
	genesisBlocks := map[string][]byte{}
	for _, channel := range []string{"settlement", "audit"} {
		genesisBlocks[channel] = admissionChannelBlock(channel)
	}
	patch := `[
  {"op":"replace","path":"/spec/global/jobs/succeededHistoryTTLSeconds","value":600},
  {"op":"add","path":"/spec/orgs/-","value":{
    "organization":{"name":"BankC","domain":"bankc.example.com","mspName":"BankCMSP"},
    "ca":{"db":"sqlite"},"peer":{"instances":1,"db":"CouchDB","prefix":"peer"}}}
]`
	runCommand(time.Minute, kubectlBin, "patch", "fabricnetwork", sampleName,
		"-n", sampleNamespace, "--type=json", "-p", patch)
	waitForFabricNetworkReadyGeneration(getFabricNetworkProbe().Metadata.Generation, 20*time.Minute)
	dir := GinkgoT().TempDir()
	By("joining BankC to settlement block 0 before its MSP has been admitted")
	prejoinChannelWithoutMembership(dir, "settlement", genesisBlocks["settlement"])

	patch = `[
  {"op":"add","path":"/spec/channels/0/orgs/-","value":{"name":"BankC","peers":["peer0"]}},
  {"op":"add","path":"/spec/channels/1/orgs/-","value":{"name":"BankC","peers":["peer0"]}}
]`
	runCommand(time.Minute, kubectlBin, "patch", "fabricnetwork", sampleName,
		"-n", sampleNamespace, "--type=json", "-p", patch)
	for index, channel := range []string{"settlement", "audit"} {
		proposalName := channel + "-bankc-local-org-add-result"
		var envelope []byte
		Eventually(func(g Gomega) {
			output, err := runCommandAllowFailure(30*time.Second, kubectlBin,
				"get", "configmap", proposalName, "-n", "fo-sample-banka", "-o", "json")
			g.Expect(err).NotTo(HaveOccurred())
			var proposal corev1.ConfigMap
			g.Expect(json.Unmarshal([]byte(output), &proposal)).To(Succeed())
			g.Expect(proposal.Data["local-org.json"]).To(ContainSubstring(`"staged"`))
			envelope = proposal.BinaryData["config-update.pb"]
			g.Expect(envelope).NotTo(BeEmpty())
		}, 20*time.Minute, 10*time.Second).Should(Succeed())

		By("checking that " + channel + " still requires approval before controller-managed joins")
		expectCommandFailure(30*time.Second, kubectlBin, "get", "job",
			channel+"-bankc-peer0-peer-join", "-n", "fo-sample-bankc")
		network := runCommandQuiet(30*time.Second, kubectlBin, "get", "fabricnetwork",
			sampleName, "-n", sampleNamespace, "-o", "json")
		var net fabricopsv1alpha1.FabricNetwork
		Expect(json.Unmarshal([]byte(network), &net)).To(Succeed())
		Expect(string(net.Status.Phase)).NotTo(Equal("Ready"))

		for _, org := range []string{"banka", "bankb"} {
			envelope = signChannelEnvelope(dir, channel, org, "admission-sign", envelope)
		}
		envelopePath := filepath.Join(dir, channel+"-signed.pb")
		Expect(os.WriteFile(envelopePath, envelope, 0600)).To(Succeed())
		secretName := channel + "-bankc-signed"
		runCommand(time.Minute, kubectlBin, "create", "secret", "generic", secretName,
			"-n", sampleNamespace, "--from-file=envelope.pb="+envelopePath)
		refPatch := fmt.Sprintf(`[{"op":"add","path":"/spec/channels/%d/orgs/2/signedUpdateRef",`+
			`"value":{"secretKeyRef":{"name":%q,"key":"envelope.pb"}}}]`, index, secretName)
		runCommand(time.Minute, kubectlBin, "patch", "fabricnetwork", sampleName,
			"-n", sampleNamespace, "--type=json", "-p", refPatch)
	}

	generation := getFabricNetworkProbe().Metadata.Generation
	waitForFabricNetworkReadyGeneration(generation, 25*time.Minute)
	for channel, genesis := range genesisBlocks {
		Expect(admissionChannelBlock(channel)).To(Equal(genesis), "admission must preserve block 0 for peer joins")
	}
	targets := append(baseSampleChaincodeTargets(), sampleChaincodeTarget{
		namespace: "fo-sample-bankc", name: "settlement-settlement-bankc-peer0-ccaas", orgName: "BankC", peerName: "peer0",
	})
	expectSampleChaincodeReady("0.0.2", 2, nodeUpgradeImage, targets)
	output := runFabricOpsctlEventually(5*time.Minute, "query", "-n", sampleNamespace,
		"--org", "BankC", "--peer", "BankC/peer0", "--channel", "settlement", "--chaincode", "settlement",
		"--function", runtimeConfig.readFunction, "--args", jsonStringArray(existingSettlement), "-o", "json", sampleName)
	Expect(output).To(ContainSubstring(existingSettlement))
	invokeAndQuerySettlement(existingSettlement+"-bankc", runtimeConfig.createFunction,
		runtimeConfig.readFunction, "BankA/peer0", "BankB/peer0")
}

func admissionChannelBlock(channel string) []byte {
	GinkgoHelper()
	output := runCommandQuiet(30*time.Second, kubectlBin, "get", "configmap",
		channel+"-channel-block", "-n", "fo-sample-orderer", "-o", "json")
	var configMap corev1.ConfigMap
	Expect(json.Unmarshal([]byte(output), &configMap)).To(Succeed())
	Expect(configMap.BinaryData[channel+".block"]).NotTo(BeEmpty())
	return configMap.BinaryData[channel+".block"]
}

func signChannelEnvelope(dir, channel, org, purpose string, envelope []byte) []byte {
	GinkgoHelper()
	logs := runAdmissionAdminJob(dir, channel, org, purpose, envelope, `cp /proposal/envelope.pb /tmp/signed.pb
peer channel signconfigtx -f /tmp/signed.pb
echo FABRICOPS_SIGNED_ENVELOPE
base64 /tmp/signed.pb | tr -d '\n'
echo`)
	_, encoded, found := strings.Cut(logs, "FABRICOPS_SIGNED_ENVELOPE\n")
	Expect(found).To(BeTrue())
	result, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
	Expect(err).NotTo(HaveOccurred())
	return result
}

func prejoinChannelWithoutMembership(dir, channel string, genesis []byte) {
	GinkgoHelper()
	logs := runAdmissionAdminJob(dir, channel, "bankc", "prejoin", genesis, fmt.Sprintf(`
configtxlator proto_decode --input /proposal/envelope.pb --type common.Block --output /tmp/genesis.json
jq -e '.data.data[0].payload.data.config.channel_group.groups.Application.groups.BankCMSP == null' /tmp/genesis.json
export CORE_PEER_ADDRESS=peer0.fo-sample-bankc.svc.cluster.local:7051
export CORE_PEER_TLS_ENABLED=true
export CORE_PEER_TLS_ROOTCERT_FILE=/tls/ca.crt
export CORE_PEER_TLS_CERT_FILE=/tls/client.crt
export CORE_PEER_TLS_KEY_FILE=/tls/client.key
peer channel join -b /proposal/envelope.pb
peer channel list > /tmp/channels.txt
grep -Fx %q /tmp/channels.txt
if peer channel fetch config /tmp/config.pb -c %q \
  -o orderer0.fo-sample-orderer.svc.cluster.local:7050 \
  --tls --cafile /proposal/orderer-ca.crt > /tmp/fetch.log 2>&1; then
  echo "Unadmitted BankC unexpectedly fetched channel config" >&2
  exit 1
fi
grep FORBIDDEN /tmp/fetch.log
echo FABRICOPS_PREJOIN_WITHOUT_MSP
`, channel, channel))
	Expect(logs).To(ContainSubstring("FABRICOPS_PREJOIN_WITHOUT_MSP"))
	// Emulate the durable receipt left by the older join-before-admission flow,
	// but only after a real peer has successfully joined the original block.
	runCommand(time.Minute, kubectlBin, "create", "configmap", channel+"-bankc-peer0-peer-join-result",
		"-n", "fo-sample-bankc", "--from-literal=channels.txt="+channel)
	expectCommandFailure(30*time.Second, kubectlBin, "get", "configmap",
		channel+"-bankc-anchor-peer-update-result", "-n", "fo-sample-bankc")
}

func runAdmissionAdminJob(dir, channel, org, purpose string, input []byte, command string) string {
	GinkgoHelper()
	namespace := "fo-sample-" + org
	name := channel + "-" + purpose + "-" + org
	inputPath := filepath.Join(dir, name+".pb")
	Expect(os.WriteFile(inputPath, input, 0600)).To(Succeed())
	configMapArgs := []string{"create", "configmap", name, "-n", namespace, "--from-file=envelope.pb=" + inputPath}
	if purpose == "prejoin" {
		// Only the orderer's public CA certificate crosses the namespace boundary.
		encoded := runCommandQuiet(30*time.Second, kubectlBin, "get", "secret", "orderer0-tls",
			"-n", "fo-sample-orderer", "-o", `jsonpath={.data.ca\.crt}`)
		ca, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
		Expect(err).NotTo(HaveOccurred())
		Expect(ca).NotTo(BeEmpty())
		caPath := filepath.Join(dir, "orderer-ca.crt")
		Expect(os.WriteFile(caPath, ca, 0600)).To(Succeed())
		configMapArgs = append(configMapArgs, "--from-file=orderer-ca.crt="+caPath)
	}
	runCommand(time.Minute, kubectlBin, configMapArgs...)
	networkJSON := runCommandQuiet(30*time.Second, kubectlBin, "get", "fabricnetwork",
		sampleName, "-n", sampleNamespace, "-o", "json")
	var network fabricopsv1alpha1.FabricNetwork
	Expect(json.Unmarshal([]byte(networkJSON), &network)).To(Succeed())
	var mspID string
	for _, candidate := range network.Spec.Orgs {
		if strings.EqualFold(candidate.Organization.Name, org) {
			mspID = candidate.Organization.MSPName
		}
	}
	Expect(mspID).NotTo(BeEmpty(), "admin Job requires a declared organization")
	// This suite uses Fabric v3 with the operator's v2.5 CLI compatibility image.
	// Discover overrides from the CR, not from bootstrap Jobs removed by cleanup.
	toolsImage := "hyperledger/fabric-tools:2.5.14"
	if network.Spec.Global.Images != nil && network.Spec.Global.Images.FabricTools != "" {
		toolsImage = network.Spec.Global.Images.FabricTools
	}
	backoff := int32(0)
	job := batchv1.Job{
		TypeMeta:   metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: batchv1.JobSpec{BackoffLimit: &backoff, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Volumes: []corev1.Volume{
				{Name: "proposal", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: name},
				}}},
				{Name: "tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
					SecretName: org + "-admin-tls",
				}}},
				{Name: "admin", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
					SecretName: org + "-admin-msp", Items: []corev1.KeyToPath{
						{Key: "config.yaml", Path: "config.yaml"}, {Key: "cacert.pem", Path: "cacerts/ca.pem"},
						{Key: "signcert.pem", Path: "signcerts/cert.pem"}, {Key: "keystore.pem", Path: "keystore/key.pem"},
						{Key: "tlscacert.pem", Path: "tlscacerts/tlsca.pem"},
					},
				}}},
			},
			Containers: []corev1.Container{{
				Name: purpose, Image: toolsImage,
				Env: []corev1.EnvVar{
					{Name: "CORE_PEER_LOCALMSPID", Value: mspID},
					{Name: "CORE_PEER_MSPCONFIGPATH", Value: "/msp"},
				},
				Command: []string{"sh", "-ec", command},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "proposal", MountPath: "/proposal", ReadOnly: true},
					{Name: "admin", MountPath: "/msp", ReadOnly: true},
					{Name: "tls", MountPath: "/tls", ReadOnly: true},
				},
			}},
		}}},
	}
	jobJSON, err := json.Marshal(job)
	Expect(err).NotTo(HaveOccurred())
	jobPath := filepath.Join(dir, name+".json")
	Expect(os.WriteFile(jobPath, jobJSON, 0600)).To(Succeed())
	runCommand(time.Minute, kubectlBin, "apply", "-f", jobPath)
	runCommand(4*time.Minute, kubectlBin, "wait", "--for=condition=complete",
		"job/"+name, "-n", namespace, "--timeout=180s")
	return runCommandQuiet(30*time.Second, kubectlBin, "logs", "job/"+name, "-n", namespace)
}
