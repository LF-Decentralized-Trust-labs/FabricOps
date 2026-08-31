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

package federated

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	managerNamespace          = "fabricops-system"
	managerName               = "fabricops-controller-manager"
	founderName               = "federated-founder"
	participantName           = "bankb-participant"
	sampleNamespace           = "default"
	founderOrdererNamespace   = "fo-federated-founder-orderer"
	founderPeerNamespace      = "fo-federated-founder-banka"
	participantPeerNamespace  = "fo-fp-bankb-participant-bankb"
	ordererServiceName        = "orderer0"
	founderPeerService        = "peer0"
	participantPeerService    = "peer0"
	ordererTLSSecretName      = "orderer0-tls"
	founderPeerTLSSecretName  = "peer0-tls"
	ordererTLSKey             = "ca.crt"
	channelBlockConfigMapName = "settlement-channel-block"
	channelBlockKey           = "settlement.block"
	nodeSettlementImage       = "ghcr.io/lf-decentralized-trust-labs/fabricops-node-settlement:0.2.1"
	federatedEndorsement      = "OR('BankAMSP.member','BankBMSP.member')"
)

var (
	repoRoot                 string
	kindBin                  string
	kubectlBin               string
	dockerBin                string
	fabricopsctlBin          string
	founderCluster           string
	participantCluster       string
	managerImage             string
	ordererNodePort          string
	founderPeerNodePort      string
	peerNodePort             string
	founderContext           string
	participantContext       string
	federatedSampleDirectory string
)

func TestFederatedE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "FabricOps Federated E2E Suite")
}

var _ = BeforeSuite(func() {
	repoRoot = mustRepoRoot()
	kindBin = envOrDefault("KIND", "kind")
	kubectlBin = envOrDefault("KUBECTL", "kubectl")
	dockerBin = envOrDefault("DOCKER", "docker")
	fabricopsctlBin = filepath.Join(repoRoot, "bin", "fabricopsctl")
	founderCluster = envOrDefault("KIND_FEDERATED_FOUNDER_CLUSTER", "fabricops-fed-founder")
	participantCluster = envOrDefault("KIND_FEDERATED_PARTICIPANT_CLUSTER", "fabricops-fed-participant")
	managerImage = envOrDefault("IMG", "controller:latest")
	ordererNodePort = envOrDefault("E2E_FEDERATED_ORDERER_NODE_PORT", "30050")
	founderPeerNodePort = envOrDefault("E2E_FEDERATED_FOUNDER_PEER_NODE_PORT", "30052")
	peerNodePort = envOrDefault("E2E_FEDERATED_PEER_NODE_PORT", "30051")
	founderContext = "kind-" + founderCluster
	participantContext = "kind-" + participantCluster
	federatedSampleDirectory = filepath.Join(repoRoot, "config", "samples", "e2e", "federated")
})

var _ = Describe("Federated join handoff", Ordered, func() {
	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			dumpDiagnostics(founderContext, "fabricnetwork", founderName)
			dumpDiagnostics(participantContext, "fabricparticipant", participantName)
		}
	})

	It("admits BankB from a participant cluster and completes participant channel/lifecycle traffic", func() {
		By("building and loading the manager image into both clusters")
		runCommand(10*time.Minute, "make", "docker-build", "IMG="+managerImage)
		runCommand(5*time.Minute, kindBin, "load", "docker-image", managerImage, "--name", founderCluster)
		runCommand(5*time.Minute, kindBin, "load", "docker-image", managerImage, "--name", participantCluster)

		By("building fabricopsctl")
		runCommand(30*time.Second, "mkdir", "-p", "bin")
		runCommand(3*time.Minute, "go", "build", "-o", fabricopsctlBin, "./cmd/fabricopsctl")

		By("building and loading the node settlement chaincode image into both clusters")
		runCommand(10*time.Minute, dockerBin, "build", "-t", nodeSettlementImage, filepath.Join("config", "samples", "chaincodes", "node_settlement"))
		runCommand(5*time.Minute, kindBin, "load", "docker-image", nodeSettlementImage, "--name", founderCluster)
		runCommand(5*time.Minute, kindBin, "load", "docker-image", nodeSettlementImage, "--name", participantCluster)

		By("generating and applying the install bundle to both clusters")
		runCommand(5*time.Minute, "make", "build-installer", "IMG="+managerImage)
		installFabricOps(founderContext)
		installFabricOps(participantContext)

		tempDir := GinkgoT().TempDir()
		founderHost := kindNodeIPAddress(founderCluster)
		ordererAddress := founderHost + ":" + ordererNodePort
		founderPeerAddress := founderHost + ":" + founderPeerNodePort
		participantPeerHost := kindNodeIPAddress(participantCluster)
		participantPeerAddress := participantPeerHost + ":" + peerNodePort
		founderManifestPath := filepath.Join(tempDir, "founder.yaml")
		participantManifestPath := filepath.Join(tempDir, "participant.yaml")
		ordererTLSPath := filepath.Join(tempDir, "orderer0-tls-ca.pem")
		founderPeerTLSPath := filepath.Join(tempDir, "banka-peer0-tls-ca.pem")
		channelBlockPath := filepath.Join(tempDir, channelBlockKey)
		participantBundlePath := filepath.Join(tempDir, "bankb-join-bundle.json")
		participantOrgPath := filepath.Join(tempDir, "bankb-org.json")

		By("creating the founder network with BankA and the ordering service")
		renderFounderManifest(founderManifestPath, ordererAddress, founderPeerAddress)
		runKubectl(founderContext, 3*time.Minute, "apply", "-f", founderManifestPath)
		runFabricOpsctl(25*time.Minute, "wait", "-n", sampleNamespace, "--context", founderContext, "--timeout", "20m", founderName)

		By("exposing the founder orderer and peer through local kind NodePorts")
		exposeFounderOrdererNodePort()
		exposeFounderPeerNodePort()

		By("exporting founder artifacts for the participant cluster")
		writeFile(ordererTLSPath, secretDataValue(founderContext, founderOrdererNamespace, ordererTLSSecretName, ordererTLSKey))
		writeFile(founderPeerTLSPath, secretDataValue(founderContext, founderPeerNamespace, founderPeerTLSSecretName, ordererTLSKey))
		writeFile(channelBlockPath, configMapValue(founderContext, founderOrdererNamespace, channelBlockConfigMapName, channelBlockKey))
		createConfigMapFromFile(participantContext, sampleNamespace, "orderer0-artifacts", "tls-ca.pem", ordererTLSPath)
		createConfigMapFromFile(participantContext, sampleNamespace, "banka-peer0-artifacts", "tls-ca.pem", founderPeerTLSPath)

		By("applying the participant manifest before the channel block is imported")
		renderParticipantManifest(
			participantManifestPath,
			ordererAddress,
			participantPeerAddress,
			participantPeerHost,
			founderPeerAddress,
		)
		runKubectl(participantContext, 3*time.Minute, "apply", "-f", participantManifestPath)
		runFabricOpsctl(
			25*time.Minute,
			"wait",
			"-n",
			sampleNamespace,
			"--context",
			participantContext,
			"--participant",
			"--for",
			"condition=LocalInfrastructureReady",
			"--timeout=20m",
			participantName,
		)

		By("exposing the participant peer through a local kind NodePort")
		exposeParticipantPeerNodePort()

		By("exporting BankB membership material from the participant cluster")
		runFabricOpsctl(
			2*time.Minute,
			"join-bundle",
			"participant",
			"-n",
			sampleNamespace,
			"--context",
			participantContext,
			"--out",
			participantBundlePath,
			participantName,
		)
		runFabricOpsctl(30*time.Second, "join-bundle", "validate", participantBundlePath)
		runFabricOpsctl(
			30*time.Second,
			"join-bundle",
			"render-org",
			"--channel",
			"settlement",
			"--out",
			participantOrgPath,
			participantBundlePath,
		)

		By("admitting BankB on the founder channel")
		createConfigMapFromFile(founderContext, sampleNamespace, "bankb-application-org", "org.json", participantOrgPath)
		founderGeneration := patchFounderExternalOrg(participantPeerHost)
		waitFounderExternalOrgReady(founderGeneration, 25*time.Minute)

		By("importing the channel block and waiting for BankB peers to join")
		createConfigMapFromFile(participantContext, sampleNamespace, "settlement-artifacts", channelBlockKey, channelBlockPath)
		waitParticipantReadyGeneration(getFabricParticipantGeneration(), 25*time.Minute)

		By("declaring participant-local chaincode lifecycle work after membership is active")
		participantGeneration := patchParticipantChaincode()
		waitParticipantReadyGeneration(participantGeneration, 25*time.Minute)
		expectParticipantChaincodeReady()

		By("declaring founder-local chaincode lifecycle work and waiting for commit")
		founderChaincodeGeneration := patchFounderChaincode()
		waitFounderReadyGeneration(founderChaincodeGeneration, 25*time.Minute)
		expectFounderChaincodeCommitted()

		By("invoking through the founder peer and querying through the participant peer")
		invokeAndQueryFederatedSettlement(fmt.Sprintf("fed-%d", time.Now().UnixNano()))
	})
})

func installFabricOps(contextName string) {
	GinkgoHelper()

	runKubectl(contextName, 3*time.Minute, "apply", "-f", "dist/install.yaml")
	runKubectl(
		contextName,
		3*time.Minute,
		"rollout",
		"status",
		"deployment/"+managerName,
		"-n",
		managerNamespace,
		"--timeout=120s",
	)
}

func exposeFounderOrdererNodePort() {
	GinkgoHelper()

	patch := fmt.Sprintf(
		`{"spec":{"type":"NodePort","ports":[{"name":"orderer","port":7050,"protocol":"TCP","targetPort":7050,"nodePort":%s},{"name":"admin","port":9443,"protocol":"TCP","targetPort":9443}]}}`,
		ordererNodePort,
	)
	runKubectl(
		founderContext,
		2*time.Minute,
		"patch",
		"service",
		ordererServiceName,
		"-n",
		founderOrdererNamespace,
		"--type=merge",
		"-p",
		patch,
	)
}

func exposeFounderPeerNodePort() {
	GinkgoHelper()

	patch := fmt.Sprintf(
		`{"spec":{"type":"NodePort","ports":[{"name":"peer","port":7051,"protocol":"TCP","targetPort":7051,"nodePort":%s},{"name":"chaincode","port":7052,"protocol":"TCP","targetPort":7052}]}}`,
		founderPeerNodePort,
	)
	runKubectl(
		founderContext,
		2*time.Minute,
		"patch",
		"service",
		founderPeerService,
		"-n",
		founderPeerNamespace,
		"--type=merge",
		"-p",
		patch,
	)
}

func exposeParticipantPeerNodePort() {
	GinkgoHelper()

	patch := fmt.Sprintf(
		`{"spec":{"type":"NodePort","ports":[{"name":"peer","port":7051,"protocol":"TCP","targetPort":7051,"nodePort":%s},{"name":"chaincode","port":7052,"protocol":"TCP","targetPort":7052}]}}`,
		peerNodePort,
	)
	runKubectl(
		participantContext,
		2*time.Minute,
		"patch",
		"service",
		participantPeerService,
		"-n",
		participantPeerNamespace,
		"--type=merge",
		"-p",
		patch,
	)
}

func renderFounderManifest(path string, ordererAddress string, founderPeerAddress string) {
	GinkgoHelper()

	template, err := os.ReadFile(filepath.Join(federatedSampleDirectory, "founder.yaml"))
	Expect(err).NotTo(HaveOccurred())
	rendered := strings.ReplaceAll(string(template), "__ORDERER_ADDRESS__", ordererAddress)
	rendered = strings.ReplaceAll(rendered, "__FOUNDER_PEER_ADDRESS__", founderPeerAddress)
	writeFile(path, []byte(rendered))
}

func renderParticipantManifest(
	path string,
	ordererAddress string,
	peerAddress string,
	peerHost string,
	founderPeerAddress string,
) {
	GinkgoHelper()

	template, err := os.ReadFile(filepath.Join(federatedSampleDirectory, "participant.yaml"))
	Expect(err).NotTo(HaveOccurred())
	rendered := strings.ReplaceAll(string(template), "__ORDERER_ADDRESS__", ordererAddress)
	rendered = strings.ReplaceAll(rendered, "__FOUNDER_PEER_ADDRESS__", founderPeerAddress)
	rendered = strings.ReplaceAll(rendered, "__PEER_ADDRESS__", peerAddress)
	rendered = strings.ReplaceAll(rendered, "__PEER_HOST__", peerHost)
	rendered = strings.ReplaceAll(rendered, "__PEER_PORT__", peerNodePort)
	writeFile(path, []byte(rendered))
}

func patchFounderExternalOrg(participantPeerHost string) int64 {
	GinkgoHelper()

	participantPeerPort, err := strconv.Atoi(peerNodePort)
	Expect(err).NotTo(HaveOccurred())

	value := []map[string]any{
		{
			"name":  "BankB",
			"mspID": "BankBMSP",
			"applicationOrgRef": map[string]any{
				"configMapKeyRef": map[string]string{
					"name": "bankb-application-org",
					"key":  "org.json",
				},
			},
			"anchorPeers": []map[string]any{
				{
					"host": participantPeerHost,
					"port": participantPeerPort,
				},
			},
		},
	}
	patch := marshalJSON([]map[string]any{{
		"op":    "add",
		"path":  "/spec/channels/0/externalOrgs",
		"value": value,
	}})
	runKubectl(founderContext, 2*time.Minute, "patch", "fabricnetwork", founderName, "-n", sampleNamespace, "--type=json", "-p", patch)
	return getFabricNetworkGeneration()
}

func patchParticipantChaincode() int64 {
	GinkgoHelper()

	value := []map[string]any{
		{
			"name":              "settlement",
			"channel":           "settlement",
			"version":           "1.0",
			"packageLabel":      "settlement_settlement_1.0",
			"sequence":          1,
			"endorsementPolicy": federatedEndorsement,
			"image":             nodeSettlementImage,
		},
	}
	patch := marshalJSON([]map[string]any{{
		"op":    "add",
		"path":  "/spec/chaincodes",
		"value": value,
	}})
	runKubectl(
		participantContext,
		2*time.Minute,
		"patch",
		"fabricparticipant",
		participantName,
		"-n",
		sampleNamespace,
		"--type=json",
		"-p",
		patch,
	)
	return getFabricParticipantGeneration()
}

func patchFounderChaincode() int64 {
	GinkgoHelper()

	value := []map[string]any{
		{
			"name":              "settlement",
			"channel":           "settlement",
			"version":           "1.0",
			"packageLabel":      "settlement_settlement_1.0",
			"sequence":          1,
			"endorsementPolicy": federatedEndorsement,
			"image":             nodeSettlementImage,
		},
	}
	patch := marshalJSON([]map[string]any{{
		"op":    "replace",
		"path":  "/spec/chaincodes",
		"value": value,
	}})
	runKubectl(
		founderContext,
		2*time.Minute,
		"patch",
		"fabricnetwork",
		founderName,
		"-n",
		sampleNamespace,
		"--type=json",
		"-p",
		patch,
	)
	return getFabricNetworkGeneration()
}

func waitFounderExternalOrgReady(generation int64, timeout time.Duration) {
	GinkgoHelper()

	Eventually(func(g Gomega) {
		probe := getFabricNetworkProbe()
		g.Expect(readyConditionObserved(probe.Status.Conditions, generation)).To(BeTrue())
		g.Expect(probe.Status.ChannelStatus).NotTo(BeEmpty())
		channel := probe.Status.ChannelStatus[0]
		g.Expect(channel.Name).To(Equal("settlement"))
		g.Expect(channel.ExternalOrgs).To(ContainElement(SatisfyAll(
			HaveField("Name", "BankB"),
			HaveField("MSPID", "BankBMSP"),
			HaveField("Ready", true),
		)))
	}, timeout, 10*time.Second).Should(Succeed())
}

func waitFounderReadyGeneration(generation int64, timeout time.Duration) {
	GinkgoHelper()

	Eventually(func(g Gomega) {
		probe := getFabricNetworkProbe()
		g.Expect(readyConditionObserved(probe.Status.Conditions, generation)).To(BeTrue())
	}, timeout, 10*time.Second).Should(Succeed())
}

func waitParticipantReadyGeneration(generation int64, timeout time.Duration) {
	GinkgoHelper()

	Eventually(func(g Gomega) {
		probe := getFabricParticipantProbe()
		g.Expect(readyConditionObserved(probe.Status.Conditions, generation)).To(BeTrue())
	}, timeout, 10*time.Second).Should(Succeed())
}

func expectFounderChaincodeCommitted() {
	GinkgoHelper()

	probe := getFabricNetworkProbe()
	Expect(probe.Status.ChaincodeStatus).To(ContainElement(SatisfyAll(
		HaveField("Name", "settlement"),
		HaveField("Channel", "settlement"),
		HaveField("Committed", true),
		HaveField("Ready", true),
	)))
}

func expectParticipantChaincodeReady() {
	GinkgoHelper()

	probe := getFabricParticipantProbe()
	Expect(probe.Status.ChaincodeStatus).To(ContainElement(SatisfyAll(
		HaveField("Name", "settlement"),
		HaveField("Channel", "settlement"),
		HaveField("Installed", true),
		HaveField("Approved", true),
		HaveField("Ready", true),
	)))
}

func invokeAndQueryFederatedSettlement(smokeID string) {
	GinkgoHelper()

	runFabricOpsctlEventually(
		8*time.Minute,
		"invoke",
		"-n",
		sampleNamespace,
		"--context",
		founderContext,
		"--org",
		"BankA",
		"--channel",
		"settlement",
		"--chaincode",
		"settlement",
		"--function",
		"createSettlement",
		"--args",
		jsonStringArray(smokeID, "BankA", "BankB", "100", "USD"),
		"-o",
		"json",
		founderName,
	)

	output := runFabricOpsctlEventually(
		8*time.Minute,
		"query",
		"--participant",
		"-n",
		sampleNamespace,
		"--context",
		participantContext,
		"--org",
		"BankB",
		"--channel",
		"settlement",
		"--chaincode",
		"settlement",
		"--function",
		"readSettlement",
		"--args",
		jsonStringArray(smokeID),
		"-o",
		"json",
		participantName,
	)
	Expect(output).To(ContainSubstring(smokeID))

	participantSmokeID := smokeID + "-participant"
	runFabricOpsctlEventually(
		8*time.Minute,
		"invoke",
		"--participant",
		"-n",
		sampleNamespace,
		"--context",
		participantContext,
		"--org",
		"BankB",
		"--peer",
		"BankB/peer0",
		"--peer",
		"BankA/peer0",
		"--channel",
		"settlement",
		"--chaincode",
		"settlement",
		"--function",
		"createSettlement",
		"--args",
		jsonStringArray(participantSmokeID, "BankB", "BankA", "75", "USD"),
		"-o",
		"json",
		participantName,
	)

	output = runFabricOpsctlEventually(
		8*time.Minute,
		"query",
		"--participant",
		"-n",
		sampleNamespace,
		"--context",
		participantContext,
		"--org",
		"BankB",
		"--peer",
		"BankB/peer0",
		"--channel",
		"settlement",
		"--chaincode",
		"settlement",
		"--function",
		"readSettlement",
		"--args",
		jsonStringArray(participantSmokeID),
		"-o",
		"json",
		participantName,
	)
	Expect(output).To(ContainSubstring(participantSmokeID))
}

type conditionProbe struct {
	Type               string `json:"type"`
	Status             string `json:"status"`
	ObservedGeneration int64  `json:"observedGeneration"`
}

type fabricNetworkProbe struct {
	Metadata struct {
		Generation int64 `json:"generation"`
	} `json:"metadata"`
	Status struct {
		Conditions    []conditionProbe `json:"conditions"`
		ChannelStatus []struct {
			Name         string `json:"name"`
			ExternalOrgs []struct {
				Name  string `json:"name"`
				MSPID string `json:"mspID"`
				Ready bool   `json:"ready"`
			} `json:"externalOrgs"`
		} `json:"channelStatus"`
		ChaincodeStatus []struct {
			Name      string `json:"name"`
			Channel   string `json:"channel"`
			Committed bool   `json:"committed"`
			Ready     bool   `json:"ready"`
		} `json:"chaincodeStatus"`
	} `json:"status"`
}

type fabricParticipantProbe struct {
	Metadata struct {
		Generation int64 `json:"generation"`
	} `json:"metadata"`
	Status struct {
		Conditions      []conditionProbe `json:"conditions"`
		ChaincodeStatus []struct {
			Name      string `json:"name"`
			Channel   string `json:"channel"`
			Installed bool   `json:"installed"`
			Approved  bool   `json:"approved"`
			Ready     bool   `json:"ready"`
		} `json:"chaincodeStatus"`
	} `json:"status"`
}

func getFabricNetworkGeneration() int64 {
	GinkgoHelper()

	return getFabricNetworkProbe().Metadata.Generation
}

func getFabricNetworkProbe() fabricNetworkProbe {
	GinkgoHelper()

	output := runKubectlQuiet(founderContext, 30*time.Second, "get", "fabricnetwork", founderName, "-n", sampleNamespace, "-o", "json")
	var probe fabricNetworkProbe
	Expect(json.Unmarshal([]byte(output), &probe)).To(Succeed())
	return probe
}

func getFabricParticipantGeneration() int64 {
	GinkgoHelper()

	return getFabricParticipantProbe().Metadata.Generation
}

func getFabricParticipantProbe() fabricParticipantProbe {
	GinkgoHelper()

	output := runKubectlQuiet(participantContext, 30*time.Second, "get", "fabricparticipant", participantName, "-n", sampleNamespace, "-o", "json")
	var probe fabricParticipantProbe
	Expect(json.Unmarshal([]byte(output), &probe)).To(Succeed())
	return probe
}

func readyConditionObserved(conditions []conditionProbe, generation int64) bool {
	for _, condition := range conditions {
		if condition.Type == "Ready" {
			return condition.Status == "True" && condition.ObservedGeneration == generation
		}
	}
	return false
}

func createConfigMapFromFile(contextName string, namespace string, name string, key string, path string) {
	GinkgoHelper()

	runKubectl(contextName, 30*time.Second, "delete", "configmap", name, "-n", namespace, "--ignore-not-found")
	runKubectl(contextName, 30*time.Second, "create", "configmap", name, "-n", namespace, "--from-file="+key+"="+path)
}

func configMapValue(contextName string, namespace string, name string, key string) []byte {
	GinkgoHelper()

	output := runKubectlQuiet(contextName, 30*time.Second, "get", "configmap", name, "-n", namespace, "-o", "json")
	var configMap struct {
		Data       map[string]string `json:"data"`
		BinaryData map[string]string `json:"binaryData"`
	}
	Expect(json.Unmarshal([]byte(output), &configMap)).To(Succeed())
	if value, ok := configMap.BinaryData[key]; ok {
		decoded, err := base64.StdEncoding.DecodeString(value)
		Expect(err).NotTo(HaveOccurred())
		return decoded
	}
	if value, ok := configMap.Data[key]; ok {
		return []byte(value)
	}
	Fail(fmt.Sprintf("ConfigMap %s/%s is missing key %q", namespace, name, key))
	return nil
}

func secretDataValue(contextName string, namespace string, name string, key string) []byte {
	GinkgoHelper()

	output := runKubectlQuiet(contextName, 30*time.Second, "get", "secret", name, "-n", namespace, "-o", "json")
	var secret struct {
		Data map[string]string `json:"data"`
	}
	Expect(json.Unmarshal([]byte(output), &secret)).To(Succeed())
	value, ok := secret.Data[key]
	if !ok {
		Fail(fmt.Sprintf("Secret %s/%s is missing key %q", namespace, name, key))
	}
	decoded, err := base64.StdEncoding.DecodeString(value)
	Expect(err).NotTo(HaveOccurred())
	return decoded
}

func writeFile(path string, contents []byte) {
	GinkgoHelper()

	Expect(os.WriteFile(path, contents, 0o600)).To(Succeed())
}

func kindNodeIPAddress(clusterName string) string {
	GinkgoHelper()

	output := runCommandQuiet(30*time.Second, dockerBin, "inspect", clusterName+"-control-plane")
	var nodes []struct {
		NetworkSettings struct {
			Networks map[string]struct {
				IPAddress string `json:"IPAddress"`
			} `json:"Networks"`
		} `json:"NetworkSettings"`
	}
	Expect(json.Unmarshal([]byte(output), &nodes)).To(Succeed())
	Expect(nodes).NotTo(BeEmpty())
	for _, network := range nodes[0].NetworkSettings.Networks {
		if strings.TrimSpace(network.IPAddress) != "" {
			return network.IPAddress
		}
	}
	Fail("kind control-plane node does not have a Docker network IP address")
	return ""
}

func marshalJSON(value any) string {
	GinkgoHelper()

	encoded, err := json.Marshal(value)
	Expect(err).NotTo(HaveOccurred())
	return string(encoded)
}

func jsonStringArray(values ...string) string {
	GinkgoHelper()

	encoded, err := json.Marshal(values)
	Expect(err).NotTo(HaveOccurred())
	return string(encoded)
}

func runFabricOpsctl(timeout time.Duration, args ...string) string {
	GinkgoHelper()

	return runCommand(timeout, fabricopsctlBin, args...)
}

func runFabricOpsctlEventually(timeout time.Duration, args ...string) string {
	GinkgoHelper()

	var output string
	Eventually(func(g Gomega) {
		var err error
		output, err = runCommandAllowFailure(3*time.Minute, fabricopsctlBin, args...)
		g.Expect(err).NotTo(HaveOccurred(), output)
	}, timeout, 10*time.Second).Should(Succeed())
	return output
}

func runKubectl(contextName string, timeout time.Duration, args ...string) string {
	GinkgoHelper()

	return runCommand(timeout, kubectlBin, append([]string{"--context", contextName}, args...)...)
}

func runKubectlQuiet(contextName string, timeout time.Duration, args ...string) string {
	GinkgoHelper()

	return runCommandWithEnvAndLogging(timeout, nil, false, kubectlBin, append([]string{"--context", contextName}, args...)...)
}

func runCommand(timeout time.Duration, name string, args ...string) string {
	return runCommandWithEnvAndLogging(timeout, nil, true, name, args...)
}

func runCommandQuiet(timeout time.Duration, name string, args ...string) string {
	return runCommandWithEnvAndLogging(timeout, nil, false, name, args...)
}

func runCommandAllowFailure(timeout time.Duration, name string, args ...string) (string, error) {
	GinkgoHelper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = repoRoot
	cmd.Env = os.Environ()

	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	commandLine := strings.Join(append([]string{name}, args...), " ")
	err := cmd.Run()
	text := output.String()
	if ctx.Err() == context.DeadlineExceeded {
		return text, fmt.Errorf("command timed out after %s: %s\n%s", timeout, commandLine, text)
	}
	if err != nil {
		return text, fmt.Errorf("command failed: %s\n%s: %w", commandLine, text, err)
	}
	return text, nil
}

func runCommandWithEnvAndLogging(timeout time.Duration, extraEnv []string, logOutput bool, name string, args ...string) string {
	GinkgoHelper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(), extraEnv...)

	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	commandLine := strings.Join(append([]string{name}, args...), " ")
	if logOutput {
		fmt.Fprintf(GinkgoWriter, "\n$ %s\n", commandLine)
	}

	err := cmd.Run()
	text := output.String()
	if logOutput && text != "" {
		fmt.Fprintln(GinkgoWriter, text)
	}

	if ctx.Err() == context.DeadlineExceeded {
		if !logOutput {
			fmt.Fprintf(GinkgoWriter, "\n$ %s\n", commandLine)
			if text != "" {
				fmt.Fprintln(GinkgoWriter, text)
			}
		}
		Fail(fmt.Sprintf("command timed out after %s: %s\n%s", timeout, commandLine, text))
	}

	if err != nil && !logOutput {
		fmt.Fprintf(GinkgoWriter, "\n$ %s\n", commandLine)
		if text != "" {
			fmt.Fprintln(GinkgoWriter, text)
		}
	}
	Expect(err).NotTo(HaveOccurred(), "command failed: %s\n%s", commandLine, text)
	return text
}

func dumpDiagnostics(contextName string, resourceKind string, resourceName string) {
	runDiagnostic(contextName, 30*time.Second, "get", resourceKind, "-A", "-o", "wide")
	runDiagnostic(contextName, 30*time.Second, "get", "pods", "-A", "-o", "wide")
	runDiagnostic(contextName, 30*time.Second, "get", "jobs", "-A")
	runDiagnostic(contextName, 30*time.Second, "describe", resourceKind, resourceName, "-n", sampleNamespace)
	runDiagnostic(contextName, 30*time.Second, "logs", "-n", managerNamespace, "deployment/"+managerName, "-c", "manager", "--tail=200")
	runDiagnostic(contextName, 30*time.Second, "get", "events", "-A", "--sort-by=.lastTimestamp")
}

func runDiagnostic(contextName string, timeout time.Duration, args ...string) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	commandArgs := append([]string{"--context", contextName}, args...)
	cmd := exec.CommandContext(ctx, kubectlBin, commandArgs...)
	cmd.Dir = repoRoot
	cmd.Env = os.Environ()

	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	commandLine := strings.Join(append([]string{kubectlBin}, commandArgs...), " ")
	fmt.Fprintf(GinkgoWriter, "\n# diagnostics: %s\n", commandLine)
	_ = cmd.Run()
	if output.Len() > 0 {
		fmt.Fprintln(GinkgoWriter, output.String())
	}
}

func mustRepoRoot() string {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		Fail("could not discover test filename")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", "..", ".."))
}

func envOrDefault(key string, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
