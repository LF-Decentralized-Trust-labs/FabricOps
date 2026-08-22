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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sigs.k8s.io/yaml"
)

const (
	managerNamespace                  = "fabricops-system"
	managerName                       = "fabricops-controller-manager"
	sampleName                        = "fabricnetwork-sample"
	sampleNamespace                   = "default"
	runtimeNode                       = "node"
	runtimeGo                         = "go"
	runtimeJava                       = "java"
	succeededJobCleanupAnnotation     = "fabricops.io/succeeded-job-cleanup"
	fabricNetworkLabel                = "fabricops.io/fabricnetwork"
	fabricNetworkNamespaceLabel       = "fabricops.io/fabricnetwork-namespace"
	fastCleanupTTLSeconds             = int32(10)
	nodeSettlementUpgradeImageDefault = "ghcr.io/lf-decentralized-trust-labs/fabricops-node-settlement:0.2.0"
)

var (
	repoRoot         string
	kindBin          string
	kubectlBin       string
	fabricopsctlBin  string
	kindCluster      string
	managerImage     string
	runtimeConfig    sampleRuntimeConfig
	nodeUpgradeImage string
)

type sampleRuntimeConfig struct {
	name                 string
	sampleManifest       string
	chaincodeDir         string
	image                string
	createFunction       string
	readFunction         string
	richQueryFunction    string
	runNodeLifecycleFlow bool
}

type sampleChaincodeTarget struct {
	namespace string
	name      string
	orgName   string
	peerName  string
}

type namespacedName struct {
	namespace string
	name      string
}

func (item namespacedName) String() string {
	return item.namespace + "/" + item.name
}

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "FabricOps E2E Suite")
}

var _ = BeforeSuite(func() {
	repoRoot = mustRepoRoot()
	kindBin = envOrDefault("KIND", "kind")
	kubectlBin = envOrDefault("KUBECTL", "kubectl")
	fabricopsctlBin = filepath.Join(repoRoot, "bin", "fabricopsctl")
	kindCluster = envOrDefault("KIND_CLUSTER", "fabricops-test-e2e")
	managerImage = envOrDefault("IMG", "controller:latest")
	runtimeConfig = selectedSampleRuntimeConfig()
	nodeUpgradeImage = envOrDefault("NODE_SETTLEMENT_UPGRADE_IMAGE", nodeSettlementUpgradeImageDefault)
})

type sampleFabricNetworkManifest struct {
	Spec struct {
		Chaincodes []struct {
			Name  string `json:"name"`
			Image string `json:"image"`
		} `json:"chaincodes"`
	} `json:"spec"`
}

func selectedSampleRuntimeConfig() sampleRuntimeConfig {
	GinkgoHelper()

	runtime := envOrDefault("E2E_CHAINCODE_RUNTIME", runtimeNode)
	config := sampleRuntimeConfig{
		name:           runtime,
		sampleManifest: envOrDefault("E2E_SAMPLE_MANIFEST", filepath.Join("config", "samples", "e2e", runtime, "fabricnetwork.yaml")),
	}

	switch runtime {
	case runtimeNode:
		config.chaincodeDir = filepath.Join("config", "samples", "chaincodes", "node_settlement")
		config.createFunction = "createSettlement"
		config.readFunction = "readSettlement"
		config.richQueryFunction = "querySettlementsByOwner"
		config.runNodeLifecycleFlow = true
	case runtimeGo:
		config.chaincodeDir = filepath.Join("config", "samples", "chaincodes", "go_settlement")
		config.createFunction = "CreateSettlement"
		config.readFunction = "ReadSettlement"
		config.richQueryFunction = "QuerySettlementsByOwner"
	case runtimeJava:
		config.chaincodeDir = filepath.Join("config", "samples", "chaincodes", "java_settlement")
		config.createFunction = "createSettlement"
		config.readFunction = "readSettlement"
		config.richQueryFunction = "querySettlementsByOwner"
	default:
		Fail(fmt.Sprintf("unsupported E2E_CHAINCODE_RUNTIME %q", runtime))
	}

	config.image = sampleChaincodeImageDefault(config.sampleManifest)
	return config
}

func sampleChaincodeImageDefault(sampleManifest string) string {
	GinkgoHelper()

	samplePath := sampleManifest
	if !filepath.IsAbs(samplePath) {
		samplePath = filepath.Join(repoRoot, samplePath)
	}
	data, err := os.ReadFile(samplePath)
	Expect(err).NotTo(HaveOccurred())

	var manifest sampleFabricNetworkManifest
	Expect(yaml.Unmarshal(data, &manifest)).To(Succeed())
	for _, chaincode := range manifest.Spec.Chaincodes {
		if chaincode.Name == "settlement" {
			Expect(chaincode.Image).NotTo(BeEmpty())
			return chaincode.Image
		}
	}
	Fail("sample FabricNetwork must declare the settlement chaincode image")
	return ""
}

var _ = Describe("Kind bundle install", Ordered, func() {
	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			dumpDiagnostics()
		}
	})

	It("reconciles the selected sample runtime and invokes its CCaaS chaincode", func() {
		By("using the target kind context")
		runCommand(30*time.Second, kubectlBin, "config", "use-context", "kind-"+kindCluster)

		By("building and loading the manager image")
		runCommand(10*time.Minute, "make", "docker-build", "IMG="+managerImage)
		runCommand(5*time.Minute, kindBin, "load", "docker-image", managerImage, "--name", kindCluster)

		By("building fabricopsctl")
		runCommand(30*time.Second, "mkdir", "-p", "bin")
		runCommand(3*time.Minute, "go", "build", "-o", fabricopsctlBin, "./cmd/fabricopsctl")

		By("building and loading the local " + runtimeConfig.name + " settlement chaincode image")
		buildArgs := []string{"build", "-t", runtimeConfig.image}
		if runtimeConfig.runNodeLifecycleFlow {
			buildArgs = append(buildArgs, "-t", nodeUpgradeImage)
		}
		buildArgs = append(buildArgs, runtimeConfig.chaincodeDir)
		runCommand(10*time.Minute, "docker", buildArgs...)
		runCommand(5*time.Minute, kindBin, "load", "docker-image", runtimeConfig.image, "--name", kindCluster)
		if runtimeConfig.runNodeLifecycleFlow {
			runCommand(5*time.Minute, kindBin, "load", "docker-image", nodeUpgradeImage, "--name", kindCluster)
		}

		By("generating and applying the install bundle")
		runCommand(5*time.Minute, "make", "build-installer", "IMG="+managerImage)
		runCommand(3*time.Minute, kubectlBin, "apply", "-f", "dist/install.yaml")
		runCommand(3*time.Minute, kubectlBin, "rollout", "status", "deployment/"+managerName, "-n", managerNamespace, "--timeout=120s")

		By("applying the sample FabricNetwork")
		runCommand(2*time.Minute, kubectlBin, "apply", "-f", runtimeConfig.sampleManifest)

		By("waiting for FabricOps to provision the Fabric network")
		runFabricOpsctl(25*time.Minute, "wait", "-n", sampleNamespace, "--timeout", "20m", sampleName)

		By("verifying the sample chaincode status and workload image")
		smokeID := fmt.Sprintf("e2e-%d", time.Now().Unix())
		expectSampleChaincodeReady("0.0.1", 1, runtimeConfig.image, baseSampleChaincodeTargets())

		By("invoking and querying the sample " + runtimeConfig.name + " CCaaS chaincode through BankA and BankB endorsement sets")
		invokeAndQuerySettlement(smokeID+"-"+runtimeConfig.name, runtimeConfig.createFunction, runtimeConfig.readFunction, "BankA/peer0", "BankB/peer0")

		if !runtimeConfig.runNodeLifecycleFlow {
			return
		}

		By("running the private-data smoke for package and collection wiring")
		runCommandWithEnv(5*time.Minute, []string{
			"SMOKE_ID=" + smokeID,
			"PRIVATE_SMOKE_ENABLED=true",
		}, "config/samples/chaincodes/node_settlement/invoke_smoke.sh")

		By("proving successful output-backed helper Jobs and pods are cleaned up after the sample cleanup window")
		cleanupCandidates := cleanupEligibleSucceededJobs()
		Expect(cleanupCandidates).NotTo(BeEmpty())
		cleanupGeneration := patchSampleSucceededJobCleanupTTL(fastCleanupTTLSeconds)
		waitForFabricNetworkReadyGeneration(cleanupGeneration, 25*time.Minute)
		expectCleanupEligibleSucceededJobsRemoved(cleanupCandidates, 4*time.Minute)
		expectRetainedChannelProofJobs()

		By("scaling BankB with a new explicit channel peer")
		scaledGeneration := patchSampleBankBPeerScaleUp()
		waitForFabricNetworkReadyGeneration(scaledGeneration, 25*time.Minute)
		expectSampleChaincodeReady("0.0.1", 1, runtimeConfig.image, scaledSampleChaincodeTargets())

		By("invoking through the newly joined BankB peer")
		invokeAndQuerySettlement(smokeID+"-scale", runtimeConfig.createFunction, runtimeConfig.readFunction, "BankA/peer0", "BankB/peer1")

		By("scaling BankB back down while retaining peer state")
		scaledDownGeneration := patchSampleBankBPeerScaleDown()
		waitForFabricNetworkReadyGeneration(scaledDownGeneration, 25*time.Minute)
		expectSampleChaincodeReady("0.0.1", 1, runtimeConfig.image, baseSampleChaincodeTargets())
		expectSampleBankBPeer1ScaledDown()

		status := runCommand(30*time.Second, kubectlBin, "get", "fabricnetwork", sampleName, "-n", sampleNamespace, "-o", "jsonpath={.status.phase}{\"\\n\"}{range .status.conditions[*]}{.type}={.status} {.reason}{\"\\n\"}{end}")
		Expect(status).To(ContainSubstring("Ready\n"))
		Expect(status).To(ContainSubstring("Ready=True FabricNetworkReady"))

		By("upgrading the sample chaincode declaratively")
		upgradedGeneration := patchSampleChaincode("0.0.2", 2, nodeUpgradeImage)

		By("waiting for FabricOps to complete the chaincode upgrade")
		waitForFabricNetworkReadyGeneration(upgradedGeneration, 25*time.Minute)

		By("verifying the upgraded chaincode status and workload image")
		expectSampleChaincodeReady("0.0.2", 2, nodeUpgradeImage, baseSampleChaincodeTargets())

		By("invoking and querying the upgraded chaincode")
		upgradeSmokeID := smokeID + "-upgrade"
		invokeAndQuerySettlement(upgradeSmokeID, runtimeConfig.createFunction, runtimeConfig.readFunction, "BankA/peer0", "BankB/peer0")

		By("confirming cleanup continues after chaincode upgrades without losing channel proof Jobs")
		expectNoCleanupEligibleSucceededJobs(4 * time.Minute)
		expectRetainedChannelProofJobs()
	})
})

type fabricNetworkProbe struct {
	Metadata struct {
		Generation int64 `json:"generation"`
	} `json:"metadata"`
	Status struct {
		Phase      string `json:"phase"`
		Conditions []struct {
			Type               string `json:"type"`
			Status             string `json:"status"`
			Reason             string `json:"reason"`
			ObservedGeneration int64  `json:"observedGeneration"`
		} `json:"conditions"`
		ChaincodeStatus []struct {
			Name         string `json:"name"`
			Version      string `json:"version"`
			PackageLabel string `json:"packageLabel"`
			Sequence     int32  `json:"sequence"`
			Ready        bool   `json:"ready"`
			Workloads    struct {
				Desired int32 `json:"desired"`
				Ready   int32 `json:"ready"`
			} `json:"workloads"`
			Targets []struct {
				OrgName       string `json:"orgName"`
				Namespace     string `json:"namespace"`
				PeerName      string `json:"peerName"`
				WorkloadName  string `json:"workloadName"`
				Installed     bool   `json:"installed"`
				WorkloadReady bool   `json:"workloadReady"`
			} `json:"targets"`
		} `json:"chaincodeStatus"`
	} `json:"status"`
}

func getFabricNetworkProbe() fabricNetworkProbe {
	GinkgoHelper()

	output := runCommandQuiet(30*time.Second, kubectlBin, "get", "fabricnetwork", sampleName, "-n", sampleNamespace, "-o", "json")
	var probe fabricNetworkProbe
	Expect(json.Unmarshal([]byte(output), &probe)).To(Succeed())
	return probe
}

func invokeAndQuerySettlement(smokeID string, createFunction string, readFunction string, peers ...string) {
	GinkgoHelper()

	invokeArgs := []string{
		"invoke",
		"-n", sampleNamespace,
		"--org", "BankA",
		"--channel", "settlement",
		"--chaincode", "settlement",
		"--function", createFunction,
		"--args", jsonStringArray(smokeID, "alice", "bob", "100", "USD"),
		"-o", "json",
	}
	for _, peer := range peers {
		invokeArgs = append(invokeArgs, "--peer", peer)
	}
	invokeArgs = append(invokeArgs, sampleName)
	runFabricOpsctlEventually(8*time.Minute, invokeArgs...)

	for _, peer := range peers {
		queryArgs := []string{
			"query",
			"-n", sampleNamespace,
			"--org", orgNameFromPeerSelector(peer),
			"--peer", peer,
			"--channel", "settlement",
			"--chaincode", "settlement",
			"--function", readFunction,
			"--args", jsonStringArray(smokeID),
			"-o", "json",
			sampleName,
		}
		output := runFabricOpsctlEventually(5*time.Minute, queryArgs...)
		Expect(output).To(ContainSubstring(smokeID))

		richQueryArgs := []string{
			"query",
			"-n", sampleNamespace,
			"--org", orgNameFromPeerSelector(peer),
			"--peer", peer,
			"--channel", "settlement",
			"--chaincode", "settlement",
			"--function", runtimeConfig.richQueryFunction,
			"--args", jsonStringArray("alice"),
			"-o", "json",
			sampleName,
		}
		output = runFabricOpsctlEventually(5*time.Minute, richQueryArgs...)
		Expect(output).To(ContainSubstring(smokeID))
	}
}

func jsonStringArray(values ...string) string {
	GinkgoHelper()

	encoded, err := json.Marshal(values)
	Expect(err).NotTo(HaveOccurred())
	return string(encoded)
}

func orgNameFromPeerSelector(peer string) string {
	GinkgoHelper()

	parts := strings.SplitN(peer, "/", 2)
	Expect(parts).To(HaveLen(2), "peer selector must use Org/peer")
	return parts[0]
}

func waitForFabricNetworkReadyGeneration(generation int64, timeout time.Duration) {
	GinkgoHelper()

	Eventually(func(g Gomega) {
		probe := getFabricNetworkProbe()
		g.Expect(probe.Status.Phase).To(Equal("Ready"))
		for _, condition := range probe.Status.Conditions {
			if condition.Type == "Ready" {
				g.Expect(condition.Status).To(Equal("True"))
				g.Expect(condition.Reason).To(Equal("FabricNetworkReady"))
				g.Expect(condition.ObservedGeneration).To(Equal(generation))
				return
			}
		}
		g.Expect(probe.Status.Conditions).To(ContainElement(HaveField("Type", "Ready")))
	}, timeout, 10*time.Second).Should(Succeed())
}

func patchSampleChaincode(version string, sequence int32, image string) int64 {
	GinkgoHelper()

	patch := fmt.Sprintf(
		`[{"op":"replace","path":"/spec/chaincodes/0/version","value":%q},{"op":"replace","path":"/spec/chaincodes/0/sequence","value":%d},{"op":"replace","path":"/spec/chaincodes/0/image","value":%q}]`,
		version,
		sequence,
		image,
	)
	runCommand(2*time.Minute, kubectlBin, "patch", "fabricnetwork", sampleName, "-n", sampleNamespace, "--type=json", "-p", patch)
	return getFabricNetworkProbe().Metadata.Generation
}

func patchSampleSucceededJobCleanupTTL(seconds int32) int64 {
	GinkgoHelper()

	patch := fmt.Sprintf(
		`[{"op":"replace","path":"/spec/global/jobs/succeededHistoryTTLSeconds","value":%d}]`,
		seconds,
	)
	runCommand(2*time.Minute, kubectlBin, "patch", "fabricnetwork", sampleName, "-n", sampleNamespace, "--type=json", "-p", patch)
	return getFabricNetworkProbe().Metadata.Generation
}

func patchSampleBankBPeerScaleUp() int64 {
	GinkgoHelper()

	patch := `[` +
		`{"op":"replace","path":"/spec/orgs/2/peer/instances","value":2},` +
		`{"op":"add","path":"/spec/channels/0/orgs/1/peers/-","value":"peer1"},` +
		`{"op":"add","path":"/spec/channels/1/orgs/1/peers/-","value":"peer1"}` +
		`]`
	runCommand(2*time.Minute, kubectlBin, "patch", "fabricnetwork", sampleName, "-n", sampleNamespace, "--type=json", "-p", patch)
	return getFabricNetworkProbe().Metadata.Generation
}

func patchSampleBankBPeerScaleDown() int64 {
	GinkgoHelper()

	patch := `[` +
		`{"op":"replace","path":"/spec/orgs/2/peer/instances","value":1},` +
		`{"op":"remove","path":"/spec/channels/0/orgs/1/peers/1"},` +
		`{"op":"remove","path":"/spec/channels/1/orgs/1/peers/1"}` +
		`]`
	runCommand(2*time.Minute, kubectlBin, "patch", "fabricnetwork", sampleName, "-n", sampleNamespace, "--type=json", "-p", patch)
	return getFabricNetworkProbe().Metadata.Generation
}

func baseSampleChaincodeTargets() []sampleChaincodeTarget {
	return []sampleChaincodeTarget{
		{namespace: "fo-sample-banka", name: "settlement-settlement-banka-peer0-ccaas", orgName: "BankA", peerName: "peer0"},
		{namespace: "fo-sample-banka", name: "settlement-settlement-banka-peer1-ccaas", orgName: "BankA", peerName: "peer1"},
		{namespace: "fo-sample-bankb", name: "settlement-settlement-bankb-peer0-ccaas", orgName: "BankB", peerName: "peer0"},
	}
}

func scaledSampleChaincodeTargets() []sampleChaincodeTarget {
	targets := baseSampleChaincodeTargets()
	targets = append(targets, sampleChaincodeTarget{
		namespace: "fo-sample-bankb",
		name:      "settlement-settlement-bankb-peer1-ccaas",
		orgName:   "BankB",
		peerName:  "peer1",
	})
	return targets
}

func expectSampleBankBPeer1ScaledDown() {
	GinkgoHelper()

	runCommand(30*time.Second, kubectlBin, "get", "pvc", "peer1-data", "-n", "fo-sample-bankb")
	expectCommandFailure(30*time.Second, kubectlBin, "get", "deployment", "peer1", "-n", "fo-sample-bankb")
	expectCommandFailure(30*time.Second, kubectlBin, "get", "service", "peer1", "-n", "fo-sample-bankb")
	expectCommandFailure(30*time.Second, kubectlBin, "get", "service", "peer1-operations", "-n", "fo-sample-bankb")
	expectCommandFailure(30*time.Second, kubectlBin, "get", "deployment", "settlement-settlement-bankb-peer1-ccaas", "-n", "fo-sample-bankb")
	expectCommandFailure(30*time.Second, kubectlBin, "get", "service", "settlement-settlement-bankb-peer1-ccaas", "-n", "fo-sample-bankb")
}

type kubernetesJobList struct {
	Items []struct {
		Metadata struct {
			Name        string            `json:"name"`
			Namespace   string            `json:"namespace"`
			Annotations map[string]string `json:"annotations"`
		} `json:"metadata"`
		Status struct {
			Succeeded int32 `json:"succeeded"`
		} `json:"status"`
	} `json:"items"`
}

func cleanupEligibleSucceededJobs() []namespacedName {
	GinkgoHelper()

	output := runCommandQuiet(
		30*time.Second,
		kubectlBin,
		"get",
		"jobs",
		"-A",
		"-l",
		fabricNetworkLabel+"="+sampleName+","+fabricNetworkNamespaceLabel+"="+sampleNamespace,
		"-o",
		"json",
	)
	var jobs kubernetesJobList
	Expect(json.Unmarshal([]byte(output), &jobs)).To(Succeed())

	var names []namespacedName
	for _, job := range jobs.Items {
		if job.Status.Succeeded < 1 || job.Metadata.Annotations[succeededJobCleanupAnnotation] != "true" {
			continue
		}
		names = append(names, namespacedName{
			namespace: job.Metadata.Namespace,
			name:      job.Metadata.Name,
		})
	}
	return names
}

func expectCleanupEligibleSucceededJobsRemoved(candidates []namespacedName, timeout time.Duration) {
	GinkgoHelper()

	expectNoCleanupEligibleSucceededJobs(timeout)
	Eventually(func(g Gomega) {
		for _, job := range candidates {
			output := runCommandQuiet(
				30*time.Second,
				kubectlBin,
				"get",
				"pods",
				"-n",
				job.namespace,
				"-l",
				"job-name="+job.name,
				"-o",
				"jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}",
			)
			g.Expect(strings.TrimSpace(output)).To(BeEmpty(), "expected cleanup to remove pods for %s", job.String())
		}
	}, timeout, 5*time.Second).Should(Succeed())
}

func expectNoCleanupEligibleSucceededJobs(timeout time.Duration) {
	GinkgoHelper()

	Eventually(func(g Gomega) {
		jobs := cleanupEligibleSucceededJobs()
		g.Expect(jobNames(jobs)).To(BeEmpty())
	}, timeout, 5*time.Second).Should(Succeed())
}

func expectRetainedChannelProofJobs() {
	GinkgoHelper()

	jobs := runCommand(
		30*time.Second,
		kubectlBin,
		"get",
		"jobs",
		"-A",
		"-l",
		fabricNetworkLabel+"="+sampleName+","+fabricNetworkNamespaceLabel+"="+sampleNamespace,
		"-o",
		"jsonpath={range .items[*]}{.metadata.namespace}/{.metadata.name}{\"\\n\"}{end}",
	)
	Expect(jobs).NotTo(ContainSubstring("orderer-join"))
	Expect(jobs).NotTo(ContainSubstring("peer-join"))
	Expect(jobs).NotTo(ContainSubstring("anchor-peer-update"))
	Expect(strings.TrimSpace(jobs)).To(BeEmpty())
}

func jobNames(items []namespacedName) []string {
	names := make([]string, 0, len(items))
	for _, item := range items {
		names = append(names, item.String())
	}
	return names
}

func expectSampleChaincodeReady(version string, sequence int32, image string, targets []sampleChaincodeTarget) {
	GinkgoHelper()

	probe := getFabricNetworkProbe()
	Expect(probe.Status.ChaincodeStatus).To(HaveLen(1))
	Expect(probe.Status.ChaincodeStatus[0].Version).To(Equal(version))
	Expect(probe.Status.ChaincodeStatus[0].Sequence).To(Equal(sequence))
	Expect(probe.Status.ChaincodeStatus[0].PackageLabel).To(Equal("settlement_settlement_" + version))
	Expect(probe.Status.ChaincodeStatus[0].Ready).To(BeTrue())
	Expect(probe.Status.ChaincodeStatus[0].Workloads.Desired).To(Equal(int32(len(targets))))
	Expect(probe.Status.ChaincodeStatus[0].Workloads.Ready).To(Equal(int32(len(targets))))
	Expect(probe.Status.ChaincodeStatus[0].Targets).To(HaveLen(len(targets)))

	for _, item := range targets {
		Expect(probe.Status.ChaincodeStatus[0].Targets).To(ContainElement(SatisfyAll(
			HaveField("OrgName", item.orgName),
			HaveField("Namespace", item.namespace),
			HaveField("PeerName", item.peerName),
			HaveField("WorkloadName", item.name),
			HaveField("Installed", true),
			HaveField("WorkloadReady", true),
		)))
		deploymentImage := strings.TrimSpace(runCommand(
			30*time.Second,
			kubectlBin,
			"get",
			"deployment",
			item.name,
			"-n",
			item.namespace,
			"-o",
			"jsonpath={.spec.template.spec.containers[?(@.name==\"chaincode\")].image}",
		))
		Expect(deploymentImage).To(Equal(image))
	}
}

func runCommand(timeout time.Duration, name string, args ...string) string {
	return runCommandWithEnv(timeout, nil, name, args...)
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

func expectCommandFailure(timeout time.Duration, name string, args ...string) string {
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
		Fail(fmt.Sprintf("command timed out after %s: %s\n%s", timeout, commandLine, text))
	}
	Expect(err).To(HaveOccurred(), "expected command to fail: %s\n%s", commandLine, text)
	return text
}

func runCommandWithEnv(timeout time.Duration, extraEnv []string, name string, args ...string) string {
	return runCommandWithEnvAndLogging(timeout, extraEnv, true, name, args...)
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

func runDiagnostic(timeout time.Duration, name string, args ...string) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = repoRoot
	cmd.Env = os.Environ()

	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	commandLine := strings.Join(append([]string{name}, args...), " ")
	fmt.Fprintf(GinkgoWriter, "\n# diagnostics: %s\n", commandLine)
	_ = cmd.Run()
	if output.Len() > 0 {
		fmt.Fprintln(GinkgoWriter, output.String())
	}
}

func dumpDiagnostics() {
	runDiagnostic(30*time.Second, kubectlBin, "get", "fabricnetwork", "-A", "-o", "wide")
	runDiagnostic(30*time.Second, kubectlBin, "get", "pods", "-A", "-o", "wide")
	runDiagnostic(30*time.Second, kubectlBin, "get", "jobs", "-A")
	runDiagnostic(30*time.Second, kubectlBin, "describe", "fabricnetwork", sampleName, "-n", sampleNamespace)
	runDiagnostic(30*time.Second, kubectlBin, "logs", "-n", managerNamespace, "deployment/"+managerName, "-c", "manager", "--tail=200")
	runDiagnostic(30*time.Second, kubectlBin, "get", "events", "-A", "--sort-by=.lastTimestamp")
}

func mustRepoRoot() string {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		Fail("could not discover test filename")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
}

func envOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
