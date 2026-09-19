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

package bft

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
)

const (
	managerNamespace      = "fabricops-system"
	managerName           = "fabricops-controller-manager"
	sampleName            = "fabricnetwork-bft"
	sampleNamespace       = "default"
	bftSampleManifest     = "config/samples/e2e/bft/fabricnetwork.yaml"
	bftFabricToolsImage   = "fabricops-fabric-tools:e2e"
	expectedBFTOrderers   = int32(4)
	expectedBFTChannelOrg = "BankA"
)

var (
	repoRoot        string
	kindBin         string
	kubectlBin      string
	fabricopsctlBin string
	kindCluster     string
	managerImage    string
	fabricToolsImg  string
)

func TestBFTE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "FabricOps BFT E2E Suite")
}

var _ = BeforeSuite(func() {
	repoRoot = mustRepoRoot()
	kindBin = envOrDefault("KIND", "kind")
	kubectlBin = envOrDefault("KUBECTL", "kubectl")
	fabricopsctlBin = filepath.Join(repoRoot, "bin", "fabricopsctl")
	kindCluster = envOrDefault("KIND_CLUSTER", "fabricops-test-e2e-bft")
	managerImage = envOrDefault("IMG", "controller:latest")
	fabricToolsImg = envOrDefault("FABRIC_TOOLS_IMAGE", bftFabricToolsImage)
})

var _ = Describe("Kind BFT bundle install", Ordered, func() {
	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			dumpDiagnostics()
		}
	})

	It("bootstraps a Fabric v3 BFT channel", func() {
		By("using the target kind context")
		runCommand(30*time.Second, kubectlBin, "config", "use-context", "kind-"+kindCluster)

		By("building and loading the manager image")
		runCommand(10*time.Minute, "make", "docker-build", "IMG="+managerImage)
		runCommand(5*time.Minute, kindBin, "load", "docker-image", managerImage, "--name", kindCluster)

		By("building and loading the Fabric v3 tools helper image")
		runCommand(15*time.Minute, "make", "docker-build-fabric-tools", "FABRIC_TOOLS_IMAGE="+fabricToolsImg)
		runCommand(5*time.Minute, kindBin, "load", "docker-image", fabricToolsImg, "--name", kindCluster)

		By("building fabricopsctl")
		runCommand(30*time.Second, "mkdir", "-p", "bin")
		runCommand(3*time.Minute, "go", "build", "-o", fabricopsctlBin, "./cmd/fabricopsctl")

		By("generating and applying the install bundle")
		runCommand(5*time.Minute, "make", "build-installer", "IMG="+managerImage)
		runCommand(3*time.Minute, kubectlBin, "apply", "-f", "dist/install.yaml")
		runCommand(3*time.Minute, kubectlBin, "rollout", "status", "deployment/"+managerName, "-n", managerNamespace, "--timeout=120s")

		By("applying the BFT sample FabricNetwork")
		runCommand(2*time.Minute, kubectlBin, "apply", "-f", bftSampleManifestPath())

		By("waiting for FabricOps to provision the BFT Fabric network")
		runFabricOpsctl(25*time.Minute, "wait", "-n", sampleNamespace, "--timeout", "20m", sampleName)

		By("verifying BFT channel readiness")
		expectBFTChannelReady()
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
		ChannelStatus []struct {
			Name        string `json:"name"`
			ConfigReady bool   `json:"configReady"`
			BlockReady  bool   `json:"blockReady"`
			Orderers    struct {
				Desired int32 `json:"desired"`
				Ready   int32 `json:"ready"`
			} `json:"orderers"`
			Peers struct {
				Desired int32 `json:"desired"`
				Ready   int32 `json:"ready"`
			} `json:"peers"`
			Orgs []struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace"`
				Ready     bool   `json:"ready"`
			} `json:"orgs"`
			Ready bool `json:"ready"`
		} `json:"channelStatus"`
	} `json:"status"`
}

func expectBFTChannelReady() {
	GinkgoHelper()

	probe := getFabricNetworkProbe()
	Expect(probe.Status.Phase).To(Equal("Ready"))
	Expect(probe.Status.ChannelStatus).To(HaveLen(1))

	channel := probe.Status.ChannelStatus[0]
	Expect(channel.Name).To(Equal("settlement"))
	Expect(channel.ConfigReady).To(BeTrue())
	Expect(channel.BlockReady).To(BeTrue())
	Expect(channel.Ready).To(BeTrue())
	Expect(channel.Orderers.Desired).To(Equal(expectedBFTOrderers))
	Expect(channel.Orderers.Ready).To(Equal(expectedBFTOrderers))
	Expect(channel.Peers.Desired).To(Equal(int32(1)))
	Expect(channel.Peers.Ready).To(Equal(int32(1)))
	Expect(channel.Orgs).To(ContainElement(SatisfyAll(
		HaveField("Name", expectedBFTChannelOrg),
		HaveField("Namespace", "fo-fabricnetwork-bft-banka"),
		HaveField("Ready", true),
	)))
}

func getFabricNetworkProbe() fabricNetworkProbe {
	GinkgoHelper()

	output := runCommandQuiet(30*time.Second, kubectlBin, "get", "fabricnetwork", sampleName, "-n", sampleNamespace, "-o", "json")
	var probe fabricNetworkProbe
	Expect(json.Unmarshal([]byte(output), &probe)).To(Succeed())
	return probe
}

func bftSampleManifestPath() string {
	GinkgoHelper()

	samplePath := filepath.Join(repoRoot, bftSampleManifest)
	if fabricToolsImg == bftFabricToolsImage {
		return samplePath
	}

	data, err := os.ReadFile(samplePath)
	Expect(err).NotTo(HaveOccurred())

	rendered := strings.Replace(
		string(data),
		"fabricTools: "+bftFabricToolsImage,
		"fabricTools: "+fabricToolsImg,
		1,
	)
	Expect(rendered).NotTo(Equal(string(data)), "BFT sample manifest must include the default Fabric tools image")

	tmp, err := os.CreateTemp("", "fabricops-bft-*.yaml")
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(os.Remove, tmp.Name())

	_, err = tmp.WriteString(rendered)
	Expect(err).NotTo(HaveOccurred())
	Expect(tmp.Close()).To(Succeed())
	return tmp.Name()
}

func runCommand(timeout time.Duration, name string, args ...string) string {
	return runCommandWithEnvAndLogging(timeout, nil, true, name, args...)
}

func runCommandQuiet(timeout time.Duration, name string, args ...string) string {
	return runCommandWithEnvAndLogging(timeout, nil, false, name, args...)
}

func runFabricOpsctl(timeout time.Duration, args ...string) string {
	GinkgoHelper()

	return runCommand(timeout, fabricopsctlBin, args...)
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
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", "..", ".."))
}

func envOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
