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
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func testOrdererAdminCredentialScope() {
	GinkgoHelper()
	const (
		hostNamespace = "fo-sample-orderer"
		appNamespace  = "fo-sample-banka"
		copyName      = "settlement-orderer-admin-tls"
		holderName    = "legacy-admin-credential-reader"
	)
	secretMetadata := func(name string) metav1.ObjectMeta {
		// Exclude annotations from CLI output: last-applied can embed Secret data.
		output := runCommandQuiet(time.Minute, kubectlBin, "get", "secret", name, "-n", hostNamespace,
			"-o", "jsonpath-as-json={.metadata['uid','resourceVersion','labels']}")
		var fields []json.RawMessage
		Expect(json.Unmarshal([]byte(output), &fields)).To(Succeed())
		Expect(fields).To(HaveLen(3))
		var metadata metav1.ObjectMeta
		Expect(json.Unmarshal(fields[0], &metadata.UID)).To(Succeed())
		Expect(json.Unmarshal(fields[1], &metadata.ResourceVersion)).To(Succeed())
		Expect(json.Unmarshal(fields[2], &metadata.Labels)).To(Succeed())
		return metadata
	}
	lookupUID := func(namespace, name string) string {
		return runCommandQuiet(time.Minute, kubectlBin, "get", "secret", name, "-n", namespace,
			"--ignore-not-found=true", "-o", "jsonpath={.metadata.uid}")
	}
	for _, namespace := range []string{appNamespace, "fo-sample-bankb"} {
		Expect(lookupUID(namespace, copyName)).To(BeEmpty(), "application org received orderer admin credentials")
	}
	hostCopy := secretMetadata(copyName)
	source := secretMetadata("orderer-admin-tls")
	networkUID := runCommandQuiet(time.Minute, kubectlBin, "get", "fabricnetwork", sampleName,
		"-n", sampleNamespace, "-o", "jsonpath={.metadata.uid}")
	Expect(hostCopy.UID).NotTo(BeEmpty())

	// Seed an upgrade-like state while the controller is stopped so its initial
	// cache includes the unfinished consumer before attempting credential cleanup.
	DeferCleanup(func() {
		runCommand(time.Minute, kubectlBin, "scale", "deployment/"+managerName, "-n", managerNamespace, "--replicas=1")
	})
	runCommand(time.Minute, kubectlBin, "scale", "deployment/"+managerName, "-n", managerNamespace, "--replicas=0")
	runCommand(2*time.Minute, kubectlBin, "rollout", "status", "deployment/"+managerName,
		"-n", managerNamespace, "--timeout=90s")
	labels := maps.Clone(hostCopy.Labels)
	labels["fabricops.io/org"] = "banka"
	annotations := map[string]string{
		"fabricops.io/fabricnetwork": sampleName, "fabricops.io/fabricnetwork-namespace": sampleNamespace,
		"fabricops.io/fabricnetwork-uid": networkUID, "fabricops.io/org": "BankA",
	}
	legacy := corev1.Secret{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Name: copyName, Namespace: appNamespace, Labels: labels, Annotations: annotations},
		StringData: map[string]string{
			"ca.crt": "synthetic-root", "client.crt": "synthetic-cert", "client.key": "synthetic-key"}}
	automount := false
	holder := corev1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{Name: holderName, Namespace: appNamespace}, Spec: corev1.PodSpec{
			AutomountServiceAccountToken: &automount,
			NodeSelector:                 map[string]string{"fabricops.io/e2e-legacy-holder": string(hostCopy.UID)},
			Containers: []corev1.Container{{Name: "hold", Image: "registry.k8s.io/pause:3.10",
				VolumeMounts: []corev1.VolumeMount{{Name: "admin", MountPath: "/admin", ReadOnly: true}}}},
			Volumes: []corev1.Volume{{Name: "admin", VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: copyName}}}},
		}}
	tempDir := GinkgoT().TempDir()
	writeFixture := func(name string, value any) string {
		encoded, err := json.Marshal(value)
		Expect(err).NotTo(HaveOccurred())
		path := filepath.Join(tempDir, name)
		Expect(os.WriteFile(path, encoded, 0o600)).To(Succeed())
		return path
	}
	DeferCleanup(func() {
		runCommand(time.Minute, kubectlBin, "delete", "pod", holderName, "-n", appNamespace,
			"--ignore-not-found=true", "--grace-period=0", "--force")
		runCommand(time.Minute, kubectlBin, "delete", "secret", copyName, "-n", appNamespace, "--ignore-not-found=true")
	})
	runCommand(time.Minute, kubectlBin, "create", "-f", writeFixture("holder.json", holder))
	legacyPath := writeFixture("legacy.json", legacy)
	runCommand(time.Minute, kubectlBin, "create", "-f", legacyPath)
	legacyUID := lookupUID(appNamespace, copyName)
	Expect(legacyUID).NotTo(BeEmpty())
	runCommand(time.Minute, kubectlBin, "scale", "deployment/"+managerName, "-n", managerNamespace, "--replicas=1")
	runCommand(2*time.Minute, kubectlBin, "rollout", "status", "deployment/"+managerName,
		"-n", managerNamespace, "--timeout=90s")
	Eventually(func(g Gomega) {
		output, err := runCommandAllowFailure(time.Minute, kubectlBin, "logs", "deployment/"+managerName,
			"-n", managerNamespace, "-c", "manager", "--tail=100")
		g.Expect(err).NotTo(HaveOccurred(), output)
		g.Expect(output).To(ContainSubstring("waiting for Pod " + appNamespace + "/" + holderName))
	}, 2*time.Minute, 2*time.Second).Should(Succeed(), "controller did not observe the legacy consumer")
	Consistently(func() string { return lookupUID(appNamespace, copyName) },
		10*time.Second, time.Second).Should(Equal(legacyUID), "active legacy consumer lost its credentials")
	runCommand(time.Minute, kubectlBin, "delete", "pod", holderName, "-n", appNamespace, "--grace-period=0", "--force")
	Eventually(func() string { return lookupUID(appNamespace, copyName) },
		2*time.Minute, 2*time.Second).Should(BeEmpty(), "drained legacy credentials were not retired")
	legacy.Annotations["fabricops.io/fabricnetwork-uid"] = "previous-network"
	runCommand(time.Minute, kubectlBin, "create", "-f", writeFixture("foreign.json", legacy))
	foreignUID := lookupUID(appNamespace, copyName)
	Expect(foreignUID).NotTo(BeEmpty())
	Consistently(func() string { return lookupUID(appNamespace, copyName) },
		10*time.Second, time.Second).Should(Equal(foreignUID), "operator deleted a foreign-UID Secret")
	Expect(secretMetadata(copyName).UID).To(Equal(hostCopy.UID))
	after := secretMetadata("orderer-admin-tls")
	Expect(after.UID).To(Equal(source.UID))
	Expect(after.ResourceVersion).To(Equal(source.ResourceVersion), "source identity was rewritten")
	runFabricOpsctl(3*time.Minute, "wait", "-n", sampleNamespace, "--timeout", "2m", sampleName)
}
