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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/netip"
	"path/filepath"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	ordererDNS         = "orderer.founder.fabricops.test"
	founderPeerDNS     = "peer0.banka.fabricops.test"
	participantPeerDNS = "peer0.bankb.fabricops.test"
	mismatchDNS        = "wrong-name.fabricops.test"
	metallbManifest    = "https://raw.githubusercontent.com/metallb/metallb/v0.16.2/config/manifests/metallb-native.yaml"
)

func TestLoadBalancerAddresses(t *testing.T) {
	for _, cidr := range []string{"172.18.0.0/16", "192.168.42.0/24"} {
		addresses, err := loadBalancerAddresses(cidr)
		if err != nil || len(addresses) != 3 {
			t.Fatalf("%s: %v, %v", cidr, addresses, err)
		}
		prefix := netip.MustParsePrefix(cidr)
		for i, address := range addresses {
			if !prefix.Contains(netip.MustParseAddr(address)) || (i > 0 && address == addresses[i-1]) {
				t.Fatalf("invalid address plan: %v", addresses)
			}
		}
	}
	for _, cidr := range []string{"invalid", "fd00::/64", "192.168.1.0/30"} {
		if _, err := loadBalancerAddresses(cidr); err == nil {
			t.Errorf("expected unsupported subnet %q to fail", cidr)
		}
	}
}

func loadBalancerAddresses(cidr string) ([]string, error) {
	prefix, err := netip.ParsePrefix(cidr)
	if err != nil || !prefix.Addr().Is4() || prefix.Bits() > 24 || prefix.Bits() < 8 {
		return nil, fmt.Errorf("expected an IPv4 Docker subnet between /8 and /24: %q", cidr)
	}
	base := prefix.Masked().Addr().As4()
	start := uint64(binary.BigEndian.Uint32(base[:])) + (uint64(1) << (32 - prefix.Bits())) - 16
	addresses := make([]string, 3)
	for i := range addresses {
		var value [4]byte
		binary.BigEndian.PutUint32(value[:], uint32(start+uint64(i)))
		addresses[i] = netip.AddrFrom4(value).String()
	}
	return addresses, nil
}

func setupFederatedNetworking(tempDir string) ([]string, []string) {
	GinkgoHelper()
	output := runCommandQuiet(30*time.Second, dockerBin, "network", "inspect", "kind")
	var networks []struct {
		IPAM       struct{ Config []struct{ Subnet string } }
		Containers map[string]struct{ IPv4Address string }
	}
	Expect(json.Unmarshal([]byte(output), &networks)).To(Succeed())
	Expect(networks).To(HaveLen(1))
	var subnet string
	for _, config := range networks[0].IPAM.Config {
		prefix, err := netip.ParsePrefix(config.Subnet)
		if err == nil && prefix.Addr().Is4() {
			subnet = config.Subnet
			break
		}
	}
	addresses, err := loadBalancerAddresses(subnet)
	Expect(err).NotTo(HaveOccurred())
	for _, container := range networks[0].Containers {
		prefix, err := netip.ParsePrefix(container.IPv4Address)
		if err == nil {
			Expect(addresses).NotTo(ContainElement(prefix.Addr().String()), "test VIP already belongs to a Docker container")
		}
	}
	sources := []string{subnet}
	for _, contextName := range []string{founderContext, participantContext} {
		var nodes corev1.NodeList
		output := runKubectlQuiet(contextName, time.Minute, "get", "nodes", "-o", "json")
		Expect(json.Unmarshal([]byte(output), &nodes)).To(Succeed())
		for _, node := range nodes.Items {
			sources = append(sources, node.Spec.PodCIDRs...)
		}
	}
	for index, contextName := range []string{founderContext, participantContext} {
		runKubectl(contextName, 3*time.Minute, "apply", "-f", metallbManifest)
		runKubectl(contextName, 5*time.Minute, "rollout", "status", "deployment/controller",
			"-n", "metallb-system", "--timeout=240s")
		runKubectl(contextName, 5*time.Minute, "rollout", "status", "daemonset/speaker",
			"-n", "metallb-system", "--timeout=240s")
		runKubectl(contextName, time.Minute, "label", "nodes", "--all",
			"node.kubernetes.io/exclude-from-external-load-balancers-")
		poolAddresses := []string{addresses[0] + "/32", addresses[1] + "/32"}
		if index == 1 {
			poolAddresses = []string{addresses[2] + "/32"}
		}
		pool := map[string]any{"apiVersion": "metallb.io/v1beta1", "kind": "IPAddressPool",
			"metadata": map[string]string{"name": "fabricops-e2e", "namespace": "metallb-system"},
			"spec":     map[string]any{"addresses": poolAddresses}}
		advertisement := map[string]any{"apiVersion": "metallb.io/v1beta1", "kind": "L2Advertisement",
			"metadata": map[string]string{"name": "fabricops-e2e", "namespace": "metallb-system"},
			"spec":     map[string]any{"ipAddressPools": []string{"fabricops-e2e"}}}
		path := filepath.Join(tempDir, fmt.Sprintf("metallb-%d.json", index))
		resources := map[string]any{"apiVersion": "v1", "kind": "List", "items": []any{pool, advertisement}}
		writeFile(path, []byte(marshalJSON(resources)))
		Eventually(func(g Gomega) {
			output, err := runCommandAllowFailure(time.Minute, kubectlBin, "--context", contextName, "apply", "-f", path)
			g.Expect(err).NotTo(HaveOccurred(), output)
		}, 2*time.Minute, 5*time.Second).Should(Succeed())
		configureFederatedDNS(contextName, addresses)
	}
	return addresses, sources
}

func configureFederatedDNS(contextName string, addresses []string) {
	GinkgoHelper()
	var config corev1.ConfigMap
	output := runKubectlQuiet(contextName, time.Minute, "get", "configmap", "coredns", "-n", "kube-system", "-o", "json")
	Expect(json.Unmarshal([]byte(output), &config)).To(Succeed())
	zone := fmt.Sprintf("fabricops.test:53 {\n errors\n hosts {\n %s %s\n %s %s %s\n %s %s\n ttl 1\n }\n}\n",
		addresses[0], ordererDNS, addresses[1], founderPeerDNS, mismatchDNS, addresses[2], participantPeerDNS)
	corefile := config.Data["Corefile"]
	if !strings.HasPrefix(corefile, zone) {
		corefile = zone + corefile
	}
	patch := marshalJSON(map[string]any{"data": map[string]string{"Corefile": corefile}})
	runKubectl(contextName, time.Minute, "patch", "configmap", "coredns", "-n", "kube-system", "--type=merge", "-p", patch)
	runKubectl(contextName, time.Minute, "rollout", "restart", "deployment/coredns", "-n", "kube-system")
	runKubectl(contextName, 3*time.Minute, "rollout", "status", "deployment/coredns",
		"-n", "kube-system", "--timeout=120s")
}

func expectExternalService(contextName, namespace, name, address string, port int32) {
	GinkgoHelper()
	var service corev1.Service
	output := runKubectlQuiet(contextName, time.Minute, "get", "service", name+"-external", "-n", namespace, "-o", "json")
	Expect(json.Unmarshal([]byte(output), &service)).To(Succeed())
	Expect(service.Spec.Type).To(Equal(corev1.ServiceTypeLoadBalancer))
	Expect(service.Spec.Ports).To(HaveLen(1), "only the Fabric client port may be published")
	Expect(service.Spec.Ports[0].Port).To(Equal(port))
	Expect(service.Status.LoadBalancer.Ingress).To(ContainElement(HaveField("IP", address)))
	uid := service.UID
	Consistently(func(g Gomega) {
		output, err := runCommandAllowFailure(time.Minute, kubectlBin, "--context", contextName,
			"get", "service", name+"-external", "-n", namespace, "-o", "json")
		g.Expect(err).NotTo(HaveOccurred(), output)
		var current corev1.Service
		g.Expect(json.Unmarshal([]byte(output), &current)).To(Succeed())
		g.Expect(current.UID).To(Equal(uid), "reconciliation must not recreate an allocated Service")
		g.Expect(current.Status.LoadBalancer.Ingress).To(ContainElement(HaveField("IP", address)))
	}, 10*time.Second, time.Second).Should(Succeed())
	output = runKubectlQuiet(contextName, time.Minute, "get", "service", name, "-n", namespace, "-o", "json")
	Expect(json.Unmarshal([]byte(output), &service)).To(Succeed())
	Expect(service.Spec.Type).To(Equal(corev1.ServiceTypeClusterIP), "internal services must remain private")
}

func expectDNSHostnameVerification(tempDir string) {
	GinkgoHelper()
	// This public CA root is already imported into BankB's cluster for normal traffic.
	// Both DNS names resolve to the same VIP; only one is in BankA's certificate.
	for index, host := range []string{founderPeerDNS, mismatchDNS} {
		name := fmt.Sprintf("tls-hostname-probe-%d", index)
		script := fmt.Sprintf("timeout 20 openssl s_client -connect %s:7051 -servername %s -verify_hostname %s "+
			"-verify_return_error -CAfile /roots/tls-ca.pem </dev/null", host, host, host)
		pod := corev1.Pod{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: sampleNamespace},
			Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever,
				Containers: []corev1.Container{{Name: "probe", Image: "hyperledger/fabric-tools:2.5.14",
					Command:      []string{"sh", "-c", script},
					VolumeMounts: []corev1.VolumeMount{{Name: "roots", MountPath: "/roots", ReadOnly: true}}}},
				Volumes: []corev1.Volume{{Name: "roots", VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{
						Name: "banka-peer0-artifacts"}}}}},
			},
		}
		path := filepath.Join(tempDir, name+".json")
		writeFile(path, []byte(marshalJSON(pod)))
		runKubectl(participantContext, time.Minute, "delete", "pod", name, "--ignore-not-found=true")
		runKubectl(participantContext, time.Minute, "apply", "-f", path)
		Eventually(func() string {
			return runKubectlQuiet(participantContext, time.Minute, "get", "pod", name, "-o", "jsonpath={.status.phase}")
		}, 5*time.Minute, 2*time.Second).Should(Or(Equal("Succeeded"), Equal("Failed")))
		logs := runKubectlQuiet(participantContext, time.Minute, "logs", name)
		exitCode := runKubectlQuiet(participantContext, time.Minute, "get", "pod", name,
			"-o", "jsonpath={.status.containerStatuses[0].state.terminated.exitCode}")
		if index == 0 {
			Expect(exitCode).To(Equal("0"), logs)
			Expect(logs).To(ContainSubstring("Verification: OK"))
		} else {
			Expect(exitCode).NotTo(Equal("0"), logs)
			Expect(logs).To(ContainSubstring("hostname mismatch"))
		}
		runKubectl(participantContext, time.Minute, "delete", "pod", name)
	}
}
