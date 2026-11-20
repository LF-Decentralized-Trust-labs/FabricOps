//go:build integration

// Copyright contributors to FabricOps. SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func pkcs11ApplicationDefinition(org fabricopsv1alpha1.Org) (fabricopsv1alpha1.Channel, fabricopsv1alpha1.Chaincode) {
	channel := fabricopsv1alpha1.Channel{Name: "custody-app", Orgs: []fabricopsv1alpha1.ChannelOrg{{Name: org.Organization.Name, Peers: []string{"peer0"}}}}
	chaincode := fabricopsv1alpha1.Chaincode{Name: "settlement", Channel: channel.Name, Version: "1", Image: "fabricops-pkcs11-kind:chaincode"}
	return channel, chaincode
}

func TestPKCS11ApplicationClientIsolation(t *testing.T) {
	net, _, _, _, _ := channelConfigOperationFixture()
	org := pkcs11RuntimeOrgFixture(t)
	org.Orderers = []fabricopsv1alpha1.OrdererGroup{{GroupName: "proof", Type: "raft", Instances: 1, Prefix: "orderer"}}
	net.Spec.Orgs = []fabricopsv1alpha1.Org{org}
	_, pod, err := buildPKCS11ApplicationClient(net, org, "proof", "expected-package-id")
	if err != nil {
		t.Fatal(err)
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Fatal("application client can acquire an API token")
	}
	script := pod.Spec.Containers[0].Command[2]
	preflight, approval := strings.Index(script, "leader-config.pb"), strings.Index(script, "approveformyorg")
	if preflight < 0 || approval < 0 || preflight > approval {
		t.Fatal("lifecycle writes do not wait for a serving Raft leader")
	}
	allowed := map[string]bool{
		identitySecretName(adminIdentityName(org), secretKindMSP): true,
		identitySecretName(adminIdentityName(org), secretKindTLS): true,
		identitySecretName("peer0", secretKindMSP):                true,
		identitySecretName("peer0", secretKindTLS):                true,
		identitySecretName("orderer0", secretKindTLS):             true,
	}
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil || volume.Projected != nil {
			t.Fatal("application client has token storage or projected credentials")
		}
		if volume.Secret == nil {
			continue
		}
		if !allowed[volume.Secret.SecretName] || len(volume.Secret.Items) == 0 {
			t.Fatal("application client has unscoped Secret access")
		}
		for _, item := range volume.Secret.Items {
			if item.Key == "server.key" || (item.Key == mspKeyStoreKey && volume.Secret.SecretName != identitySecretName(adminIdentityName(org), secretKindMSP)) || (item.Key == "client.key" && volume.Secret.SecretName != identitySecretName(adminIdentityName(org), secretKindTLS)) {
				t.Fatal("application client has a workload private key")
			}
		}
	}
	for _, env := range pod.Spec.Containers[0].Env {
		if strings.HasPrefix(env.Name, "FABRICOPS_PKCS11_") || env.ValueFrom != nil {
			t.Fatal("application client received provider credentials")
		}
	}
}

func buildPKCS11ApplicationClient(net *fabricopsv1alpha1.FabricNetwork, org fabricopsv1alpha1.Org, namespace, packageID string) (*corev1.ConfigMap, *corev1.Pod, error) {
	channel, chaincode := pkcs11ApplicationDefinition(org)
	config, pod, err := buildPKCS11EndorsementClient(net, org, namespace)
	if err != nil {
		return nil, nil, err
	}
	config.Name = channelConfigMapName(channel.Name)
	config.Data["configtx.yaml"], err = buildConfigtxYAML(net, channel)
	if err != nil {
		return nil, nil, err
	}
	pod.Name = "application-client"
	container := &pod.Spec.Containers[0]
	for i := range pod.Spec.Volumes {
		volume := &pod.Spec.Volumes[i]
		if volume.Name == "config" {
			volume.ConfigMap.Name = config.Name
		}
		if volume.Name == "tls-public" {
			volume.Secret.SecretName = identitySecretName("orderer0", secretKindTLS)
		}
	}
	for i := range container.Env {
		env := &container.Env[i]
		switch env.Name {
		case "FABRICOPS_PROOF_CHANNEL":
			env.Value = channel.Name
		case "FABRICOPS_PROOF_BLOCK":
			env.Value = channelOutputDir + "/" + channelBlockFileName(channel.Name)
		case "PROOF_PROFILE":
			env.Value = channelProfileName(channel.Name)
		case "CORE_PEER_TLS_ROOTCERT_FILE":
			env.Value = "/proof/peer-tls/ca.crt"
		}
	}
	mode := int32(0440)
	pod.Spec.Volumes = append(pod.Spec.Volumes,
		corev1.Volume{Name: "admin-tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: identitySecretName(adminIdentityName(org), secretKindTLS), Items: adminTLSSecretItems(), DefaultMode: &mode}}},
		corev1.Volume{Name: "peer-ca", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: identitySecretName("peer0", secretKindTLS), Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}, DefaultMode: &mode}}},
		corev1.Volume{Name: "package", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: chaincodePackageConfigMapName(chaincode, org)}}}},
	)
	container.VolumeMounts = append(container.VolumeMounts,
		corev1.VolumeMount{Name: "admin", MountPath: chaincodeAdminMSPPath, ReadOnly: true},
		corev1.VolumeMount{Name: "admin-tls", MountPath: chaincodeAdminTLSPath, ReadOnly: true},
		corev1.VolumeMount{Name: "peer-ca", MountPath: "/proof/peer-tls", ReadOnly: true},
		corev1.VolumeMount{Name: "peer-ca", MountPath: chaincodePeerTLSPath(org, "peer0"), ReadOnly: true},
		corev1.VolumeMount{Name: "tls-public", MountPath: chaincodeOrdererTLSPath("orderer0"), ReadOnly: true},
		corev1.VolumeMount{Name: "package", MountPath: chaincodePackageInputDir, ReadOnly: true},
		corev1.VolumeMount{Name: "output", MountPath: channelBlockDir},
		corev1.VolumeMount{Name: "output", MountPath: chaincodeOutputDir},
	)
	container.TerminationMessagePath = channelOutputDir + "/receipt.json"
	container.TerminationMessagePolicy = corev1.TerminationMessageFallbackToLogsOnError
	container.Env = append(container.Env,
		corev1.EnvVar{Name: "FABRICOPS_PROOF_MODE", Value: "application"},
		corev1.EnvVar{Name: "FABRICOPS_PROOF_RECEIPT", Value: container.TerminationMessagePath},
		corev1.EnvVar{Name: "FABRICOPS_PROOF_ORDERER", Value: serviceDNS("orderer0", namespace, ordererPort)},
		corev1.EnvVar{Name: "FABRICOPS_PROOF_ORDERER_CA", Value: chaincodeOrdererTLSPath("orderer0") + "/ca.crt"},
	)
	orderer := ordererInstance{org: org, group: org.Orderers[0], name: "orderer0", namespace: namespace}
	peer := chaincodeLifecyclePeer{org: org, peerName: "peer0", namespace: namespace}
	subprocess := func(script string) string { return "\n(\n" + script + "\n)\n" }
	script := `set -eu
umask 077
configtxgen -configPath "$PROOF_CONFIG" -profile "$PROOF_PROFILE" -channelID "$FABRICOPS_PROOF_CHANNEL" -outputBlock "$FABRICOPS_PROOF_BLOCK"
`
	script += subprocess(joinOrdererScript(channel.Name, ordererAdminAddress(orderer), chaincodeAdminTLSPath))
	script += fmt.Sprintf(`n=0
until peer channel fetch config %q -c %q -o %q --tls --cafile %q; do
  n=$((n + 1))
  if [ "$n" -ge 30 ]; then echo 'Raft leader did not serve channel config'; exit 1; fi
  sleep 1
done
`, channelOutputDir+"/leader-config.pb", channel.Name, ordererClientAddress(orderer), chaincodeOrdererTLSPath("orderer0")+"/ca.crt")
	script += "peer channel join -b \"$FABRICOPS_PROOF_BLOCK\"\n"
	script += subprocess(installChaincodePackageScript(org, "peer0", namespace, true))
	script += fmt.Sprintf("test \"$(cat %s/%s)\" = %q\n", chaincodeOutputDir, chaincodePackageIDFile, packageID)
	script += subprocess(approveChaincodeDefinitionScript(net, channel, chaincode, org, "peer0", namespace, packageID, orderer))
	script += subprocess(commitChaincodeDefinitionScript(net, channel, chaincode, peer, orderer, []chaincodeLifecyclePeer{peer}))
	script += "exec fabricops-pkcs11-endorsement-proof"
	container.Command = []string{"sh", "-ec", script}
	return config, pod, nil
}

func testPKCS11KindApplication(t *testing.T, ctx context.Context, c client.Client, kube kubernetes.Interface, r *FabricNetworkReconciler, net *fabricopsv1alpha1.FabricNetwork, org fabricopsv1alpha1.Org, namespace string, sensitive []string) *corev1.ConfigMap {
	t.Helper()
	if err := r.ensureEnrollmentRBAC(ctx, net, org, namespace); err != nil {
		t.Fatal(err)
	}
	job := buildWorkloadEnrollmentJob(net, org, namespace, "orderer0", componentOrderer, ordererWorkloadCSRHosts(org.Orderers[0], "orderer0", namespace))
	job.Spec.BackoffLimit = new(int32)
	actor := &job.Spec.Template.Spec.InitContainers[0]
	actor.Image = "fabricops-pkcs11-kind:smoke"
	script := actor.Command[len(actor.Command)-1]
	actor.Command = []string{"sh", "-ec", "mkdir -p " + enrollmentWorkDir + "\n(\n" + script + "\n) > " + enrollmentWorkDir + "/orderer.log 2>&1 || { echo 'Proof orderer enrollment failed'; exit 1; }"}
	for i := range job.Spec.Template.Spec.Volumes {
		if volume := &job.Spec.Template.Spec.Volumes[i]; volume.Name == enrollmentVolumeName {
			volume.EmptyDir.Medium = corev1.StorageMediumMemory
		}
	}
	if err := c.Create(ctx, job); err != nil {
		t.Fatal(err)
	}
	waitPKCS11Kind(t, ctx, "software orderer enrollment", 4*time.Minute, func() (bool, error) {
		var current batchv1.Job
		if err := c.Get(ctx, client.ObjectKeyFromObject(job), &current); err != nil {
			return false, err
		}
		if jobFailed(current) {
			return false, fmt.Errorf("proof orderer enrollment failed")
		}
		return jobSucceeded(current), nil
	})
	pvc, err := buildDataPVC(net, org, namespace, "orderer0", componentOrderer)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, pvc); err != nil {
		t.Fatal(err)
	}
	deployment := buildOrdererDeployment(net, org, org.Orderers[0], 0, namespace)
	deployment.Spec.Template.Spec.AutomountServiceAccountToken = new(bool)
	deployment.Spec.Template.Spec.Containers[0].Image = "fabricops-pkcs11-kind:smoke"
	deployment.Spec.Template.Spec.Containers[0].Command = []string{"orderer"}
	deployment.Spec.Template.Spec.Containers[0].Env = append(deployment.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: "FABRIC_LOGGING_SPEC", Value: "warning"})
	for _, object := range []client.Object{deployment, buildOrdererService(net, org, org.Orderers[0], 0, namespace)} {
		if err := c.Create(ctx, object); err != nil {
			t.Fatal(err)
		}
	}
	waitPKCS11Kind(t, ctx, "real TLS Raft orderer readiness", 3*time.Minute, func() (bool, error) {
		var current appsv1.Deployment
		if err := c.Get(ctx, client.ObjectKeyFromObject(deployment), &current); err != nil {
			return false, err
		}
		return current.Status.ReadyReplicas == 1, nil
	})
	_, chaincode := pkcs11ApplicationDefinition(org)
	pack, err := buildChaincodePackageConfigMap(net, chaincode, org)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(pack.BinaryData[pack.Data[chaincodePackageFileKey]])
	packageID := pack.Data[chaincodePackageLabelKey] + ":" + hex.EncodeToString(hash[:])
	cc := buildChaincodeDeployment(net, chaincode, org, "peer0", packageID)
	cc.Spec.Template.Spec.AutomountServiceAccountToken = new(bool)
	for _, object := range []client.Object{pack, cc, buildChaincodeService(net, chaincode, org, "peer0")} {
		if err := c.Create(ctx, object); err != nil {
			t.Fatal(err)
		}
	}
	waitPKCS11Kind(t, ctx, "Go CCaaS service readiness", 3*time.Minute, func() (bool, error) {
		var current appsv1.Deployment
		if err := c.Get(ctx, client.ObjectKeyFromObject(cc), &current); err != nil {
			return false, err
		}
		return current.Status.ReadyReplicas == 1, nil
	})
	config, pod, err := buildPKCS11ApplicationClient(net, org, namespace, packageID)
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range []client.Object{config, pod} {
		if err := c.Create(ctx, object); err != nil {
			t.Fatal(err)
		}
	}
	completed := waitPKCS11ApplicationClient(t, ctx, c, pod)
	var receipt map[string]string
	if len(completed.Status.ContainerStatuses) != 1 || completed.Status.ContainerStatuses[0].State.Terminated == nil {
		t.Fatal("application proof has no termination receipt")
	}
	message := completed.Status.ContainerStatuses[0].State.Terminated.Message
	if json.Unmarshal([]byte(message), &receipt) != nil || !strings.HasPrefix(receipt["id"], "proof-") || len(receipt["txID"]) != 64 || len(receipt["envelopeHash"]) != 64 {
		t.Fatal("application proof has no valid public transaction receipt")
	}
	var peers corev1.PodList
	if err := c.List(ctx, &peers, client.InNamespace(namespace), client.MatchingLabels{labelComponent: componentPeer}); err != nil {
		t.Fatal(err)
	}
	if len(peers.Items) != 1 {
		t.Fatal("expected one provider-backed peer")
	}
	result := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "application-receipt", Namespace: namespace, Annotations: map[string]string{"proof-peer-uid": string(peers.Items[0].UID)}}, Data: map[string]string{"receipt.json": message}}
	if err := c.Create(ctx, result); err != nil {
		t.Fatal(err)
	}
	scanPKCS11KindLogs(t, ctx, kube, namespace, sensitive, false)
	return result
}

func waitPKCS11ApplicationClient(t *testing.T, ctx context.Context, c client.Client, pod *corev1.Pod) *corev1.Pod {
	t.Helper()
	var current corev1.Pod
	waitPKCS11Kind(t, ctx, pod.Name+" verifies application endorsement, VALID commit and state", 5*time.Minute, func() (bool, error) {
		if err := c.Get(ctx, client.ObjectKeyFromObject(pod), &current); err != nil {
			return false, err
		}
		if current.Status.Phase == corev1.PodFailed {
			return false, fmt.Errorf("application proof client failed")
		}
		return current.Status.Phase == corev1.PodSucceeded, nil
	})
	return &current
}

func testPKCS11KindApplicationRestart(t *testing.T, ctx context.Context, c client.Client, kube kubernetes.Interface, net *fabricopsv1alpha1.FabricNetwork, org fabricopsv1alpha1.Org, namespace string, receipt *corev1.ConfigMap, sensitive []string) {
	t.Helper()
	var peers corev1.PodList
	if err := c.List(ctx, &peers, client.InNamespace(namespace), client.MatchingLabels{labelComponent: componentPeer}); err != nil {
		t.Fatal(err)
	}
	if len(peers.Items) != 1 || string(peers.Items[0].UID) == receipt.Annotations["proof-peer-uid"] {
		t.Fatal("peer was not replaced before persisted application proof")
	}
	_, pod, err := buildPKCS11ApplicationClient(net, org, namespace, "unused-for-read")
	if err != nil {
		t.Fatal(err)
	}
	pod.Name = "application-read-after-restart"
	container := &pod.Spec.Containers[0]
	container.Command = []string{"fabricops-pkcs11-endorsement-proof"}
	container.TerminationMessagePath = "/dev/termination-log"
	for i := range container.Env {
		if container.Env[i].Name == "FABRICOPS_PROOF_MODE" {
			container.Env[i].Value = "application-read"
		}
		if container.Env[i].Name == "FABRICOPS_PROOF_RECEIPT" {
			container.Env[i].Value = "/proof/receipt/receipt.json"
		}
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: "receipt", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: receipt.Name}}}})
	container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: "receipt", MountPath: "/proof/receipt", ReadOnly: true})
	if err := c.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	waitPKCS11ApplicationClient(t, ctx, c, pod)
	scanPKCS11KindLogs(t, ctx, kube, namespace, sensitive, false)
}
