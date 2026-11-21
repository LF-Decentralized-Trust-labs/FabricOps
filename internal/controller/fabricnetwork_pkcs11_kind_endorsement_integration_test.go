//go:build integration

// Copyright contributors to FabricOps. SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func buildPKCS11EndorsementClient(net *fabricopsv1alpha1.FabricNetwork, org fabricopsv1alpha1.Org, namespace string) (*corev1.ConfigMap, *corev1.Pod, error) {
	// The offline genesis fixture has a placeholder consenter, not a running
	// orderer. Reuse real enrolled CA/TLS material; no consensus claim is made.
	fixture := net.DeepCopy()
	fixture.Spec.Orgs[0].Orderers = []fabricopsv1alpha1.OrdererGroup{{GroupName: "proof", Type: "raft", Instances: 1, Prefix: "orderer"}}
	channel := fabricopsv1alpha1.Channel{Name: "custody-proof", Orgs: []fabricopsv1alpha1.ChannelOrg{{Name: org.Organization.Name, Peers: []string{"peer0"}}}}
	config, err := buildChannelConfigMap(fixture, channel, org, namespace)
	if err != nil {
		return nil, nil, err
	}
	no, yes := false, true
	uid, mode := int64(1001), int32(0440)
	work := resource.MustParse("64Mi")
	peerTLS := channelOrdererTLSPath("orderer0")
	block := channelOutputDir + "/" + channelBlockFileName(channel.Name)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "endorsement-client", Namespace: namespace},
		Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, AutomountServiceAccountToken: &no,
			SecurityContext: &corev1.PodSecurityContext{RunAsUser: &uid, RunAsGroup: &uid, FSGroup: &uid, RunAsNonRoot: &yes},
			Volumes: []corev1.Volume{
				{Name: "admin", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: identitySecretName(adminIdentityName(org), secretKindMSP), Items: mspSecretItems(true), DefaultMode: &mode}}},
				{Name: "peer-public", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: identitySecretName("peer0", secretKindMSP), Items: []corev1.KeyToPath{{Key: mspSignCertKey, Path: "cert.pem"}}, DefaultMode: &mode}}},
				{Name: "tls-public", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: identitySecretName("peer0", secretKindTLS), Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}, {Key: "server.crt", Path: "server.crt"}}, DefaultMode: &mode}}},
				{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: config.Name}}}},
				{Name: "output", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: &work}}},
			},
			Containers: []corev1.Container{{Name: "client", Image: "fabricops-pkcs11-kind:smoke",
				Command: []string{"sh", "-ec", `umask 077
configtxgen -configPath "$PROOF_CONFIG" -profile "$PROOF_PROFILE" -channelID "$FABRICOPS_PROOF_CHANNEL" -outputBlock "$FABRICOPS_PROOF_BLOCK"
peer channel join -b "$FABRICOPS_PROOF_BLOCK"
exec fabricops-pkcs11-endorsement-proof`},
				Env: []corev1.EnvVar{
					{Name: "CORE_PEER_MSPCONFIGPATH", Value: channelOrgMSPPath(org)}, {Name: "CORE_PEER_LOCALMSPID", Value: org.Organization.MSPName},
					{Name: "CORE_PEER_ADDRESS", Value: serviceDNS("peer0", namespace, peerPort)}, {Name: "CORE_PEER_TLS_ENABLED", Value: "true"},
					{Name: "CORE_PEER_TLS_ROOTCERT_FILE", Value: peerTLS + "/ca.crt"}, {Name: "CORE_PEER_BCCSP_DEFAULT", Value: "SW"},
					{Name: "FABRIC_LOGGING_SPEC", Value: "warning"}, {Name: "FABRICOPS_PROOF_PEER_CERT", Value: "/proof/peer/cert.pem"},
					{Name: "FABRICOPS_PROOF_CHANNEL", Value: channel.Name}, {Name: "FABRICOPS_PROOF_BLOCK", Value: block},
					{Name: "PROOF_CONFIG", Value: channelConfigDir}, {Name: "PROOF_PROFILE", Value: channelProfileName(channel.Name)},
				},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "admin", MountPath: channelOrgMSPPath(org), ReadOnly: true}, {Name: "peer-public", MountPath: "/proof/peer", ReadOnly: true},
					{Name: "tls-public", MountPath: peerTLS, ReadOnly: true}, {Name: "config", MountPath: channelConfigDir, ReadOnly: true}, {Name: "output", MountPath: channelOutputDir},
				},
			}},
		},
	}
	return config, pod, nil
}

func testPKCS11KindEndorsement(t *testing.T, ctx context.Context, c client.Client, kube kubernetes.Interface, r *FabricNetworkReconciler, net *fabricopsv1alpha1.FabricNetwork, org fabricopsv1alpha1.Org, namespace string, sensitive []string) {
	t.Helper()
	if err := r.ensureEnrollmentRBAC(ctx, net, org, namespace); err != nil {
		t.Fatal(err)
	}
	job := buildAdminEnrollmentJob(net, org, namespace)
	job.Spec.BackoffLimit = new(int32)
	// Use the production software-admin scripts with the pinned test image.
	// Capture CA registration diagnostics in tmpfs, never Pod logs.
	job.Spec.Template.Spec.InitContainers[0].Image = "fabricops-pkcs11-kind:smoke"
	job.Spec.Template.Spec.InitContainers[0].Command = []string{"sh", "-ec", "mkdir -p " + enrollmentWorkDir + "\n(\n" + adminEnrollmentScript() + "\n) > " + enrollmentWorkDir + "/admin.log 2>&1 || { echo 'Proof admin enrollment failed'; exit 1; }"}
	for i := range job.Spec.Template.Spec.Volumes {
		volume := &job.Spec.Template.Spec.Volumes[i]
		if volume.Name == enrollmentVolumeName {
			volume.EmptyDir.Medium = corev1.StorageMediumMemory
		}
	}
	if err := c.Create(ctx, job); err != nil {
		t.Fatal(err)
	}
	waitPKCS11Kind(t, ctx, "software admin enrollment for isolated channel proof", 4*time.Minute, func() (bool, error) {
		var current batchv1.Job
		if err := c.Get(ctx, client.ObjectKeyFromObject(job), &current); err != nil {
			return false, err
		}
		if jobFailed(current) {
			return false, fmt.Errorf("proof admin enrollment failed")
		}
		return jobSucceeded(current), nil
	})
	config, pod, err := buildPKCS11EndorsementClient(net, org, namespace)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, config); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	waitPKCS11Kind(t, ctx, "channel query endorsement matches the published provider-backed peer identity", 3*time.Minute, func() (bool, error) {
		var current corev1.Pod
		if err := c.Get(ctx, client.ObjectKeyFromObject(pod), &current); err != nil {
			return false, err
		}
		if current.Status.Phase == corev1.PodFailed {
			return false, fmt.Errorf("isolated channel endorsement client failed")
		}
		return current.Status.Phase == corev1.PodSucceeded, nil
	})
	logs, err := kube.CoreV1().Pods(namespace).GetLogs(pod.Name, &corev1.PodLogOptions{Container: "client"}).DoRaw(ctx)
	if err != nil || !strings.Contains(string(logs), "PASS: two fresh channel queries have certificate-bound peer endorsements") {
		t.Fatal("client success lacks endorsement evidence")
	}
	scanPKCS11KindLogs(t, ctx, kube, namespace, sensitive, false)
}

func TestPKCS11EndorsementClientIsolation(t *testing.T) {
	net, _, _, _, _ := channelConfigOperationFixture()
	org := pkcs11RuntimeOrgFixture(t)
	net.Spec.Orgs = []fabricopsv1alpha1.Org{org}
	_, pod, err := buildPKCS11EndorsementClient(net, org, "proof")
	if err != nil {
		t.Fatal(err)
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Fatal("client can acquire an API token")
	}
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil || (volume.Secret != nil && (volume.Secret.SecretName == "token-pin" || volume.Secret.SecretName == "provider-config")) {
			t.Fatal("client can access provider credentials or token state")
		}
	}
	for _, env := range pod.Spec.Containers[0].Env {
		if env.Name == "FABRICOPS_PROOF_BLOCK" && !strings.HasPrefix(env.Value, channelOutputDir+"/") {
			t.Fatal("genesis block is outside the client's writable output mount")
		}
		if strings.HasPrefix(env.Name, "FABRICOPS_PKCS11_") {
			t.Fatal("client received provider inputs")
		}
	}
}
