//go:build integration

// Copyright contributors to FabricOps. SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func pkcs11KindClients(t *testing.T, cluster string) (client.Client, kubernetes.Interface, *runtime.Scheme) {
	t.Helper()
	if !strings.HasPrefix(cluster, "fabricops-pkcs11-proof-") || os.Getenv("KUBECONFIG") == "" {
		t.Fatal("refusing a non-proof cluster or implicit kubeconfig")
	}
	loading := clientcmd.NewDefaultClientConfigLoadingRules()
	raw, err := loading.Load()
	if err != nil {
		t.Fatal(err)
	}
	if raw.CurrentContext != "kind-"+cluster {
		t.Fatal("refusing unexpected Kubernetes context")
	}
	cfg, err := clientcmd.NewNonInteractiveClientConfig(*raw, raw.CurrentContext, &clientcmd.ConfigOverrides{}, loading).ClientConfig()
	if err != nil {
		t.Fatal(err)
	}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := fabricopsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c, kube, scheme
}

func scanPKCS11KindLogs(t *testing.T, ctx context.Context, kube kubernetes.Interface, namespace string, sensitive []string, diagnostic bool) {
	t.Helper()
	pods, err := kube.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		if !diagnostic {
			t.Fatal(err)
		}
		return
	}
	for _, pod := range pods.Items {
		if diagnostic {
			t.Logf("Pod %s: %s", pod.Name, pod.Status.Phase)
		}
		for _, container := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
			logs, err := kube.CoreV1().Pods(namespace).GetLogs(pod.Name, &corev1.PodLogOptions{Container: container.Name}).DoRaw(ctx)
			if err != nil {
				if !diagnostic {
					t.Fatal("Pod log retrieval failed:", err)
				}
				continue
			}
			text := string(logs)
			for _, secret := range sensitive {
				if secret != "" && strings.Contains(text, secret) {
					if !diagnostic {
						t.Fatal("credential appeared in Pod logs")
					}
					text = strings.ReplaceAll(text, secret, "[REDACTED]")
				}
			}
			if strings.Contains(text, "PRIVATE KEY-----") {
				if !diagnostic {
					t.Fatal("private key appeared in Pod logs")
				}
				continue
			}
			if diagnostic {
				lines := strings.Split(text, "\n")
				if len(lines) > 30 {
					lines = lines[len(lines)-30:]
				}
				t.Log(strings.Join(lines, "\n"))
			}
		}
	}
}

func waitPKCS11Kind(t *testing.T, ctx context.Context, name string, limit time.Duration, check func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		ok, err := check()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if ok {
			t.Log("PASS:", name)
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatal("timed out:", name)
}

func pkcs11KindPodReady(ctx context.Context, c client.Client, namespace, name string) (bool, error) {
	var pod corev1.Pod
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &pod); err != nil {
		return false, err
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true, nil
		}
	}
	return false, nil
}

func TestPKCS11KindRuntimeProof(t *testing.T) {
	cluster := os.Getenv("FABRICOPS_PKCS11_KIND_CLUSTER")
	if cluster == "" {
		t.Skip("run make test-pkcs11-kind for an isolated runtime proof")
	}
	logf.SetLogger(zap.New())
	c, kube, scheme := pkcs11KindClients(t, cluster)
	ctx, cancel := context.WithTimeout(context.Background(), 22*time.Minute)
	defer cancel()
	wait := func(name string, limit time.Duration, check func() (bool, error)) {
		waitPKCS11Kind(t, ctx, name, limit, check)
	}
	create := func(object client.Object) {
		t.Helper()
		if err := c.Create(ctx, object); err != nil {
			t.Fatal(err)
		}
	}
	net := &fabricopsv1alpha1.FabricNetwork{ObjectMeta: metav1.ObjectMeta{Name: "pkcs-kind", Namespace: "default"}}
	org := pkcs11RuntimeOrgFixture(t)
	org.CA.DB, org.Peer.Instances, org.Peer.DB = "sqlite", 1, "LevelDB"
	rt := org.Peer.MSPSigning.PKCS11.Runtime
	rt.StateClaims = rt.StateClaims[:1]
	rt.EnrollmentImage, rt.PeerImage = "fabricops-pkcs11-kind:enrollment", "fabricops-pkcs11-kind:peer"
	net.Spec = fabricopsv1alpha1.FabricNetworkSpec{Global: fabricopsv1alpha1.GlobalConfig{FabricVersion: "3.1.0", TLS: true}, Orgs: []fabricopsv1alpha1.Org{org}, Channels: []fabricopsv1alpha1.Channel{}, Chaincodes: []fabricopsv1alpha1.Chaincode{}}
	create(net)
	if !strings.Contains(strings.Join(validateFabricNetworkTopology(net), ";"), "PKCS11 is unavailable") {
		t.Fatal("runtime proof bypassed the public gate")
	}
	ns := buildOrgNamespace(net, org)
	create(ns)
	r := &FabricNetworkReconciler{Client: c, Scheme: scheme}
	var sensitive []string
	scrub := func(value string) string {
		for _, secret := range sensitive {
			if secret != "" {
				value = strings.ReplaceAll(value, secret, "[REDACTED]")
			}
		}
		return value
	}
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		diag, done := context.WithTimeout(context.Background(), 30*time.Second)
		defer done()
		scanPKCS11KindLogs(t, diag, kube, ns.Name, sensitive, true)
	})
	if err := r.reconcileIdentityMaterial(ctx, net, org, ns.Name); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{caBootstrapSecretName(org), identityEnrollmentSecretName("peer0"), adminEnrollmentSecretName(org)} {
		var secret corev1.Secret
		if err := c.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: name}, &secret); err != nil {
			t.Fatal(err)
		}
		for key, value := range secret.Data {
			if key != caBootstrapUsernameKey {
				sensitive = append(sensitive, string(value))
			}
		}
	}
	randomSecret := func() string {
		bytes := make([]byte, 24)
		if _, err := rand.Read(bytes); err != nil {
			t.Fatal(err)
		}
		return hex.EncodeToString(bytes)
	}
	pin, soPIN := randomSecret(), randomSecret()
	sensitive = append(sensitive, pin, soPIN)
	create(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "token-pin", Namespace: ns.Name}, StringData: map[string]string{"pin": pin, "so-pin": soPIN}})
	create(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "provider-config", Namespace: ns.Name}, StringData: map[string]string{"softhsm2.conf": "directories.tokendir = /var/lib/fabricops-hsm/tokens\nobjectstore.backend = file\nlog.level = ERROR\n"}})
	caContainer := corev1.Container{
		Name: "ca", Image: "fabricops-pkcs11-kind:smoke",
		Command: []string{"sh", "-ec", `exec fabric-ca-server start --loglevel warning -b "$BOOTSTRAP" --home /ca`},
		Env: []corev1.EnvVar{{Name: "BOOTSTRAP", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: caBootstrapSecretName(org)}, Key: caBootstrapUserPassKey,
		}}}},
		ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/cainfo", Port: intstr.FromInt32(caPort)}}},
		VolumeMounts:   []corev1.VolumeMount{{Name: "ca", MountPath: "/ca"}},
	}
	ca := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-ca", Namespace: ns.Name, Labels: orgLabels(net, org, componentCA)},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{caContainer}, Volumes: []corev1.Volume{{Name: "ca", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}}},
	}
	create(ca)
	create(buildCAService(net, org, ns.Name))
	readyPod := func(name string) (bool, error) {
		return pkcs11KindPodReady(ctx, c, ns.Name, name)
	}
	wait("test CA readiness", 2*time.Minute, func() (bool, error) { return readyPod(ca.Name) })
	claim := preparedPKCS11Claim(ns.Name, net.UID)
	claim.UID, claim.Spec.VolumeName, claim.Status = "", "", corev1.PersistentVolumeClaimStatus{}
	create(claim)
	prepSpec := buildWorkloadEnrollmentJob(net, org, ns.Name, "peer0", componentPeer, nil).Spec.Template.Spec
	var tokenVolumes []corev1.Volume
	for _, volume := range prepSpec.Volumes {
		if volume.Name == pkcs11ConfigVolume || volume.Name == pkcs11StateVolume {
			tokenVolumes = append(tokenVolumes, volume)
		}
	}
	// External preparation is intentionally separate from the unchanged staged
	// enrollment template: initialize the empty test token, then release its PVC.
	prep := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "prepare-token", Namespace: ns.Name}, Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, Volumes: tokenVolumes, Containers: []corev1.Container{{Name: "prepare", Image: "fabricops-pkcs11-kind:smoke", Command: []string{"sh", "-ec", `mkdir -p /var/lib/fabricops-hsm/tokens
softhsm2-util --init-token --free --label peers --pin "$PIN" --so-pin "$SO_PIN" >/dev/null 2>&1
chown -R 1001:1001 /var/lib/fabricops-hsm/tokens`}, Env: []corev1.EnvVar{{Name: "SOFTHSM2_CONF", Value: pkcs11ConfigPath + "/config"}, {Name: "PIN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: org.Peer.MSPSigning.PKCS11.PINSecretKeyRef.DeepCopy()}}, {Name: "SO_PIN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "token-pin"}, Key: "so-pin"}}}}, VolumeMounts: []corev1.VolumeMount{{Name: pkcs11ConfigVolume, MountPath: pkcs11ConfigPath, ReadOnly: true}, {Name: pkcs11StateVolume, MountPath: pkcs11StatePath}}}}}}
	create(prep)
	wait("external token preparation", 2*time.Minute, func() (bool, error) {
		var pod corev1.Pod
		if err := c.Get(ctx, client.ObjectKeyFromObject(prep), &pod); err != nil {
			return false, err
		}
		if pod.Status.Phase == corev1.PodFailed {
			return false, fmt.Errorf("token preparation failed")
		}
		return pod.Status.Phase == corev1.PodSucceeded, nil
	})
	if err := c.Delete(ctx, prep, client.GracePeriodSeconds(0)); err != nil {
		t.Fatal(err)
	}
	wait("preparation Pod release", time.Minute, func() (bool, error) {
		err := c.Get(ctx, client.ObjectKeyFromObject(prep), &corev1.Pod{})
		return apierrors.IsNotFound(err), client.IgnoreNotFound(err)
	})
	if _, err := r.reconcilePeers(ctx, net, org, ns.Name); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: "peer0"}, &appsv1.Deployment{}); !apierrors.IsNotFound(err) {
		t.Fatal("peer started before enrollment")
	}
	if err := r.reconcileWorkloadEnrollment(ctx, net, org, ns.Name, "peer0", componentPeer, peerWorkloadCSRHosts(org, "peer0", ns.Name)); err != nil {
		t.Fatal(err)
	}
	wait("non-root enrollment and scoped publication", 4*time.Minute, func() (bool, error) {
		var job batchv1.Job
		if err := c.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: workloadEnrollmentJobName("peer0")}, &job); err != nil {
			return false, err
		}
		if jobFailed(job) {
			return false, fmt.Errorf("enrollment Job failed")
		}
		return jobSucceeded(job), nil
	})
	var msp corev1.Secret
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: "peer0-msp"}, &msp); err != nil {
		t.Fatal(err)
	}
	if problem := identitySecretValidationError(msp, secretKindPeerPKCS11MSP, true); problem != "" {
		t.Fatal("invalid public MSP:", problem)
	}
	scanPKCS11KindLogs(t, ctx, kube, ns.Name, sensitive, false)
	wait("durable handoff and enrollment Pod disappearance", 2*time.Minute, func() (bool, error) { return r.pkcs11PeerEnrollmentReady(ctx, net, org, ns.Name, "peer0") })
	// Sign a fresh challenge using the token object selected by the certificate's
	// Fabric SKI, then verify with that exact published certificate on the host.
	proof := buildWorkloadEnrollmentJob(net, org, ns.Name, "peer0", componentPeer, nil).Spec.Template.Spec
	actor := proof.InitContainers[0]
	actor.Name, actor.Image, actor.Command = "inspect", "fabricops-pkcs11-kind:smoke", []string{"sleep", "600"}
	actor.Env = append(actor.Env, corev1.EnvVar{Name: "PIN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: org.Peer.MSPSigning.PKCS11.PINSecretKeyRef.DeepCopy()}})
	proof.InitContainers, proof.Containers, proof.ServiceAccountName = nil, []corev1.Container{actor}, ""
	create(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "inspect-token", Namespace: ns.Name}, Spec: proof})
	wait("non-root token inspection", time.Minute, func() (bool, error) { return readyPod("inspect-token") })
	block, _ := pem.Decode(msp.Data[mspSignCertKey])
	if block == nil {
		t.Fatal("published certificate is not PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	pub, ok := cert.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		t.Fatal("expected an ECDSA certificate")
	}
	encodedKey, err := pub.ECDH()
	if err != nil {
		t.Fatal(err)
	}
	ski := sha256.Sum256(encodedKey.Bytes())
	challenge := make([]byte, sha256.Size)
	if _, err := rand.Read(challenge); err != nil {
		t.Fatal(err)
	}
	command := fmt.Sprintf(`umask 077; printf '%%s' '%s' | base64 -d > /run/fabricops/challenge
pkcs11-tool --module "$FABRICOPS_PKCS11_LIBRARY" --token-label peers --login --pin "$PIN" --list-objects --type privkey > /run/fabricops/attributes 2>&1
grep -q 'never extractable' /run/fabricops/attributes; grep -q 'sensitive' /run/fabricops/attributes
pkcs11-tool --module "$FABRICOPS_PKCS11_LIBRARY" --token-label peers --login --pin "$PIN" --sign --mechanism ECDSA --id '%s' --input-file /run/fabricops/challenge --output-file /run/fabricops/signature > /run/fabricops/sign.log 2>&1
base64 /run/fabricops/signature`, base64.StdEncoding.EncodeToString(challenge), hex.EncodeToString(ski[:]))
	output, err := exec.CommandContext(ctx, "kubectl", "--context", "kind-"+cluster, "-n", ns.Name, "exec", "inspect-token", "--", "sh", "-ec", command).CombinedOutput()
	if err != nil {
		t.Fatal("token challenge failed:", scrub(string(output)))
	}
	signature, err := base64.StdEncoding.DecodeString(strings.Join(strings.Fields(string(output)), ""))
	if err != nil || len(signature) != 64 || !ecdsa.Verify(pub, challenge, new(big.Int).SetBytes(signature[:len(signature)/2]), new(big.Int).SetBytes(signature[len(signature)/2:])) {
		t.Fatal("token signature does not match published certificate")
	}
	t.Log("PASS: certificate-bound token signature; sensitive, never-extractable key")
	if err := c.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "inspect-token", Namespace: ns.Name}}, client.GracePeriodSeconds(0)); err != nil {
		t.Fatal(err)
	}
	wait("inspection Pod release", time.Minute, func() (bool, error) {
		err := c.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: "inspect-token"}, &corev1.Pod{})
		return apierrors.IsNotFound(err), client.IgnoreNotFound(err)
	})
	if _, err := r.reconcilePeers(ctx, net, org, ns.Name); err != nil {
		t.Fatal(err)
	}
	wait("TLS PKCS11 peer readiness", 3*time.Minute, func() (bool, error) {
		var deployment appsv1.Deployment
		if err := c.Get(ctx, client.ObjectKey{Namespace: ns.Name, Name: "peer0"}, &deployment); err != nil {
			return false, err
		}
		return deployment.Status.ReadyReplicas == 1, nil
	})
	var peers corev1.PodList
	if err := c.List(ctx, &peers, client.InNamespace(ns.Name), client.MatchingLabels{labelComponent: componentPeer}); err != nil {
		t.Fatal(err)
	}
	var peerPod corev1.Pod
	for _, pod := range peers.Items {
		if strings.HasPrefix(pod.Name, "peer0-") && pod.Status.Phase == corev1.PodRunning {
			peerPod = pod
		}
	}
	if peerPod.UID == "" {
		t.Fatal("running peer Pod not found")
	}
	if err := c.Delete(ctx, &peerPod); err != nil {
		t.Fatal(err)
	}
	wait("peer restart using the persisted token", 3*time.Minute, func() (bool, error) {
		var pods corev1.PodList
		if err := c.List(ctx, &pods, client.InNamespace(ns.Name)); err != nil {
			return false, err
		}
		for _, pod := range pods.Items {
			if pod.UID != peerPod.UID && strings.HasPrefix(pod.Name, "peer0-") {
				return readyPod(pod.Name)
			}
		}
		return false, nil
	})
	scanPKCS11KindLogs(t, ctx, kube, ns.Name, sensitive, false)
	testPKCS11KindEndorsement(t, ctx, c, kube, r, net, org, ns.Name, sensitive)
	testPKCS11KindFailures(t, ctx, c, kube, net, org, ns.Name, sensitive)
	testPKCS11KindReadiness(t, ctx, c, kube, ns.Name)
	scanPKCS11KindLogs(t, ctx, kube, ns.Name, sensitive, false)
	t.Log("PASS: staged Kubernetes templates work; the public PKCS11 backend remains disabled")
}
