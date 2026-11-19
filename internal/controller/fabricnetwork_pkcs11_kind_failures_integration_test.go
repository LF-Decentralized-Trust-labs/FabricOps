//go:build integration

// Copyright contributors to FabricOps. SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func waitPKCS11PeerFailure(t *testing.T, ctx context.Context, kube kubernetes.Interface, pod *corev1.Pod, pattern string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	seconds := int64(120)
	stream, err := kube.CoreV1().Pods(pod.Namespace).Watch(ctx, metav1.ListOptions{
		FieldSelector:   fields.OneTermEqualSelector("metadata.name", pod.Name).String(),
		ResourceVersion: pod.ResourceVersion, TimeoutSeconds: &seconds,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Stop()
	expected := regexp.MustCompile("(?i)" + pattern)
	for {
		select {
		case <-ctx.Done():
			t.Fatal("fault-injection context expired")
		case event, open := <-stream.ResultChan():
			if !open {
				t.Fatal("Pod watch ended before a provider-specific failure")
			}
			if event.Type == watch.Error {
				t.Fatal("Pod watch failed:", apierrors.FromObject(event.Object))
			}
			actual, ok := event.Object.(*corev1.Pod)
			if !ok {
				continue
			}
			if actual.UID != pod.UID {
				t.Fatal("fault Pod was replaced during observation")
			}
			var logs []byte
			if actual.Status.Phase == corev1.PodFailed {
				logs, err = kube.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{Container: containerPeer}).DoRaw(ctx)
				if err != nil {
					t.Fatal("failed peer logs unavailable:", err)
				}
			}
			complete, err := pkcs11PeerFailedClosed(*actual, logs, expected)
			if err != nil {
				t.Fatalf("%s: %v", pod.Name, err)
			}
			if complete {
				t.Log("PASS:", pod.Name, "exited nonzero with the expected error and never became ready")
				return
			}
		}
	}
}

func removePKCS11KindPod(t *testing.T, ctx context.Context, c client.Client, pod *corev1.Pod) {
	t.Helper()
	if err := c.Delete(ctx, pod); client.IgnoreNotFound(err) != nil {
		t.Fatal(err)
	}
	waitPKCS11Kind(t, ctx, "fault Pod release", time.Minute, func() (bool, error) {
		err := c.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{})
		return apierrors.IsNotFound(err), client.IgnoreNotFound(err)
	})
}

func testPKCS11KindFailures(t *testing.T, ctx context.Context, c client.Client, kube kubernetes.Interface, net *fabricopsv1alpha1.FabricNetwork, org fabricopsv1alpha1.Org, namespace string, sensitive []string) {
	t.Helper()
	create := func(object client.Object) {
		t.Helper()
		if err := c.Create(ctx, object); err != nil {
			t.Fatal(err)
		}
	}
	var deployment appsv1.Deployment
	key := client.ObjectKey{Namespace: namespace, Name: "peer0"}
	if err := c.Get(ctx, key, &deployment); err != nil {
		t.Fatal(err)
	}
	before := deployment.DeepCopy()
	zero := int32(0)
	deployment.Spec.Replicas = &zero
	if err := c.Patch(ctx, &deployment, client.MergeFrom(before)); err != nil {
		t.Fatal(err)
	}
	waitPKCS11Kind(t, ctx, "healthy peer releases token before fault injection", time.Minute, func() (bool, error) {
		var pods corev1.PodList
		if err := c.List(ctx, &pods, client.InNamespace(namespace), client.MatchingLabels(deployment.Spec.Selector.MatchLabels)); err != nil {
			return false, err
		}
		return len(pods.Items) == 0, nil
	})
	var originalNamespace corev1.Namespace
	if err := c.Get(ctx, client.ObjectKey{Name: namespace}, &originalNamespace); err != nil {
		t.Fatal(err)
	}
	var snapshots []corev1.Secret
	for _, name := range []string{"peer0-msp", "peer0-tls", "token-pin"} {
		var secret corev1.Secret
		if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &secret); err != nil {
			t.Fatal(err)
		}
		snapshots = append(snapshots, secret)
	}
	// Create a second, empty test token without changing the original token/key.
	base := buildPeerDeployment(net, org, 0, namespace).Spec.Template.Spec
	base.RestartPolicy = corev1.RestartPolicyNever
	prepare := base.DeepCopy()
	prepare.Containers[0].Name, prepare.Containers[0].Image = "prepare-empty", "fabricops-pkcs11-kind:smoke"
	prepare.Containers[0].Command = []string{"sh", "-ec", `softhsm2-util --init-token --free --label proof-empty-token --pin "$FABRICOPS_PKCS11_PIN" --so-pin "$PROOF_SO_PIN" >/dev/null 2>&1`}
	prepare.Containers[0].ReadinessProbe, prepare.Containers[0].LivenessProbe = nil, nil
	prepare.Containers[0].Env = append(prepare.Containers[0].Env, corev1.EnvVar{Name: "PROOF_SO_PIN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "token-pin"}, Key: "so-pin"}}})
	preparer := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "proof-prepare-empty", Namespace: namespace}, Spec: *prepare}
	create(preparer)
	waitPKCS11Kind(t, ctx, "empty test token preparation", time.Minute, func() (bool, error) {
		var pod corev1.Pod
		if err := c.Get(ctx, client.ObjectKeyFromObject(preparer), &pod); err != nil {
			return false, err
		}
		if pod.Status.Phase == corev1.PodFailed {
			return false, fmt.Errorf("empty test token initialization failed")
		}
		return pod.Status.Phase == corev1.PodSucceeded, nil
	})
	removePKCS11KindPod(t, ctx, c, preparer)
	create(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "proof-wrong-pin", Namespace: namespace}, StringData: map[string]string{"pin": "deliberately-invalid-proof-pin"}})
	sensitive = append(sensitive, "deliberately-invalid-proof-pin")
	authority, err := generateTestIdentityAuthority(org)
	if err != nil {
		t.Fatal(err)
	}
	mismatch, err := buildTestMSPSecret(net, org, namespace, "peer0", componentPeer, secretKindMSP, authority)
	if err != nil {
		t.Fatal(err)
	}
	delete(mismatch.Data, mspKeyStoreKey)
	mismatch.Name = "proof-mismatched-msp"
	if problem := identitySecretValidationError(*mismatch, secretKindPeerPKCS11MSP, net.Spec.Global.TLS); problem != "" {
		t.Fatal("invalid mismatch fixture:", problem)
	}
	create(mismatch)
	for _, fault := range []struct {
		name, env, value, msp, pattern string
		secret                         bool
	}{
		{name: "wrong-pin", env: "FABRICOPS_PKCS11_PIN", value: "proof-wrong-pin", secret: true, pattern: "CKR_PIN_INCORRECT|CKR_PIN_LOCKED"},
		{name: "missing-module", env: "FABRICOPS_PKCS11_LIBRARY", value: "/nonexistent/provider.so", pattern: "/nonexistent/provider\\.so"},
		{name: "missing-token", env: "FABRICOPS_PKCS11_TOKEN_LABEL", value: "proof-nonexistent-token", pattern: "could not find token with label"},
		{name: "missing-key", env: "FABRICOPS_PKCS11_TOKEN_LABEL", value: "proof-empty-token", pattern: "KeyMaterial not found|private key.*not found|key.*not found"},
		{name: "certificate-mismatch", msp: mismatch.Name, pattern: "KeyMaterial not found|private key.*not found|key.*not found"},
	} {
		spec := base.DeepCopy()
		for i := range spec.Containers[0].Env {
			env := &spec.Containers[0].Env[i]
			if env.Name == fault.env {
				env.Value, env.ValueFrom = fault.value, nil
				if fault.secret {
					env.Value = ""
					env.ValueFrom = &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: fault.value}, Key: "pin"}}
				}
			}
		}
		if fault.msp != "" {
			for i := range spec.Volumes {
				if spec.Volumes[i].Secret != nil && spec.Volumes[i].Secret.SecretName == "peer0-msp" {
					spec.Volumes[i].Secret.SecretName = fault.msp
				}
			}
		}
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "proof-" + fault.name, Namespace: namespace}, Spec: *spec}
		create(pod)
		waitPKCS11PeerFailure(t, ctx, kube, pod, fault.pattern)
		scanPKCS11KindLogs(t, ctx, kube, namespace, sensitive, false)
		removePKCS11KindPod(t, ctx, c, pod)
	}
	for _, snapshot := range snapshots {
		var current corev1.Secret
		if err := c.Get(ctx, client.ObjectKeyFromObject(&snapshot), &current); err != nil {
			t.Fatal(err)
		}
		if current.UID != snapshot.UID || !reflect.DeepEqual(current.Data, snapshot.Data) {
			t.Fatal("fault injection changed the original identity or PIN Secret")
		}
	}
	var currentNamespace corev1.Namespace
	if err := c.Get(ctx, client.ObjectKey{Name: namespace}, &currentNamespace); err != nil {
		t.Fatal(err)
	}
	if currentNamespace.Annotations[annotationPKCS11StateRetention] != originalNamespace.Annotations[annotationPKCS11StateRetention] {
		t.Fatal("fault injection changed durable handoff evidence")
	}
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: workloadEnrollmentJobName("peer0")}, &batchv1.Job{}); !apierrors.IsNotFound(err) {
		t.Fatal("fault injection recreated enrollment")
	}
	if err := c.Get(ctx, key, &deployment); err != nil {
		t.Fatal(err)
	}
	before = deployment.DeepCopy()
	one := int32(1)
	deployment.Spec.Replicas = &one
	if err := c.Patch(ctx, &deployment, client.MergeFrom(before)); err != nil {
		t.Fatal(err)
	}
	waitPKCS11Kind(t, ctx, "original peer recovers after provider fault cases without reenrollment", 3*time.Minute, func() (bool, error) {
		var pods corev1.PodList
		if err := c.List(ctx, &pods, client.InNamespace(namespace), client.MatchingLabels(deployment.Spec.Selector.MatchLabels)); err != nil {
			return false, err
		}
		for _, pod := range pods.Items {
			if strings.HasPrefix(pod.Name, "peer0-") {
				return pkcs11KindPodReady(ctx, c, namespace, pod.Name)
			}
		}
		return false, nil
	})
	scanPKCS11KindLogs(t, ctx, kube, namespace, sensitive, false)
}
