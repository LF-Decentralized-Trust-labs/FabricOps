//go:build integration

// Copyright contributors to FabricOps. SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func testPKCS11KindReadiness(t *testing.T, ctx context.Context, c client.Client, kube kubernetes.Interface, namespace string) {
	t.Helper()
	var pods corev1.PodList
	if err := c.List(ctx, &pods, client.InNamespace(namespace), client.MatchingLabels{labelComponent: componentPeer}); err != nil {
		t.Fatal(err)
	}
	var original *corev1.Pod
	for i := range pods.Items {
		pod := &pods.Items[i]
		if strings.HasPrefix(pod.Name, "peer0-") && pod.DeletionTimestamp == nil {
			original = pod.DeepCopy()
		}
	}
	if original == nil || len(original.Status.ContainerStatuses) != 1 || !original.Status.ContainerStatuses[0].Ready {
		t.Fatal("ready peer not found before live provider fault")
	}
	var provider corev1.Secret
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: "provider-config"}, &provider); err != nil {
		t.Fatal(err)
	}
	saved := provider.DeepCopy()
	var beforeNamespace corev1.Namespace
	if err := c.Get(ctx, client.ObjectKey{Name: namespace}, &beforeNamespace); err != nil {
		t.Fatal(err)
	}
	var identities []corev1.Secret
	for _, name := range []string{"peer0-msp", "peer0-tls", "token-pin"} {
		var secret corev1.Secret
		if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &secret); err != nil {
			t.Fatal(err)
		}
		identities = append(identities, secret)
	}
	// Change only fresh-session token visibility, never the existing token/key.
	provider.Data["softhsm2.conf"] = []byte("directories.tokendir = /nonexistent/proof-token-state\nobjectstore.backend = file\nlog.level = ERROR\n")
	if err := c.Update(ctx, &provider); err != nil {
		t.Fatal(err)
	}
	restore := func() error {
		var current corev1.Secret
		if err := c.Get(ctx, client.ObjectKeyFromObject(saved), &current); err != nil {
			return err
		}
		if current.UID != saved.UID {
			return fmt.Errorf("provider Secret was replaced")
		}
		current.Data = saved.Data
		return c.Update(ctx, &current)
	}
	defer func() {
		if err := restore(); err != nil {
			t.Error("restore provider configuration:", err)
		}
	}()
	check := func(wantReady bool) (bool, error) {
		var current corev1.Pod
		if err := c.Get(ctx, client.ObjectKeyFromObject(original), &current); err != nil {
			return false, err
		}
		if current.UID != original.UID || len(current.Status.ContainerStatuses) != 1 {
			return false, fmt.Errorf("peer Pod was replaced during live fault")
		}
		status := current.Status.ContainerStatuses[0]
		if status.State.Running == nil || status.RestartCount != original.Status.ContainerStatuses[0].RestartCount || status.ContainerID != original.Status.ContainerStatuses[0].ContainerID {
			return false, fmt.Errorf("peer restarted during live fault")
		}
		ready, err := pkcs11KindPodReady(ctx, c, namespace, current.Name)
		return ready == wantReady && status.Ready == wantReady, err
	}
	waitPKCS11Kind(t, ctx, "running peer loses readiness when fresh provider sessions fail", 3*time.Minute, func() (bool, error) { return check(false) })
	if err := exec.CommandContext(ctx, "kubectl", "--context", "kind-"+os.Getenv("FABRICOPS_PKCS11_KIND_CLUSTER"), "-n", namespace, "exec", original.Name, "--", "sh", "-ec", `grep -qx 'directories.tokendir = /nonexistent/proof-token-state' "$SOFTHSM2_CONF"`).Run(); err != nil {
		t.Fatal("readiness loss preceded the intended provider configuration projection")
	}
	output, err := exec.CommandContext(ctx, "kubectl", "--context", "kind-"+os.Getenv("FABRICOPS_PKCS11_KIND_CLUSTER"), "-n", namespace, "exec", original.Name, "--", "/usr/local/bin/fabricops-pkcs11-ready").CombinedOutput()
	if err == nil || strings.TrimSpace(string(output)) != "PKCS11 peer is not ready\ncommand terminated with exit code 1" {
		t.Fatal("provider fault did not produce the bounded readiness refusal")
	}
	if _, err := kube.CoreV1().Pods(namespace).ProxyGet("http", original.Name, "9443", "healthz", nil).DoRaw(ctx); err != nil {
		t.Fatal("peer operations endpoint stopped serving during provider readiness loss:", err)
	}
	if err := restore(); err != nil {
		t.Fatal(err)
	}
	waitPKCS11Kind(t, ctx, "same peer regains provider readiness without restart or reenrollment", 3*time.Minute, func() (bool, error) { return check(true) })
	for _, snapshot := range identities {
		var current corev1.Secret
		if err := c.Get(ctx, client.ObjectKeyFromObject(&snapshot), &current); err != nil {
			t.Fatal(err)
		}
		if current.UID != snapshot.UID || !reflect.DeepEqual(current.Data, snapshot.Data) {
			t.Fatal("live fault changed identity or PIN material")
		}
	}
	var afterNamespace corev1.Namespace
	if err := c.Get(ctx, client.ObjectKey{Name: namespace}, &afterNamespace); err != nil {
		t.Fatal(err)
	}
	if afterNamespace.Annotations[annotationPKCS11StateRetention] != beforeNamespace.Annotations[annotationPKCS11StateRetention] {
		t.Fatal("live fault changed the enrollment handoff")
	}
	var jobs batchv1.JobList
	if err := c.List(ctx, &jobs, client.InNamespace(namespace)); err != nil {
		t.Fatal(err)
	}
	for _, job := range jobs.Items {
		if job.Name == workloadEnrollmentJobName("peer0") {
			t.Fatal("live fault recreated enrollment Job")
		}
	}
}
