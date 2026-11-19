// Copyright contributors to FabricOps. SPDX-License-Identifier: Apache-2.0

package controller

import (
	"fmt"
	"regexp"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func pkcs11PeerFailedClosed(pod corev1.Pod, logs []byte, expected *regexp.Regexp) (bool, error) {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return false, fmt.Errorf("fault-injected peer became Kubernetes-ready")
		}
	}
	for _, container := range pod.Status.ContainerStatuses {
		if container.Ready {
			return false, fmt.Errorf("fault-injected container became ready")
		}
		if container.Name != containerPeer || container.State.Terminated == nil {
			continue
		}
		if container.State.Terminated.ExitCode == 0 {
			return false, fmt.Errorf("fault-injected peer exited successfully")
		}
		if pod.Status.Phase != corev1.PodFailed {
			continue
		}
		if !expected.Match(logs) {
			return false, fmt.Errorf("peer failure did not match the expected provider error")
		}
		return true, nil
	}
	return false, nil
}

func TestPKCS11FailureEvidenceRejectsFalsePositives(t *testing.T) {
	expected := regexp.MustCompile("CKR_PIN_INCORRECT")
	for _, test := range []struct {
		name                string
		phase               corev1.PodPhase
		ready               bool
		podReady            bool
		exit                int32
		log                 string
		complete, wantError bool
	}{
		{name: "pending", phase: corev1.PodPending},
		{name: "still running", phase: corev1.PodRunning},
		{name: "became ready", phase: corev1.PodRunning, ready: true, wantError: true},
		{name: "Pod readiness observed", phase: corev1.PodFailed, podReady: true, exit: 1, log: "CKR_PIN_INCORRECT", wantError: true},
		{name: "clean exit", phase: corev1.PodSucceeded, log: "CKR_PIN_INCORRECT", wantError: true},
		{name: "wrong failure", phase: corev1.PodFailed, exit: 1, log: "permission denied", wantError: true},
		{name: "provider rejected", phase: corev1.PodFailed, exit: 1, log: "CKR_PIN_INCORRECT", complete: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			pod := corev1.Pod{Status: corev1.PodStatus{Phase: test.phase}}
			if test.podReady {
				pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
			}
			status := corev1.ContainerStatus{Name: containerPeer, Ready: test.ready}
			if test.phase == corev1.PodFailed || test.phase == corev1.PodSucceeded {
				status.State.Terminated = &corev1.ContainerStateTerminated{ExitCode: test.exit}
			}
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{status}
			complete, err := pkcs11PeerFailedClosed(pod, []byte(test.log), expected)
			if complete != test.complete || (err != nil) != test.wantError {
				t.Fatalf("unexpected failure classification: complete=%v error=%v", complete, err)
			}
		})
	}
}
