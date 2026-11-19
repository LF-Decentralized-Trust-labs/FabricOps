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

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const annotationPKCS11EnrollmentPlan = "fabricops.io/pkcs11-enrollment-plan"

// This records publication/sequencing evidence, not an attestation that the
// token contains the certificate's private key or that CSI has detached a disk.
type pkcs11EnrollmentHandoff struct {
	JobUID     types.UID `json:"jobUID"`
	PlanDigest string    `json:"planDigest"`
	MSPUID     types.UID `json:"mspUID"`
	MSPDigest  string    `json:"mspDigest"`
	Drained    bool      `json:"drained"`
}

func pkcs11HandoffDigest(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func validPKCS11Handoff(h pkcs11EnrollmentHandoff) bool {
	if h.JobUID == "" || h.MSPUID == "" {
		return false
	}
	for _, digest := range []string{h.PlanDigest, h.MSPDigest} {
		decoded, err := hex.DecodeString(digest)
		if err != nil || len(decoded) != sha256.Size {
			return false
		}
	}
	return true
}

func validatePKCS11EnrollmentJob(job, expected *batchv1.Job) (bool, error) {
	if job.UID == "" || !publisherObjectOwned(job, expected) || !apiequality.Semantic.DeepDerivative(expected.Spec.Template.Spec, job.Spec.Template.Spec) ||
		job.Spec.Template.Spec.HostNetwork || job.Spec.Template.Spec.HostPID || job.Spec.Template.Spec.HostIPC ||
		(job.Spec.Completions != nil && *job.Spec.Completions != 1) || (job.Spec.Parallelism != nil && *job.Spec.Parallelism != 1) {
		return false, fmt.Errorf("PKCS11 enrollment Job ownership or plan requires review")
	}
	if jobFailed(*job) {
		return false, fmt.Errorf("PKCS11 enrollment Job failed; automatic reenrollment is disabled")
	}
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			return job.Status.Active == 0, nil
		}
	}
	return false, nil
}

func (r *FabricNetworkReconciler) pkcs11PeerBinding(ctx context.Context, net *fabricopsv1alpha1.FabricNetwork, org fabricopsv1alpha1.Org, namespace, peer string) (string, pkcs11ClaimBinding, error) {
	var ns corev1.Namespace
	if err := r.Get(ctx, client.ObjectKey{Name: namespace}, &ns); err != nil {
		return "", pkcs11ClaimBinding{}, err
	}
	var record pkcs11StateRetention
	if err := json.Unmarshal([]byte(ns.Annotations[annotationPKCS11StateRetention]), &record); err != nil {
		return "", pkcs11ClaimBinding{}, fmt.Errorf("PKCS11 retention record requires review")
	}
	for _, state := range org.Peer.MSPSigning.PKCS11.Runtime.StateClaims {
		if state.Peer == peer {
			binding := record.ClaimBindings[state.ClaimName]
			if record.NetworkUID != net.UID || record.NamespaceUID != ns.UID || binding.UID == "" || binding.Peer != peer {
				return "", binding, fmt.Errorf("PKCS11 retained claim ownership requires review")
			}
			return state.ClaimName, binding, nil
		}
	}
	return "", pkcs11ClaimBinding{}, fmt.Errorf("PKCS11 retained peer claim is missing")
}

func (r *FabricNetworkReconciler) pkcs11PeerEnrollmentReady(ctx context.Context, net *fabricopsv1alpha1.FabricNetwork, org fabricopsv1alpha1.Org, namespace, peer string) (bool, error) {
	if err := r.validateAndBindPKCS11StateClaim(ctx, net, org, namespace, peer); err != nil {
		return false, err
	}
	claimName, binding, err := r.pkcs11PeerBinding(ctx, net, org, namespace, peer)
	if err != nil {
		return false, err
	}
	expected := buildWorkloadEnrollmentJob(net, org, namespace, peer, componentPeer, peerWorkloadCSRHosts(org, peer, namespace))
	var job batchv1.Job
	jobErr := r.Get(ctx, client.ObjectKeyFromObject(expected), &job)
	if jobErr != nil && !apierrors.IsNotFound(jobErr) {
		return false, jobErr
	}
	if jobErr == nil {
		if !job.DeletionTimestamp.IsZero() {
			if binding.Enrollment == nil || binding.Enrollment.JobUID != job.UID {
				return false, fmt.Errorf("PKCS11 enrollment Job was deleted before handoff; recovery review is required")
			}
			return false, nil
		}
		complete, err := validatePKCS11EnrollmentJob(&job, expected)
		if err != nil || !complete {
			return false, err
		}
	} else if binding.Enrollment == nil {
		enrolled, err := r.workloadIdentityEnrolled(ctx, net, org, namespace, peer, componentPeer)
		if err != nil {
			return false, err
		}
		if enrolled {
			return false, fmt.Errorf("PKCS11 published material has no enrollment Job or handoff; recovery review is required")
		}
		return false, nil
	}
	if binding.Enrollment != nil && (binding.Enrollment.PlanDigest != expected.Annotations[annotationPKCS11EnrollmentPlan] || (jobErr == nil && job.UID != binding.Enrollment.JobUID)) {
		return false, fmt.Errorf("PKCS11 enrollment plan or Job UID changed; recovery review is required")
	}
	enrolled, err := r.workloadIdentityEnrolled(ctx, net, org, namespace, peer, componentPeer)
	if err != nil {
		return false, err
	}
	if !enrolled {
		if binding.Enrollment != nil {
			return false, fmt.Errorf("PKCS11 published identity no longer matches the handoff; recovery review is required")
		}
		return false, nil
	}
	var msp corev1.Secret
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: identitySecretName(peer, secretKindMSP)}, &msp); err != nil {
		return false, err
	}
	for _, kind := range []string{secretKindMSP, secretKindTLS} {
		if kind == secretKindTLS && !net.Spec.Global.TLS {
			continue
		}
		published := msp.DeepCopy()
		if kind == secretKindTLS {
			if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: identitySecretName(peer, kind)}, published); err != nil {
				return false, err
			}
		}
		desired := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Annotations: resourceAnnotations(net, org), Labels: identityLabels(net, org, componentPeer, peer, map[string]string{labelIdentityKind: kind, labelIdentitySource: identitySourceFabricCA})}}
		if !publisherObjectOwned(published, desired) {
			return false, fmt.Errorf("PKCS11 published identity ownership requires review")
		}
	}
	digest, err := pkcs11HandoffDigest(msp.Data)
	if err != nil {
		return false, err
	}
	if msp.UID == "" {
		return false, fmt.Errorf("PKCS11 published MSP must have an API-assigned UID")
	}
	if binding.Enrollment == nil {
		binding.Enrollment = &pkcs11EnrollmentHandoff{JobUID: job.UID, PlanDigest: expected.Annotations[annotationPKCS11EnrollmentPlan], MSPUID: msp.UID, MSPDigest: digest}
		return false, r.updateOrgPKCS11StateRetention(ctx, net, org, map[string]pkcs11ClaimBinding{claimName: binding})
	}
	if binding.Enrollment.MSPUID != msp.UID || binding.Enrollment.MSPDigest != digest {
		return false, fmt.Errorf("PKCS11 published MSP changed; recovery review is required")
	}
	if jobErr == nil {
		propagation := metav1.DeletePropagationBackground
		// Preconditions protect same-name replacement and concurrent Job changes.
		if err := r.Delete(ctx, &job, &client.DeleteOptions{PropagationPolicy: &propagation, Preconditions: &metav1.Preconditions{UID: &job.UID, ResourceVersion: &job.ResourceVersion}}); client.IgnoreNotFound(err) != nil {
			return false, err
		}
		return false, nil
	}
	if binding.Enrollment.Drained {
		return true, nil
	}
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(namespace)); err != nil {
		return false, err
	}
	for _, pod := range pods.Items {
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == claimName {
				return false, nil
			}
		}
	}
	handoff := *binding.Enrollment
	handoff.Drained = true
	binding.Enrollment = &handoff
	if err := r.updateOrgPKCS11StateRetention(ctx, net, org, map[string]pkcs11ClaimBinding{claimName: binding}); err != nil {
		return false, err
	}
	return true, nil
}
