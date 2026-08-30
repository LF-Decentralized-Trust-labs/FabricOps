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
	"fmt"
	"slices"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

const (
	defaultCertificateRenewBefore = 30 * 24 * time.Hour

	annotationIdentityRevision = "fabricops.io/identity-revision"

	identityKindRenewal = "identity-renewal"

	certificateRenewalModeAdmin    = "admin"
	certificateRenewalModeWorkload = "workload"
)

type managedCertificateRef struct {
	namespace    string
	secretName   string
	secretKind   string
	key          string
	component    string
	workloadName string
	renewable    bool
	renewalMode  string
	csrHosts     []string
}

type certificateInventoryItem struct {
	status fabricopsv1alpha1.CertificateStatus
	ref    managedCertificateRef
	raw    []byte
}

type certificateRenewalRequest struct {
	workloadName string
	component    string
	mode         string
	csrHosts     []string
	revision     string
	jobName      string
	indices      []int
	seed         []byte
}

func (r *FabricNetworkReconciler) reconcileCertificateLifecycle(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	caReady bool,
	identityReady bool,
) ([]fabricopsv1alpha1.CertificateStatus, bool, string, error) {
	items, err := r.identityCertificateInventory(ctx, net, org, namespace, time.Now())
	if err != nil {
		return nil, false, "", err
	}

	statuses := certificateStatuses(items)
	requests := certificateRenewalRequests(items)
	renewalRequired := certificateStatusesNeedRenewal(statuses)
	if len(requests) == 0 {
		return statuses, renewalRequired, "", nil
	}

	if !identityReady {
		return statuses, renewalRequired, "Identity material is not ready; certificate renewal is waiting for enrollment to finish", nil
	}
	if !caReady {
		return statuses, renewalRequired, "Fabric CA is not ready; certificate renewal is waiting", nil
	}
	if err := r.ensureEnrollmentRBAC(ctx, net, org, namespace); err != nil {
		return statuses, renewalRequired, "", err
	}

	renewalMessages := []string{}
	for _, request := range requests {
		job := buildCertificateRenewalJob(net, org, namespace, request)
		if err := r.ensureJob(ctx, job); err != nil {
			return statuses, renewalRequired, "", err
		}

		state, message, err := r.certificateRenewalJobState(ctx, namespace, request.jobName)
		if err != nil {
			return statuses, renewalRequired, "", err
		}
		setCertificateRenewalJobStatus(statuses, request, state, message)
		if state == fabricopsv1alpha1.CertificateStateRenewalFailed {
			renewalMessages = append(renewalMessages, fmt.Sprintf("%s: %s", request.jobName, message))
		}
	}

	return statuses, certificateStatusesNeedRenewal(statuses), strings.Join(renewalMessages, "; "), nil
}

func (r *FabricNetworkReconciler) identityCertificateInventory(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	now time.Time,
) ([]certificateInventoryItem, error) {
	refs := managedIdentityCertificateRefs(net, org, namespace)
	items := make([]certificateInventoryItem, 0, len(refs))

	for _, ref := range refs {
		status := fabricopsv1alpha1.CertificateStatus{
			Name:         certificateStatusName(ref),
			Namespace:    ref.namespace,
			SecretName:   ref.secretName,
			SecretKind:   ref.secretKind,
			Key:          ref.key,
			Component:    ref.component,
			WorkloadName: ref.workloadName,
			Renewable:    ref.renewable,
			State:        fabricopsv1alpha1.CertificateStateMissing,
		}

		var secret corev1.Secret
		err := r.Get(ctx, client.ObjectKey{
			Namespace: ref.namespace,
			Name:      ref.secretName,
		}, &secret)
		if apierrors.IsNotFound(err) {
			status.Message = "Secret not found"
			items = append(items, certificateInventoryItem{status: status, ref: ref})
			continue
		}
		if err != nil {
			return nil, err
		}
		if secret.Labels[labelIdentitySource] != identitySourceFabricCA {
			status.State = fabricopsv1alpha1.CertificateStateInvalid
			status.Message = "Secret is not Fabric CA-managed"
			items = append(items, certificateInventoryItem{status: status, ref: ref})
			continue
		}
		if secret.Labels[labelIdentityKind] != "" && secret.Labels[labelIdentityKind] != ref.secretKind {
			status.State = fabricopsv1alpha1.CertificateStateInvalid
			status.Message = fmt.Sprintf("Secret has identity kind %q, expected %q", secret.Labels[labelIdentityKind], ref.secretKind)
			items = append(items, certificateInventoryItem{status: status, ref: ref})
			continue
		}

		data, ok := secret.Data[ref.key]
		if !ok {
			status.Message = "Certificate key not found"
			items = append(items, certificateInventoryItem{status: status, ref: ref})
			continue
		}

		cert, err := parsePEMCertificate(data)
		if err != nil {
			status.State = fabricopsv1alpha1.CertificateStateInvalid
			status.Message = err.Error()
			items = append(items, certificateInventoryItem{status: status, ref: ref})
			continue
		}

		status.Subject = cert.Subject.String()
		status.Issuer = cert.Issuer.String()
		status.NotBefore = metav1.NewTime(cert.NotBefore)
		status.NotAfter = metav1.NewTime(cert.NotAfter)
		status.RenewalTime = metav1.NewTime(cert.NotAfter.Add(-defaultCertificateRenewBefore))
		status.State, status.Message = certificateState(now, cert.NotBefore, cert.NotAfter, ref.renewable)

		items = append(items, certificateInventoryItem{
			status: status,
			ref:    ref,
			raw:    cert.Raw,
		})
	}

	return items, nil
}

func managedIdentityCertificateRefs(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
) []managedCertificateRef {
	tlsEnabled := net.Spec.Global.TLS
	refs := []managedCertificateRef{}
	adminName := adminIdentityName(org)

	refs = appendMSPCertificateRefs(refs, namespace, adminName, componentAdmin, secretKindAdminMSP, tlsEnabled, certificateRenewalModeAdmin, nil)
	if tlsEnabled {
		refs = append(refs,
			managedCertificateRef{
				namespace:    namespace,
				secretName:   identitySecretName(adminName, secretKindTLS),
				secretKind:   secretKindAdminTLS,
				key:          tlsCACertKey,
				component:    componentAdmin,
				workloadName: adminName,
				renewalMode:  certificateRenewalModeAdmin,
			},
			managedCertificateRef{
				namespace:    namespace,
				secretName:   identitySecretName(adminName, secretKindTLS),
				secretKind:   secretKindAdminTLS,
				key:          tlsClientCertKey,
				component:    componentAdmin,
				workloadName: adminName,
				renewable:    true,
				renewalMode:  certificateRenewalModeAdmin,
			},
		)
	}

	for _, group := range org.Orderers {
		for i := 0; i < group.Instances; i++ {
			name := sanitizeName(fmt.Sprintf("%s%d", group.Prefix, i))
			refs = appendMSPCertificateRefs(
				refs,
				namespace,
				name,
				componentOrderer,
				secretKindMSP,
				tlsEnabled,
				certificateRenewalModeWorkload,
				ordererWorkloadCSRHosts(group, name, namespace),
			)
			if tlsEnabled {
				refs = appendTLSCertificateRefs(refs, namespace, name, componentOrderer, ordererWorkloadCSRHosts(group, name, namespace))
			}
		}
	}

	if org.Peer == nil {
		return refs
	}

	for i := 0; i < org.Peer.Instances; i++ {
		name := sanitizeName(fmt.Sprintf("%s%d", org.Peer.Prefix, i))
		csrHosts := peerWorkloadCSRHosts(org, name, namespace)
		refs = appendMSPCertificateRefs(refs, namespace, name, componentPeer, secretKindMSP, tlsEnabled, certificateRenewalModeWorkload, csrHosts)
		if tlsEnabled {
			refs = appendTLSCertificateRefs(refs, namespace, name, componentPeer, csrHosts)
		}
	}

	return refs
}

func appendMSPCertificateRefs(
	refs []managedCertificateRef,
	namespace string,
	workloadName string,
	component string,
	secretKind string,
	tlsEnabled bool,
	renewalMode string,
	csrHosts []string,
) []managedCertificateRef {
	secretName := identitySecretName(workloadName, secretKindMSP)
	refs = append(refs,
		managedCertificateRef{
			namespace:    namespace,
			secretName:   secretName,
			secretKind:   secretKind,
			key:          mspCACertKey,
			component:    component,
			workloadName: workloadName,
			renewalMode:  renewalMode,
			csrHosts:     csrHosts,
		},
		managedCertificateRef{
			namespace:    namespace,
			secretName:   secretName,
			secretKind:   secretKind,
			key:          mspSignCertKey,
			component:    component,
			workloadName: workloadName,
			renewable:    true,
			renewalMode:  renewalMode,
			csrHosts:     csrHosts,
		},
	)
	if tlsEnabled {
		refs = append(refs, managedCertificateRef{
			namespace:    namespace,
			secretName:   secretName,
			secretKind:   secretKind,
			key:          mspTLSCACertKey,
			component:    component,
			workloadName: workloadName,
			renewalMode:  renewalMode,
			csrHosts:     csrHosts,
		})
	}

	return refs
}

func appendTLSCertificateRefs(
	refs []managedCertificateRef,
	namespace string,
	workloadName string,
	component string,
	csrHosts []string,
) []managedCertificateRef {
	secretName := identitySecretName(workloadName, secretKindTLS)
	return append(refs,
		managedCertificateRef{
			namespace:    namespace,
			secretName:   secretName,
			secretKind:   secretKindTLS,
			key:          tlsCACertKey,
			component:    component,
			workloadName: workloadName,
			renewalMode:  certificateRenewalModeWorkload,
			csrHosts:     csrHosts,
		},
		managedCertificateRef{
			namespace:    namespace,
			secretName:   secretName,
			secretKind:   secretKindTLS,
			key:          tlsServerCertKey,
			component:    component,
			workloadName: workloadName,
			renewable:    true,
			renewalMode:  certificateRenewalModeWorkload,
			csrHosts:     csrHosts,
		},
	)
}

func certificateState(
	now time.Time,
	notBefore time.Time,
	notAfter time.Time,
	renewable bool,
) (fabricopsv1alpha1.CertificateState, string) {
	if now.Before(notBefore) {
		return fabricopsv1alpha1.CertificateStateInvalid, "Certificate is not valid yet"
	}
	if !now.Before(notAfter) {
		if renewable {
			return fabricopsv1alpha1.CertificateStateExpired, "Certificate expired and will be renewed by Fabric CA"
		}
		return fabricopsv1alpha1.CertificateStateExpired, "CA root material is expired and requires CA rollover or refreshed enrollment output"
	}
	if !now.Add(defaultCertificateRenewBefore).Before(notAfter) {
		if renewable {
			return fabricopsv1alpha1.CertificateStateRenewalDue, "Certificate is inside the renewal window"
		}
		return fabricopsv1alpha1.CertificateStateRenewalDue, "CA root material is inside the renewal window"
	}

	return fabricopsv1alpha1.CertificateStateValid, ""
}

func certificateStatuses(items []certificateInventoryItem) []fabricopsv1alpha1.CertificateStatus {
	statuses := make([]fabricopsv1alpha1.CertificateStatus, 0, len(items))
	for _, item := range items {
		statuses = append(statuses, item.status)
	}
	return statuses
}

func certificateRenewalRequests(items []certificateInventoryItem) []certificateRenewalRequest {
	requestsByWorkload := map[string]*certificateRenewalRequest{}

	for i, item := range items {
		if !item.ref.renewable || !certificateStateNeedsRenewal(item.status.State) {
			continue
		}

		key := item.ref.workloadName
		request, ok := requestsByWorkload[key]
		if !ok {
			request = &certificateRenewalRequest{
				workloadName: item.ref.workloadName,
				component:    item.ref.component,
				mode:         item.ref.renewalMode,
				csrHosts:     append([]string(nil), item.ref.csrHosts...),
			}
			requestsByWorkload[key] = request
		}

		request.indices = append(request.indices, i)
		request.seed = append(request.seed, []byte(item.status.SecretName+"/"+item.status.Key)...)
		request.seed = append(request.seed, item.raw...)
	}

	requests := make([]certificateRenewalRequest, 0, len(requestsByWorkload))
	for _, request := range requestsByWorkload {
		request.revision = shortDigest(request.seed)
		request.jobName = certificateRenewalJobName(request.workloadName, request.revision)
		requests = append(requests, *request)
	}
	slices.SortFunc(requests, func(a, b certificateRenewalRequest) int {
		return strings.Compare(a.jobName, b.jobName)
	})

	return requests
}

func buildCertificateRenewalJob(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	request certificateRenewalRequest,
) *batchv1.Job {
	var job *batchv1.Job
	if request.mode == certificateRenewalModeAdmin {
		job = buildAdminEnrollmentJob(net, org, namespace)
	} else {
		job = buildWorkloadEnrollmentJob(net, org, namespace, request.workloadName, request.component, request.csrHosts)
	}

	job.Name = request.jobName
	retagCertificateRenewalJob(job)
	return job
}

func retagCertificateRenewalJob(job *batchv1.Job) {
	if job.Labels == nil {
		job.Labels = map[string]string{}
	}
	job.Labels[labelIdentityKind] = identityKindRenewal
	if job.Spec.Template.Labels == nil {
		job.Spec.Template.Labels = map[string]string{}
	}
	job.Spec.Template.Labels[labelIdentityKind] = identityKindRenewal
}

func certificateRenewalJobName(workloadName string, revision string) string {
	return sanitizeName(workloadName + "-renew-" + revision)
}

func (r *FabricNetworkReconciler) certificateRenewalJobState(
	ctx context.Context,
	namespace string,
	jobName string,
) (fabricopsv1alpha1.CertificateState, string, error) {
	var job batchv1.Job
	err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: jobName}, &job)
	if apierrors.IsNotFound(err) {
		return fabricopsv1alpha1.CertificateStateRenewing, "Renewal job is pending", nil
	}
	if err != nil {
		return "", "", err
	}
	if jobFailed(job) {
		return fabricopsv1alpha1.CertificateStateRenewalFailed, "Renewal job failed; last-known-good Secret data was retained", nil
	}
	if jobSucceeded(job) {
		return fabricopsv1alpha1.CertificateStateRenewalDue, "Renewal job completed; waiting for updated Secret data", nil
	}

	return fabricopsv1alpha1.CertificateStateRenewing, "Renewal job is running", nil
}

func setCertificateRenewalJobStatus(
	statuses []fabricopsv1alpha1.CertificateStatus,
	request certificateRenewalRequest,
	state fabricopsv1alpha1.CertificateState,
	message string,
) {
	for _, index := range request.indices {
		statuses[index].RenewalJobName = request.jobName
		statuses[index].State = state
		statuses[index].Message = message
	}
}

func certificateStateNeedsRenewal(state fabricopsv1alpha1.CertificateState) bool {
	return state == fabricopsv1alpha1.CertificateStateExpired || state == fabricopsv1alpha1.CertificateStateRenewalDue
}

func certificateStatusesNeedRenewal(statuses []fabricopsv1alpha1.CertificateStatus) bool {
	for _, status := range statuses {
		switch status.State {
		case fabricopsv1alpha1.CertificateStateExpired,
			fabricopsv1alpha1.CertificateStateRenewalDue,
			fabricopsv1alpha1.CertificateStateRenewing,
			fabricopsv1alpha1.CertificateStateRenewalFailed:
			return true
		}
	}
	return false
}

func certificateLifecycleConditionStatus(
	statuses []fabricopsv1alpha1.OrgStatus,
) (metav1.ConditionStatus, string, string) {
	problems := []string{}
	reason := "CertificateLifecycleReady"
	for _, orgStatus := range statuses {
		for _, certificate := range orgStatus.Certificates {
			if certificate.State == "" || certificate.State == fabricopsv1alpha1.CertificateStateValid {
				continue
			}
			if reason == "CertificateLifecycleReady" || certificateStatePriority(certificate.State) > certificateReasonPriority(reason) {
				reason = certificateReason(certificate.State)
			}
			problems = append(problems, certificateProblemMessage(orgStatus.Name, certificate))
		}
	}

	if len(problems) == 0 {
		return metav1.ConditionTrue, reason, "All Fabric certificates are valid"
	}

	if len(problems) > 4 {
		problems = append(problems[:4], fmt.Sprintf("%d more certificate items need attention", len(problems)-4))
	}
	return metav1.ConditionFalse, reason, strings.Join(problems, "; ")
}

func certificateReason(state fabricopsv1alpha1.CertificateState) string {
	switch state {
	case fabricopsv1alpha1.CertificateStateRenewalFailed:
		return "CertificateRenewalFailed"
	case fabricopsv1alpha1.CertificateStateExpired:
		return "CertificateExpired"
	case fabricopsv1alpha1.CertificateStateInvalid:
		return "CertificateMaterialInvalid"
	case fabricopsv1alpha1.CertificateStateMissing:
		return "CertificateMaterialMissing"
	case fabricopsv1alpha1.CertificateStateRenewing:
		return "CertificateRenewalRunning"
	case fabricopsv1alpha1.CertificateStateRenewalDue:
		return "CertificateRenewalRequired"
	default:
		return "CertificateLifecyclePending"
	}
}

func certificateStatePriority(state fabricopsv1alpha1.CertificateState) int {
	switch state {
	case fabricopsv1alpha1.CertificateStateRenewalFailed:
		return 60
	case fabricopsv1alpha1.CertificateStateExpired:
		return 50
	case fabricopsv1alpha1.CertificateStateInvalid:
		return 40
	case fabricopsv1alpha1.CertificateStateMissing:
		return 30
	case fabricopsv1alpha1.CertificateStateRenewing:
		return 20
	case fabricopsv1alpha1.CertificateStateRenewalDue:
		return 10
	default:
		return 0
	}
}

func certificateReasonPriority(reason string) int {
	switch reason {
	case "CertificateRenewalFailed":
		return 60
	case "CertificateExpired":
		return 50
	case "CertificateMaterialInvalid":
		return 40
	case "CertificateMaterialMissing":
		return 30
	case "CertificateRenewalRunning":
		return 20
	case "CertificateRenewalRequired":
		return 10
	default:
		return 0
	}
}

func certificateProblemMessage(orgName string, certificate fabricopsv1alpha1.CertificateStatus) string {
	message := fmt.Sprintf("%s %s %s", orgName, certificate.Name, certificate.State)
	if certificate.RenewalJobName != "" {
		message += " via Job " + certificate.RenewalJobName
	}
	if certificate.Message != "" {
		message += ": " + certificate.Message
	}
	return message
}

func certificateStatusName(ref managedCertificateRef) string {
	return ref.secretName + "/" + ref.key
}

func (r *FabricNetworkReconciler) setDeploymentIdentityRevision(
	ctx context.Context,
	deploy *appsv1.Deployment,
	net *fabricopsv1alpha1.FabricNetwork,
	workloadName string,
) error {
	revision, err := r.identityMaterialRevision(ctx, deploy.Namespace, workloadName, net.Spec.Global.TLS)
	if err != nil {
		return err
	}
	if revision == "" {
		return nil
	}

	if deploy.Spec.Template.Annotations == nil {
		deploy.Spec.Template.Annotations = map[string]string{}
	}
	deploy.Spec.Template.Annotations[annotationIdentityRevision] = revision
	return nil
}

func (r *FabricNetworkReconciler) identityMaterialRevision(
	ctx context.Context,
	namespace string,
	workloadName string,
	tlsEnabled bool,
) (string, error) {
	secretRefs := []struct {
		name string
		keys []string
	}{
		{name: identitySecretName(workloadName, secretKindMSP), keys: mspSecretKeys(tlsEnabled)},
	}
	if tlsEnabled {
		secretRefs = append(secretRefs, struct {
			name string
			keys []string
		}{name: identitySecretName(workloadName, secretKindTLS), keys: tlsSecretKeys()})
	}

	seed := []byte{}
	for _, ref := range secretRefs {
		var secret corev1.Secret
		err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: ref.name}, &secret)
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		if err != nil {
			return "", err
		}

		keys := append([]string(nil), ref.keys...)
		slices.Sort(keys)
		seed = append(seed, []byte(ref.name)...)
		for _, key := range keys {
			seed = append(seed, []byte(key)...)
			seed = append(seed, secret.Data[key]...)
		}
	}

	return shortDigest(seed), nil
}

func shortDigest(seed []byte) string {
	sum := sha256.Sum256(seed)
	return hex.EncodeToString(sum[:])[:10]
}
