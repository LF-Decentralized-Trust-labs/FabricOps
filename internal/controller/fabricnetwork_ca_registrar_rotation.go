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
	"bytes"
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

const (
	secretKindCABootstrapNext     = "ca-bootstrap-next"
	secretKindCABootstrapPrevious = "ca-bootstrap-previous"
	identityKindCARegistrarRotate = "ca-registrar-rotation"

	annotationCARegistrarRotationRequestID = "fabricops.io/ca-registrar-rotation-request-id"
	annotationCARegistrarRotationTime      = "fabricops.io/ca-registrar-rotation-time"
	annotationCARegistrarPreviousSecret    = "fabricops.io/ca-registrar-previous-secret"

	envCARegistrarNextUsername = "FABRICOPS_CA_REGISTRAR_NEXT_USERNAME"
	envCARegistrarNextPassword = "FABRICOPS_CA_REGISTRAR_NEXT_PASSWORD"
)

func (r *FabricNetworkReconciler) reconcileCARegistrarRotation(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	caReady bool,
) (fabricopsv1alpha1.CARegistrarRotationStatus, error) {
	requestID := caRegistrarRotationRequestID(org)
	status := fabricopsv1alpha1.CARegistrarRotationStatus{
		ObservedRequestID: requestID,
		Phase:             fabricopsv1alpha1.CARegistrarRotationPhaseIdle,
		ActiveSecretName:  caBootstrapSecretName(org),
	}

	active, activeFound, err := r.getCARegistrarSecret(ctx, namespace, status.ActiveSecretName)
	if err != nil {
		return status, err
	}
	if activeFound {
		status.ActiveUsername = credentialSecretUsername(active)
		status.LastRotationTime = caRegistrarSecretRotationTime(active)
	}

	previousSecretName := caRegistrarPreviousSecretName(org)
	if _, previousFound, err := r.getCARegistrarSecret(ctx, namespace, previousSecretName); err != nil {
		return status, err
	} else if previousFound {
		status.PreviousSecretName = previousSecretName
	}

	if requestID == "" {
		return status, nil
	}

	status.StagedSecretName = caRegistrarStagedSecretName(org, requestID)
	status.RotationJobName = caRegistrarRotationJobName(org, requestID)
	status.PreviousSecretName = previousSecretName

	staged, err := r.ensureCARegistrarStagedSecret(ctx, net, org, namespace, requestID)
	if err != nil {
		return status, err
	}

	if !activeFound {
		status.Phase = fabricopsv1alpha1.CARegistrarRotationPhaseFailed
		status.Message = "Active CA bootstrap Secret is missing"
		return status, nil
	}
	if reason := caBootstrapSecretValidationError(active); reason != "" {
		status.Phase = fabricopsv1alpha1.CARegistrarRotationPhaseFailed
		status.Message = "Active CA bootstrap Secret is invalid: " + reason
		return status, nil
	}

	if caRegistrarRotationPromoted(active, staged, requestID) {
		status.Phase = fabricopsv1alpha1.CARegistrarRotationPhaseReady
		status.ActiveUsername = credentialSecretUsername(active)
		status.Message = "CA bootstrap registrar rotation completed"
		return status, nil
	}

	if !caReady {
		status.Phase = fabricopsv1alpha1.CARegistrarRotationPhaseWaitingForCA
		status.Message = "Waiting for Fabric CA readiness before rotating the bootstrap registrar"
		return status, nil
	}

	if err := r.ensureEnrollmentRBAC(ctx, net, org, namespace); err != nil {
		return status, err
	}
	if err := r.ensureJob(ctx, buildCARegistrarRotationJob(net, org, namespace, requestID)); err != nil {
		return status, err
	}

	phase, message, err := r.caRegistrarRotationJobState(ctx, namespace, status.RotationJobName)
	if err != nil {
		return status, err
	}
	if phase != fabricopsv1alpha1.CARegistrarRotationPhaseReady {
		status.Phase = phase
		status.Message = message
		return status, nil
	}

	rotationTime := metav1.NewTime(time.Now())
	if err := r.promoteCARegistrarSecret(ctx, net, org, namespace, active, staged, requestID, rotationTime); err != nil {
		return status, err
	}

	status.Phase = fabricopsv1alpha1.CARegistrarRotationPhaseReady
	status.ActiveUsername = credentialSecretUsername(staged)
	status.LastRotationTime = rotationTime
	status.Message = "CA bootstrap registrar rotation completed"
	return status, nil
}

func caRegistrarRotationRequestID(org fabricopsv1alpha1.Org) string {
	if org.CA.Registrar == nil || org.CA.Registrar.Rotation == nil {
		return ""
	}
	return org.CA.Registrar.Rotation.RequestID
}

func caRegistrarStagedSecretName(org fabricopsv1alpha1.Org, requestID string) string {
	return sanitizeName(caBootstrapSecretName(org) + "-next-" + shortDigest([]byte(requestID)))
}

func caRegistrarPreviousSecretName(org fabricopsv1alpha1.Org) string {
	return sanitizeName(caBootstrapSecretName(org) + "-previous")
}

func caRegistrarRotationJobName(org fabricopsv1alpha1.Org, requestID string) string {
	return sanitizeName(caBootstrapSecretName(org) + "-rotate-" + shortDigest([]byte(requestID)))
}

func caRegistrarNextUsername(requestID string) string {
	return sanitizeName(caBootstrapUsername + "-" + shortDigest([]byte(requestID)))
}

func (r *FabricNetworkReconciler) getCARegistrarSecret(
	ctx context.Context,
	namespace string,
	name string,
) (corev1.Secret, bool, error) {
	var secret corev1.Secret
	err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &secret)
	if apierrors.IsNotFound(err) {
		return corev1.Secret{}, false, nil
	}
	if err != nil {
		return corev1.Secret{}, false, err
	}
	return secret, true, nil
}

func credentialSecretUsername(secret corev1.Secret) string {
	return string(secret.Data[caBootstrapUsernameKey])
}

func caRegistrarSecretRotationTime(secret corev1.Secret) metav1.Time {
	if secret.Annotations == nil || secret.Annotations[annotationCARegistrarRotationTime] == "" {
		return metav1.Time{}
	}
	rotationTime, err := time.Parse(time.RFC3339, secret.Annotations[annotationCARegistrarRotationTime])
	if err != nil {
		return metav1.Time{}
	}
	return metav1.NewTime(rotationTime)
}

func (r *FabricNetworkReconciler) ensureCARegistrarStagedSecret(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	requestID string,
) (corev1.Secret, error) {
	desired, err := buildCARegistrarStagedSecret(net, org, namespace, requestID)
	if err != nil {
		return corev1.Secret{}, err
	}

	if err := r.ensureSecret(ctx, desired, enrollmentCredentialSecretValidationError); err != nil {
		return corev1.Secret{}, err
	}

	secret, _, err := r.getCARegistrarSecret(ctx, namespace, desired.Name)
	return secret, err
}

func buildCARegistrarStagedSecret(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	requestID string,
) (*corev1.Secret, error) {
	password, err := generateBootstrapPassword()
	if err != nil {
		return nil, err
	}

	username := caRegistrarNextUsername(requestID)
	name := caRegistrarStagedSecretName(org, requestID)
	userPass := username + ":" + password
	annotations := resourceAnnotations(net, org)
	annotations[annotationCARegistrarRotationRequestID] = requestID

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: identityLabels(net, org, componentCA, name, map[string]string{
				labelIdentityKind: secretKindCABootstrapNext,
			}),
			Annotations: annotations,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			caBootstrapUsernameKey: []byte(username),
			caBootstrapPasswordKey: []byte(password),
			caBootstrapUserPassKey: []byte(userPass),
		},
	}, nil
}

func caRegistrarRotationPromoted(active corev1.Secret, staged corev1.Secret, requestID string) bool {
	if active.Annotations[annotationCARegistrarRotationRequestID] != requestID {
		return false
	}
	for _, key := range caBootstrapSecretKeys() {
		if !bytes.Equal(active.Data[key], staged.Data[key]) {
			return false
		}
	}
	return true
}

func buildCARegistrarRotationJob(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	requestID string,
) *batchv1.Job {
	bootstrapName := caBootstrapSecretName(org)
	stagedName := caRegistrarStagedSecretName(org, requestID)
	labels := identityLabels(net, org, componentCA, bootstrapName, map[string]string{
		labelIdentityKind: identityKindCARegistrarRotate,
	})
	annotations := resourceAnnotations(net, org)
	annotations[annotationCARegistrarRotationRequestID] = requestID
	backoffLimit := int32(4)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        caRegistrarRotationJobName(org, requestID),
			Namespace:   namespace,
			Labels:      labels,
			Annotations: succeededJobCleanupAnnotations(annotations),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: annotations,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: enrollmentServiceAccountName(org),
					RestartPolicy:      corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:      identityKindCARegistrarRotate,
							Image:     caImage(),
							Command:   []string{"sh", "-ec", caRegistrarRotationScript()},
							Env:       caRegistrarRotationEnv(org, namespace, bootstrapName, stagedName),
							Resources: componentResourceRequirements(componentCA),
						},
					},
				},
			},
		},
	}
}

func caRegistrarRotationEnv(
	org fabricopsv1alpha1.Org,
	namespace string,
	bootstrapName string,
	stagedName string,
) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: envCAAddress, Value: serviceDNS(sanitizeName(org.Organization.Name+"-ca"), namespace, caPort)},
		{
			Name: envCABootstrapUserPass,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: bootstrapName},
					Key:                  caBootstrapUserPassKey,
				},
			},
		},
		{
			Name: envCARegistrarNextUsername,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: stagedName},
					Key:                  caBootstrapUsernameKey,
				},
			},
		},
		{
			Name: envCARegistrarNextPassword,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: stagedName},
					Key:                  caBootstrapPasswordKey,
				},
			},
		},
	}
}

func caRegistrarRotationScript() string {
	return `set -eu

bootstrap_home="` + enrollmentWorkDir + `/bootstrap"
staged_home="` + enrollmentWorkDir + `/staged"

mkdir -p "$bootstrap_home" "$staged_home"

fabric-ca-client enroll \
  -u "http://${FABRICOPS_CA_BOOTSTRAP_USER_PASS}@${FABRICOPS_CA_ADDRESS}" \
  --mspdir "$bootstrap_home/msp"

if ! fabric-ca-client register \
  --id.name "$FABRICOPS_CA_REGISTRAR_NEXT_USERNAME" \
  --id.secret "$FABRICOPS_CA_REGISTRAR_NEXT_PASSWORD" \
  --id.type admin \
  --id.attrs "hf.Registrar.Roles=*,hf.Registrar.Attributes=*,hf.Revoker=true,admin=true:ecert" \
  --url "http://${FABRICOPS_CA_ADDRESS}" \
  --mspdir "$bootstrap_home/msp"; then
  echo "Staged registrar identity may already be registered; validating staged credentials"
fi

fabric-ca-client enroll \
  -u "http://${FABRICOPS_CA_REGISTRAR_NEXT_USERNAME}:${FABRICOPS_CA_REGISTRAR_NEXT_PASSWORD}@${FABRICOPS_CA_ADDRESS}" \
  --mspdir "$staged_home/msp"
`
}

func (r *FabricNetworkReconciler) caRegistrarRotationJobState(
	ctx context.Context,
	namespace string,
	jobName string,
) (fabricopsv1alpha1.CARegistrarRotationPhase, string, error) {
	var job batchv1.Job
	err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: jobName}, &job)
	if apierrors.IsNotFound(err) {
		return fabricopsv1alpha1.CARegistrarRotationPhaseStaging, "Registrar rotation job is pending", nil
	}
	if err != nil {
		return "", "", err
	}
	if jobFailed(job) {
		return fabricopsv1alpha1.CARegistrarRotationPhaseFailed, "Registrar rotation job failed; active bootstrap Secret was retained", nil
	}
	if jobSucceeded(job) {
		return fabricopsv1alpha1.CARegistrarRotationPhaseReady, "Registrar rotation job completed", nil
	}

	return fabricopsv1alpha1.CARegistrarRotationPhaseRotating, "Registrar rotation job is running", nil
}

func (r *FabricNetworkReconciler) promoteCARegistrarSecret(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	active corev1.Secret,
	staged corev1.Secret,
	requestID string,
	rotationTime metav1.Time,
) error {
	previous := buildCARegistrarPreviousSecret(net, org, namespace, active)
	if err := r.upsertSecretData(ctx, previous); err != nil {
		return err
	}

	activeSecret := buildPromotedCARegistrarSecret(net, org, namespace, staged, requestID, rotationTime)
	return r.upsertSecretData(ctx, activeSecret)
}

func buildCARegistrarPreviousSecret(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	active corev1.Secret,
) *corev1.Secret {
	name := caRegistrarPreviousSecretName(org)
	annotations := resourceAnnotations(net, org)
	if active.Annotations[annotationCARegistrarRotationRequestID] != "" {
		annotations[annotationCARegistrarRotationRequestID] = active.Annotations[annotationCARegistrarRotationRequestID]
	}
	if active.Annotations[annotationCARegistrarRotationTime] != "" {
		annotations[annotationCARegistrarRotationTime] = active.Annotations[annotationCARegistrarRotationTime]
	}

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: identityLabels(net, org, componentCA, name, map[string]string{
				labelIdentityKind: secretKindCABootstrapPrevious,
			}),
			Annotations: annotations,
		},
		Type: corev1.SecretTypeOpaque,
		Data: copySecretData(active.Data),
	}
}

func buildPromotedCARegistrarSecret(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	staged corev1.Secret,
	requestID string,
	rotationTime metav1.Time,
) *corev1.Secret {
	name := caBootstrapSecretName(org)
	annotations := resourceAnnotations(net, org)
	annotations[annotationCARegistrarRotationRequestID] = requestID
	annotations[annotationCARegistrarRotationTime] = rotationTime.Time.UTC().Format(time.RFC3339)
	annotations[annotationCARegistrarPreviousSecret] = caRegistrarPreviousSecretName(org)

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: identityLabels(net, org, componentCA, name, map[string]string{
				labelIdentityKind: secretKindCABootstrap,
			}),
			Annotations: annotations,
		},
		Type: corev1.SecretTypeOpaque,
		Data: copySecretData(staged.Data),
	}
}

func (r *FabricNetworkReconciler) upsertSecretData(ctx context.Context, desired *corev1.Secret) error {
	var existing corev1.Secret
	key := client.ObjectKeyFromObject(desired)

	err := r.Get(ctx, key, &existing)
	if apierrors.IsNotFound(err) {
		log := logf.FromContext(ctx)
		log.Info("Creating Secret", "name", desired.Name, "namespace", desired.Namespace)
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	return r.updateObjectWithRetry(ctx, desired, func(object client.Object) (bool, error) {
		existing := object.(*corev1.Secret)
		changed := mergeLabels(&existing.Labels, desired.Labels)
		if mergeAnnotations(&existing.Annotations, desired.Annotations) {
			changed = true
		}
		if existing.Type != desired.Type {
			existing.Type = desired.Type
			changed = true
		}
		if !secretDataEqual(existing.Data, desired.Data) {
			existing.Data = copySecretData(desired.Data)
			changed = true
		}
		if !changed {
			return false, nil
		}

		log := logf.FromContext(ctx)
		log.Info("Updating Secret", "name", desired.Name, "namespace", desired.Namespace)
		return true, nil
	})
}

func secretDataEqual(a map[string][]byte, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for key, aValue := range a {
		if !bytes.Equal(aValue, b[key]) {
			return false
		}
	}
	return true
}

func caRegistrarRotationBlocksWork(
	status fabricopsv1alpha1.CARegistrarRotationStatus,
) bool {
	switch status.Phase {
	case fabricopsv1alpha1.CARegistrarRotationPhaseWaitingForCA,
		fabricopsv1alpha1.CARegistrarRotationPhaseStaging,
		fabricopsv1alpha1.CARegistrarRotationPhaseRotating:
		return true
	default:
		return false
	}
}

func caRegistrarRotationConditionProblem(
	orgName string,
	status fabricopsv1alpha1.CARegistrarRotationStatus,
) (string, int, bool) {
	switch status.Phase {
	case fabricopsv1alpha1.CARegistrarRotationPhaseFailed:
		message := fmt.Sprintf("%s CA registrar rotation Failed", orgName)
		if status.RotationJobName != "" {
			message += " via Job " + status.RotationJobName
		}
		if status.Message != "" {
			message += ": " + status.Message
		}
		return message, 70, true
	case fabricopsv1alpha1.CARegistrarRotationPhaseWaitingForCA,
		fabricopsv1alpha1.CARegistrarRotationPhaseStaging,
		fabricopsv1alpha1.CARegistrarRotationPhaseRotating:
		message := fmt.Sprintf("%s CA registrar rotation %s", orgName, status.Phase)
		if status.RotationJobName != "" {
			message += " via Job " + status.RotationJobName
		}
		if status.Message != "" {
			message += ": " + status.Message
		}
		return message, 25, true
	default:
		return "", 0, false
	}
}
