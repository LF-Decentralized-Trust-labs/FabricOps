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
	"fmt"
	"maps"
	"reflect"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	fabricopsv1alpha1 "github.com/dpereowei/fabricops/api/v1alpha1"
)

const (
	enrollmentWorkDir = "/fabricops/enrollment"

	enrollAdminContainerName     = "enroll-admin"
	publishAdminContainerName    = "publish-admin-identity"
	enrollWorkloadContainerName  = "enroll-workload"
	publishWorkloadContainerName = "publish-workload-identity"

	envCAAddress             = "FABRICOPS_CA_ADDRESS"
	envCABootstrapUserPass   = "FABRICOPS_CA_BOOTSTRAP_USER_PASS"
	envAdminUsername         = "FABRICOPS_ADMIN_USERNAME"
	envAdminPassword         = "FABRICOPS_ADMIN_PASSWORD"
	envAdminName             = "FABRICOPS_ADMIN_NAME"
	envAdminMSPSecret        = "FABRICOPS_ADMIN_MSP_SECRET"
	envAdminTLSSecret        = "FABRICOPS_ADMIN_TLS_SECRET"
	envWorkloadUsername      = "FABRICOPS_WORKLOAD_USERNAME"
	envWorkloadPassword      = "FABRICOPS_WORKLOAD_PASSWORD"
	envWorkloadName          = "FABRICOPS_WORKLOAD_NAME"
	envWorkloadType          = "FABRICOPS_WORKLOAD_TYPE"
	envWorkloadCSRHosts      = "FABRICOPS_WORKLOAD_CSR_HOSTS"
	envWorkloadMSPSecret     = "FABRICOPS_WORKLOAD_MSP_SECRET"
	envWorkloadTLSSecret     = "FABRICOPS_WORKLOAD_TLS_SECRET"
	envTLSEnabled            = "FABRICOPS_TLS_ENABLED"
	envPodNamespace          = "POD_NAMESPACE"
	enrollmentVolumeName     = "enrollment-output"
	enrollmentServiceAccount = "identity-enroller"
)

func kubectlImage() string {
	return "bitnami/kubectl@sha256:08afc880eea24f36572644ccae85fb3e573a6ff1b7161135a3ae9a5eab222df2"
}

func adminEnrollmentJobName(org fabricopsv1alpha1.Org) string {
	return sanitizeName(adminIdentityName(org) + "-enroll")
}

func workloadEnrollmentJobName(workloadName string) string {
	return sanitizeName(workloadName + "-enroll")
}

func enrollmentServiceAccountName(org fabricopsv1alpha1.Org) string {
	return sanitizeName(adminIdentityName(org) + "-" + enrollmentServiceAccount)
}

func (r *FabricNetworkReconciler) reconcileAdminEnrollment(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
) error {
	enrolled, err := r.adminIdentityEnrolled(ctx, net, org, namespace)
	if err != nil {
		return err
	}
	if enrolled {
		return nil
	}

	if err := r.ensureEnrollmentRBAC(ctx, net, org, namespace); err != nil {
		return err
	}

	return r.ensureJob(ctx, buildAdminEnrollmentJob(net, org, namespace))
}

func (r *FabricNetworkReconciler) reconcileWorkloadEnrollments(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
) error {
	if err := r.ensureEnrollmentRBAC(ctx, net, org, namespace); err != nil {
		return err
	}

	for _, group := range org.Orderers {
		for i := 0; i < group.Instances; i++ {
			name := sanitizeName(fmt.Sprintf("%s%d", group.Prefix, i))
			if err := r.reconcileWorkloadEnrollment(ctx, net, org, namespace, name, componentOrderer, ordererWorkloadCSRHosts(group, name, namespace)); err != nil {
				return err
			}
		}
	}

	if org.Peer == nil {
		return nil
	}

	for i := 0; i < org.Peer.Instances; i++ {
		name := sanitizeName(fmt.Sprintf("%s%d", org.Peer.Prefix, i))
		if err := r.reconcileWorkloadEnrollment(ctx, net, org, namespace, name, componentPeer, peerWorkloadCSRHosts(org, name, namespace)); err != nil {
			return err
		}
	}

	return nil
}

func (r *FabricNetworkReconciler) reconcileWorkloadEnrollment(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	workloadName string,
	component string,
	csrHosts []string,
) error {
	enrolled, err := r.workloadIdentityEnrolled(ctx, net, namespace, workloadName)
	if err != nil {
		return err
	}
	if enrolled {
		return nil
	}

	return r.ensureJob(ctx, buildWorkloadEnrollmentJob(net, org, namespace, workloadName, component, csrHosts))
}

func (r *FabricNetworkReconciler) ensureEnrollmentRBAC(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
) error {
	if err := r.ensureServiceAccount(ctx, buildEnrollmentServiceAccount(net, org, namespace)); err != nil {
		return err
	}
	if err := r.ensureRole(ctx, buildEnrollmentRole(net, org, namespace)); err != nil {
		return err
	}
	return r.ensureRoleBinding(ctx, buildEnrollmentRoleBinding(net, org, namespace))
}

func (r *FabricNetworkReconciler) adminIdentityEnrolled(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
) (bool, error) {
	adminName := adminIdentityName(org)

	return r.fabricCAIdentityEnrolled(ctx, net, namespace, adminName, secretKindAdminMSP, secretKindAdminTLS)
}

func (r *FabricNetworkReconciler) workloadIdentityEnrolled(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	namespace string,
	workloadName string,
) (bool, error) {
	return r.fabricCAIdentityEnrolled(ctx, net, namespace, workloadName, secretKindMSP, secretKindTLS)
}

func (r *FabricNetworkReconciler) fabricCAIdentityEnrolled(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	namespace string,
	workloadName string,
	mspKind string,
	tlsKind string,
) (bool, error) {
	mspSecret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      identitySecretName(workloadName, secretKindMSP),
	}, mspSecret); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	if mspSecret.Labels[labelIdentitySource] != identitySourceFabricCA {
		return false, nil
	}
	if identitySecretValidationError(*mspSecret, mspKind, net.Spec.Global.TLS) != "" {
		return false, nil
	}

	if !net.Spec.Global.TLS {
		return true, nil
	}

	tlsSecret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      identitySecretName(workloadName, secretKindTLS),
	}, tlsSecret); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	if tlsSecret.Labels[labelIdentitySource] != identitySourceFabricCA {
		return false, nil
	}
	if identitySecretValidationError(*tlsSecret, tlsKind, net.Spec.Global.TLS) != "" {
		return false, nil
	}

	return true, nil
}

func buildEnrollmentServiceAccount(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:        enrollmentServiceAccountName(org),
			Namespace:   namespace,
			Labels:      orgLabels(net, org, componentAdmin),
			Annotations: resourceAnnotations(net, org),
		},
	}
}

func buildEnrollmentRole(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
) *rbacv1.Role {
	return &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:        enrollmentServiceAccountName(org),
			Namespace:   namespace,
			Labels:      orgLabels(net, org, componentAdmin),
			Annotations: resourceAnnotations(net, org),
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"secrets"},
				Verbs:     []string{"get", "create", "update", "patch"},
			},
		},
	}
}

func buildEnrollmentRoleBinding(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
) *rbacv1.RoleBinding {
	name := enrollmentServiceAccountName(org)

	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      orgLabels(net, org, componentAdmin),
			Annotations: resourceAnnotations(net, org),
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      name,
				Namespace: namespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     name,
		},
	}
}

func buildAdminEnrollmentJob(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
) *batchv1.Job {
	adminName := adminIdentityName(org)
	labels := identityLabels(net, org, componentAdmin, adminName, map[string]string{
		labelIdentityKind: secretKindAdminEnroll,
	})
	annotations := resourceAnnotations(net, org)
	backoffLimit := int32(4)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        adminEnrollmentJobName(org),
			Namespace:   namespace,
			Labels:      labels,
			Annotations: succeededJobCleanupAnnotations(annotations),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: resourceAnnotations(net, org),
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: enrollmentServiceAccountName(org),
					RestartPolicy:      corev1.RestartPolicyNever,
					Volumes: []corev1.Volume{
						{
							Name: enrollmentVolumeName,
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
					InitContainers: []corev1.Container{
						{
							Name:      enrollAdminContainerName,
							Image:     caImage(),
							Command:   []string{"sh", "-ec", adminEnrollmentScript()},
							Env:       adminEnrollmentEnv(org, namespace),
							Resources: componentResourceRequirements(componentCA),
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      enrollmentVolumeName,
									MountPath: enrollmentWorkDir,
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:      publishAdminContainerName,
							Image:     kubectlImage(),
							Command:   []string{"sh", "-ec", publishAdminIdentityScript()},
							Env:       publishAdminIdentityEnv(net, org),
							Resources: componentResourceRequirements(componentKubectl),
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      enrollmentVolumeName,
									MountPath: enrollmentWorkDir,
								},
							},
						},
					},
				},
			},
		},
	}
}

func buildWorkloadEnrollmentJob(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	workloadName string,
	component string,
	csrHosts []string,
) *batchv1.Job {
	labels := identityLabels(net, org, component, workloadName, map[string]string{
		labelIdentityKind: secretKindWorkloadEnroll,
	})
	annotations := resourceAnnotations(net, org)
	backoffLimit := int32(4)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        workloadEnrollmentJobName(workloadName),
			Namespace:   namespace,
			Labels:      labels,
			Annotations: succeededJobCleanupAnnotations(annotations),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: resourceAnnotations(net, org),
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: enrollmentServiceAccountName(org),
					RestartPolicy:      corev1.RestartPolicyNever,
					Volumes: []corev1.Volume{
						{
							Name: enrollmentVolumeName,
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
					InitContainers: []corev1.Container{
						{
							Name:      enrollWorkloadContainerName,
							Image:     caImage(),
							Command:   []string{"sh", "-ec", workloadEnrollmentScript()},
							Env:       workloadEnrollmentEnv(net, org, namespace, workloadName, component, csrHosts),
							Resources: componentResourceRequirements(componentCA),
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      enrollmentVolumeName,
									MountPath: enrollmentWorkDir,
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:      publishWorkloadContainerName,
							Image:     kubectlImage(),
							Command:   []string{"sh", "-ec", publishWorkloadIdentityScript()},
							Env:       publishWorkloadIdentityEnv(net, workloadName),
							Resources: componentResourceRequirements(componentKubectl),
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      enrollmentVolumeName,
									MountPath: enrollmentWorkDir,
								},
							},
						},
					},
				},
			},
		},
	}
}

func adminEnrollmentEnv(org fabricopsv1alpha1.Org, namespace string) []corev1.EnvVar {
	adminName := adminIdentityName(org)

	return []corev1.EnvVar{
		{Name: envCAAddress, Value: serviceDNS(sanitizeName(org.Organization.Name+"-ca"), namespace, caPort)},
		{
			Name: envCABootstrapUserPass,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: caBootstrapSecretName(org)},
					Key:                  caBootstrapUserPassKey,
				},
			},
		},
		{
			Name: envAdminUsername,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: adminEnrollmentSecretName(org)},
					Key:                  caBootstrapUsernameKey,
				},
			},
		},
		{
			Name: envAdminPassword,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: adminEnrollmentSecretName(org)},
					Key:                  caBootstrapPasswordKey,
				},
			},
		},
		{Name: envAdminName, Value: adminName},
	}
}

func workloadEnrollmentEnv(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	workloadName string,
	component string,
	csrHosts []string,
) []corev1.EnvVar {
	enrollmentSecretName := identityEnrollmentSecretName(workloadName)

	return []corev1.EnvVar{
		{Name: envCAAddress, Value: serviceDNS(sanitizeName(org.Organization.Name+"-ca"), namespace, caPort)},
		{
			Name: envCABootstrapUserPass,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: caBootstrapSecretName(org)},
					Key:                  caBootstrapUserPassKey,
				},
			},
		},
		{
			Name: envWorkloadUsername,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: enrollmentSecretName},
					Key:                  caBootstrapUsernameKey,
				},
			},
		},
		{
			Name: envWorkloadPassword,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: enrollmentSecretName},
					Key:                  caBootstrapPasswordKey,
				},
			},
		},
		{Name: envWorkloadName, Value: workloadName},
		{Name: envWorkloadType, Value: component},
		{Name: envWorkloadCSRHosts, Value: strings.Join(csrHosts, ",")},
		{Name: envTLSEnabled, Value: boolString(net.Spec.Global.TLS)},
	}
}

func publishAdminIdentityEnv(net *fabricopsv1alpha1.FabricNetwork, org fabricopsv1alpha1.Org) []corev1.EnvVar {
	adminName := adminIdentityName(org)

	return []corev1.EnvVar{
		{Name: envAdminName, Value: adminName},
		{Name: envAdminMSPSecret, Value: identitySecretName(adminName, secretKindMSP)},
		{Name: envAdminTLSSecret, Value: identitySecretName(adminName, secretKindTLS)},
		{Name: envTLSEnabled, Value: boolString(net.Spec.Global.TLS)},
		{
			Name: envPodNamespace,
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.namespace",
				},
			},
		},
	}
}

func publishWorkloadIdentityEnv(net *fabricopsv1alpha1.FabricNetwork, workloadName string) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: envWorkloadName, Value: workloadName},
		{Name: envWorkloadMSPSecret, Value: identitySecretName(workloadName, secretKindMSP)},
		{Name: envWorkloadTLSSecret, Value: identitySecretName(workloadName, secretKindTLS)},
		{Name: envTLSEnabled, Value: boolString(net.Spec.Global.TLS)},
		{
			Name: envPodNamespace,
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.namespace",
				},
			},
		},
	}
}

func boolString(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

func adminEnrollmentScript() string {
	return `set -eu

copy_first() {
  src_dir="$1"
  dest="$2"
  file="$(find "$src_dir" -type f | head -n 1)"
  test -n "$file"
  cp "$file" "$dest"
}

bootstrap_home="` + enrollmentWorkDir + `/bootstrap"
admin_msp_home="` + enrollmentWorkDir + `/admin-msp"
admin_tls_home="` + enrollmentWorkDir + `/admin-tls"
output_dir="` + enrollmentWorkDir + `/output"

mkdir -p "$bootstrap_home" "$admin_msp_home" "$admin_tls_home" "$output_dir/msp" "$output_dir/tls"

fabric-ca-client enroll \
  -u "http://${FABRICOPS_CA_BOOTSTRAP_USER_PASS}@${FABRICOPS_CA_ADDRESS}" \
  --mspdir "$bootstrap_home/msp"

if ! fabric-ca-client register \
  --id.name "$FABRICOPS_ADMIN_USERNAME" \
  --id.secret "$FABRICOPS_ADMIN_PASSWORD" \
  --id.type admin \
  --id.attrs "hf.Registrar.Roles=*,hf.Registrar.Attributes=*,hf.Revoker=true,admin=true:ecert" \
  --url "http://${FABRICOPS_CA_ADDRESS}" \
  --mspdir "$bootstrap_home/msp"; then
  echo "Admin identity may already be registered; continuing to enrollment"
fi

fabric-ca-client enroll \
  -u "http://${FABRICOPS_ADMIN_USERNAME}:${FABRICOPS_ADMIN_PASSWORD}@${FABRICOPS_CA_ADDRESS}" \
  --mspdir "$admin_msp_home/msp"

fabric-ca-client enroll \
  -u "http://${FABRICOPS_ADMIN_USERNAME}:${FABRICOPS_ADMIN_PASSWORD}@${FABRICOPS_CA_ADDRESS}" \
  --enrollment.profile tls \
  --csr.hosts "$FABRICOPS_ADMIN_USERNAME" \
  --mspdir "$admin_tls_home/tls"

cat > "$output_dir/msp/config.yaml" <<'FABRICOPS_MSP_CONFIG'
` + mspConfigYAML() + `FABRICOPS_MSP_CONFIG

copy_first "$admin_msp_home/msp/cacerts" "$output_dir/msp/cacert.pem"
copy_first "$admin_msp_home/msp/signcerts" "$output_dir/msp/signcert.pem"
copy_first "$admin_msp_home/msp/keystore" "$output_dir/msp/keystore.pem"
copy_first "$admin_tls_home/tls/tlscacerts" "$output_dir/msp/tlscacert.pem"
copy_first "$admin_tls_home/tls/tlscacerts" "$output_dir/tls/ca.crt"
copy_first "$admin_tls_home/tls/signcerts" "$output_dir/tls/client.crt"
copy_first "$admin_tls_home/tls/keystore" "$output_dir/tls/client.key"
chmod -R a+rX "$output_dir"
`
}

func publishAdminIdentityScript() string {
	return `set -eu

output_dir="` + enrollmentWorkDir + `/output"

kubectl -n "$POD_NAMESPACE" create secret generic "$FABRICOPS_ADMIN_MSP_SECRET" \
  --from-file=config.yaml="$output_dir/msp/config.yaml" \
  --from-file=cacert.pem="$output_dir/msp/cacert.pem" \
  --from-file=tlscacert.pem="$output_dir/msp/tlscacert.pem" \
  --from-file=signcert.pem="$output_dir/msp/signcert.pem" \
  --from-file=keystore.pem="$output_dir/msp/keystore.pem" \
  --dry-run=client -o yaml | kubectl -n "$POD_NAMESPACE" apply -f -

kubectl -n "$POD_NAMESPACE" label secret "$FABRICOPS_ADMIN_MSP_SECRET" \
  fabricops.io/identity-kind=admin-msp \
  fabricops.io/identity-source=fabric-ca \
  fabricops.io/workload="$FABRICOPS_ADMIN_NAME" \
  --overwrite

if [ "$FABRICOPS_TLS_ENABLED" = "true" ]; then
  kubectl -n "$POD_NAMESPACE" create secret generic "$FABRICOPS_ADMIN_TLS_SECRET" \
    --from-file=ca.crt="$output_dir/tls/ca.crt" \
    --from-file=client.crt="$output_dir/tls/client.crt" \
    --from-file=client.key="$output_dir/tls/client.key" \
    --dry-run=client -o yaml | kubectl -n "$POD_NAMESPACE" apply -f -

  kubectl -n "$POD_NAMESPACE" label secret "$FABRICOPS_ADMIN_TLS_SECRET" \
    fabricops.io/identity-kind=admin-tls \
    fabricops.io/identity-source=fabric-ca \
    fabricops.io/workload="$FABRICOPS_ADMIN_NAME" \
    --overwrite
fi
`
}

func workloadEnrollmentScript() string {
	return `set -eu

copy_first() {
  src_dir="$1"
  dest="$2"
  file="$(find "$src_dir" -type f | head -n 1)"
  test -n "$file"
  cp "$file" "$dest"
}

bootstrap_home="` + enrollmentWorkDir + `/bootstrap"
workload_msp_home="` + enrollmentWorkDir + `/workload-msp"
workload_tls_home="` + enrollmentWorkDir + `/workload-tls"
output_dir="` + enrollmentWorkDir + `/output"

mkdir -p "$bootstrap_home" "$workload_msp_home" "$output_dir/msp"

if [ "$FABRICOPS_TLS_ENABLED" = "true" ]; then
  mkdir -p "$workload_tls_home" "$output_dir/tls"
fi

fabric-ca-client enroll \
  -u "http://${FABRICOPS_CA_BOOTSTRAP_USER_PASS}@${FABRICOPS_CA_ADDRESS}" \
  --mspdir "$bootstrap_home/msp"

if ! fabric-ca-client register \
  --id.name "$FABRICOPS_WORKLOAD_USERNAME" \
  --id.secret "$FABRICOPS_WORKLOAD_PASSWORD" \
  --id.type "$FABRICOPS_WORKLOAD_TYPE" \
  --url "http://${FABRICOPS_CA_ADDRESS}" \
  --mspdir "$bootstrap_home/msp"; then
  echo "Workload identity may already be registered; continuing to enrollment"
fi

fabric-ca-client enroll \
  -u "http://${FABRICOPS_WORKLOAD_USERNAME}:${FABRICOPS_WORKLOAD_PASSWORD}@${FABRICOPS_CA_ADDRESS}" \
  --mspdir "$workload_msp_home/msp"

cat > "$output_dir/msp/config.yaml" <<'FABRICOPS_MSP_CONFIG'
` + mspConfigYAML() + `FABRICOPS_MSP_CONFIG

copy_first "$workload_msp_home/msp/cacerts" "$output_dir/msp/cacert.pem"
copy_first "$workload_msp_home/msp/signcerts" "$output_dir/msp/signcert.pem"
copy_first "$workload_msp_home/msp/keystore" "$output_dir/msp/keystore.pem"

if [ "$FABRICOPS_TLS_ENABLED" = "true" ]; then
  fabric-ca-client enroll \
    -u "http://${FABRICOPS_WORKLOAD_USERNAME}:${FABRICOPS_WORKLOAD_PASSWORD}@${FABRICOPS_CA_ADDRESS}" \
    --enrollment.profile tls \
    --csr.hosts "$FABRICOPS_WORKLOAD_CSR_HOSTS" \
    --mspdir "$workload_tls_home/tls"

  copy_first "$workload_tls_home/tls/tlscacerts" "$output_dir/msp/tlscacert.pem"
  copy_first "$workload_tls_home/tls/tlscacerts" "$output_dir/tls/ca.crt"
  copy_first "$workload_tls_home/tls/signcerts" "$output_dir/tls/server.crt"
  copy_first "$workload_tls_home/tls/keystore" "$output_dir/tls/server.key"
fi

chmod -R a+rX "$output_dir"
`
}

func publishWorkloadIdentityScript() string {
	return `set -eu

output_dir="` + enrollmentWorkDir + `/output"

if [ "$FABRICOPS_TLS_ENABLED" = "true" ]; then
  kubectl -n "$POD_NAMESPACE" create secret generic "$FABRICOPS_WORKLOAD_MSP_SECRET" \
    --from-file=config.yaml="$output_dir/msp/config.yaml" \
    --from-file=cacert.pem="$output_dir/msp/cacert.pem" \
    --from-file=tlscacert.pem="$output_dir/msp/tlscacert.pem" \
    --from-file=signcert.pem="$output_dir/msp/signcert.pem" \
    --from-file=keystore.pem="$output_dir/msp/keystore.pem" \
    --dry-run=client -o yaml | kubectl -n "$POD_NAMESPACE" apply -f -
else
  kubectl -n "$POD_NAMESPACE" create secret generic "$FABRICOPS_WORKLOAD_MSP_SECRET" \
    --from-file=config.yaml="$output_dir/msp/config.yaml" \
    --from-file=cacert.pem="$output_dir/msp/cacert.pem" \
    --from-file=signcert.pem="$output_dir/msp/signcert.pem" \
    --from-file=keystore.pem="$output_dir/msp/keystore.pem" \
    --dry-run=client -o yaml | kubectl -n "$POD_NAMESPACE" apply -f -
fi

kubectl -n "$POD_NAMESPACE" label secret "$FABRICOPS_WORKLOAD_MSP_SECRET" \
  fabricops.io/identity-kind=msp \
  fabricops.io/identity-source=fabric-ca \
  fabricops.io/workload="$FABRICOPS_WORKLOAD_NAME" \
  --overwrite

if [ "$FABRICOPS_TLS_ENABLED" = "true" ]; then
  kubectl -n "$POD_NAMESPACE" create secret generic "$FABRICOPS_WORKLOAD_TLS_SECRET" \
    --from-file=ca.crt="$output_dir/tls/ca.crt" \
    --from-file=server.crt="$output_dir/tls/server.crt" \
    --from-file=server.key="$output_dir/tls/server.key" \
    --dry-run=client -o yaml | kubectl -n "$POD_NAMESPACE" apply -f -

  kubectl -n "$POD_NAMESPACE" label secret "$FABRICOPS_WORKLOAD_TLS_SECRET" \
    fabricops.io/identity-kind=tls \
    fabricops.io/identity-source=fabric-ca \
    fabricops.io/workload="$FABRICOPS_WORKLOAD_NAME" \
    --overwrite
fi
`
}

func (r *FabricNetworkReconciler) ensureServiceAccount(ctx context.Context, desired *corev1.ServiceAccount) error {
	var existing corev1.ServiceAccount
	key := client.ObjectKeyFromObject(desired)

	err := r.Get(ctx, key, &existing)
	if apierrors.IsNotFound(err) {
		log := logf.FromContext(ctx)
		log.Info("Creating ServiceAccount", "name", desired.Name, "namespace", desired.Namespace)
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	return r.updateObjectWithRetry(ctx, desired, func(object client.Object) (bool, error) {
		existing := object.(*corev1.ServiceAccount)
		changed := mergeLabels(&existing.Labels, desired.Labels)
		if mergeAnnotations(&existing.Annotations, desired.Annotations) {
			changed = true
		}
		if !changed {
			return false, nil
		}

		log := logf.FromContext(ctx)
		log.Info("Updating ServiceAccount", "name", desired.Name, "namespace", desired.Namespace)
		return true, nil
	})
}

func (r *FabricNetworkReconciler) ensureRole(ctx context.Context, desired *rbacv1.Role) error {
	var existing rbacv1.Role
	key := client.ObjectKeyFromObject(desired)

	err := r.Get(ctx, key, &existing)
	if apierrors.IsNotFound(err) {
		log := logf.FromContext(ctx)
		log.Info("Creating Role", "name", desired.Name, "namespace", desired.Namespace)
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	return r.updateObjectWithRetry(ctx, desired, func(object client.Object) (bool, error) {
		existing := object.(*rbacv1.Role)
		changed := mergeLabels(&existing.Labels, desired.Labels)
		if mergeAnnotations(&existing.Annotations, desired.Annotations) {
			changed = true
		}
		if !reflect.DeepEqual(existing.Rules, desired.Rules) {
			existing.Rules = desired.Rules
			changed = true
		}
		if !changed {
			return false, nil
		}

		log := logf.FromContext(ctx)
		log.Info("Updating Role", "name", desired.Name, "namespace", desired.Namespace)
		return true, nil
	})
}

func (r *FabricNetworkReconciler) ensureRoleBinding(ctx context.Context, desired *rbacv1.RoleBinding) error {
	var existing rbacv1.RoleBinding
	key := client.ObjectKeyFromObject(desired)

	err := r.Get(ctx, key, &existing)
	if apierrors.IsNotFound(err) {
		log := logf.FromContext(ctx)
		log.Info("Creating RoleBinding", "name", desired.Name, "namespace", desired.Namespace)
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	return r.updateObjectWithRetry(ctx, desired, func(object client.Object) (bool, error) {
		existing := object.(*rbacv1.RoleBinding)
		changed := mergeLabels(&existing.Labels, desired.Labels)
		if mergeAnnotations(&existing.Annotations, desired.Annotations) {
			changed = true
		}
		if !reflect.DeepEqual(existing.Subjects, desired.Subjects) {
			existing.Subjects = desired.Subjects
			changed = true
		}
		if !reflect.DeepEqual(existing.RoleRef, desired.RoleRef) {
			existing.RoleRef = desired.RoleRef
			changed = true
		}
		if !changed {
			return false, nil
		}

		log := logf.FromContext(ctx)
		log.Info("Updating RoleBinding", "name", desired.Name, "namespace", desired.Namespace)
		return true, nil
	})
}

func (r *FabricNetworkReconciler) ensureJob(ctx context.Context, desired *batchv1.Job) error {
	var existing batchv1.Job
	key := client.ObjectKeyFromObject(desired)

	err := r.Get(ctx, key, &existing)
	if apierrors.IsNotFound(err) {
		log := logf.FromContext(ctx)
		log.Info("Creating Job", "name", desired.Name, "namespace", desired.Namespace)
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	return r.updateObjectWithRetry(ctx, desired, func(object client.Object) (bool, error) {
		existing := object.(*batchv1.Job)
		changed := mergeLabels(&existing.Labels, desired.Labels)
		desiredAnnotations := maps.Clone(desired.Annotations)
		if existing.Annotations[annotationSucceededJobCleanup] != "true" {
			delete(desiredAnnotations, annotationSucceededJobCleanup)
		}
		if mergeAnnotations(&existing.Annotations, desiredAnnotations) {
			changed = true
		}
		if !changed {
			return false, nil
		}

		log := logf.FromContext(ctx)
		log.Info("Updating Job metadata", "name", desired.Name, "namespace", desired.Namespace)
		return true, nil
	})
}

func mergeLabels(existing *map[string]string, desired map[string]string) bool {
	return mergeStringMap(existing, desired)
}

func mergeAnnotations(existing *map[string]string, desired map[string]string) bool {
	return mergeStringMap(existing, desired)
}

func mergeStringMap(existing *map[string]string, desired map[string]string) bool {
	changed := false
	if *existing == nil {
		*existing = map[string]string{}
		changed = len(desired) > 0
	}

	for key, value := range desired {
		if (*existing)[key] != value {
			(*existing)[key] = value
			changed = true
		}
	}

	return changed
}
