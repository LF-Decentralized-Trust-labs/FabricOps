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
	"reflect"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const pkcs11PublisherTokenVolume = "pkcs11-publisher-api"

func pkcs11PublisherName(peer string) string {
	return sanitizeName(peer + "-identity-publisher")
}

func pkcs11PublisherRBAC(net *fabricopsv1alpha1.FabricNetwork, org fabricopsv1alpha1.Org, namespace, peer string) (*corev1.ServiceAccount, *rbacv1.Role, *rbacv1.RoleBinding) {
	name := pkcs11PublisherName(peer)
	metadata := metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: orgLabels(net, org, componentPeer), Annotations: resourceAnnotations(net, org)}
	automount := false
	account := &corev1.ServiceAccount{ObjectMeta: metadata, AutomountServiceAccountToken: &automount}
	names := []string{identitySecretName(peer, secretKindMSP)}
	if net.Spec.Global.TLS {
		names = append(names, identitySecretName(peer, secretKindTLS))
	}
	// Kubernetes cannot restrict create by resourceNames. The controller prepares
	// the destinations; the publisher can only read/patch those existing objects.
	role := &rbacv1.Role{ObjectMeta: metadata, Rules: []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"secrets"}, ResourceNames: names, Verbs: []string{"get", "patch"}}}}
	binding := &rbacv1.RoleBinding{ObjectMeta: metadata,
		Subjects: []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: name, Namespace: namespace}},
		RoleRef:  rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: name}}
	return account, role, binding
}

func applyPKCS11PublisherAccess(pod *corev1.PodSpec, peer string) {
	automount, mode, expiration := false, int32(0440), int64(3600)
	pod.AutomountServiceAccountToken = &automount
	pod.ServiceAccountName = pkcs11PublisherName(peer)
	pod.Volumes = append(pod.Volumes, corev1.Volume{Name: pkcs11PublisherTokenVolume, VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
		DefaultMode: &mode,
		Sources: []corev1.VolumeProjection{
			{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token", ExpirationSeconds: &expiration}},
			{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"}, Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}}},
			{DownwardAPI: &corev1.DownwardAPIProjection{Items: []corev1.DownwardAPIVolumeFile{{Path: "namespace", FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}}}},
		},
	}}})
	pod.Containers[0].VolumeMounts = append(pod.Containers[0].VolumeMounts, corev1.VolumeMount{Name: pkcs11PublisherTokenVolume, MountPath: "/var/run/secrets/kubernetes.io/serviceaccount", ReadOnly: true})
}

// This is staged behind the closed PKCS11 gate with the workload builders.
func (r *FabricNetworkReconciler) preparePKCS11Publisher(ctx context.Context, net *fabricopsv1alpha1.FabricNetwork, org fabricopsv1alpha1.Org, namespace, peer string) error {
	if len(validatePKCS11Runtime(org)) > 0 || net.UID == "" {
		return fmt.Errorf("PKCS11 publisher requires valid runtime access and a network UID")
	}
	peerFound := false
	for _, state := range org.Peer.MSPSigning.PKCS11.Runtime.StateClaims {
		peerFound = peerFound || state.Peer == peer
	}
	if !peerFound {
		return fmt.Errorf("PKCS11 publisher requires a declared peer")
	}
	account, role, binding := pkcs11PublisherRBAC(net, org, namespace, peer)
	for _, name := range role.Rules[0].ResourceNames {
		kind, validationKind := secretKindMSP, secretKindPeerPKCS11MSP
		if name == identitySecretName(peer, secretKindTLS) {
			kind, validationKind = secretKindTLS, secretKindTLS
		}
		desired := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace,
			Labels: identityLabels(net, org, componentPeer, peer, map[string]string{labelIdentityKind: kind, labelIdentitySource: identitySourceFabricCA}), Annotations: resourceAnnotations(net, org)}, Type: corev1.SecretTypeOpaque}
		var existing corev1.Secret
		err := r.Get(ctx, client.ObjectKeyFromObject(desired), &existing)
		if apierrors.IsNotFound(err) {
			if err := r.Create(ctx, desired); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if !pkcs11PublisherObjectOwned(&existing, desired) || existing.Type != corev1.SecretTypeOpaque ||
			(existing.Immutable != nil && *existing.Immutable) || (len(existing.Data) > 0 && identitySecretValidationError(existing, validationKind, net.Spec.Global.TLS) != "") {
			return fmt.Errorf("PKCS11 publisher destination requires review; refusing to overwrite existing identity material")
		}
	}
	for _, desired := range []client.Object{account, role, binding} {
		if err := r.ensurePKCS11PublisherObject(ctx, desired); err != nil {
			return err
		}
	}
	return nil
}

func (r *FabricNetworkReconciler) ensurePKCS11PublisherObject(ctx context.Context, desired client.Object) error {
	initial := desired.DeepCopyObject().(client.Object)
	if err := r.Get(ctx, client.ObjectKeyFromObject(desired), initial); err != nil {
		if apierrors.IsNotFound(err) {
			return r.Create(ctx, desired)
		}
		return err
	}
	return r.updateObjectWithRetry(ctx, desired, func(existing client.Object) (bool, error) {
		if existing.GetUID() != initial.GetUID() || !pkcs11PublisherObjectOwned(existing, desired) {
			return false, fmt.Errorf("PKCS11 publisher RBAC ownership requires review")
		}
		before := existing.DeepCopyObject()
		switch object := existing.(type) {
		case *corev1.ServiceAccount:
			object.AutomountServiceAccountToken = desired.(*corev1.ServiceAccount).AutomountServiceAccountToken
		case *rbacv1.Role:
			object.Rules = desired.(*rbacv1.Role).Rules
		case *rbacv1.RoleBinding:
			binding := desired.(*rbacv1.RoleBinding)
			object.Subjects, object.RoleRef = binding.Subjects, binding.RoleRef
		}
		return !reflect.DeepEqual(before, existing), nil
	})
}

func pkcs11PublisherObjectOwned(existing, desired client.Object) bool {
	if !existing.GetDeletionTimestamp().IsZero() {
		return false
	}
	for key, value := range desired.GetLabels() {
		if existing.GetLabels()[key] != value {
			return false
		}
	}
	for key, value := range desired.GetAnnotations() {
		if existing.GetAnnotations()[key] != value {
			return false
		}
	}
	return true
}
