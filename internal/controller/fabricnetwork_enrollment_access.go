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

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const enrollmentPublisherTokenVolume = "enrollment-publisher-api"

func applyEnrollmentPublisherToken(pod *corev1.PodSpec) {
	// Software publisher images may run non-root without a pod fsGroup. Only
	// the publisher mounts these files; read-only mode must permit that UID.
	automount, mode, expiration := false, int32(0444), int64(3600)
	pod.AutomountServiceAccountToken = &automount
	pod.Volumes = append(pod.Volumes, corev1.Volume{Name: enrollmentPublisherTokenVolume, VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
		DefaultMode: &mode,
		Sources: []corev1.VolumeProjection{
			{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token", ExpirationSeconds: &expiration}},
			{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"}, Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}}},
			{DownwardAPI: &corev1.DownwardAPIProjection{Items: []corev1.DownwardAPIVolumeFile{{Path: "namespace", FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}}}},
		},
	}}})
	pod.Containers[0].VolumeMounts = append(pod.Containers[0].VolumeMounts, corev1.VolumeMount{Name: enrollmentPublisherTokenVolume, MountPath: "/var/run/secrets/kubernetes.io/serviceaccount", ReadOnly: true})
}

type softwareEnrollmentIdentity struct {
	name, component, mspKind, tlsKind string
}

func softwareEnrollmentIdentities(org fabricopsv1alpha1.Org) []softwareEnrollmentIdentity {
	identities := []softwareEnrollmentIdentity{{adminIdentityName(org), componentAdmin, secretKindAdminMSP, secretKindAdminTLS}}
	for _, group := range org.Orderers {
		for i := 0; i < group.Instances; i++ {
			identities = append(identities, softwareEnrollmentIdentity{sanitizeName(fmt.Sprintf("%s%d", group.Prefix, i)), componentOrderer, secretKindMSP, secretKindTLS})
		}
	}
	if org.Peer != nil && workloadMSPSecretKind(org, componentPeer) == secretKindMSP {
		for i := 0; i < org.Peer.Instances; i++ {
			identities = append(identities, softwareEnrollmentIdentity{sanitizeName(fmt.Sprintf("%s%d", org.Peer.Prefix, i)), componentPeer, secretKindMSP, secretKindTLS})
		}
	}
	return identities
}

func enrollmentOutputSecrets(net *fabricopsv1alpha1.FabricNetwork, org fabricopsv1alpha1.Org, namespace string) []*corev1.Secret {
	var outputs []*corev1.Secret
	for _, identity := range softwareEnrollmentIdentities(org) {
		kinds := []string{secretKindMSP}
		if net.Spec.Global.TLS {
			kinds = append(kinds, secretKindTLS)
		}
		for _, kind := range kinds {
			identityKind := identity.mspKind
			if kind == secretKindTLS {
				identityKind = identity.tlsKind
			}
			outputs = append(outputs, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
				Name: identitySecretName(identity.name, kind), Namespace: namespace,
				Labels:      identityLabels(net, org, identity.component, identity.name, map[string]string{labelIdentityKind: identityKind}),
				Annotations: resourceAnnotations(net, org),
			}, Type: corev1.SecretTypeOpaque})
		}
	}
	return outputs
}

func (r *FabricNetworkReconciler) ensureEnrollmentOutputs(ctx context.Context, net *fabricopsv1alpha1.FabricNetwork, org fabricopsv1alpha1.Org, namespace string) error {
	for _, desired := range enrollmentOutputSecrets(net, org, namespace) {
		var existing corev1.Secret
		if err := r.Get(ctx, client.ObjectKeyFromObject(desired), &existing); err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}
			if err := r.Create(ctx, desired); err != nil {
				return err
			}
			continue
		}
		if err := ensureSameFabricNetworkOwner(&existing, desired); err != nil {
			return err
		}
		uid := existing.Annotations[annotationFabricNetworkUID]
		owned := publisherObjectOwned(&existing, desired)
		// Older publishers wrote only the identity source/kind/workload labels.
		// Preserve these outputs only inside this network's owned namespace.
		if !owned && uid == "" && existing.Labels[labelIdentitySource] == identitySourceFabricCA &&
			existing.Labels[labelIdentityKind] == desired.Labels[labelIdentityKind] && existing.Labels[labelWorkload] == desired.Labels[labelWorkload] {
			var ns corev1.Namespace
			if err := r.Get(ctx, client.ObjectKey{Name: namespace}, &ns); err != nil {
				return err
			}
			owned = net.UID != "" && namespaceOwnedByFabricNetwork(ns, net, org) && ns.Annotations[annotationFabricNetworkUID] == string(net.UID)
		}
		if !owned || (uid != "" && uid != string(net.UID)) || !existing.DeletionTimestamp.IsZero() || existing.Type != corev1.SecretTypeOpaque || (existing.Immutable != nil && *existing.Immutable) {
			return fmt.Errorf("enrollment output ownership or mutability requires review; existing identity material was preserved")
		}
	}
	return nil
}

func publisherObjectOwned(existing, desired client.Object) bool {
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
