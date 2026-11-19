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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const annotationPKCS11Peer = "fabricops.io/pkcs11-peer"

// API metadata is a prerequisite, not proof that the provider can open the token
// or that the expected key exists. Those checks belong to the key actor.
func (r *FabricNetworkReconciler) validateAndBindPKCS11StateClaim(ctx context.Context, net *fabricopsv1alpha1.FabricNetwork, org fabricopsv1alpha1.Org, namespace, peer string) error {
	if len(validatePKCS11Runtime(org)) > 0 || net.UID == "" || namespace != orgNamespaceName(net, org) {
		return fmt.Errorf("PKCS11 token storage requires valid runtime access and network ownership")
	}
	claimName := ""
	for _, state := range org.Peer.MSPSigning.PKCS11.Runtime.StateClaims {
		if state.Peer == peer {
			claimName = state.ClaimName
		}
	}
	if claimName == "" {
		return fmt.Errorf("PKCS11 token storage requires a declared peer claim")
	}
	var claim corev1.PersistentVolumeClaim
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: claimName}, &claim); err != nil {
		return fmt.Errorf("PKCS11 prepared token claim is unavailable: %w", err)
	}
	if claim.UID == "" || !claim.DeletionTimestamp.IsZero() || claim.Annotations[annotationFabricNetworkUID] != string(net.UID) || claim.Annotations[annotationPKCS11Peer] != peer || metav1.GetControllerOf(&claim) != nil {
		return fmt.Errorf("PKCS11 token claim requires explicit network UID and peer assignment without controller ownership or deletion")
	}
	if claim.Status.Phase != corev1.ClaimBound || claim.Spec.VolumeName == "" {
		return fmt.Errorf("PKCS11 token claim must already be prepared and Bound before enrollment")
	}
	if (claim.Spec.VolumeMode != nil && *claim.Spec.VolumeMode != corev1.PersistentVolumeFilesystem) || len(claim.Spec.AccessModes) != 1 ||
		(claim.Spec.AccessModes[0] != corev1.ReadWriteOnce && claim.Spec.AccessModes[0] != corev1.ReadWriteOncePod) {
		return fmt.Errorf("PKCS11 token storage currently requires a filesystem claim with ReadWriteOnce or ReadWriteOncePod access")
	}
	return r.updateOrgPKCS11StateRetention(ctx, net, org, map[string]pkcs11ClaimBinding{claim.Name: {UID: claim.UID, Peer: peer}})
}
