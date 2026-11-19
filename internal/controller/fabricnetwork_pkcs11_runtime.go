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
	"strings"
	"unicode"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *FabricNetworkReconciler) guardPKCS11NamespaceCleanup(ctx context.Context, net *fabricopsv1alpha1.FabricNetwork) error {
	var namespaces corev1.NamespaceList
	selector := fabricNetworkSelector(net)
	selector[labelAppManagedBy] = managedByValue
	if err := r.List(ctx, &namespaces, selector); err != nil {
		return err
	}
	// Check marker presence, not validity: damaged or stale records need review too.
	for _, namespace := range namespaces.Items {
		if _, retained := namespace.Annotations[annotationPKCS11StateRetention]; retained {
			return fmt.Errorf("PKCS11 token state prevents namespace cleanup; review the retained state in Namespace %s before explicitly releasing its hold", namespace.Name)
		}
	}
	for _, org := range net.Spec.Orgs {
		if org.Peer == nil || org.Peer.MSPSigning == nil || org.Peer.MSPSigning.PKCS11 == nil || org.Peer.MSPSigning.PKCS11.Runtime == nil || len(org.Peer.MSPSigning.PKCS11.Runtime.StateClaims) == 0 {
			continue
		}
		var namespace corev1.Namespace
		err := r.Get(ctx, client.ObjectKey{Name: orgNamespaceName(net, org)}, &namespace)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return err
		}
		if namespaceOwnedByFabricNetwork(namespace, net, org) {
			return fmt.Errorf("PKCS11 token state prevents namespace cleanup; preserve the state and explicitly remove its runtime claim references before deleting the network")
		}
	}
	return nil
}

const (
	pkcs11ConfigVolume = "pkcs11-provider-config"
	pkcs11StateVolume  = "pkcs11-token-state"
	pkcs11MemoryVolume = "pkcs11-runtime"
	pkcs11ConfigPath   = "/etc/fabricops/pkcs11-provider"
	pkcs11MemoryPath   = "/run/fabricops"
	pkcs11StatePath    = "/var/lib/fabricops-hsm/tokens"
)

func validatePKCS11Runtime(org fabricopsv1alpha1.Org) []string {
	if workloadMSPSecretKind(org, componentPeer) != secretKindPeerPKCS11MSP || org.Peer.MSPSigning.PKCS11 == nil {
		return []string{"PKCS11 peer configuration is required"}
	}
	provider := org.Peer.MSPSigning.PKCS11
	if provider.Runtime == nil {
		return []string{"runtime access is required"}
	}
	rt := provider.Runtime
	var problems []string
	for _, image := range []string{rt.EnrollmentImage, rt.PeerImage} {
		if image == "" || len(image) > 253 || strings.ContainsFunc(image, unicode.IsSpace) || strings.ContainsFunc(image, unicode.IsControl) {
			problems = append(problems, "runtime images must be explicit nonempty image references")
		}
	}
	selector := rt.ConfigSecretKeyRef
	if len(validation.IsDNS1123Subdomain(selector.Name)) != 0 || len(validation.IsConfigMapKey(selector.Key)) != 0 || (selector.Optional != nil && *selector.Optional) {
		problems = append(problems, "runtime.configSecretKeyRef must name a required org-local Secret key")
	}
	pin := provider.PINSecretKeyRef
	if len(validation.IsDNS1123Subdomain(pin.Name)) != 0 || len(validation.IsConfigMapKey(pin.Key)) != 0 || (pin.Optional != nil && *pin.Optional) {
		problems = append(problems, "runtime requires a non-optional org-local PIN Secret key")
	}
	if selector.Name == provider.PINSecretKeyRef.Name && selector.Key == provider.PINSecretKeyRef.Key {
		problems = append(problems, "runtime configuration and PIN must select different Secret keys")
	}
	if rt.ConfigEnv != "SOFTHSM2_CONF" {
		problems = append(problems, "runtime.configEnv is not supported by the current provider proof")
	}
	if rt.RunAsUser < 1 || rt.RunAsUser > 2147483647 || rt.FSGroup < 1 || rt.FSGroup > 2147483647 {
		problems = append(problems, "runtime.runAsUser and fsGroup must be explicit non-root IDs")
	}
	expected := map[string]bool{}
	reserved := map[string]bool{sanitizeName(org.Organization.Name + "-ca"): true}
	for _, group := range org.Orderers {
		for i := 0; i < group.Instances; i++ {
			reserved[sanitizeName(fmt.Sprintf("%s%d", group.Prefix, i))] = true
		}
	}
	for i := 0; i < org.Peer.Instances; i++ {
		name := sanitizeName(fmt.Sprintf("%s%d", org.Peer.Prefix, i))
		expected[name], reserved[name], reserved[couchDBName(name)] = true, true, true
		for _, kind := range []string{secretKindMSP, secretKindTLS} {
			output := identitySecretName(name, kind)
			if pin.Name == output || selector.Name == output {
				problems = append(problems, "runtime provider credentials must not alias peer identity output Secrets")
			}
		}
	}
	if len(rt.StateClaims) != len(expected) {
		problems = append(problems, "runtime.stateClaims must assign exactly one claim to every peer")
	}
	seen, claims := map[string]bool{}, map[string]bool{}
	for _, state := range rt.StateClaims {
		if !expected[state.Peer] || seen[state.Peer] {
			problems = append(problems, "runtime.stateClaims contains an unknown or duplicate peer")
		}
		seen[state.Peer] = true
		if len(validation.IsDNS1123Subdomain(state.ClaimName)) != 0 || claims[state.ClaimName] || reserved[state.ClaimName] {
			problems = append(problems, "runtime.stateClaims requires distinct valid claims separate from managed ledger, database, and CA storage")
		}
		claims[state.ClaimName] = true
	}
	return problems
}

// This stages pod templates only. Normal reconciliation is still blocked by
// the capability gate until lifecycle, permissions, and live proofs are complete.
func applyPKCS11Runtime(pod *corev1.PodSpec, org fabricopsv1alpha1.Org, peer string, enrollment bool) error {
	if problems := validatePKCS11Runtime(org); len(problems) > 0 {
		return fmt.Errorf("invalid PKCS11 runtime access: %s", strings.Join(problems, "; "))
	}
	provider := org.Peer.MSPSigning.PKCS11
	rt := provider.Runtime
	claimName := ""
	for _, state := range rt.StateClaims {
		if state.Peer == peer {
			claimName = state.ClaimName
		}
	}
	if claimName == "" {
		return fmt.Errorf("PKCS11 peer state claim is missing")
	}
	mode := int32(0440)
	memory := resource.MustParse("64Mi")
	pod.Volumes = append(pod.Volumes,
		corev1.Volume{Name: pkcs11ConfigVolume, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
			SecretName: rt.ConfigSecretKeyRef.Name, Items: []corev1.KeyToPath{{Key: rt.ConfigSecretKeyRef.Key, Path: "config"}}, DefaultMode: &mode}}},
		corev1.Volume{Name: pkcs11StateVolume, VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claimName}}},
		corev1.Volume{Name: pkcs11MemoryVolume, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: &memory}}},
	)
	user, group, nonRoot := rt.RunAsUser, rt.FSGroup, true
	policy := corev1.FSGroupChangeOnRootMismatch
	pod.SecurityContext = &corev1.PodSecurityContext{RunAsUser: &user, RunAsGroup: &group, FSGroup: &group, RunAsNonRoot: &nonRoot, FSGroupChangePolicy: &policy,
		SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}}
	var actor *corev1.Container
	if enrollment {
		actor = &pod.InitContainers[0]
		actor.Image, actor.Command = rt.EnrollmentImage, []string{"/usr/local/bin/fabricops-enroll-peer"}
		for i := range pod.Volumes {
			if pod.Volumes[i].Name == enrollmentVolumeName {
				pod.Volumes[i].EmptyDir = &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: &memory}
			}
		}
		for i := range pod.Containers[0].VolumeMounts {
			mount := &pod.Containers[0].VolumeMounts[i]
			if mount.Name == enrollmentVolumeName {
				mount.MountPath, mount.SubPath, mount.ReadOnly = enrollmentWorkDir+"/output", "output", true
			}
		}
		publisherUID := int64(1001)
		noPrivilege := false
		pod.Containers[0].SecurityContext = &corev1.SecurityContext{RunAsUser: &publisherUID, RunAsGroup: &group, RunAsNonRoot: &nonRoot, AllowPrivilegeEscalation: &noPrivilege, Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}}
		actor.Env = append(actor.Env, corev1.EnvVar{Name: "FABRICOPS_ENROLLMENT_DIR", Value: enrollmentWorkDir})
		applyPKCS11PublisherAccess(pod, peer)
	} else {
		actor = &pod.Containers[0]
		actor.Image = rt.PeerImage
		actor.Command = []string{"sh", "-ec", "umask 077\nfabricops-pkcs11-config peer /etc/hyperledger/fabric/core.yaml /run/fabricops/core.yaml\nexec peer node start"}
		actor.Env = append(actor.Env, corev1.EnvVar{Name: "FABRIC_CFG_PATH", Value: pkcs11MemoryPath})
		automount := false
		pod.AutomountServiceAccountToken = &automount
	}
	actor.Env = append(actor.Env,
		corev1.EnvVar{Name: "FABRICOPS_PKCS11_LIBRARY", Value: provider.Library},
		corev1.EnvVar{Name: "FABRICOPS_PKCS11_TOKEN_LABEL", Value: provider.TokenLabel},
		corev1.EnvVar{Name: "FABRICOPS_PKCS11_PIN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: provider.PINSecretKeyRef.DeepCopy()}},
		corev1.EnvVar{Name: rt.ConfigEnv, Value: pkcs11ConfigPath + "/config"},
		corev1.EnvVar{Name: "FABRIC_LOGGING_SPEC", Value: "warning"},
		corev1.EnvVar{Name: "TMPDIR", Value: pkcs11MemoryPath},
	)
	actor.VolumeMounts = append(actor.VolumeMounts,
		corev1.VolumeMount{Name: pkcs11ConfigVolume, MountPath: pkcs11ConfigPath, ReadOnly: true},
		corev1.VolumeMount{Name: pkcs11StateVolume, MountPath: pkcs11StatePath},
		corev1.VolumeMount{Name: pkcs11MemoryVolume, MountPath: pkcs11MemoryPath},
	)
	allowPrivilege := false
	actor.SecurityContext = &corev1.SecurityContext{AllowPrivilegeEscalation: &allowPrivilege, Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}}
	return nil
}
