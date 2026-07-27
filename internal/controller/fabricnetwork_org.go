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
	"hash/fnv"
	"maps"
	"reflect"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	fabricopsv1alpha1 "github.com/dpereowei/fabricops/api/v1alpha1"
)

const (
	maxKubernetesNameLength = 63

	labelFabricNetwork          = "fabricops.io/fabricnetwork"
	labelFabricNetworkNamespace = "fabricops.io/fabricnetwork-namespace"
	labelOrg                    = "fabricops.io/org"
	labelComponent              = "fabricops.io/component"
	labelOrdererGroup           = "fabricops.io/orderer-group"
	labelInstance               = "fabricops.io/instance"
	labelIdentityKind           = "fabricops.io/identity-kind"
	labelIdentitySource         = "fabricops.io/identity-source"
	labelWorkload               = "fabricops.io/workload"
	labelChannel                = "fabricops.io/channel"
	labelChaincode              = "fabricops.io/chaincode"
	labelEndpoint               = "fabricops.io/endpoint"

	labelAppName      = "app.kubernetes.io/name"
	labelAppManagedBy = "app.kubernetes.io/managed-by"
	labelAppPartOf    = "app.kubernetes.io/part-of"
	labelAppComponent = "app.kubernetes.io/component"

	annotationManagedBy              = "fabricops.io/managed-by"
	annotationFabricNetwork          = "fabricops.io/fabricnetwork"
	annotationFabricNetworkNamespace = "fabricops.io/fabricnetwork-namespace"
	annotationFabricNetworkUID       = "fabricops.io/fabricnetwork-uid"
	annotationOrg                    = "fabricops.io/org"

	appName         = "fabricops"
	managedByValue  = "fabricops"
	controllerName  = "fabricops-controller"
	resourceProfile = "resource-profile"

	componentCA        = "ca"
	componentAdmin     = "admin"
	componentChannel   = "channel"
	componentChaincode = "chaincode"
	componentMonitor   = "monitor"
	componentNetwork   = "network"
	componentOrderer   = "orderer"
	componentPeer      = "peer"
	componentKubectl   = "kubectl"

	endpointOperations = "operations"

	containerCA      = "fabric-ca"
	containerOrderer = "orderer"
	containerPeer    = "peer"

	caPort            int32 = 7054
	ordererPort       int32 = 7050
	ordererAdminPort  int32 = 9443
	ordererOpsPort    int32 = 8443
	peerPort          int32 = 7051
	peerChaincodePort int32 = 7052
	peerOpsPort       int32 = 9443

	caBootstrapEnvVar  = "FABRIC_CA_SERVER_BOOTSTRAP_USER_PASS"
	ccaasBuilderEnvVar = "CHAINCODE_AS_A_SERVICE_BUILDER_CONFIG"

	ordererMSPPath = "/var/hyperledger/orderer/msp"
	ordererTLSPath = "/var/hyperledger/orderer/tls"
	peerMSPPath    = "/etc/hyperledger/fabric/peer/msp"
	peerTLSPath    = "/etc/hyperledger/fabric/peer/tls"

	dataVolumeName        = "data"
	caHomePath            = "/etc/hyperledger/fabric-ca-server"
	fabricProductionPath  = "/var/hyperledger/production"
	defaultCAStorage      = "1Gi"
	defaultOrdererStorage = "5Gi"
	defaultPeerStorage    = "10Gi"

	defaultCARequestCPU      = "100m"
	defaultCARequestMemory   = "128Mi"
	defaultCALimitCPU        = "500m"
	defaultCALimitMemory     = "512Mi"
	defaultOrdererRequestCPU = "250m"
	defaultOrdererRequestMem = "256Mi"
	defaultOrdererLimitCPU   = "1"
	defaultOrdererLimitMem   = "1Gi"
	defaultPeerRequestCPU    = "250m"
	defaultPeerRequestMem    = "512Mi"
	defaultPeerLimitCPU      = "1"
	defaultPeerLimitMem      = "1Gi"
	defaultKubectlRequestCPU = "50m"
	defaultKubectlRequestMem = "64Mi"
	defaultKubectlLimitCPU   = "250m"
	defaultKubectlLimitMem   = "128Mi"
	defaultScrapeInterval    = "30s"
	defaultScrapeTimeout     = "10s"
	orgBoundaryPolicyName    = "fabricops-org-boundary"

	secretKindMSP = "msp"
	secretKindTLS = "tls"

	identitySourceFabricCA = "fabric-ca"

	mspConfigKey    = "config.yaml"
	mspCACertKey    = "cacert.pem"
	mspTLSCACertKey = "tlscacert.pem"
	mspSignCertKey  = "signcert.pem"
	mspKeyStoreKey  = "keystore.pem"

	caBootstrapUsernameKey = "username"
	caBootstrapPasswordKey = "password"
	caBootstrapUserPassKey = "user-pass"

	tlsCACertKey     = "ca.crt"
	tlsServerCertKey = "server.crt"
	tlsServerKeyKey  = "server.key"
	tlsClientCertKey = "client.crt"
	tlsClientKeyKey  = "client.key"
)

func secretVolumeDefaultMode() *int32 {
	mode := int32(0644)
	return &mode
}

func sanitizeName(name string) string {
	var b strings.Builder
	lastDash := false

	for _, r := range strings.ToLower(strings.TrimSpace(name)) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastDash = false
			continue
		}

		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}

	s := strings.Trim(b.String(), "-")
	if s == "" {
		s = "resource"
	}

	if len(s) <= maxKubernetesNameLength {
		return s
	}

	hash := fnv.New32a()
	_, _ = hash.Write([]byte(s))
	suffix := fmt.Sprintf("%08x", hash.Sum32())
	prefixLength := maxKubernetesNameLength - len(suffix) - 1
	prefix := strings.TrimRight(s[:prefixLength], "-")
	if prefix == "" {
		prefix = s[:prefixLength]
	}

	return prefix + "-" + suffix
}

func networkNamespaceSlug(net *fabricopsv1alpha1.FabricNetwork) string {
	networkName := compactNetworkName(net.Name)
	controlNamespace := sanitizeName(net.Namespace)

	if controlNamespace == "" || controlNamespace == "default" {
		return networkName
	}

	return sanitizeName(controlNamespace + "-" + networkName)
}

func compactNetworkName(name string) string {
	s := sanitizeName(name)
	for _, prefix := range []string{"fabricnetwork-", "fabric-network-", "network-"} {
		if trimmed := strings.TrimPrefix(s, prefix); trimmed != s && trimmed != "" {
			return trimmed
		}
	}

	return s
}

func serviceDNS(name, namespace string, port int32) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local:%d", name, namespace, port)
}

func identitySecretName(workloadName, kind string) string {
	return sanitizeName(fmt.Sprintf("%s-%s", workloadName, kind))
}

func baseLabels(net *fabricopsv1alpha1.FabricNetwork) map[string]string {
	return map[string]string{
		labelFabricNetwork:          sanitizeName(net.Name),
		labelFabricNetworkNamespace: sanitizeName(net.Namespace),
		labelAppName:                appName,
		labelAppManagedBy:           managedByValue,
		labelAppPartOf:              sanitizeName(net.Name),
	}
}

func resourceAnnotations(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
) map[string]string {
	annotations := map[string]string{
		annotationManagedBy:              controllerName,
		annotationFabricNetwork:          net.Name,
		annotationFabricNetworkNamespace: net.Namespace,
		annotationOrg:                    org.Organization.Name,
	}

	if net.UID != "" {
		annotations[annotationFabricNetworkUID] = string(net.UID)
	}

	return annotations
}

func mergeMap(base map[string]string, extra map[string]string) map[string]string {
	out := map[string]string{}
	maps.Copy(out, base)
	maps.Copy(out, extra)
	return out
}

func componentResourceRequirements(component string) corev1.ResourceRequirements {
	switch component {
	case componentCA:
		return resourceRequirements(defaultCARequestCPU, defaultCARequestMemory, defaultCALimitCPU, defaultCALimitMemory)
	case componentOrderer:
		return resourceRequirements(defaultOrdererRequestCPU, defaultOrdererRequestMem, defaultOrdererLimitCPU, defaultOrdererLimitMem)
	case componentPeer:
		return resourceRequirements(defaultPeerRequestCPU, defaultPeerRequestMem, defaultPeerLimitCPU, defaultPeerLimitMem)
	case componentKubectl:
		return resourceRequirements(defaultKubectlRequestCPU, defaultKubectlRequestMem, defaultKubectlLimitCPU, defaultKubectlLimitMem)
	default:
		return resourceRequirements(defaultCARequestCPU, defaultCARequestMemory, defaultCALimitCPU, defaultCALimitMemory)
	}
}

func resourceRequirements(requestCPU, requestMemory, limitCPU, limitMemory string) corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(requestCPU),
			corev1.ResourceMemory: resource.MustParse(requestMemory),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(limitCPU),
			corev1.ResourceMemory: resource.MustParse(limitMemory),
		},
	}
}

func mspSecretItems(tlsEnabled bool) []corev1.KeyToPath {
	items := []corev1.KeyToPath{
		{Key: mspConfigKey, Path: "config.yaml"},
		{Key: mspCACertKey, Path: "cacerts/ca.pem"},
		{Key: mspSignCertKey, Path: "signcerts/cert.pem"},
		{Key: mspKeyStoreKey, Path: "keystore/key.pem"},
	}

	if tlsEnabled {
		items = append(items, corev1.KeyToPath{
			Key:  mspTLSCACertKey,
			Path: "tlscacerts/tlsca.pem",
		})
	}

	return items
}

func tlsSecretItems() []corev1.KeyToPath {
	return []corev1.KeyToPath{
		{Key: tlsCACertKey, Path: "ca.crt"},
		{Key: tlsServerCertKey, Path: "server.crt"},
		{Key: tlsServerKeyKey, Path: "server.key"},
	}
}

func identityVolumes(workloadName string, tlsEnabled bool) []corev1.Volume {
	volumes := []corev1.Volume{
		{
			Name: secretKindMSP,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  identitySecretName(workloadName, secretKindMSP),
					Items:       mspSecretItems(tlsEnabled),
					DefaultMode: secretVolumeDefaultMode(),
				},
			},
		},
	}

	if tlsEnabled {
		volumes = append(volumes, corev1.Volume{
			Name: secretKindTLS,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  identitySecretName(workloadName, secretKindTLS),
					Items:       tlsSecretItems(),
					DefaultMode: secretVolumeDefaultMode(),
				},
			},
		})
	}

	return volumes
}

func identityVolumeMounts(mspPath, tlsPath string, tlsEnabled bool) []corev1.VolumeMount {
	mounts := []corev1.VolumeMount{
		{
			Name:      secretKindMSP,
			MountPath: mspPath,
			ReadOnly:  true,
		},
	}

	if tlsEnabled {
		mounts = append(mounts, corev1.VolumeMount{
			Name:      secretKindTLS,
			MountPath: tlsPath,
			ReadOnly:  true,
		})
	}

	return mounts
}

func dataPVCName(workloadName string) string {
	return sanitizeName(workloadName + "-data")
}

func dataVolume(workloadName string) corev1.Volume {
	return corev1.Volume{
		Name: dataVolumeName,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: dataPVCName(workloadName),
			},
		},
	}
}

func dataVolumeMount(path string) corev1.VolumeMount {
	return corev1.VolumeMount{
		Name:      dataVolumeName,
		MountPath: path,
	}
}

type storageSettings struct {
	size             string
	storageClassName *string
}

func storageSettingsForComponent(
	net *fabricopsv1alpha1.FabricNetwork,
	component string,
) storageSettings {
	settings := storageSettings{
		size: defaultStorageSize(component),
	}

	if net.Spec.Global.Storage == nil {
		return settings
	}

	config := storageConfigForComponent(net.Spec.Global.Storage, component)
	if config == nil {
		return settings
	}
	if config.Size != "" {
		settings.size = config.Size
	}
	settings.storageClassName = config.StorageClassName

	return settings
}

func defaultStorageSize(component string) string {
	switch component {
	case componentCA:
		return defaultCAStorage
	case componentOrderer:
		return defaultOrdererStorage
	case componentPeer:
		return defaultPeerStorage
	default:
		return defaultPeerStorage
	}
}

func storageConfigForComponent(
	config *fabricopsv1alpha1.StorageConfig,
	component string,
) *fabricopsv1alpha1.ComponentStorageConfig {
	switch component {
	case componentCA:
		return config.CA
	case componentOrderer:
		return config.Orderer
	case componentPeer:
		return config.Peer
	default:
		return nil
	}
}

type identitySecretRequirement struct {
	namespace string
	name      string
	kind      string
	keys      []string
}

func mspSecretKeys(tlsEnabled bool) []string {
	keys := []string{
		mspConfigKey,
		mspCACertKey,
		mspSignCertKey,
		mspKeyStoreKey,
	}

	if tlsEnabled {
		keys = append(keys, mspTLSCACertKey)
	}

	return keys
}

func tlsSecretKeys() []string {
	return []string{
		tlsCACertKey,
		tlsServerCertKey,
		tlsServerKeyKey,
	}
}

func adminTLSSecretKeys() []string {
	return []string{
		tlsCACertKey,
		tlsClientCertKey,
		tlsClientKeyKey,
	}
}

func caBootstrapSecretKeys() []string {
	return []string{
		caBootstrapUsernameKey,
		caBootstrapPasswordKey,
		caBootstrapUserPassKey,
	}
}

func requiredIdentitySecrets(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
) []identitySecretRequirement {
	tlsEnabled := net.Spec.Global.TLS
	requirements := []identitySecretRequirement{}
	adminName := adminIdentityName(org)

	requirements = append(requirements, identitySecretRequirement{
		namespace: namespace,
		name:      caBootstrapSecretName(org),
		kind:      secretKindCABootstrap,
		keys:      caBootstrapSecretKeys(),
	})
	requirements = append(requirements, identitySecretRequirement{
		namespace: namespace,
		name:      adminEnrollmentSecretName(org),
		kind:      secretKindAdminEnroll,
		keys:      caBootstrapSecretKeys(),
	})

	requirements = append(requirements, identitySecretRequirement{
		namespace: namespace,
		name:      identitySecretName(adminName, secretKindMSP),
		kind:      secretKindAdminMSP,
		keys:      mspSecretKeys(tlsEnabled),
	})

	if tlsEnabled {
		requirements = append(requirements, identitySecretRequirement{
			namespace: namespace,
			name:      identitySecretName(adminName, secretKindTLS),
			kind:      secretKindAdminTLS,
			keys:      adminTLSSecretKeys(),
		})
	}

	for _, group := range org.Orderers {
		for i := 0; i < group.Instances; i++ {
			name := sanitizeName(fmt.Sprintf("%s%d", group.Prefix, i))
			requirements = append(requirements, identitySecretRequirement{
				namespace: namespace,
				name:      identityEnrollmentSecretName(name),
				kind:      secretKindWorkloadEnroll,
				keys:      caBootstrapSecretKeys(),
			})
			requirements = append(requirements, identitySecretRequirement{
				namespace: namespace,
				name:      identitySecretName(name, secretKindMSP),
				kind:      secretKindMSP,
				keys:      mspSecretKeys(tlsEnabled),
			})

			if tlsEnabled {
				requirements = append(requirements, identitySecretRequirement{
					namespace: namespace,
					name:      identitySecretName(name, secretKindTLS),
					kind:      secretKindTLS,
					keys:      tlsSecretKeys(),
				})
			}
		}
	}

	if org.Peer == nil {
		return requirements
	}

	for i := 0; i < org.Peer.Instances; i++ {
		name := sanitizeName(fmt.Sprintf("%s%d", org.Peer.Prefix, i))
		requirements = append(requirements, identitySecretRequirement{
			namespace: namespace,
			name:      identityEnrollmentSecretName(name),
			kind:      secretKindWorkloadEnroll,
			keys:      caBootstrapSecretKeys(),
		})
		requirements = append(requirements, identitySecretRequirement{
			namespace: namespace,
			name:      identitySecretName(name, secretKindMSP),
			kind:      secretKindMSP,
			keys:      mspSecretKeys(tlsEnabled),
		})

		if tlsEnabled {
			requirements = append(requirements, identitySecretRequirement{
				namespace: namespace,
				name:      identitySecretName(name, secretKindTLS),
				kind:      secretKindTLS,
				keys:      tlsSecretKeys(),
			})
		}
	}

	return requirements
}

func missingSecretKeys(secret corev1.Secret, keys []string) []string {
	missing := []string{}
	for _, key := range keys {
		if _, ok := secret.Data[key]; !ok {
			missing = append(missing, key)
		}
	}

	return missing
}

func (r *FabricNetworkReconciler) identityMaterialStatus(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
) (bool, string, error) {
	missing := []string{}

	for _, requirement := range requiredIdentitySecrets(net, org, namespace) {
		var secret corev1.Secret
		err := r.Get(ctx, client.ObjectKey{
			Namespace: requirement.namespace,
			Name:      requirement.name,
		}, &secret)
		if apierrors.IsNotFound(err) {
			missing = append(missing, fmt.Sprintf("%s/%s", requirement.namespace, requirement.name))
			continue
		}
		if err != nil {
			return false, "", err
		}

		missingKeys := missingSecretKeys(secret, requirement.keys)
		if len(missingKeys) > 0 {
			missing = append(missing, fmt.Sprintf("%s/%s missing keys: %s", requirement.namespace, requirement.name, strings.Join(missingKeys, ",")))
			continue
		}

		if validationError := identitySecretValidationError(secret, requirement.kind, net.Spec.Global.TLS); validationError != "" {
			missing = append(missing, fmt.Sprintf("%s/%s invalid: %s", requirement.namespace, requirement.name, validationError))
		}
	}

	if len(missing) > 0 {
		return false, "Missing identity material: " + strings.Join(missing, "; "), nil
	}

	return true, "", nil
}

func orgLabels(net *fabricopsv1alpha1.FabricNetwork, org fabricopsv1alpha1.Org, component string) map[string]string {
	labels := baseLabels(net)
	labels[labelOrg] = sanitizeName(org.Organization.Name)
	labels[labelComponent] = component
	labels[labelAppComponent] = component
	return labels
}

func operationsServiceName(workloadName string) string {
	return sanitizeName(workloadName + "-operations")
}

func operationsServiceLabels(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	component string,
	workloadName string,
) map[string]string {
	labels := orgLabels(net, org, component)
	labels[labelEndpoint] = endpointOperations
	labels[labelWorkload] = workloadName
	labels[labelAppComponent] = component + "-operations"
	return labels
}

func serviceMonitorEnabled(net *fabricopsv1alpha1.FabricNetwork) bool {
	return net.Spec.Global.Observability != nil &&
		net.Spec.Global.Observability.ServiceMonitor != nil &&
		net.Spec.Global.Observability.ServiceMonitor.Enabled
}

func networkPolicyEnabled(net *fabricopsv1alpha1.FabricNetwork) bool {
	return net.Spec.Global.NetworkPolicy != nil && net.Spec.Global.NetworkPolicy.Enabled
}

func serviceMonitorName(net *fabricopsv1alpha1.FabricNetwork, org fabricopsv1alpha1.Org) string {
	return sanitizeName(networkNamespaceSlug(net) + "-" + org.Organization.Name + "-operations")
}

func serviceMonitorInterval(net *fabricopsv1alpha1.FabricNetwork) string {
	if net.Spec.Global.Observability != nil &&
		net.Spec.Global.Observability.ServiceMonitor != nil &&
		net.Spec.Global.Observability.ServiceMonitor.Interval != "" {
		return net.Spec.Global.Observability.ServiceMonitor.Interval
	}

	return defaultScrapeInterval
}

func serviceMonitorScrapeTimeout(net *fabricopsv1alpha1.FabricNetwork) string {
	if net.Spec.Global.Observability != nil &&
		net.Spec.Global.Observability.ServiceMonitor != nil &&
		net.Spec.Global.Observability.ServiceMonitor.ScrapeTimeout != "" {
		return net.Spec.Global.Observability.ServiceMonitor.ScrapeTimeout
	}

	return defaultScrapeTimeout
}

func serviceMonitorMetadataLabels(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
) map[string]string {
	labels := map[string]string{}
	if net.Spec.Global.Observability != nil &&
		net.Spec.Global.Observability.ServiceMonitor != nil {
		maps.Copy(labels, net.Spec.Global.Observability.ServiceMonitor.Labels)
	}

	return mergeMap(labels, orgLabels(net, org, componentMonitor))
}

func serviceMonitorSelectorLabels(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
) map[string]string {
	return map[string]string{
		labelFabricNetwork:          sanitizeName(net.Name),
		labelFabricNetworkNamespace: sanitizeName(net.Namespace),
		labelOrg:                    sanitizeName(org.Organization.Name),
		labelEndpoint:               endpointOperations,
	}
}

func orgBoundaryNetworkPolicyName() string {
	return orgBoundaryPolicyName
}

func fabricNetworkNamespaceSelectorLabels(net *fabricopsv1alpha1.FabricNetwork) map[string]string {
	return map[string]string{
		labelFabricNetwork:          sanitizeName(net.Name),
		labelFabricNetworkNamespace: sanitizeName(net.Namespace),
		labelAppManagedBy:           managedByValue,
	}
}

func fabricNetworkPodSelectorLabels(net *fabricopsv1alpha1.FabricNetwork) map[string]string {
	return baseLabels(net)
}

func orgPodSelectorLabels(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
) map[string]string {
	labels := fabricNetworkPodSelectorLabels(net)
	labels[labelOrg] = sanitizeName(org.Organization.Name)
	return labels
}

func sameFabricNetworkPeer(net *fabricopsv1alpha1.FabricNetwork) networkingv1.NetworkPolicyPeer {
	return networkingv1.NetworkPolicyPeer{
		NamespaceSelector: &metav1.LabelSelector{
			MatchLabels: fabricNetworkNamespaceSelectorLabels(net),
		},
		PodSelector: &metav1.LabelSelector{
			MatchLabels: fabricNetworkPodSelectorLabels(net),
		},
	}
}

func kubeSystemNamespacePeer() networkingv1.NetworkPolicyPeer {
	return networkingv1.NetworkPolicyPeer{
		NamespaceSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{
				"kubernetes.io/metadata.name": "kube-system",
			},
		},
	}
}

func networkPolicyPort(protocol corev1.Protocol, port int32) networkingv1.NetworkPolicyPort {
	portValue := intstr.FromInt32(port)
	return networkingv1.NetworkPolicyPort{
		Protocol: &protocol,
		Port:     &portValue,
	}
}

func tcpReadinessProbe(port int32) *corev1.Probe {
	return tcpSocketProbe(port, 5, 10, 2, 6)
}

func tcpLivenessProbe(port int32) *corev1.Probe {
	return tcpSocketProbe(port, 30, 20, 2, 3)
}

func tcpSocketProbe(
	port int32,
	initialDelaySeconds int32,
	periodSeconds int32,
	timeoutSeconds int32,
	failureThreshold int32,
) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{
				Port: intstr.FromInt32(port),
			},
		},
		InitialDelaySeconds: initialDelaySeconds,
		PeriodSeconds:       periodSeconds,
		TimeoutSeconds:      timeoutSeconds,
		SuccessThreshold:    1,
		FailureThreshold:    failureThreshold,
	}
}

func operationsReadinessProbe() *corev1.Probe {
	return operationsHealthProbe(5, 10, 2, 6)
}

func operationsHealthProbe(
	initialDelaySeconds int32,
	periodSeconds int32,
	timeoutSeconds int32,
	failureThreshold int32,
) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   "/healthz",
				Port:   intstr.FromString(endpointOperations),
				Scheme: corev1.URISchemeHTTP,
			},
		},
		InitialDelaySeconds: initialDelaySeconds,
		PeriodSeconds:       periodSeconds,
		TimeoutSeconds:      timeoutSeconds,
		SuccessThreshold:    1,
		FailureThreshold:    failureThreshold,
	}
}

func buildOrgNetworkPolicy(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
) *networkingv1.NetworkPolicy {
	tcp := corev1.ProtocolTCP
	udp := corev1.ProtocolUDP
	sameNetworkPeer := sameFabricNetworkPeer(net)

	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:        orgBoundaryNetworkPolicyName(),
			Namespace:   namespace,
			Labels:      orgLabels(net, org, componentNetwork),
			Annotations: resourceAnnotations(net, org),
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: orgPodSelectorLabels(net, org),
			},
			PolicyTypes: []networkingv1.PolicyType{
				networkingv1.PolicyTypeIngress,
				networkingv1.PolicyTypeEgress,
			},
			Ingress: []networkingv1.NetworkPolicyIngressRule{
				{
					From: []networkingv1.NetworkPolicyPeer{sameNetworkPeer},
				},
			},
			Egress: []networkingv1.NetworkPolicyEgressRule{
				{
					To: []networkingv1.NetworkPolicyPeer{sameNetworkPeer},
				},
				{
					To: []networkingv1.NetworkPolicyPeer{kubeSystemNamespacePeer()},
					Ports: []networkingv1.NetworkPolicyPort{
						networkPolicyPort(udp, 53),
						networkPolicyPort(tcp, 53),
					},
				},
				{
					Ports: []networkingv1.NetworkPolicyPort{
						networkPolicyPort(tcp, 443),
						networkPolicyPort(tcp, 6443),
					},
				},
			},
		},
	}
}

func stringMapInterface(values map[string]string) map[string]any {
	out := map[string]any{}
	for key, value := range values {
		out[key] = value
	}
	return out
}

func caImage() string {
	return "hyperledger/fabric-ca:1.5.15"
}

func fabricComponentImage(component, version string) string {
	if version == "" {
		version = "2.5.12"
	}
	return fmt.Sprintf("hyperledger/fabric-%s:%s", component, version)
}

func (r *FabricNetworkReconciler) ensureDeployment(
	ctx context.Context,
	desired *appsv1.Deployment,
) error {
	var existing appsv1.Deployment
	key := client.ObjectKeyFromObject(desired)

	err := r.Get(ctx, key, &existing)
	if err == nil {
		return r.updateObjectWithRetry(ctx, desired, func(object client.Object) (bool, error) {
			existing := object.(*appsv1.Deployment)
			changed := false
			if existing.Labels == nil {
				existing.Labels = map[string]string{}
				changed = true
			}
			for key, value := range desired.Labels {
				if existing.Labels[key] != value {
					existing.Labels[key] = value
					changed = true
				}
			}
			if mergeAnnotations(&existing.Annotations, desired.Annotations) {
				changed = true
			}
			if !reflect.DeepEqual(existing.Spec.Replicas, desired.Spec.Replicas) {
				existing.Spec.Replicas = desired.Spec.Replicas
				changed = true
			}
			if !reflect.DeepEqual(existing.Spec.Strategy, desired.Spec.Strategy) {
				existing.Spec.Strategy = desired.Spec.Strategy
				changed = true
			}
			if !reflect.DeepEqual(existing.Spec.Template.Labels, desired.Spec.Template.Labels) {
				existing.Spec.Template.Labels = desired.Spec.Template.Labels
				changed = true
			}
			if !reflect.DeepEqual(existing.Spec.Template.Annotations, desired.Spec.Template.Annotations) {
				existing.Spec.Template.Annotations = desired.Spec.Template.Annotations
				changed = true
			}
			if containers, containerChanged := syncManagedContainers(existing.Spec.Template.Spec.Containers, desired.Spec.Template.Spec.Containers); containerChanged {
				existing.Spec.Template.Spec.Containers = containers
				changed = true
			}
			if !reflect.DeepEqual(existing.Spec.Template.Spec.Volumes, desired.Spec.Template.Spec.Volumes) {
				existing.Spec.Template.Spec.Volumes = desired.Spec.Template.Spec.Volumes
				changed = true
			}
			if !changed {
				return false, nil
			}

			log := logf.FromContext(ctx)
			log.Info("Updating Deployment", "name", desired.Name, "namespace", desired.Namespace)
			return true, nil
		})
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	log := logf.FromContext(ctx)
	log.Info("Creating Deployment", "name", desired.Name, "namespace", desired.Namespace)
	return r.Create(ctx, desired)
}

func syncManagedContainers(existing []corev1.Container, desired []corev1.Container) ([]corev1.Container, bool) {
	if len(existing) != len(desired) {
		return desired, true
	}

	changed := false
	containers := append([]corev1.Container(nil), existing...)
	for i := range desired {
		if containers[i].Name != desired[i].Name {
			containers[i] = desired[i]
			changed = true
			continue
		}

		if containers[i].Image != desired[i].Image {
			containers[i].Image = desired[i].Image
			changed = true
		}
		if containers[i].ImagePullPolicy != desired[i].ImagePullPolicy {
			containers[i].ImagePullPolicy = desired[i].ImagePullPolicy
			changed = true
		}
		if !reflect.DeepEqual(containers[i].Command, desired[i].Command) {
			containers[i].Command = desired[i].Command
			changed = true
		}
		if !reflect.DeepEqual(containers[i].Args, desired[i].Args) {
			containers[i].Args = desired[i].Args
			changed = true
		}
		if !reflect.DeepEqual(containers[i].Env, desired[i].Env) {
			containers[i].Env = desired[i].Env
			changed = true
		}
		if !reflect.DeepEqual(containers[i].Ports, desired[i].Ports) {
			containers[i].Ports = desired[i].Ports
			changed = true
		}
		if !reflect.DeepEqual(containers[i].ReadinessProbe, desired[i].ReadinessProbe) {
			containers[i].ReadinessProbe = desired[i].ReadinessProbe
			changed = true
		}
		if !reflect.DeepEqual(containers[i].LivenessProbe, desired[i].LivenessProbe) {
			containers[i].LivenessProbe = desired[i].LivenessProbe
			changed = true
		}
		if !reflect.DeepEqual(containers[i].VolumeMounts, desired[i].VolumeMounts) {
			containers[i].VolumeMounts = desired[i].VolumeMounts
			changed = true
		}
		if !reflect.DeepEqual(containers[i].Resources, desired[i].Resources) {
			containers[i].Resources = desired[i].Resources
			changed = true
		}
	}

	return containers, changed
}

func buildDataPVC(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	workloadName string,
	component string,
) (*corev1.PersistentVolumeClaim, error) {
	settings := storageSettingsForComponent(net, component)
	quantity, err := resource.ParseQuantity(settings.size)
	if err != nil {
		return nil, fmt.Errorf("invalid %s storage size %q: %w", component, settings.size, err)
	}

	labels := orgLabels(net, org, component)
	labels[labelWorkload] = workloadName

	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        dataPVCName(workloadName),
			Namespace:   namespace,
			Labels:      labels,
			Annotations: resourceAnnotations(net, org),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			StorageClassName: settings.storageClassName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: quantity,
				},
			},
		},
	}, nil
}

func (r *FabricNetworkReconciler) ensurePersistentVolumeClaim(
	ctx context.Context,
	desired *corev1.PersistentVolumeClaim,
) error {
	var existing corev1.PersistentVolumeClaim
	key := client.ObjectKeyFromObject(desired)

	err := r.Get(ctx, key, &existing)
	if err == nil {
		return r.updateObjectWithRetry(ctx, desired, func(object client.Object) (bool, error) {
			existing := object.(*corev1.PersistentVolumeClaim)
			changed := false
			if existing.Labels == nil {
				existing.Labels = map[string]string{}
				changed = true
			}
			for key, value := range desired.Labels {
				if existing.Labels[key] != value {
					existing.Labels[key] = value
					changed = true
				}
			}
			if mergeAnnotations(&existing.Annotations, desired.Annotations) {
				changed = true
			}
			if !changed {
				return false, nil
			}

			log := logf.FromContext(ctx)
			log.Info("Updating PersistentVolumeClaim", "name", desired.Name, "namespace", desired.Namespace)
			return true, nil
		})
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	log := logf.FromContext(ctx)
	log.Info("Creating PersistentVolumeClaim", "name", desired.Name, "namespace", desired.Namespace)
	return r.Create(ctx, desired)
}

func (r *FabricNetworkReconciler) ensureService(
	ctx context.Context,
	desired *corev1.Service,
) error {
	var existing corev1.Service
	key := client.ObjectKeyFromObject(desired)

	err := r.Get(ctx, key, &existing)
	if err == nil {
		return r.updateObjectWithRetry(ctx, desired, func(object client.Object) (bool, error) {
			existing := object.(*corev1.Service)
			changed := mergeLabels(&existing.Labels, desired.Labels)
			if mergeAnnotations(&existing.Annotations, desired.Annotations) {
				changed = true
			}
			if !reflect.DeepEqual(existing.Spec.Selector, desired.Spec.Selector) {
				existing.Spec.Selector = desired.Spec.Selector
				changed = true
			}
			if !reflect.DeepEqual(existing.Spec.Ports, desired.Spec.Ports) {
				existing.Spec.Ports = desired.Spec.Ports
				changed = true
			}
			if !changed {
				return false, nil
			}

			log := logf.FromContext(ctx)
			log.Info("Updating Service", "name", desired.Name, "namespace", desired.Namespace)
			return true, nil
		})
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	log := logf.FromContext(ctx)
	log.Info("Creating Service", "name", desired.Name, "namespace", desired.Namespace)
	return r.Create(ctx, desired)
}

func (r *FabricNetworkReconciler) ensureNetworkPolicy(
	ctx context.Context,
	desired *networkingv1.NetworkPolicy,
) error {
	var existing networkingv1.NetworkPolicy
	key := client.ObjectKeyFromObject(desired)

	err := r.Get(ctx, key, &existing)
	if err == nil {
		return r.updateObjectWithRetry(ctx, desired, func(object client.Object) (bool, error) {
			existing := object.(*networkingv1.NetworkPolicy)
			changed := mergeLabels(&existing.Labels, desired.Labels)
			if mergeAnnotations(&existing.Annotations, desired.Annotations) {
				changed = true
			}
			if !reflect.DeepEqual(existing.Spec, desired.Spec) {
				existing.Spec = desired.Spec
				changed = true
			}
			if !changed {
				return false, nil
			}

			log := logf.FromContext(ctx)
			log.Info("Updating NetworkPolicy", "name", desired.Name, "namespace", desired.Namespace)
			return true, nil
		})
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	log := logf.FromContext(ctx)
	log.Info("Creating NetworkPolicy", "name", desired.Name, "namespace", desired.Namespace)
	return r.Create(ctx, desired)
}

func (r *FabricNetworkReconciler) ensureNetworkPolicyAbsent(
	ctx context.Context,
	desired *networkingv1.NetworkPolicy,
) error {
	var existing networkingv1.NetworkPolicy
	key := client.ObjectKeyFromObject(desired)

	if err := r.Get(ctx, key, &existing); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !hasFabricNetworkOwner(&existing) {
		return nil
	}
	if err := ensureSameFabricNetworkOwner(&existing, desired); err != nil {
		return err
	}

	log := logf.FromContext(ctx)
	log.Info("Deleting NetworkPolicy", "name", desired.Name, "namespace", desired.Namespace)
	return r.Delete(ctx, &existing)
}

func (r *FabricNetworkReconciler) deleteOwnedObject(
	ctx context.Context,
	object client.Object,
	expectedOwner client.Object,
	opts ...client.DeleteOption,
) error {
	if !hasFabricNetworkOwner(object) {
		return nil
	}
	if err := ensureSameFabricNetworkOwner(object, expectedOwner); err != nil {
		return err
	}

	log := logf.FromContext(ctx)
	log.Info("Deleting object", "object", objectDescription(object))
	return client.IgnoreNotFound(r.Delete(ctx, object, opts...))
}

func hasFabricNetworkOwner(object client.Object) bool {
	if _, ok := fabricNetworkOwnerFromAnnotations(object); ok {
		return true
	}
	if _, ok := fabricNetworkOwnerFromLabels(object); ok {
		return true
	}
	return false
}

func buildOrgServiceMonitor(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
) *unstructured.Unstructured {
	serviceMonitor := &unstructured.Unstructured{}
	serviceMonitor.SetAPIVersion("monitoring.coreos.com/v1")
	serviceMonitor.SetKind("ServiceMonitor")
	serviceMonitor.SetName(serviceMonitorName(net, org))
	serviceMonitor.SetNamespace(namespace)
	serviceMonitor.SetLabels(serviceMonitorMetadataLabels(net, org))
	serviceMonitor.SetAnnotations(resourceAnnotations(net, org))

	endpoint := map[string]any{
		"port":            endpointOperations,
		"path":            "/metrics",
		"scheme":          "http",
		"interval":        serviceMonitorInterval(net),
		"scrapeTimeout":   serviceMonitorScrapeTimeout(net),
		"honorLabels":     true,
		"honorTimestamps": true,
	}
	spec := map[string]any{
		"selector": map[string]any{
			"matchLabels": stringMapInterface(serviceMonitorSelectorLabels(net, org)),
		},
		"endpoints": []any{endpoint},
	}
	serviceMonitor.Object["spec"] = spec

	return serviceMonitor
}

func (r *FabricNetworkReconciler) ensureServiceMonitor(
	ctx context.Context,
	desired *unstructured.Unstructured,
) error {
	var existing unstructured.Unstructured
	existing.SetAPIVersion(desired.GetAPIVersion())
	existing.SetKind(desired.GetKind())

	err := r.Get(ctx, client.ObjectKeyFromObject(desired), &existing)
	if err == nil {
		return r.updateObjectWithRetry(ctx, desired, func(object client.Object) (bool, error) {
			existing := object.(*unstructured.Unstructured)
			changed := false
			labels := existing.GetLabels()
			if mergeLabels(&labels, desired.GetLabels()) {
				existing.SetLabels(labels)
				changed = true
			}
			annotations := existing.GetAnnotations()
			if mergeAnnotations(&annotations, desired.GetAnnotations()) {
				existing.SetAnnotations(annotations)
				changed = true
			}

			desiredSpec, _, _ := unstructured.NestedMap(desired.Object, "spec")
			existingSpec, _, _ := unstructured.NestedMap(existing.Object, "spec")
			if !reflect.DeepEqual(existingSpec, desiredSpec) {
				if err := unstructured.SetNestedMap(existing.Object, desiredSpec, "spec"); err != nil {
					return false, err
				}
				changed = true
			}
			if !changed {
				return false, nil
			}

			log := logf.FromContext(ctx)
			log.Info("Updating ServiceMonitor", "name", desired.GetName(), "namespace", desired.GetNamespace())
			return true, nil
		})
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	log := logf.FromContext(ctx)
	log.Info("Creating ServiceMonitor", "name", desired.GetName(), "namespace", desired.GetNamespace())
	return r.Create(ctx, desired)
}

func (r *FabricNetworkReconciler) isDeploymentReady(
	ctx context.Context,
	namespace, name string,
) (bool, error) {
	status, err := r.deploymentWorkloadStatus(ctx, namespace, name)
	if err != nil {
		return false, err
	}

	return workloadReady(status), nil
}

func (r *FabricNetworkReconciler) deploymentWorkloadStatus(
	ctx context.Context,
	namespace, name string,
) (fabricopsv1alpha1.WorkloadStatus, error) {
	var deploy appsv1.Deployment
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &deploy); err != nil {
		return fabricopsv1alpha1.WorkloadStatus{}, err
	}

	if deploy.Spec.Replicas == nil {
		return fabricopsv1alpha1.WorkloadStatus{}, nil
	}

	ready := deploy.Status.ReadyReplicas
	if deploy.Status.ObservedGeneration < deploy.Generation ||
		deploy.Status.UpdatedReplicas < *deploy.Spec.Replicas ||
		deploy.Status.AvailableReplicas < *deploy.Spec.Replicas {
		ready = 0
	}

	return fabricopsv1alpha1.WorkloadStatus{
		Desired: *deploy.Spec.Replicas,
		Ready:   ready,
	}, nil
}

func buildCADeployment(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
) *appsv1.Deployment {
	name := sanitizeName(org.Organization.Name + "-ca")
	replicas := int32(1)
	labels := orgLabels(net, org, componentCA)

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      labels,
			Annotations: resourceAnnotations(net, org),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RecreateDeploymentStrategyType,
			},
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: resourceAnnotations(net, org),
				},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						dataVolume(name),
					},
					Containers: []corev1.Container{
						{
							Name:  containerCA,
							Image: caImage(),
							Env: []corev1.EnvVar{
								{Name: "FABRIC_CA_HOME", Value: caHomePath},
								{Name: "FABRIC_CA_SERVER_CA_NAME", Value: sanitizeName(org.Organization.Name)},
								{Name: "FABRIC_CA_SERVER_PORT", Value: fmt.Sprintf("%d", caPort)},
								{
									Name: caBootstrapEnvVar,
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: caBootstrapSecretName(org),
											},
											Key: caBootstrapUserPassKey,
										},
									},
								},
							},
							Command: []string{
								"sh", "-c",
								"fabric-ca-server start -b \"$" + caBootstrapEnvVar + "\" -d",
							},
							Ports: []corev1.ContainerPort{
								{ContainerPort: caPort, Name: "ca", Protocol: corev1.ProtocolTCP},
							},
							ReadinessProbe: tcpReadinessProbe(caPort),
							LivenessProbe:  tcpLivenessProbe(caPort),
							Resources:      componentResourceRequirements(componentCA),
							VolumeMounts: []corev1.VolumeMount{
								dataVolumeMount(caHomePath),
							},
						},
					},
				},
			},
		},
	}
}

func buildCAService(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
) *corev1.Service {
	name := sanitizeName(org.Organization.Name + "-ca")
	labels := orgLabels(net, org, componentCA)

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      labels,
			Annotations: resourceAnnotations(net, org),
		},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports: []corev1.ServicePort{
				{
					Name:       "ca",
					Port:       caPort,
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromInt32(caPort),
				},
			},
		},
	}
}

func buildOrdererDeployment(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	group fabricopsv1alpha1.OrdererGroup,
	instance int,
	namespace string,
) *appsv1.Deployment {
	name := sanitizeName(fmt.Sprintf("%s%d", group.Prefix, instance))
	replicas := int32(1)
	selector := map[string]string{
		labelFabricNetwork:          sanitizeName(net.Name),
		labelFabricNetworkNamespace: sanitizeName(net.Namespace),
		labelOrg:                    sanitizeName(org.Organization.Name),
		labelComponent:              componentOrderer,
		labelOrdererGroup:           sanitizeName(group.GroupName),
		labelInstance:               fmt.Sprintf("%d", instance),
	}
	labels := mergeMap(orgLabels(net, org, componentOrderer), map[string]string{
		labelOrdererGroup: sanitizeName(group.GroupName),
		labelInstance:     fmt.Sprintf("%d", instance),
		labelWorkload:     name,
	})
	tlsEnabled := net.Spec.Global.TLS
	env := []corev1.EnvVar{
		{Name: "ORDERER_GENERAL_LISTENADDRESS", Value: "0.0.0.0"},
		{Name: "ORDERER_GENERAL_LISTENPORT", Value: fmt.Sprintf("%d", ordererPort)},
		{Name: "ORDERER_GENERAL_BOOTSTRAPMETHOD", Value: "none"},
		{Name: "ORDERER_GENERAL_LOCALMSPID", Value: org.Organization.MSPName},
		{Name: "ORDERER_GENERAL_LOCALMSPDIR", Value: ordererMSPPath},
		{Name: "ORDERER_CHANNELPARTICIPATION_ENABLED", Value: "true"},
		{Name: "ORDERER_ADMIN_LISTENADDRESS", Value: fmt.Sprintf("0.0.0.0:%d", ordererAdminPort)},
		{Name: "ORDERER_OPERATIONS_LISTENADDRESS", Value: fmt.Sprintf("0.0.0.0:%d", ordererOpsPort)},
		{Name: "ORDERER_OPERATIONS_TLS_ENABLED", Value: "false"},
		{Name: "ORDERER_METRICS_PROVIDER", Value: "prometheus"},
	}

	if tlsEnabled {
		env = append(env,
			corev1.EnvVar{Name: "ORDERER_GENERAL_TLS_ENABLED", Value: "true"},
			corev1.EnvVar{Name: "ORDERER_GENERAL_TLS_PRIVATEKEY", Value: ordererTLSPath + "/server.key"},
			corev1.EnvVar{Name: "ORDERER_GENERAL_TLS_CERTIFICATE", Value: ordererTLSPath + "/server.crt"},
			corev1.EnvVar{Name: "ORDERER_GENERAL_TLS_ROOTCAS", Value: "[" + ordererTLSPath + "/ca.crt]"},
			corev1.EnvVar{Name: "ORDERER_GENERAL_CLUSTER_CLIENTCERTIFICATE", Value: ordererTLSPath + "/server.crt"},
			corev1.EnvVar{Name: "ORDERER_GENERAL_CLUSTER_CLIENTPRIVATEKEY", Value: ordererTLSPath + "/server.key"},
			corev1.EnvVar{Name: "ORDERER_GENERAL_CLUSTER_ROOTCAS", Value: "[" + ordererTLSPath + "/ca.crt]"},
			corev1.EnvVar{Name: "ORDERER_ADMIN_TLS_ENABLED", Value: "true"},
			corev1.EnvVar{Name: "ORDERER_ADMIN_TLS_PRIVATEKEY", Value: ordererTLSPath + "/server.key"},
			corev1.EnvVar{Name: "ORDERER_ADMIN_TLS_CERTIFICATE", Value: ordererTLSPath + "/server.crt"},
			corev1.EnvVar{Name: "ORDERER_ADMIN_TLS_ROOTCAS", Value: "[" + ordererTLSPath + "/ca.crt]"},
			corev1.EnvVar{Name: "ORDERER_ADMIN_TLS_CLIENTAUTHREQUIRED", Value: "true"},
			corev1.EnvVar{Name: "ORDERER_ADMIN_TLS_CLIENTROOTCAS", Value: "[" + ordererTLSPath + "/ca.crt]"},
		)
	}

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      labels,
			Annotations: resourceAnnotations(net, org),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RecreateDeploymentStrategyType,
			},
			Selector: &metav1.LabelSelector{
				MatchLabels: selector,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: resourceAnnotations(net, org),
				},
				Spec: corev1.PodSpec{
					Volumes: append(identityVolumes(name, net.Spec.Global.TLS), dataVolume(name)),
					Containers: []corev1.Container{
						{
							Name:  containerOrderer,
							Image: fabricComponentImage("orderer", net.Spec.Global.FabricVersion),
							Env:   env,
							Ports: []corev1.ContainerPort{
								{ContainerPort: ordererPort, Name: "orderer", Protocol: corev1.ProtocolTCP},
								{ContainerPort: ordererAdminPort, Name: "admin", Protocol: corev1.ProtocolTCP},
								{ContainerPort: ordererOpsPort, Name: endpointOperations, Protocol: corev1.ProtocolTCP},
							},
							ReadinessProbe: operationsReadinessProbe(),
							LivenessProbe:  tcpLivenessProbe(ordererPort),
							Resources:      componentResourceRequirements(componentOrderer),
							VolumeMounts: append(
								identityVolumeMounts(ordererMSPPath, ordererTLSPath, tlsEnabled),
								dataVolumeMount(fabricProductionPath),
							),
						},
					},
				},
			},
		},
	}
}

func buildOrdererService(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	group fabricopsv1alpha1.OrdererGroup,
	instance int,
	namespace string,
) *corev1.Service {
	name := sanitizeName(fmt.Sprintf("%s%d", group.Prefix, instance))
	selector := map[string]string{
		labelFabricNetwork:          sanitizeName(net.Name),
		labelFabricNetworkNamespace: sanitizeName(net.Namespace),
		labelOrg:                    sanitizeName(org.Organization.Name),
		labelComponent:              componentOrderer,
		labelOrdererGroup:           sanitizeName(group.GroupName),
		labelInstance:               fmt.Sprintf("%d", instance),
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      mergeMap(orgLabels(net, org, componentOrderer), selector),
			Annotations: resourceAnnotations(net, org),
		},
		Spec: corev1.ServiceSpec{
			Selector: selector,
			Ports: []corev1.ServicePort{
				{
					Name:       "orderer",
					Port:       ordererPort,
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromInt32(ordererPort),
				},
				{
					Name:       "admin",
					Port:       ordererAdminPort,
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromInt32(ordererAdminPort),
				},
			},
		},
	}
}

func buildOrdererOperationsService(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	group fabricopsv1alpha1.OrdererGroup,
	instance int,
	namespace string,
) *corev1.Service {
	name := sanitizeName(fmt.Sprintf("%s%d", group.Prefix, instance))
	selector := map[string]string{
		labelFabricNetwork:          sanitizeName(net.Name),
		labelFabricNetworkNamespace: sanitizeName(net.Namespace),
		labelOrg:                    sanitizeName(org.Organization.Name),
		labelComponent:              componentOrderer,
		labelOrdererGroup:           sanitizeName(group.GroupName),
		labelInstance:               fmt.Sprintf("%d", instance),
	}
	labels := operationsServiceLabels(net, org, componentOrderer, name)
	labels[labelOrdererGroup] = sanitizeName(group.GroupName)
	labels[labelInstance] = fmt.Sprintf("%d", instance)

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        operationsServiceName(name),
			Namespace:   namespace,
			Labels:      labels,
			Annotations: resourceAnnotations(net, org),
		},
		Spec: corev1.ServiceSpec{
			Selector: selector,
			Ports: []corev1.ServicePort{
				{
					Name:       endpointOperations,
					Port:       ordererOpsPort,
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromInt32(ordererOpsPort),
				},
			},
		},
	}
}

func buildPeerDeployment(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	instance int,
	namespace string,
) *appsv1.Deployment {
	name := sanitizeName(fmt.Sprintf("%s%d", org.Peer.Prefix, instance))
	peer := peerInstance{org: org, name: name, namespace: namespace}
	replicas := int32(1)
	peerAddress := peerAddress(peer)
	peerAdvertisedAddress := peerAdvertisedAddress(peer)
	chaincodeAddress := serviceDNS(name, namespace, peerChaincodePort)
	tlsEnabled := net.Spec.Global.TLS
	selector := map[string]string{
		labelFabricNetwork:          sanitizeName(net.Name),
		labelFabricNetworkNamespace: sanitizeName(net.Namespace),
		labelOrg:                    sanitizeName(org.Organization.Name),
		labelComponent:              componentPeer,
		labelInstance:               fmt.Sprintf("%d", instance),
	}
	labels := mergeMap(orgLabels(net, org, componentPeer), map[string]string{
		labelInstance: fmt.Sprintf("%d", instance),
		labelWorkload: name,
	})
	env := []corev1.EnvVar{
		{Name: "CORE_PEER_ID", Value: name},
		{Name: "CORE_PEER_ADDRESS", Value: peerAddress},
		{Name: "CORE_PEER_LISTENADDRESS", Value: fmt.Sprintf("0.0.0.0:%d", peerPort)},
		{Name: "CORE_PEER_CHAINCODEADDRESS", Value: chaincodeAddress},
		{Name: "CORE_PEER_CHAINCODELISTENADDRESS", Value: fmt.Sprintf("0.0.0.0:%d", peerChaincodePort)},
		{Name: "CORE_PEER_GOSSIP_ENDPOINT", Value: peerAddress},
		{Name: "CORE_PEER_GOSSIP_EXTERNALENDPOINT", Value: peerAdvertisedAddress},
		{Name: "CORE_PEER_LOCALMSPID", Value: org.Organization.MSPName},
		{Name: "CORE_PEER_MSPCONFIGPATH", Value: peerMSPPath},
		{Name: "CORE_VM_ENDPOINT", Value: ""},
		{Name: ccaasBuilderEnvVar, Value: ccaasBuilderConfig(name)},
		{Name: "CORE_OPERATIONS_LISTENADDRESS", Value: fmt.Sprintf("0.0.0.0:%d", peerOpsPort)},
		{Name: "CORE_OPERATIONS_TLS_ENABLED", Value: "false"},
		{Name: "CORE_METRICS_PROVIDER", Value: "prometheus"},
	}

	if tlsEnabled {
		env = append(env,
			corev1.EnvVar{Name: "CORE_PEER_TLS_ENABLED", Value: "true"},
			corev1.EnvVar{Name: "CORE_PEER_TLS_CERT_FILE", Value: peerTLSPath + "/server.crt"},
			corev1.EnvVar{Name: "CORE_PEER_TLS_KEY_FILE", Value: peerTLSPath + "/server.key"},
			corev1.EnvVar{Name: "CORE_PEER_TLS_ROOTCERT_FILE", Value: peerTLSPath + "/ca.crt"},
		)
	}

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      labels,
			Annotations: resourceAnnotations(net, org),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RecreateDeploymentStrategyType,
			},
			Selector: &metav1.LabelSelector{
				MatchLabels: selector,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: resourceAnnotations(net, org),
				},
				Spec: corev1.PodSpec{
					Volumes: append(identityVolumes(name, net.Spec.Global.TLS), dataVolume(name)),
					Containers: []corev1.Container{
						{
							Name:  containerPeer,
							Image: fabricComponentImage("peer", net.Spec.Global.FabricVersion),
							Env:   env,
							Ports: []corev1.ContainerPort{
								{ContainerPort: peerPort, Name: "peer", Protocol: corev1.ProtocolTCP},
								{ContainerPort: peerChaincodePort, Name: "chaincode", Protocol: corev1.ProtocolTCP},
								{ContainerPort: peerOpsPort, Name: endpointOperations, Protocol: corev1.ProtocolTCP},
							},
							ReadinessProbe: tcpReadinessProbe(peerPort),
							LivenessProbe:  tcpLivenessProbe(peerPort),
							Resources:      componentResourceRequirements(componentPeer),
							VolumeMounts: append(
								identityVolumeMounts(peerMSPPath, peerTLSPath, tlsEnabled),
								dataVolumeMount(fabricProductionPath),
							),
						},
					},
				},
			},
		},
	}
}

func ccaasBuilderConfig(peerName string) string {
	return fmt.Sprintf(`{"peer_hostname":"%s"}`, sanitizeName(peerName))
}

func buildPeerService(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	instance int,
	namespace string,
) *corev1.Service {
	name := sanitizeName(fmt.Sprintf("%s%d", org.Peer.Prefix, instance))
	selector := map[string]string{
		labelFabricNetwork:          sanitizeName(net.Name),
		labelFabricNetworkNamespace: sanitizeName(net.Namespace),
		labelOrg:                    sanitizeName(org.Organization.Name),
		labelComponent:              componentPeer,
		labelInstance:               fmt.Sprintf("%d", instance),
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      mergeMap(orgLabels(net, org, componentPeer), selector),
			Annotations: resourceAnnotations(net, org),
		},
		Spec: corev1.ServiceSpec{
			Selector: selector,
			Ports: []corev1.ServicePort{
				{
					Name:       "peer",
					Port:       peerPort,
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromInt32(peerPort),
				},
				{
					Name:       "chaincode",
					Port:       peerChaincodePort,
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromInt32(peerChaincodePort),
				},
			},
		},
	}
}

func buildPeerOperationsService(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	instance int,
	namespace string,
) *corev1.Service {
	name := sanitizeName(fmt.Sprintf("%s%d", org.Peer.Prefix, instance))
	selector := map[string]string{
		labelFabricNetwork:          sanitizeName(net.Name),
		labelFabricNetworkNamespace: sanitizeName(net.Namespace),
		labelOrg:                    sanitizeName(org.Organization.Name),
		labelComponent:              componentPeer,
		labelInstance:               fmt.Sprintf("%d", instance),
	}
	labels := operationsServiceLabels(net, org, componentPeer, name)
	labels[labelInstance] = fmt.Sprintf("%d", instance)

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        operationsServiceName(name),
			Namespace:   namespace,
			Labels:      labels,
			Annotations: resourceAnnotations(net, org),
		},
		Spec: corev1.ServiceSpec{
			Selector: selector,
			Ports: []corev1.ServicePort{
				{
					Name:       endpointOperations,
					Port:       peerOpsPort,
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromInt32(peerOpsPort),
				},
			},
		},
	}
}

func (r *FabricNetworkReconciler) reconcileCA(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
) (bool, error) {
	name := sanitizeName(org.Organization.Name + "-ca")
	pvc, err := buildDataPVC(net, org, namespace, name, componentCA)
	if err != nil {
		return false, err
	}
	if err := r.ensurePersistentVolumeClaim(ctx, pvc); err != nil {
		return false, err
	}

	deploy := buildCADeployment(net, org, namespace)
	if err := r.ensureDeployment(ctx, deploy); err != nil {
		return false, err
	}

	svc := buildCAService(net, org, namespace)
	if err := r.ensureService(ctx, svc); err != nil {
		return false, err
	}

	ready, err := r.isDeploymentReady(ctx, namespace, deploy.Name)
	if err != nil {
		return false, err
	}

	return ready, nil
}

func (r *FabricNetworkReconciler) reconcileOrderers(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
) (fabricopsv1alpha1.WorkloadStatus, error) {
	status := fabricopsv1alpha1.WorkloadStatus{}

	for _, group := range org.Orderers {
		for i := 0; i < group.Instances; i++ {
			deploy := buildOrdererDeployment(net, org, group, i, namespace)
			pvc, err := buildDataPVC(net, org, namespace, deploy.Name, componentOrderer)
			if err != nil {
				return status, err
			}
			if err := r.ensurePersistentVolumeClaim(ctx, pvc); err != nil {
				return status, err
			}

			if err := r.ensureDeployment(ctx, deploy); err != nil {
				return status, err
			}

			svc := buildOrdererService(net, org, group, i, namespace)
			if err := r.ensureService(ctx, svc); err != nil {
				return status, err
			}

			opsSvc := buildOrdererOperationsService(net, org, group, i, namespace)
			if err := r.ensureService(ctx, opsSvc); err != nil {
				return status, err
			}

			deploymentStatus, err := r.deploymentWorkloadStatus(ctx, namespace, deploy.Name)
			if err != nil {
				return status, err
			}
			status.Desired += deploymentStatus.Desired
			status.Ready += deploymentStatus.Ready
		}
	}

	return status, nil
}

func (r *FabricNetworkReconciler) reconcilePeers(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
) (fabricopsv1alpha1.WorkloadStatus, error) {
	status := fabricopsv1alpha1.WorkloadStatus{}
	desiredPeers := desiredPeerNames(org)

	if org.Peer == nil {
		return status, r.cleanupScaledDownPeerWorkloads(ctx, net, org, namespace, desiredPeers)
	}

	for i := 0; i < org.Peer.Instances; i++ {
		deploy := buildPeerDeployment(net, org, i, namespace)
		pvc, err := buildDataPVC(net, org, namespace, deploy.Name, componentPeer)
		if err != nil {
			return status, err
		}
		if err := r.ensurePersistentVolumeClaim(ctx, pvc); err != nil {
			return status, err
		}

		if err := r.ensureDeployment(ctx, deploy); err != nil {
			return status, err
		}

		svc := buildPeerService(net, org, i, namespace)
		if err := r.ensureService(ctx, svc); err != nil {
			return status, err
		}

		opsSvc := buildPeerOperationsService(net, org, i, namespace)
		if err := r.ensureService(ctx, opsSvc); err != nil {
			return status, err
		}

		deploymentStatus, err := r.deploymentWorkloadStatus(ctx, namespace, deploy.Name)
		if err != nil {
			return status, err
		}
		status.Desired += deploymentStatus.Desired
		status.Ready += deploymentStatus.Ready
	}

	if err := r.cleanupScaledDownPeerWorkloads(ctx, net, org, namespace, desiredPeers); err != nil {
		return status, err
	}

	return status, nil
}

func (r *FabricNetworkReconciler) cleanupScaledDownPeerWorkloads(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	desiredPeers map[string]struct{},
) error {
	selector := client.MatchingLabels{
		labelFabricNetwork:          sanitizeName(net.Name),
		labelFabricNetworkNamespace: sanitizeName(net.Namespace),
		labelOrg:                    sanitizeName(org.Organization.Name),
		labelComponent:              componentPeer,
	}
	expectedOwner := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   namespace,
			Labels:      orgLabels(net, org, componentPeer),
			Annotations: resourceAnnotations(net, org),
		},
	}

	var deployments appsv1.DeploymentList
	if err := r.List(ctx, &deployments, client.InNamespace(namespace), selector); err != nil {
		return err
	}
	for i := range deployments.Items {
		deployment := &deployments.Items[i]
		if _, ok := desiredPeers[deployment.Name]; ok {
			continue
		}
		expectedOwner.Name = deployment.Name
		if err := r.deleteOwnedObject(ctx, deployment, expectedOwner); err != nil {
			return err
		}
	}

	desiredServices := map[string]struct{}{}
	for name := range desiredPeers {
		desiredServices[name] = struct{}{}
		desiredServices[operationsServiceName(name)] = struct{}{}
	}

	var services corev1.ServiceList
	if err := r.List(ctx, &services, client.InNamespace(namespace), selector); err != nil {
		return err
	}
	for i := range services.Items {
		service := &services.Items[i]
		if _, ok := desiredServices[service.Name]; ok {
			continue
		}
		expectedOwner.Name = service.Name
		if err := r.deleteOwnedObject(ctx, service, expectedOwner); err != nil {
			return err
		}
	}

	return nil
}
