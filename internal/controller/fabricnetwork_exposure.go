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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"maps"
	stdlibnet "net"
	"reflect"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

const externalClientEndpoint = "external-client"
const externalAnnotationKeys = "fabricops.io/external-annotation-keys"

func validateExternalExposure(path string, endpoint fabricopsv1alpha1.ExternalEndpoint) []string {
	if endpoint.Exposure == nil {
		return nil
	}
	e := endpoint.Exposure
	var problems []string
	if e.Type != "LoadBalancer" && e.Type != "UserManaged" {
		problems = append(problems, path+" exposure type must be LoadBalancer or UserManaged")
	}
	host := endpointHost(endpoint.Address)
	if stdlibnet.ParseIP(host) != nil || !strings.Contains(host, ".") || len(validation.IsDNS1123Subdomain(host)) != 0 {
		problems = append(problems, path+" exposure requires a DNS hostname in address")
	}
	if endpoint.TLSHostnameOverride != "" {
		problems = append(problems, path+" exposure does not support TLS hostname overrides")
	}
	if len(e.AllowedSourceCIDRs) == 0 {
		problems = append(problems, path+" exposure requires allowedSourceCIDRs")
	}
	for _, cidr := range e.AllowedSourceCIDRs {
		if _, _, err := stdlibnet.ParseCIDR(cidr); err != nil {
			problems = append(problems, path+" exposure has invalid source CIDR "+cidr)
		}
	}
	if e.LoadBalancerClass != nil && (e.Type != "LoadBalancer" || len(validation.IsQualifiedName(*e.LoadBalancerClass)) != 0) {
		problems = append(problems, path+" loadBalancerClass requires LoadBalancer and a qualified class name")
	}
	if e.Type != "LoadBalancer" && len(e.ServiceAnnotations) != 0 {
		problems = append(problems, path+" serviceAnnotations requires LoadBalancer")
	}
	for key := range e.ServiceAnnotations {
		if strings.HasPrefix(key, "fabricops.io/") || len(validation.IsQualifiedName(key)) != 0 {
			problems = append(problems, path+" service annotation key is invalid or reserved: "+key)
		}
	}
	return problems
}

func buildExternalClientService(net *fabricopsv1alpha1.FabricNetwork, org fabricopsv1alpha1.Org,
	internal *corev1.Service, endpoint fabricopsv1alpha1.ExternalEndpoint) *corev1.Service {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name: sanitizeName(internal.Name + "-external"), Namespace: internal.Namespace, Labels: maps.Clone(internal.Labels),
	}, Spec: corev1.ServiceSpec{Selector: maps.Clone(internal.Spec.Selector)}}
	svc.Labels[labelEndpoint] = externalClientEndpoint
	svc.Annotations = mergeMap(resourceAnnotations(net, org), endpoint.Exposure.ServiceAnnotations)
	keys := make([]string, 0, len(endpoint.Exposure.ServiceAnnotations))
	for key := range endpoint.Exposure.ServiceAnnotations {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	encoded, _ := json.Marshal(keys)
	svc.Annotations[externalAnnotationKeys] = string(encoded)
	port := internal.Spec.Ports[0]
	port.Port = endpointPort(endpoint.Address, port.Port)
	svc.Spec.Ports = []corev1.ServicePort{port}
	svc.Spec.Type = corev1.ServiceTypeLoadBalancer
	svc.Spec.ExternalTrafficPolicy = corev1.ServiceExternalTrafficPolicyLocal
	svc.Spec.LoadBalancerSourceRanges = slices.Clone(endpoint.Exposure.AllowedSourceCIDRs)
	svc.Spec.LoadBalancerClass = endpoint.Exposure.LoadBalancerClass
	return svc
}

func buildExternalIngressPolicy(svc *corev1.Service, endpoint fabricopsv1alpha1.ExternalEndpoint) *networkingv1.NetworkPolicy {
	from := []networkingv1.NetworkPolicyPeer{}
	for _, cidr := range endpoint.Exposure.AllowedSourceCIDRs {
		from = append(from, networkingv1.NetworkPolicyPeer{IPBlock: &networkingv1.IPBlock{CIDR: cidr}})
	}
	return &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{
		Name: svc.Name, Namespace: svc.Namespace, Labels: maps.Clone(svc.Labels), Annotations: maps.Clone(svc.Annotations),
	}, Spec: networkingv1.NetworkPolicySpec{
		PodSelector: metav1.LabelSelector{MatchLabels: svc.Spec.Selector},
		PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
		Ingress: []networkingv1.NetworkPolicyIngressRule{{From: from,
			Ports: []networkingv1.NetworkPolicyPort{networkPolicyPort(corev1.ProtocolTCP, svc.Spec.Ports[0].TargetPort.IntVal)}}},
	}}
}

func exposureOwnedBy(object client.Object, net *fabricopsv1alpha1.FabricNetwork) bool {
	a := object.GetAnnotations()
	return a[annotationFabricNetwork] == net.Name && a[annotationFabricNetworkNamespace] == net.Namespace &&
		a[annotationFabricNetworkUID] == string(net.UID)
}

func (r *FabricNetworkReconciler) ensureExternalClientService(ctx context.Context, desired *corev1.Service) error {
	current := &corev1.Service{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(desired), current); apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	} else if err != nil {
		return err
	}
	return r.updateObjectWithRetry(ctx, desired, func(object client.Object) (bool, error) {
		svc := object.(*corev1.Service)
		a := desired.Annotations
		if svc.Annotations[annotationFabricNetworkUID] != a[annotationFabricNetworkUID] ||
			svc.Annotations[annotationFabricNetwork] != a[annotationFabricNetwork] ||
			svc.Annotations[annotationFabricNetworkNamespace] != a[annotationFabricNetworkNamespace] {
			return false, fmt.Errorf("external Service %s/%s is not owned by this network UID", svc.Namespace, svc.Name)
		}
		if desired.Spec.LoadBalancerClass != nil && svc.Spec.LoadBalancerClass != nil &&
			*desired.Spec.LoadBalancerClass != *svc.Spec.LoadBalancerClass {
			return false, fmt.Errorf("external Service %s loadBalancerClass cannot be changed in place", svc.Name)
		}
		before := svc.DeepCopy()
		var oldKeys []string
		if raw := svc.Annotations[externalAnnotationKeys]; raw != "" {
			if err := json.Unmarshal([]byte(raw), &oldKeys); err != nil {
				return false, fmt.Errorf("external Service annotation ownership is malformed")
			}
		}
		for _, key := range oldKeys {
			if _, keep := desired.Annotations[key]; !keep && !strings.HasPrefix(key, "fabricops.io/") {
				delete(svc.Annotations, key)
			}
		}
		mergeAnnotations(&svc.Annotations, desired.Annotations)
		mergeLabels(&svc.Labels, desired.Labels)
		svc.Spec.Type = desired.Spec.Type
		svc.Spec.Selector = desired.Spec.Selector
		ports := slices.Clone(desired.Spec.Ports)
		for i := range ports {
			for _, old := range svc.Spec.Ports {
				if old.Name == ports[i].Name {
					ports[i].NodePort = old.NodePort
				}
			}
		}
		svc.Spec.Ports = ports
		svc.Spec.ExternalTrafficPolicy = desired.Spec.ExternalTrafficPolicy
		svc.Spec.LoadBalancerSourceRanges = desired.Spec.LoadBalancerSourceRanges
		if desired.Spec.LoadBalancerClass != nil {
			svc.Spec.LoadBalancerClass = desired.Spec.LoadBalancerClass
		}
		return !reflect.DeepEqual(before, svc), nil
	})
}

type externalExposureTarget struct {
	endpoint  fabricopsv1alpha1.ExternalEndpoint
	internal  *corev1.Service
	component string
}

func orgExposureTargets(net *fabricopsv1alpha1.FabricNetwork, org fabricopsv1alpha1.Org, namespace string) []externalExposureTarget {
	var targets []externalExposureTarget
	for _, group := range org.Orderers {
		for i := 0; i < group.Instances; i++ {
			internal := buildOrdererService(net, org, group, i, namespace)
			if endpoint, ok := externalEndpointForWorkload(group.ExternalEndpoints, internal.Name); ok && endpoint.Exposure != nil {
				targets = append(targets, externalExposureTarget{endpoint, internal, componentOrderer})
			}
		}
	}
	if org.Peer != nil {
		for i := 0; i < org.Peer.Instances; i++ {
			internal := buildPeerService(net, org, i, namespace)
			if endpoint, ok := externalEndpointForWorkload(org.Peer.ExternalEndpoints, internal.Name); ok && endpoint.Exposure != nil {
				targets = append(targets, externalExposureTarget{endpoint, internal, componentPeer})
			}
		}
	}
	return targets
}

func (r *FabricNetworkReconciler) reconcileExternalExposure(ctx context.Context, net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org, namespace string) ([]fabricopsv1alpha1.ExternalEndpointStatus, error) {
	var statuses []fabricopsv1alpha1.ExternalEndpointStatus
	services, policies := map[string]bool{}, map[string]bool{}
	for _, target := range orgExposureTargets(net, org, namespace) {
		endpoint := target.endpoint
		svc := buildExternalClientService(net, org, target.internal, endpoint)
		policyName := ""
		if networkPolicyEnabled(net) {
			policy := buildExternalIngressPolicy(svc, endpoint)
			policyName = policy.Name
			policies[policy.Name] = true
			if err := r.ensureExternalIngressPolicy(ctx, net, policy); err != nil {
				return statuses, err
			}
		}
		status := fabricopsv1alpha1.ExternalEndpointStatus{Name: endpoint.Name, Component: target.component,
			IngressPolicyName: policyName,
			InternalAddress:   serviceDNS(target.internal.Name, namespace, target.internal.Spec.Ports[0].Port),
			AdvertisedAddress: endpoint.Address, ExposureType: endpoint.Exposure.Type,
			Reason: "ConfigurationReady", Message: "Certificate matches the advertised DNS name; remote reachability is not verified"}
		if endpoint.Exposure.Type == "UserManaged" && policyName == "" {
			status.Message += "; source restrictions must be enforced by user-managed infrastructure"
		}
		provisioned := true
		if endpoint.Exposure.Type == "LoadBalancer" {
			services[svc.Name] = true
			if err := r.ensureExternalClientService(ctx, svc); err != nil {
				return statuses, err
			}
			if err := r.Get(ctx, client.ObjectKeyFromObject(svc), svc); err != nil {
				return statuses, err
			}
			status.ServiceName = svc.Name
			for _, ingress := range svc.Status.LoadBalancer.Ingress {
				if ingress.IP != "" {
					status.AllocatedAddresses = append(status.AllocatedAddresses, ingress.IP)
				}
				if ingress.Hostname != "" {
					status.AllocatedAddresses = append(status.AllocatedAddresses, ingress.Hostname)
				}
			}
			provisioned = len(status.AllocatedAddresses) > 0
			if !provisioned {
				status.Reason, status.Message = "LoadBalancerPending", "Waiting for a load balancer address; DNS is user-managed"
			}
		}
		if err := r.verifyExternalTLS(ctx, namespace, endpoint); err != nil {
			status.Reason, status.Message = "TLSIdentityPending", err.Error()
		} else {
			status.TLSValid = true
		}
		status.ConfigurationReady = status.TLSValid && provisioned
		statuses = append(statuses, status)
	}
	if err := r.cleanupExternalExposure(ctx, net, namespace, services, policies); err != nil {
		return statuses, err
	}
	return statuses, nil
}

func (r *FabricNetworkReconciler) verifyExternalTLS(ctx context.Context, namespace string,
	endpoint fabricopsv1alpha1.ExternalEndpoint) error {
	secret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: identitySecretName(endpoint.Name, secretKindTLS)}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("waiting for workload TLS identity for %s", endpoint.Name)
		}
		return err
	}
	pair, err := tls.X509KeyPair(secret.Data[tlsServerCertKey], secret.Data[tlsServerKeyKey])
	if err != nil {
		return fmt.Errorf("workload %s TLS certificate/key is invalid", endpoint.Name)
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return fmt.Errorf("workload %s TLS certificate is invalid", endpoint.Name)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(secret.Data[tlsCACertKey]) {
		return fmt.Errorf("workload %s TLS roots are invalid", endpoint.Name)
	}
	intermediates := x509.NewCertPool()
	for _, raw := range pair.Certificate[1:] {
		cert, err := x509.ParseCertificate(raw)
		if err != nil {
			return fmt.Errorf("workload %s TLS chain is invalid", endpoint.Name)
		}
		intermediates.AddCert(cert)
	}
	for _, host := range append([]string{endpointHost(endpoint.Address)}, endpoint.TLSHosts...) {
		host = strings.TrimSpace(host)
		if strings.HasPrefix(host, "*.") && slices.Contains(leaf.DNSNames, host) {
			continue
		}
		if _, err := leaf.Verify(x509.VerifyOptions{DNSName: host, Roots: roots, Intermediates: intermediates}); err != nil {
			return fmt.Errorf("workload %s TLS identity does not validate for %s; review enrollment/renewal before publication", endpoint.Name, host)
		}
	}
	return nil
}

func (r *FabricNetworkReconciler) cleanupExternalExposure(ctx context.Context, net *fabricopsv1alpha1.FabricNetwork,
	namespace string, services, policies map[string]bool) error {
	selector := client.MatchingLabels{labelEndpoint: externalClientEndpoint,
		labelFabricNetwork: sanitizeName(net.Name), labelFabricNetworkNamespace: sanitizeName(net.Namespace)}
	svcs := &corev1.ServiceList{}
	if err := r.List(ctx, svcs, client.InNamespace(namespace), selector); err != nil {
		return err
	}
	for i := range svcs.Items {
		svc := &svcs.Items[i]
		if !services[svc.Name] && exposureOwnedBy(svc, net) {
			if err := client.IgnoreNotFound(r.Delete(ctx, svc, client.Preconditions{UID: &svc.UID})); err != nil {
				return err
			}
		}
	}
	nps := &networkingv1.NetworkPolicyList{}
	if err := r.List(ctx, nps, client.InNamespace(namespace), selector); err != nil {
		return err
	}
	for i := range nps.Items {
		policy := &nps.Items[i]
		if !policies[policy.Name] && exposureOwnedBy(policy, net) {
			if err := client.IgnoreNotFound(r.Delete(ctx, policy, client.Preconditions{UID: &policy.UID})); err != nil {
				return err
			}
		}
	}
	return nil
}

func externalExposureConfigured(orgs []fabricopsv1alpha1.OrgStatus) (bool, string) {
	for _, org := range orgs {
		for _, endpoint := range org.ExternalEndpoints {
			if !endpoint.ConfigurationReady {
				return false, org.Name + "/" + endpoint.Name + ": " + endpoint.Message
			}
		}
	}
	return true, "Declared external endpoint configuration is ready; remote reachability is not verified"
}

func externalExposureCondition(net *fabricopsv1alpha1.FabricNetwork, orgs []fabricopsv1alpha1.OrgStatus,
	conditions []metav1.Condition) []metav1.Condition {
	conditions = slices.Clone(conditions)
	expected := 0
	for _, org := range net.Spec.Orgs {
		expected += len(orgExposureTargets(net, org, orgNamespaceName(net, org)))
	}
	if expected == 0 {
		apiMeta.RemoveStatusCondition(&conditions, "ExternalEndpointsConfigured")
		return conditions
	}
	ready, message := externalExposureConfigured(orgs)
	observed := 0
	for _, org := range orgs {
		observed += len(org.ExternalEndpoints)
	}
	if observed != expected {
		ready, message = false, "Waiting to observe all declared external endpoint configurations"
	}
	status, reason := metav1.ConditionFalse, "ExternalConfigurationPending"
	if ready {
		status, reason = metav1.ConditionTrue, "ExternalConfigurationReady"
	}
	apiMeta.SetStatusCondition(&conditions, metav1.Condition{Type: "ExternalEndpointsConfigured", Status: status,
		Reason: reason, Message: message, ObservedGeneration: net.Generation})
	return conditions
}

func (r *FabricNetworkReconciler) ensureExternalIngressPolicy(ctx context.Context, net *fabricopsv1alpha1.FabricNetwork,
	desired *networkingv1.NetworkPolicy) error {
	current := &networkingv1.NetworkPolicy{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(desired), current); apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	} else if err != nil {
		return err
	}
	return r.updateObjectWithRetry(ctx, desired, func(object client.Object) (bool, error) {
		policy := object.(*networkingv1.NetworkPolicy)
		if !exposureOwnedBy(policy, net) {
			return false, fmt.Errorf("external NetworkPolicy %s is not owned by this network UID", policy.Name)
		}
		before := policy.DeepCopy()
		mergeLabels(&policy.Labels, desired.Labels)
		mergeAnnotations(&policy.Annotations, desired.Annotations)
		policy.Spec = desired.Spec
		return !reflect.DeepEqual(before, policy), nil
	})
}
