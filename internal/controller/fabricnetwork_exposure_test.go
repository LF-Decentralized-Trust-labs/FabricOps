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
	"reflect"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

func exposureFixture() (*fabricopsv1alpha1.FabricNetwork, fabricopsv1alpha1.Org, fabricopsv1alpha1.ExternalEndpoint) {
	endpoint := fabricopsv1alpha1.ExternalEndpoint{Name: "orderer0", Address: "orderer.example.com:443",
		Exposure: &fabricopsv1alpha1.ExternalEndpointExposure{Type: "LoadBalancer",
			AllowedSourceCIDRs: []string{"203.0.113.0/24"}}}
	org := fabricopsv1alpha1.Org{Organization: fabricopsv1alpha1.OrgMeta{Name: "Orderer", MSPName: "OrdererMSP"},
		Orderers: []fabricopsv1alpha1.OrdererGroup{{GroupName: "raft", Prefix: "orderer", Instances: 1,
			ExternalEndpoints: []fabricopsv1alpha1.ExternalEndpoint{endpoint}}}}
	net := &fabricopsv1alpha1.FabricNetwork{ObjectMeta: metav1.ObjectMeta{Name: "network", Namespace: "default", UID: "owner"},
		Spec: fabricopsv1alpha1.FabricNetworkSpec{Global: fabricopsv1alpha1.GlobalConfig{TLS: true}, Orgs: []fabricopsv1alpha1.Org{org}}}
	return net, org, endpoint
}

func TestExplicitExposureProfilesUseDNSWithoutTLSOverrides(t *testing.T) {
	net, org, endpoint := exposureFixture()
	orderer := desiredOrdererInstances(net)[0]
	peerOrg := org
	peerOrg.Orderers = nil
	endpoint.Name = "peer0"
	peerOrg.Peer = &fabricopsv1alpha1.PeerConfig{Instances: 1, Prefix: "peer", ExternalEndpoints: []fabricopsv1alpha1.ExternalEndpoint{endpoint}}
	net.Spec.Orgs = []fabricopsv1alpha1.Org{peerOrg}
	peer := desiredPeerInstances(net)[0]
	for _, host := range []string{ordererConnectionProfileTLSHost(orderer), peerConnectionProfileTLSHost(peer)} {
		if host != "" {
			t.Errorf("explicit exposure must not supply a TLS override, got %q", host)
		}
		profile := connectionProfileGRPCEndpoint(endpoint.Address, host, "CA PEM")
		if len(profile.GRPCOptions) != 0 || profile.TLSCACerts == nil {
			t.Errorf("expected trusted TLS with no hostname overrides: %+v", profile)
		}
	}
}

func TestPeerScaleDownCleanupPreservesExternalServices(t *testing.T) {
	net, org, endpoint := exposureFixture()
	org.Orderers = nil
	endpoint.Name = "peer0"
	org.Peer = &fabricopsv1alpha1.PeerConfig{Instances: 1, Prefix: "peer", ExternalEndpoints: []fabricopsv1alpha1.ExternalEndpoint{endpoint}}
	net.Spec.Orgs = []fabricopsv1alpha1.Org{org}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	internal := buildPeerService(net, org, 0, "org")
	external := buildExternalClientService(net, org, internal, endpoint)
	stale := buildPeerService(net, org, 1, "org")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(internal, external, stale).Build()
	r := &FabricNetworkReconciler{Client: c, Scheme: scheme}
	if err := r.cleanupScaledDownPeerWorkloads(t.Context(), net, org, "org", map[string]struct{}{"peer0": {}}); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []*corev1.Service{internal, external} {
		if err := c.Get(t.Context(), client.ObjectKeyFromObject(expected), &corev1.Service{}); err != nil {
			t.Errorf("cleanup removed desired service %s: %v", expected.Name, err)
		}
	}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(stale), &corev1.Service{}); !apierrors.IsNotFound(err) {
		t.Errorf("stale internal peer service was not removed: %v", err)
	}
}

func TestExternalConfigurationTracksAllocationAndTLSWithoutRotatingIdentity(t *testing.T) {
	net, org, endpoint := exposureFixture()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := networkingv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	authority, err := generateTestIdentityAuthority(org)
	if err != nil {
		t.Fatal(err)
	}
	secret, err := buildTestWorkloadTLSSecret(net, org, "org", endpoint.Name, componentOrderer,
		[]string{endpointHost(endpoint.Address)}, authority)
	if err != nil {
		t.Fatal(err)
	}
	original := secret.DeepCopy()
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1.Service{}).WithObjects(secret).Build()
	r := &FabricNetworkReconciler{Client: c, Scheme: scheme}
	statuses, err := r.reconcileExternalExposure(t.Context(), net, org, "org")
	if err != nil {
		t.Fatal(err)
	}
	if len(statuses) != 1 || !statuses[0].TLSValid || statuses[0].ConfigurationReady || statuses[0].Reason != "LoadBalancerPending" {
		t.Fatalf("unexpected unallocated state: %+v", statuses)
	}
	svc := &corev1.Service{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: "org", Name: "orderer0-external"}, svc); err != nil {
		t.Fatal(err)
	}
	svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: "192.0.2.10"}}
	if err := c.Status().Update(t.Context(), svc); err != nil {
		t.Fatal(err)
	}
	statuses, err = r.reconcileExternalExposure(t.Context(), net, org, "org")
	if err != nil {
		t.Fatal(err)
	}
	if !statuses[0].ConfigurationReady || len(statuses[0].AllocatedAddresses) != 1 {
		t.Fatalf("allocation was not observed: %+v", statuses)
	}
	org.Orderers[0].ExternalEndpoints[0].Address = "changed.example.com:443"
	statuses, err = r.reconcileExternalExposure(t.Context(), net, org, "org")
	if err != nil {
		t.Fatal(err)
	}
	if statuses[0].TLSValid || statuses[0].ConfigurationReady {
		t.Fatal("stale TLS SAN was advertised as configured")
	}
	actual := &corev1.Secret{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(secret), actual); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual.Data, original.Data) {
		t.Fatal("endpoint change replaced live identity material")
	}
}

func TestExternalExposureCleanupAndUserManagedPolicy(t *testing.T) {
	net, org, _ := exposureFixture()
	net.Spec.Global.NetworkPolicy = &fabricopsv1alpha1.NetworkPolicyConfig{Enabled: true}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := networkingv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &FabricNetworkReconciler{Client: c, Scheme: scheme}
	if _, err := r.reconcileExternalExposure(t.Context(), net, org, "org"); err != nil {
		t.Fatal(err)
	}
	org.Orderers[0].ExternalEndpoints[0].Exposure.Type = "UserManaged"
	if _, err := r.reconcileExternalExposure(t.Context(), net, org, "org"); err != nil {
		t.Fatal(err)
	}
	svcs := &corev1.ServiceList{}
	if err := c.List(t.Context(), svcs); err != nil {
		t.Fatal(err)
	}
	if len(svcs.Items) != 0 {
		t.Fatal("user-managed exposure left an operator-owned load balancer")
	}
	policies := &networkingv1.NetworkPolicyList{}
	if err := c.List(t.Context(), policies); err != nil {
		t.Fatal(err)
	}
	if len(policies.Items) != 1 {
		t.Fatal("user-managed source policy missing")
	}
	org.Orderers[0].ExternalEndpoints = nil
	if _, err := r.reconcileExternalExposure(t.Context(), net, org, "org"); err != nil {
		t.Fatal(err)
	}
	if err := c.List(t.Context(), policies); err != nil {
		t.Fatal(err)
	}
	if len(policies.Items) != 0 {
		t.Fatal("removed exposure left a stale source policy")
	}
}

var _ = Describe("External client exposure API", func() {
	It("should preserve allocated ports, roundtrip the API, and keep admin ports internal", func() {
		net, org, endpoint := exposureFixture()
		net.Name, net.UID = "external-api-roundtrip", ""
		net.Spec.Global.FabricVersion = "3.1.0"
		net.Spec.Channels = []fabricopsv1alpha1.Channel{}
		net.Spec.Chaincodes = []fabricopsv1alpha1.Chaincode{}
		org.Organization.Domain, org.CA.DB, org.Orderers[0].Type = "orderer.example.com", "sqlite", "raft"
		endpoint.Exposure.ServiceAnnotations = map[string]string{"example.com/tier": "restricted"}
		org.Orderers[0].ExternalEndpoints[0] = endpoint
		net.Spec.Orgs[0] = org
		Expect(k8sClient.Create(ctx, net)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, net)).To(Succeed()) })
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(net), net)).To(Succeed())
		Expect(net.Spec.Orgs[0].Orderers[0].ExternalEndpoints[0].Exposure).To(Equal(endpoint.Exposure))
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "external-api-roundtrip"}}
		Expect(k8sClient.Create(ctx, namespace)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, namespace)).To(Succeed()) })
		r := &FabricNetworkReconciler{Client: k8sClient}
		internal := buildOrdererService(net, org, org.Orderers[0], 0, namespace.Name)
		Expect(r.ensureService(ctx, internal)).To(Succeed())
		external := buildExternalClientService(net, org, internal, endpoint)
		Expect(r.ensureExternalClientService(ctx, external)).To(Succeed())
		actual := &corev1.Service{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(external), actual)).To(Succeed())
		clusterIP, nodePort := actual.Spec.ClusterIP, actual.Spec.Ports[0].NodePort
		Expect(clusterIP).NotTo(BeEmpty())
		Expect(nodePort).NotTo(BeZero())
		Expect(actual.Spec.Ports).To(HaveLen(1))
		Expect(actual.Spec.Ports[0].TargetPort.IntVal).To(Equal(ordererPort))
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(internal), internal)).To(Succeed())
		Expect(internal.Spec.Type).To(Equal(corev1.ServiceTypeClusterIP))
		Expect(internal.Spec.Ports).To(HaveLen(2))
		endpoint.Exposure.AllowedSourceCIDRs = []string{"198.51.100.0/24"}
		endpoint.Exposure.ServiceAnnotations = nil
		Expect(r.ensureExternalClientService(ctx, buildExternalClientService(net, org, internal, endpoint))).To(Succeed())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(external), actual)).To(Succeed())
		Expect(actual.Spec.ClusterIP).To(Equal(clusterIP))
		Expect(actual.Spec.Ports[0].NodePort).To(Equal(nodePort))
		Expect(actual.Spec.LoadBalancerSourceRanges).To(Equal(endpoint.Exposure.AllowedSourceCIDRs))
		Expect(actual.Annotations).NotTo(HaveKey("example.com/tier"))
		Expect(k8sClient.Delete(ctx, actual)).To(Succeed())
		Expect(k8sClient.Delete(ctx, internal)).To(Succeed())
	})
})

func TestExternalServiceExposesOnlyClientPort(t *testing.T) {
	net, org, endpoint := exposureFixture()
	internal := buildOrdererService(net, org, org.Orderers[0], 0, "org")
	original := internal.DeepCopy()
	external := buildExternalClientService(net, org, internal, endpoint)
	if !reflect.DeepEqual(internal, original) {
		t.Fatal("external exposure mutated the internal Service")
	}
	if external.Spec.Type != corev1.ServiceTypeLoadBalancer || len(external.Spec.Ports) != 1 ||
		external.Spec.Ports[0].Port != 443 || external.Spec.Ports[0].TargetPort.IntVal != ordererPort {
		t.Fatalf("unsafe client Service: %+v", external.Spec)
	}
	if external.Spec.ExternalTrafficPolicy != corev1.ServiceExternalTrafficPolicyLocal ||
		!reflect.DeepEqual(external.Spec.LoadBalancerSourceRanges, endpoint.Exposure.AllowedSourceCIDRs) {
		t.Fatal("source restrictions were not preserved")
	}
	policy := buildExternalIngressPolicy(external, endpoint)
	if len(policy.Spec.Ingress) != 1 || len(policy.Spec.Ingress[0].Ports) != 1 ||
		policy.Spec.Ingress[0].Ports[0].Port.IntVal != ordererPort {
		t.Fatal("external ingress policy exposed non-client ports")
	}
}

func TestExternalExposureValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*fabricopsv1alpha1.ExternalEndpoint)
	}{
		{"mode", func(e *fabricopsv1alpha1.ExternalEndpoint) { e.Exposure.Type = "Ingress" }},
		{"missing sources", func(e *fabricopsv1alpha1.ExternalEndpoint) { e.Exposure.AllowedSourceCIDRs = nil }},
		{"invalid CIDR", func(e *fabricopsv1alpha1.ExternalEndpoint) { e.Exposure.AllowedSourceCIDRs = []string{"example.com"} }},
		{"override", func(e *fabricopsv1alpha1.ExternalEndpoint) { e.TLSHostnameOverride = "localhost" }},
		{"ownership annotation", func(e *fabricopsv1alpha1.ExternalEndpoint) {
			e.Exposure.ServiceAnnotations = map[string]string{"fabricops.io/fabricnetwork": "other"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, endpoint := exposureFixture()
			tc.change(&endpoint)
			if len(validateExternalExposure("endpoint", endpoint)) == 0 {
				t.Fatal("unsafe exposure accepted")
			}
		})
	}
}

func TestExternalServicePreservesAllocationsAndRepairsDrift(t *testing.T) {
	net, org, endpoint := exposureFixture()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	internal := buildOrdererService(net, org, org.Orderers[0], 0, "org")
	desired := buildExternalClientService(net, org, internal, endpoint)
	existing := desired.DeepCopy()
	existing.Spec.ClusterIP = "10.0.0.20"
	existing.Spec.Ports[0].NodePort = 31000
	existing.Spec.LoadBalancerSourceRanges = []string{"0.0.0.0/0"}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()
	r := &FabricNetworkReconciler{Client: c, Scheme: scheme}
	if err := r.ensureExternalClientService(t.Context(), desired); err != nil {
		t.Fatal(err)
	}
	actual := &corev1.Service{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(existing), actual); err != nil {
		t.Fatal(err)
	}
	if actual.Spec.ClusterIP != "10.0.0.20" || actual.Spec.Ports[0].NodePort != 31000 ||
		!reflect.DeepEqual(actual.Spec.LoadBalancerSourceRanges, endpoint.Exposure.AllowedSourceCIDRs) {
		t.Fatalf("drift repair lost allocations or source restrictions: %+v", actual.Spec)
	}
}

func TestExternalProvisioningDoesNotGateInternalOperationsHealth(t *testing.T) {
	statuses := []fabricopsv1alpha1.OrgStatus{{IdentityReady: true, CAReady: true, OrderersReady: true, PeersReady: true,
		ExternalEndpoints: []fabricopsv1alpha1.ExternalEndpointStatus{{Reason: "LoadBalancerPending"}}}}
	if !observabilityReady(statuses) {
		t.Fatal("external allocation incorrectly gates internal operations health")
	}
}

func TestExternalPeerServiceDoesNotExposeChaincodePort(t *testing.T) {
	net, org, endpoint := exposureFixture()
	org.Peer = &fabricopsv1alpha1.PeerConfig{Prefix: "peer", Instances: 1}
	endpoint.Name, endpoint.Address = "peer0", "peer.example.com:7443"
	internal := buildPeerService(net, org, 0, "org")
	external := buildExternalClientService(net, org, internal, endpoint)
	if len(external.Spec.Ports) != 1 || external.Spec.Ports[0].Port != 7443 ||
		external.Spec.Ports[0].TargetPort.IntVal != peerPort {
		t.Fatal("peer exposure included a non-client port")
	}
}

func TestExternalConfigurationConditionRequiresObservedEndpoints(t *testing.T) {
	net, _, _ := exposureFixture()
	conditions := externalExposureCondition(net, nil, nil)
	if apiMeta.IsStatusConditionTrue(conditions, "ExternalEndpointsConfigured") {
		t.Fatal("unobserved endpoints were reported as configured")
	}
	conditions = externalExposureCondition(net, []fabricopsv1alpha1.OrgStatus{{
		ExternalEndpoints: []fabricopsv1alpha1.ExternalEndpointStatus{{ConfigurationReady: true}},
	}}, conditions)
	if !apiMeta.IsStatusConditionTrue(conditions, "ExternalEndpointsConfigured") {
		t.Fatal("configured endpoint was not reported")
	}
	net.Spec.Orgs[0].Orderers[0].ExternalEndpoints = nil
	if apiMeta.FindStatusCondition(externalExposureCondition(net, nil, conditions), "ExternalEndpointsConfigured") != nil {
		t.Fatal("removed exposure left a stale condition")
	}
}

func TestExternalServiceRefusesForeignUID(t *testing.T) {
	net, org, endpoint := exposureFixture()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	desired := buildExternalClientService(net, org, buildOrdererService(net, org, org.Orderers[0], 0, "org"), endpoint)
	foreign := desired.DeepCopy()
	foreign.Annotations[annotationFabricNetworkUID] = "another-network-uid"
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(foreign).Build()
	r := &FabricNetworkReconciler{Client: c}
	if err := r.ensureExternalClientService(t.Context(), desired); err == nil {
		t.Fatal("foreign Service UID was adopted")
	}
}
