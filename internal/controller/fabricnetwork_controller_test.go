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
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	fabricopsv1alpha1 "github.com/dpereowei/fabricops/api/v1alpha1"
)

var _ = Describe("FabricNetwork Controller", func() {
	Context("When mapping secondary resources", func() {
		It("should map annotated objects to the owning FabricNetwork", func() {
			object := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "peer0",
					Namespace: "fo-sample-banka",
					Annotations: map[string]string{
						annotationFabricNetwork:          "fabricnetwork.sample",
						annotationFabricNetworkNamespace: "control-plane",
					},
					Labels: map[string]string{
						labelFabricNetwork:          "sanitized-name",
						labelFabricNetworkNamespace: "sanitized-namespace",
					},
				},
			}

			requests := fabricNetworkRequestsForObject(context.Background(), object)

			Expect(requests).To(Equal([]reconcile.Request{
				{
					NamespacedName: types.NamespacedName{
						Name:      "fabricnetwork.sample",
						Namespace: "control-plane",
					},
				},
			}))
		})

		It("should fall back to labels when annotations are absent", func() {
			object := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "channel-config",
					Namespace: "fo-sample-orderer",
					Labels: map[string]string{
						labelFabricNetwork:          "fabricnetwork-sample",
						labelFabricNetworkNamespace: "default",
					},
				},
			}

			requests := fabricNetworkRequestsForObject(context.Background(), object)

			Expect(requests).To(Equal([]reconcile.Request{
				{
					NamespacedName: types.NamespacedName{
						Name:      "fabricnetwork-sample",
						Namespace: "default",
					},
				},
			}))
		})

		It("should ignore unrelated objects", func() {
			object := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "external-secret",
					Namespace: "default",
				},
			}

			Expect(fabricNetworkRequestsForObject(context.Background(), object)).To(BeEmpty())
		})
	})

	Context("When reconciling a resource", func() {
		const (
			resourceName      = "fabricnetwork-test"
			resourceNamespace = "default"
		)

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: resourceNamespace,
		}

		BeforeEach(func() {
			deleteFabricNetworkIfExists(ctx, typeNamespacedName)
			cleanupOrgNamespaceResources(ctx, "fo-test-orderer")
			cleanupOrgNamespaceResources(ctx, "fo-test-banka")

			By("creating the custom resource for the Kind FabricNetwork")
			resource := &fabricopsv1alpha1.FabricNetwork{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: resourceNamespace,
				},
				Spec: fabricopsv1alpha1.FabricNetworkSpec{
					Global: fabricopsv1alpha1.GlobalConfig{
						FabricVersion: "3.1.0",
						TLS:           true,
						Storage: &fabricopsv1alpha1.StorageConfig{
							CA: &fabricopsv1alpha1.ComponentStorageConfig{
								Size:             "2Gi",
								StorageClassName: stringPtr("fabricops-ca"),
							},
							Orderer: &fabricopsv1alpha1.ComponentStorageConfig{
								Size:             "8Gi",
								StorageClassName: stringPtr("fabricops-orderer"),
							},
							Peer: &fabricopsv1alpha1.ComponentStorageConfig{
								Size:             "12Gi",
								StorageClassName: stringPtr("fabricops-peer"),
							},
						},
					},
					Orgs: []fabricopsv1alpha1.Org{
						{
							Organization: fabricopsv1alpha1.OrgMeta{
								Name:    "Orderer",
								Domain:  "orderer.example.com",
								MSPName: "OrdererMSP",
							},
							CA: fabricopsv1alpha1.CAConfig{DB: "sqlite"},
							Orderers: []fabricopsv1alpha1.OrdererGroup{
								{
									GroupName: "group1",
									Type:      "raft",
									Instances: 1,
									Prefix:    componentOrderer,
								},
							},
						},
						{
							Organization: fabricopsv1alpha1.OrgMeta{
								Name:    "BankA",
								Domain:  "banka.example.com",
								MSPName: "BankAMSP",
							},
							CA: fabricopsv1alpha1.CAConfig{DB: "sqlite"},
							Peer: &fabricopsv1alpha1.PeerConfig{
								Instances: 1,
								DB:        "CouchDB",
								Prefix:    componentPeer,
							},
						},
					},
					Channels:   []fabricopsv1alpha1.Channel{},
					Chaincodes: []fabricopsv1alpha1.Chaincode{},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
		})

		AfterEach(func() {
			By("Cleanup the specific resource instance FabricNetwork")
			deleteFabricNetworkIfExists(ctx, typeNamespacedName)

			for _, namespaceName := range []string{
				"fo-test-orderer",
				"fo-test-banka",
			} {
				cleanupOrgNamespaceResources(ctx, namespaceName)
			}
		})

		It("should create org infrastructure resources in per-org namespaces", func() {
			By("Reconciling the created resource")
			controllerReconciler := &FabricNetworkReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			ordererNamespace := "fo-test-orderer"
			bankNamespace := "fo-test-banka"

			var ns corev1.Namespace
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: ordererNamespace}, &ns)).To(Succeed())
			Expect(ns.Labels[labelFabricNetwork]).To(Equal(resourceName))
			Expect(ns.Labels[labelFabricNetworkNamespace]).To(Equal(resourceNamespace))
			Expect(ns.Labels[labelOrg]).To(Equal("orderer"))
			Expect(ns.Labels[labelAppName]).To(Equal(appName))
			Expect(ns.Labels[labelAppComponent]).To(Equal("namespace"))
			Expect(ns.Annotations[annotationFabricNetwork]).To(Equal(resourceName))
			Expect(ns.Annotations[annotationOrg]).To(Equal("Orderer"))
			Expect(ns.Annotations[annotationFabricNetworkUID]).NotTo(BeEmpty())

			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: bankNamespace}, &ns)).To(Succeed())
			Expect(ns.Labels[labelFabricNetwork]).To(Equal(resourceName))
			Expect(ns.Labels[labelFabricNetworkNamespace]).To(Equal(resourceNamespace))
			Expect(ns.Labels[labelOrg]).To(Equal("banka"))
			Expect(ns.Labels[labelAppName]).To(Equal(appName))
			Expect(ns.Labels[labelAppComponent]).To(Equal("namespace"))
			Expect(ns.Annotations[annotationFabricNetwork]).To(Equal(resourceName))
			Expect(ns.Annotations[annotationOrg]).To(Equal("BankA"))
			Expect(ns.Annotations[annotationFabricNetworkUID]).NotTo(BeEmpty())

			expectDeploymentNotFound(ctx, ordererNamespace, "orderer0")
			expectServiceNotFound(ctx, ordererNamespace, "orderer0")

			var caDeploy appsv1.Deployment
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "banka-ca",
			}, &caDeploy)).To(Succeed())
			Expect(caDeploy.Spec.Strategy.Type).To(Equal(appsv1.RecreateDeploymentStrategyType))
			caContainer := caDeploy.Spec.Template.Spec.Containers[0]
			caEnv := envMap(caContainer)
			Expect(caEnv["FABRIC_CA_HOME"]).To(Equal(caHomePath))
			Expect(caEnv["FABRIC_CA_SERVER_CA_NAME"]).To(Equal("banka"))
			Expect(caEnv["FABRIC_CA_SERVER_PORT"]).To(Equal("7054"))
			Expect(envSecretRefs(caContainer)).To(HaveKeyWithValue(caBootstrapEnvVar, "banka-ca-bootstrap/user-pass"))
			Expect(caContainer.Command).To(Equal([]string{
				"sh", "-c",
				"fabric-ca-server start -b \"$FABRIC_CA_SERVER_BOOTSTRAP_USER_PASS\" -d",
			}))
			expectTCPProbe(caContainer.ReadinessProbe, caPort)
			expectTCPProbe(caContainer.LivenessProbe, caPort)
			expectContainerResources(caContainer, defaultCARequestCPU, defaultCARequestMemory, defaultCALimitCPU, defaultCALimitMemory)
			Expect(pvcVolumeNames(caDeploy.Spec.Template.Spec)).To(HaveKeyWithValue(dataVolumeName, "banka-ca-data"))
			Expect(volumeMountPaths(caContainer)).To(HaveKeyWithValue(dataVolumeName, caHomePath))
			Expect(caDeploy.Labels[labelAppComponent]).To(Equal(componentCA))
			Expect(caDeploy.Annotations[annotationOrg]).To(Equal("BankA"))
			Expect(caDeploy.Spec.Template.Annotations[annotationFabricNetwork]).To(Equal(resourceName))

			expectPersistentVolumeClaim(ctx, ordererNamespace, "orderer-ca-data", "2Gi", "fabricops-ca")
			expectPersistentVolumeClaim(ctx, bankNamespace, "banka-ca-data", "2Gi", "fabricops-ca")

			var caSvc corev1.Service
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "banka-ca",
			}, &caSvc)).To(Succeed())
			Expect(caSvc.Labels[labelAppComponent]).To(Equal(componentCA))
			Expect(caSvc.Annotations[annotationOrg]).To(Equal("BankA"))
			Expect(caSvc.Spec.Selector[labelComponent]).To(Equal(componentCA))

			expectDeploymentNotFound(ctx, bankNamespace, "peer0")
			expectServiceNotFound(ctx, bankNamespace, "peer0")

			var network fabricopsv1alpha1.FabricNetwork
			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			Expect(network.Finalizers).To(ContainElement(fabricNetworkFinalizer))
			Expect(network.Status.Phase).To(Equal(fabricopsv1alpha1.PhaseCreating))
			Expect(network.Status.Message).To(Equal("Waiting for required Fabric identity material"))
			Expect(network.Status.OrgStatus).To(HaveLen(2))
			Expect(network.Status.OrgStatus[0].Name).To(Equal("Orderer"))
			Expect(network.Status.OrgStatus[0].Namespace).To(Equal(ordererNamespace))
			Expect(network.Status.OrgStatus[0].IdentityReady).To(BeFalse())
			Expect(network.Status.OrgStatus[0].IdentityError).To(ContainSubstring("orderer-admin-msp"))
			Expect(network.Status.OrgStatus[0].IdentityError).To(ContainSubstring("orderer0-msp"))
			Expect(network.Status.OrgStatus[0].CAReady).To(BeFalse())
			Expect(network.Status.OrgStatus[0].CAEndpoint).To(Equal("http://orderer-ca.fo-test-orderer.svc.cluster.local:7054"))
			Expect(network.Status.OrgStatus[0].OrdererEndpoints).To(HaveLen(1))
			Expect(network.Status.OrgStatus[0].OrdererEndpoints[0].Name).To(Equal("orderer0"))
			Expect(network.Status.OrgStatus[0].OrdererEndpoints[0].ClientAddress).To(Equal("orderer0.fo-test-orderer.svc.cluster.local:7050"))
			Expect(network.Status.OrgStatus[0].OrdererEndpoints[0].AdminAddress).To(Equal("orderer0.fo-test-orderer.svc.cluster.local:9443"))
			Expect(network.Status.OrgStatus[0].OrdererEndpoints[0].OperationsAddress).To(Equal("http://orderer0-operations.fo-test-orderer.svc.cluster.local:8443"))
			Expect(network.Status.OrgStatus[0].Orderers.Desired).To(Equal(int32(1)))
			Expect(network.Status.OrgStatus[0].Orderers.Ready).To(Equal(int32(0)))
			Expect(network.Status.OrgStatus[0].OrderersReady).To(BeFalse())
			Expect(network.Status.OrgStatus[0].Peers.Desired).To(Equal(int32(0)))
			Expect(network.Status.OrgStatus[0].Peers.Ready).To(Equal(int32(0)))
			Expect(network.Status.OrgStatus[0].PeersReady).To(BeTrue())
			Expect(network.Status.OrgStatus[0].Ready).To(BeFalse())
			Expect(network.Status.OrgStatus[1].Name).To(Equal("BankA"))
			Expect(network.Status.OrgStatus[1].Namespace).To(Equal(bankNamespace))
			Expect(network.Status.OrgStatus[1].IdentityReady).To(BeFalse())
			Expect(network.Status.OrgStatus[1].IdentityError).To(ContainSubstring("banka-admin-msp"))
			Expect(network.Status.OrgStatus[1].IdentityError).To(ContainSubstring("peer0-msp"))
			Expect(network.Status.OrgStatus[1].CAReady).To(BeFalse())
			Expect(network.Status.OrgStatus[1].CAEndpoint).To(Equal("http://banka-ca.fo-test-banka.svc.cluster.local:7054"))
			Expect(network.Status.OrgStatus[1].Orderers.Desired).To(Equal(int32(0)))
			Expect(network.Status.OrgStatus[1].Orderers.Ready).To(Equal(int32(0)))
			Expect(network.Status.OrgStatus[1].OrderersReady).To(BeTrue())
			Expect(network.Status.OrgStatus[1].PeerEndpoints).To(HaveLen(1))
			Expect(network.Status.OrgStatus[1].PeerEndpoints[0].Name).To(Equal("peer0"))
			Expect(network.Status.OrgStatus[1].PeerEndpoints[0].Address).To(Equal("peer0.fo-test-banka.svc.cluster.local:7051"))
			Expect(network.Status.OrgStatus[1].PeerEndpoints[0].ChaincodeAddress).To(Equal("peer0.fo-test-banka.svc.cluster.local:7052"))
			Expect(network.Status.OrgStatus[1].PeerEndpoints[0].OperationsAddress).To(Equal("http://peer0-operations.fo-test-banka.svc.cluster.local:9443"))
			Expect(network.Status.OrgStatus[1].Peers.Desired).To(Equal(int32(1)))
			Expect(network.Status.OrgStatus[1].Peers.Ready).To(Equal(int32(0)))
			Expect(network.Status.OrgStatus[1].PeersReady).To(BeFalse())
			Expect(network.Status.OrgStatus[1].Ready).To(BeFalse())

			ready := apiMeta.FindStatusCondition(network.Status.Conditions, conditionReady)
			Expect(ready).NotTo(BeNil())
			Expect(ready.Status).To(Equal(metav1.ConditionFalse))
			Expect(ready.Reason).To(Equal("IdentityMaterialMissing"))

			identity := apiMeta.FindStatusCondition(network.Status.Conditions, conditionIdentityMaterialReady)
			Expect(identity).NotTo(BeNil())
			Expect(identity.Status).To(Equal(metav1.ConditionFalse))
			Expect(identity.Reason).To(Equal("IdentityMaterialMissing"))

			observability := apiMeta.FindStatusCondition(network.Status.Conditions, conditionObservabilityReady)
			Expect(observability).NotTo(BeNil())
			Expect(observability.Status).To(Equal(metav1.ConditionFalse))
			Expect(observability.Reason).To(Equal("OperationsEndpointsPending"))
			Expect(observability.Message).To(ContainSubstring("CA not ready"))

			expectIdentitySecret(ctx, ordererNamespace, caBootstrapSecretName(network.Spec.Orgs[0]), secretKindCABootstrap)
			expectIdentitySecret(ctx, ordererNamespace, adminEnrollmentSecretName(network.Spec.Orgs[0]), secretKindAdminEnroll)
			expectIdentitySecret(ctx, ordererNamespace, "orderer0-enrollment", secretKindWorkloadEnroll)
			expectSecretNotFound(ctx, ordererNamespace, identitySecretName(adminIdentityName(network.Spec.Orgs[0]), secretKindMSP))
			expectSecretNotFound(ctx, ordererNamespace, identitySecretName(adminIdentityName(network.Spec.Orgs[0]), secretKindTLS))
			expectSecretNotFound(ctx, ordererNamespace, "orderer0-msp")
			expectSecretNotFound(ctx, ordererNamespace, "orderer0-tls")
			expectIdentitySecret(ctx, bankNamespace, caBootstrapSecretName(network.Spec.Orgs[1]), secretKindCABootstrap)
			expectIdentitySecret(ctx, bankNamespace, adminEnrollmentSecretName(network.Spec.Orgs[1]), secretKindAdminEnroll)
			expectIdentitySecret(ctx, bankNamespace, "peer0-enrollment", secretKindWorkloadEnroll)
			expectSecretNotFound(ctx, bankNamespace, identitySecretName(adminIdentityName(network.Spec.Orgs[1]), secretKindMSP))
			expectSecretNotFound(ctx, bankNamespace, identitySecretName(adminIdentityName(network.Spec.Orgs[1]), secretKindTLS))
			expectSecretNotFound(ctx, bankNamespace, "peer0-msp")
			expectSecretNotFound(ctx, bankNamespace, "peer0-tls")
		})

		It("should create and repair org boundary NetworkPolicies when enabled", func() {
			var network fabricopsv1alpha1.FabricNetwork
			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			network.Spec.Global.NetworkPolicy = &fabricopsv1alpha1.NetworkPolicyConfig{Enabled: true}
			Expect(k8sClient.Update(ctx, &network)).To(Succeed())

			By("Reconciling the created resource")
			controllerReconciler := &FabricNetworkReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			ordererNamespace := "fo-test-orderer"
			bankNamespace := "fo-test-banka"

			var ordererPolicy networkingv1.NetworkPolicy
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: ordererNamespace,
				Name:      orgBoundaryNetworkPolicyName(),
			}, &ordererPolicy)).To(Succeed())
			Expect(ordererPolicy.Labels[labelAppComponent]).To(Equal(componentNetwork))
			Expect(ordererPolicy.Annotations[annotationOrg]).To(Equal("Orderer"))

			var bankPolicy networkingv1.NetworkPolicy
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      orgBoundaryNetworkPolicyName(),
			}, &bankPolicy)).To(Succeed())
			Expect(bankPolicy.Labels[labelAppComponent]).To(Equal(componentNetwork))
			Expect(bankPolicy.Annotations[annotationOrg]).To(Equal("BankA"))
			Expect(bankPolicy.Spec.PodSelector.MatchLabels).To(HaveKeyWithValue(labelFabricNetwork, resourceName))
			Expect(bankPolicy.Spec.PodSelector.MatchLabels).To(HaveKeyWithValue(labelFabricNetworkNamespace, resourceNamespace))
			Expect(bankPolicy.Spec.PodSelector.MatchLabels).To(HaveKeyWithValue(labelOrg, "banka"))
			Expect(bankPolicy.Spec.PolicyTypes).To(ConsistOf(
				networkingv1.PolicyTypeIngress,
				networkingv1.PolicyTypeEgress,
			))
			Expect(bankPolicy.Spec.Ingress).To(HaveLen(1))
			Expect(bankPolicy.Spec.Ingress[0].From).To(HaveLen(1))
			Expect(bankPolicy.Spec.Ingress[0].From[0].NamespaceSelector.MatchLabels).To(HaveKeyWithValue(labelFabricNetwork, resourceName))
			Expect(bankPolicy.Spec.Ingress[0].From[0].NamespaceSelector.MatchLabels).To(HaveKeyWithValue(labelFabricNetworkNamespace, resourceNamespace))
			Expect(bankPolicy.Spec.Ingress[0].From[0].PodSelector.MatchLabels).To(HaveKeyWithValue(labelFabricNetwork, resourceName))
			Expect(bankPolicy.Spec.Ingress[0].From[0].PodSelector.MatchLabels).NotTo(HaveKey(labelOrg))
			Expect(bankPolicy.Spec.Egress).To(HaveLen(3))

			bankPolicy.Labels = map[string]string{}
			bankPolicy.Annotations = map[string]string{}
			bankPolicy.Spec.Egress = nil
			Expect(k8sClient.Update(ctx, &bankPolicy)).To(Succeed())

			By("Reconciling after NetworkPolicy drift")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      orgBoundaryNetworkPolicyName(),
			}, &bankPolicy)).To(Succeed())
			Expect(bankPolicy.Labels[labelAppComponent]).To(Equal(componentNetwork))
			Expect(bankPolicy.Annotations[annotationOrg]).To(Equal("BankA"))
			Expect(bankPolicy.Spec.Egress).To(HaveLen(3))

			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			network.Spec.Global.NetworkPolicy = nil
			Expect(k8sClient.Update(ctx, &network)).To(Succeed())

			By("Reconciling after NetworkPolicy is disabled")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			err = k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      orgBoundaryNetworkPolicyName(),
			}, &bankPolicy)
			Expect(errors.IsNotFound(err)).To(BeTrue())
		})

		It("should not generate fallback identity secrets after enrollment failure", func() {
			By("Reconciling the created resource")
			controllerReconciler := &FabricNetworkReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			ordererNamespace := "fo-test-orderer"
			bankNamespace := "fo-test-banka"
			markDeploymentReady(ctx, ordererNamespace, "orderer-ca")
			markDeploymentReady(ctx, bankNamespace, "banka-ca")

			By("Reconciling after CAs report readiness")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			var network fabricopsv1alpha1.FabricNetwork
			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())

			By("Marking enrollment jobs as failed")
			markJobFailed(ctx, ordererNamespace, adminEnrollmentJobName(network.Spec.Orgs[0]))
			markJobFailed(ctx, ordererNamespace, workloadEnrollmentJobName("orderer0"))
			markJobFailed(ctx, bankNamespace, adminEnrollmentJobName(network.Spec.Orgs[1]))
			markJobFailed(ctx, bankNamespace, workloadEnrollmentJobName("peer0"))

			By("Reconciling after enrollment failures")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			expectSecretNotFound(ctx, ordererNamespace, identitySecretName(adminIdentityName(network.Spec.Orgs[0]), secretKindMSP))
			expectSecretNotFound(ctx, ordererNamespace, identitySecretName(adminIdentityName(network.Spec.Orgs[0]), secretKindTLS))
			expectSecretNotFound(ctx, ordererNamespace, "orderer0-msp")
			expectSecretNotFound(ctx, ordererNamespace, "orderer0-tls")
			expectSecretNotFound(ctx, bankNamespace, identitySecretName(adminIdentityName(network.Spec.Orgs[1]), secretKindMSP))
			expectSecretNotFound(ctx, bankNamespace, identitySecretName(adminIdentityName(network.Spec.Orgs[1]), secretKindTLS))
			expectSecretNotFound(ctx, bankNamespace, "peer0-msp")
			expectSecretNotFound(ctx, bankNamespace, "peer0-tls")
			expectDeploymentNotFound(ctx, ordererNamespace, "orderer0")
			expectDeploymentNotFound(ctx, bankNamespace, "peer0")

			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			Expect(network.Status.OrgStatus[0].IdentityReady).To(BeFalse())
			Expect(network.Status.OrgStatus[0].IdentityError).To(ContainSubstring("orderer-admin-msp"))
			Expect(network.Status.OrgStatus[0].IdentityError).To(ContainSubstring("orderer0-msp"))
			Expect(network.Status.OrgStatus[1].IdentityReady).To(BeFalse())
			Expect(network.Status.OrgStatus[1].IdentityError).To(ContainSubstring("banka-admin-msp"))
			Expect(network.Status.OrgStatus[1].IdentityError).To(ContainSubstring("peer0-msp"))
		})

		It("should create admin enrollment resources after org CAs are ready", func() {
			By("Reconciling the created resource")
			controllerReconciler := &FabricNetworkReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			ordererNamespace := "fo-test-orderer"
			bankNamespace := "fo-test-banka"
			markDeploymentReady(ctx, ordererNamespace, "orderer-ca")
			markDeploymentReady(ctx, bankNamespace, "banka-ca")

			By("Reconciling after CAs report readiness")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			var network fabricopsv1alpha1.FabricNetwork
			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			bankOrg := network.Spec.Orgs[1]

			var serviceAccount corev1.ServiceAccount
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      enrollmentServiceAccountName(bankOrg),
			}, &serviceAccount)).To(Succeed())
			Expect(serviceAccount.Labels[labelAppComponent]).To(Equal(componentAdmin))
			Expect(serviceAccount.Annotations[annotationOrg]).To(Equal("BankA"))

			var role rbacv1.Role
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      enrollmentServiceAccountName(bankOrg),
			}, &role)).To(Succeed())
			Expect(role.Annotations[annotationFabricNetwork]).To(Equal(resourceName))
			Expect(role.Rules).To(ContainElement(rbacv1.PolicyRule{
				APIGroups: []string{""},
				Resources: []string{"secrets"},
				Verbs:     []string{"get", "create", "update", "patch"},
			}))

			var roleBinding rbacv1.RoleBinding
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      enrollmentServiceAccountName(bankOrg),
			}, &roleBinding)).To(Succeed())
			Expect(roleBinding.RoleRef.Name).To(Equal(enrollmentServiceAccountName(bankOrg)))
			Expect(roleBinding.Subjects).To(ContainElement(rbacv1.Subject{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      enrollmentServiceAccountName(bankOrg),
				Namespace: bankNamespace,
			}))

			var job batchv1.Job
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      adminEnrollmentJobName(bankOrg),
			}, &job)).To(Succeed())
			Expect(job.Labels[labelAppComponent]).To(Equal(componentAdmin))
			Expect(job.Annotations[annotationOrg]).To(Equal("BankA"))
			Expect(job.Annotations[annotationSucceededJobCleanup]).To(Equal("true"))
			Expect(job.Spec.Template.Annotations[annotationFabricNetwork]).To(Equal(resourceName))
			Expect(job.Spec.Template.Spec.ServiceAccountName).To(Equal(enrollmentServiceAccountName(bankOrg)))
			Expect(job.Spec.Template.Spec.RestartPolicy).To(Equal(corev1.RestartPolicyNever))
			Expect(job.Spec.Template.Spec.InitContainers).To(HaveLen(1))
			Expect(job.Spec.Template.Spec.Containers).To(HaveLen(1))

			enrollContainer := job.Spec.Template.Spec.InitContainers[0]
			Expect(enrollContainer.Name).To(Equal(enrollAdminContainerName))
			Expect(enrollContainer.Image).To(Equal(caImage()))
			Expect(envMap(enrollContainer)[envCAAddress]).To(Equal("banka-ca.fo-test-banka.svc.cluster.local:7054"))
			Expect(envMap(enrollContainer)[envAdminName]).To(Equal("banka-admin"))
			Expect(envSecretRefs(enrollContainer)).To(HaveKeyWithValue(envCABootstrapUserPass, "banka-ca-bootstrap/user-pass"))
			Expect(envSecretRefs(enrollContainer)).To(HaveKeyWithValue(envAdminUsername, "banka-admin-enrollment/username"))
			Expect(envSecretRefs(enrollContainer)).To(HaveKeyWithValue(envAdminPassword, "banka-admin-enrollment/password"))
			Expect(enrollContainer.Command[2]).To(ContainSubstring("fabric-ca-client register"))
			Expect(enrollContainer.Command[2]).To(ContainSubstring("fabric-ca-client enroll"))
			expectContainerResources(enrollContainer, defaultCARequestCPU, defaultCARequestMemory, defaultCALimitCPU, defaultCALimitMemory)

			publishContainer := job.Spec.Template.Spec.Containers[0]
			Expect(publishContainer.Name).To(Equal(publishAdminContainerName))
			Expect(publishContainer.Image).To(Equal(kubectlImage()))
			Expect(envMap(publishContainer)[envAdminMSPSecret]).To(Equal("banka-admin-msp"))
			Expect(envMap(publishContainer)[envAdminTLSSecret]).To(Equal("banka-admin-tls"))
			Expect(envMap(publishContainer)[envTLSEnabled]).To(Equal("true"))
			Expect(publishContainer.Command[2]).To(ContainSubstring("kubectl -n \"$POD_NAMESPACE\" create secret generic"))
			Expect(publishContainer.Command[2]).To(ContainSubstring(labelIdentitySource + "=" + identitySourceFabricCA))
			expectContainerResources(publishContainer, defaultKubectlRequestCPU, defaultKubectlRequestMem, defaultKubectlLimitCPU, defaultKubectlLimitMem)

			var ordererJob batchv1.Job
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: ordererNamespace,
				Name:      workloadEnrollmentJobName("orderer0"),
			}, &ordererJob)).To(Succeed())
			Expect(ordererJob.Annotations[annotationSucceededJobCleanup]).To(Equal("true"))
			ordererEnrollContainer := ordererJob.Spec.Template.Spec.InitContainers[0]
			Expect(ordererEnrollContainer.Name).To(Equal(enrollWorkloadContainerName))
			Expect(envMap(ordererEnrollContainer)[envWorkloadName]).To(Equal("orderer0"))
			Expect(envMap(ordererEnrollContainer)[envWorkloadType]).To(Equal(componentOrderer))
			Expect(envSecretRefs(ordererEnrollContainer)).To(HaveKeyWithValue(envWorkloadUsername, "orderer0-enrollment/username"))
			Expect(envSecretRefs(ordererEnrollContainer)).To(HaveKeyWithValue(envWorkloadPassword, "orderer0-enrollment/password"))
			expectContainerResources(ordererEnrollContainer, defaultCARequestCPU, defaultCARequestMemory, defaultCALimitCPU, defaultCALimitMemory)

			var peerJob batchv1.Job
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      workloadEnrollmentJobName("peer0"),
			}, &peerJob)).To(Succeed())
			Expect(peerJob.Labels[labelWorkload]).To(Equal("peer0"))
			Expect(peerJob.Annotations[annotationSucceededJobCleanup]).To(Equal("true"))
			Expect(peerJob.Spec.Template.Annotations[annotationOrg]).To(Equal("BankA"))
			Expect(peerJob.Spec.Template.Spec.ServiceAccountName).To(Equal(enrollmentServiceAccountName(bankOrg)))
			Expect(peerJob.Spec.Template.Spec.RestartPolicy).To(Equal(corev1.RestartPolicyNever))
			Expect(peerJob.Spec.Template.Spec.InitContainers).To(HaveLen(1))
			Expect(peerJob.Spec.Template.Spec.Containers).To(HaveLen(1))

			workloadEnrollContainer := peerJob.Spec.Template.Spec.InitContainers[0]
			Expect(workloadEnrollContainer.Name).To(Equal(enrollWorkloadContainerName))
			Expect(workloadEnrollContainer.Image).To(Equal(caImage()))
			Expect(envMap(workloadEnrollContainer)[envCAAddress]).To(Equal("banka-ca.fo-test-banka.svc.cluster.local:7054"))
			Expect(envMap(workloadEnrollContainer)[envWorkloadName]).To(Equal("peer0"))
			Expect(envMap(workloadEnrollContainer)[envWorkloadType]).To(Equal(componentPeer))
			Expect(envMap(workloadEnrollContainer)[envWorkloadCSRHosts]).To(ContainSubstring("peer0.fo-test-banka.svc.cluster.local"))
			Expect(envMap(workloadEnrollContainer)[envTLSEnabled]).To(Equal("true"))
			Expect(envSecretRefs(workloadEnrollContainer)).To(HaveKeyWithValue(envCABootstrapUserPass, "banka-ca-bootstrap/user-pass"))
			Expect(envSecretRefs(workloadEnrollContainer)).To(HaveKeyWithValue(envWorkloadUsername, "peer0-enrollment/username"))
			Expect(envSecretRefs(workloadEnrollContainer)).To(HaveKeyWithValue(envWorkloadPassword, "peer0-enrollment/password"))
			Expect(workloadEnrollContainer.Command[2]).To(ContainSubstring("fabric-ca-client register"))
			Expect(workloadEnrollContainer.Command[2]).To(ContainSubstring("--id.type \"$FABRICOPS_WORKLOAD_TYPE\""))
			Expect(workloadEnrollContainer.Command[2]).To(ContainSubstring("fabric-ca-client enroll"))
			expectContainerResources(workloadEnrollContainer, defaultCARequestCPU, defaultCARequestMemory, defaultCALimitCPU, defaultCALimitMemory)

			workloadPublishContainer := peerJob.Spec.Template.Spec.Containers[0]
			Expect(workloadPublishContainer.Name).To(Equal(publishWorkloadContainerName))
			Expect(workloadPublishContainer.Image).To(Equal(kubectlImage()))
			Expect(envMap(workloadPublishContainer)[envWorkloadMSPSecret]).To(Equal("peer0-msp"))
			Expect(envMap(workloadPublishContainer)[envWorkloadTLSSecret]).To(Equal("peer0-tls"))
			Expect(envMap(workloadPublishContainer)[envTLSEnabled]).To(Equal("true"))
			Expect(workloadPublishContainer.Command[2]).To(ContainSubstring("kubectl -n \"$POD_NAMESPACE\" create secret generic"))
			Expect(workloadPublishContainer.Command[2]).To(ContainSubstring(labelIdentitySource + "=" + identitySourceFabricCA))
			expectContainerResources(workloadPublishContainer, defaultKubectlRequestCPU, defaultKubectlRequestMem, defaultKubectlLimitCPU, defaultKubectlLimitMem)
		})

		It("should repair mutable fields on managed resources", func() {
			By("Reconciling the created resource")
			controllerReconciler := &FabricNetworkReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			bankNamespace := "fo-test-banka"
			var network fabricopsv1alpha1.FabricNetwork
			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			bankOrg := network.Spec.Orgs[1]

			markDeploymentReady(ctx, bankNamespace, "banka-ca")

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			var caDeploy appsv1.Deployment
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "banka-ca",
			}, &caDeploy)).To(Succeed())
			caDeploy.Labels = map[string]string{}
			caDeploy.Annotations = map[string]string{}
			caDeploy.Spec.Strategy.Type = appsv1.RollingUpdateDeploymentStrategyType
			caDeploy.Spec.Template.Annotations = map[string]string{}
			caDeploy.Spec.Template.Spec.Containers[0].Resources = corev1.ResourceRequirements{}
			caDeploy.Spec.Template.Spec.Containers[0].ReadinessProbe = nil
			caDeploy.Spec.Template.Spec.Containers[0].LivenessProbe = nil
			Expect(k8sClient.Update(ctx, &caDeploy)).To(Succeed())

			var caSvc corev1.Service
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "banka-ca",
			}, &caSvc)).To(Succeed())
			caSvc.Labels = map[string]string{}
			caSvc.Annotations = map[string]string{}
			caSvc.Spec.Selector = map[string]string{labelComponent: "wrong"}
			caSvc.Spec.Ports[0].Port = 9999
			Expect(k8sClient.Update(ctx, &caSvc)).To(Succeed())

			var bootstrapSecret corev1.Secret
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      caBootstrapSecretName(bankOrg),
			}, &bootstrapSecret)).To(Succeed())
			bootstrapSecret.Labels = map[string]string{}
			bootstrapSecret.Annotations = map[string]string{}
			bootstrapSecret.Data = map[string][]byte{
				caBootstrapUsernameKey: []byte("broken"),
			}
			Expect(k8sClient.Update(ctx, &bootstrapSecret)).To(Succeed())

			enrollmentName := enrollmentServiceAccountName(bankOrg)
			var serviceAccount corev1.ServiceAccount
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      enrollmentName,
			}, &serviceAccount)).To(Succeed())
			serviceAccount.Labels = map[string]string{}
			serviceAccount.Annotations = map[string]string{}
			Expect(k8sClient.Update(ctx, &serviceAccount)).To(Succeed())

			var role rbacv1.Role
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      enrollmentName,
			}, &role)).To(Succeed())
			role.Labels = map[string]string{}
			role.Annotations = map[string]string{}
			role.Rules = []rbacv1.PolicyRule{
				{
					APIGroups: []string{""},
					Resources: []string{"configmaps"},
					Verbs:     []string{"get"},
				},
			}
			Expect(k8sClient.Update(ctx, &role)).To(Succeed())

			var roleBinding rbacv1.RoleBinding
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      enrollmentName,
			}, &roleBinding)).To(Succeed())
			roleBinding.Labels = map[string]string{}
			roleBinding.Annotations = map[string]string{}
			roleBinding.Subjects = nil
			Expect(k8sClient.Update(ctx, &roleBinding)).To(Succeed())

			By("Reconciling after managed fields were changed")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "banka-ca",
			}, &caDeploy)).To(Succeed())
			Expect(caDeploy.Labels[labelAppComponent]).To(Equal(componentCA))
			Expect(caDeploy.Annotations[annotationOrg]).To(Equal("BankA"))
			Expect(caDeploy.Spec.Strategy.Type).To(Equal(appsv1.RecreateDeploymentStrategyType))
			Expect(caDeploy.Spec.Template.Annotations[annotationFabricNetwork]).To(Equal(resourceName))
			caContainer := caDeploy.Spec.Template.Spec.Containers[0]
			expectTCPProbe(caContainer.ReadinessProbe, caPort)
			expectTCPProbe(caContainer.LivenessProbe, caPort)
			expectContainerResources(caContainer, defaultCARequestCPU, defaultCARequestMemory, defaultCALimitCPU, defaultCALimitMemory)
			repairedCAGeneration := caDeploy.Generation

			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "banka-ca",
			}, &caSvc)).To(Succeed())
			Expect(caSvc.Labels[labelAppComponent]).To(Equal(componentCA))
			Expect(caSvc.Annotations[annotationOrg]).To(Equal("BankA"))
			Expect(caSvc.Spec.Selector[labelComponent]).To(Equal(componentCA))
			Expect(servicePorts(caSvc)).To(ContainElements(caPort))

			expectIdentitySecret(ctx, bankNamespace, caBootstrapSecretName(bankOrg), secretKindCABootstrap)
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      caBootstrapSecretName(bankOrg),
			}, &bootstrapSecret)).To(Succeed())
			Expect(bootstrapSecret.Labels[labelAppComponent]).To(Equal(componentCA))
			Expect(bootstrapSecret.Annotations[annotationOrg]).To(Equal("BankA"))

			markDeploymentReady(ctx, bankNamespace, "banka-ca")

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      enrollmentName,
			}, &serviceAccount)).To(Succeed())
			Expect(serviceAccount.Labels[labelAppComponent]).To(Equal(componentAdmin))
			Expect(serviceAccount.Annotations[annotationOrg]).To(Equal("BankA"))

			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      enrollmentName,
			}, &role)).To(Succeed())
			Expect(role.Labels[labelAppComponent]).To(Equal(componentAdmin))
			Expect(role.Annotations[annotationOrg]).To(Equal("BankA"))
			Expect(role.Rules).To(ContainElement(rbacv1.PolicyRule{
				APIGroups: []string{""},
				Resources: []string{"secrets"},
				Verbs:     []string{"get", "create", "update", "patch"},
			}))

			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      enrollmentName,
			}, &roleBinding)).To(Succeed())
			Expect(roleBinding.Labels[labelAppComponent]).To(Equal(componentAdmin))
			Expect(roleBinding.Annotations[annotationOrg]).To(Equal("BankA"))
			Expect(roleBinding.Subjects).To(ContainElement(rbacv1.Subject{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      enrollmentName,
				Namespace: bankNamespace,
			}))

			By("Reconciling again after drift repair")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "banka-ca",
			}, &caDeploy)).To(Succeed())
			Expect(caDeploy.Generation).To(Equal(repairedCAGeneration))
		})

		It("should refuse to update generated resources owned by another FabricNetwork", func() {
			By("Reconciling the created resource")
			controllerReconciler := &FabricNetworkReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			bankNamespace := "fo-test-banka"

			var caSvc corev1.Service
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "banka-ca",
			}, &caSvc)).To(Succeed())
			caSvc.Annotations[annotationFabricNetwork] = "other-network"
			caSvc.Annotations[annotationFabricNetworkNamespace] = "other-control"
			caSvc.Labels[labelFabricNetwork] = "other-network"
			caSvc.Labels[labelFabricNetworkNamespace] = "other-control"
			Expect(k8sClient.Update(ctx, &caSvc)).To(Succeed())

			By("Reconciling after a managed resource claimed another owner")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).To(MatchError(ContainSubstring("Service fo-test-banka/banka-ca is owned by FabricNetwork other-control/other-network")))
			Expect(err).To(MatchError(ContainSubstring("refusing to update for FabricNetwork default/fabricnetwork-test")))

			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "banka-ca",
			}, &caSvc)).To(Succeed())
			Expect(caSvc.Annotations[annotationFabricNetwork]).To(Equal("other-network"))
			Expect(caSvc.Annotations[annotationFabricNetworkNamespace]).To(Equal("other-control"))
		})

		It("should remove generated org namespaces when the FabricNetwork is deleted", func() {
			deleteNamespacedName := types.NamespacedName{
				Name:      "fabricnetwork-delete-test",
				Namespace: resourceNamespace,
			}
			deleteResource := &fabricopsv1alpha1.FabricNetwork{
				ObjectMeta: metav1.ObjectMeta{
					Name:      deleteNamespacedName.Name,
					Namespace: deleteNamespacedName.Namespace,
				},
				Spec: fabricopsv1alpha1.FabricNetworkSpec{
					Global: fabricopsv1alpha1.GlobalConfig{
						FabricVersion: "3.1.0",
						TLS:           true,
					},
					Orgs: []fabricopsv1alpha1.Org{
						{
							Organization: fabricopsv1alpha1.OrgMeta{
								Name:    "DeleteOrderer",
								Domain:  "delete-orderer.example.com",
								MSPName: "DeleteOrdererMSP",
							},
							CA: fabricopsv1alpha1.CAConfig{DB: "sqlite"},
						},
						{
							Organization: fabricopsv1alpha1.OrgMeta{
								Name:    "DeleteBank",
								Domain:  "delete-bank.example.com",
								MSPName: "DeleteBankMSP",
							},
							CA: fabricopsv1alpha1.CAConfig{DB: "sqlite"},
						},
					},
					Channels:   []fabricopsv1alpha1.Channel{},
					Chaincodes: []fabricopsv1alpha1.Chaincode{},
				},
			}
			Expect(k8sClient.Create(ctx, deleteResource)).To(Succeed())

			By("Reconciling the created resource")
			controllerReconciler := &FabricNetworkReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: deleteNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			var network fabricopsv1alpha1.FabricNetwork
			Expect(k8sClient.Get(ctx, deleteNamespacedName, &network)).To(Succeed())
			Expect(network.Finalizers).To(ContainElement(fabricNetworkFinalizer))
			Expect(k8sClient.Delete(ctx, &network)).To(Succeed())

			By("Reconciling deletion")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: deleteNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() bool {
				var deleted fabricopsv1alpha1.FabricNetwork
				err := k8sClient.Get(ctx, deleteNamespacedName, &deleted)
				return errors.IsNotFound(err)
			}).Should(BeTrue())

			for _, namespaceName := range []string{"fo-delete-test-deleteorderer", "fo-delete-test-deletebank"} {
				Eventually(func() bool {
					var namespace corev1.Namespace
					err := k8sClient.Get(ctx, types.NamespacedName{Name: namespaceName}, &namespace)
					return errors.IsNotFound(err) || !namespace.DeletionTimestamp.IsZero()
				}).Should(BeTrue())
			}
		})

		It("should mark the network ready only after all org workloads are ready", func() {
			By("Reconciling the created resource")
			controllerReconciler := &FabricNetworkReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			ordererNamespace := "fo-test-orderer"
			bankNamespace := "fo-test-banka"

			markDeploymentReady(ctx, ordererNamespace, "orderer-ca")
			markDeploymentReady(ctx, bankNamespace, "banka-ca")

			By("Reconciling after CAs report readiness")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			var network fabricopsv1alpha1.FabricNetwork
			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			writeEnrolledOrgIdentitySecrets(ctx, &network, network.Spec.Orgs[0], ordererNamespace)
			writeEnrolledOrgIdentitySecrets(ctx, &network, network.Spec.Orgs[1], bankNamespace)

			By("Reconciling after enrolled identities are published")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			var ordererDeploy appsv1.Deployment
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: ordererNamespace,
				Name:      "orderer0",
			}, &ordererDeploy)).To(Succeed())
			Expect(ordererDeploy.Spec.Strategy.Type).To(Equal(appsv1.RecreateDeploymentStrategyType))
			ordererContainer := ordererDeploy.Spec.Template.Spec.Containers[0]
			ordererEnv := envMap(ordererContainer)
			Expect(ordererEnv["ORDERER_GENERAL_LISTENADDRESS"]).To(Equal("0.0.0.0"))
			Expect(ordererEnv["ORDERER_GENERAL_LISTENPORT"]).To(Equal("7050"))
			Expect(ordererEnv["ORDERER_GENERAL_BOOTSTRAPMETHOD"]).To(Equal("none"))
			Expect(ordererEnv).NotTo(HaveKey("ORDERER_GENERAL_CLUSTER_LISTENADDRESS"))
			Expect(ordererEnv).NotTo(HaveKey("ORDERER_GENERAL_CLUSTER_LISTENPORT"))
			Expect(ordererEnv["ORDERER_GENERAL_LOCALMSPID"]).To(Equal("OrdererMSP"))
			Expect(ordererEnv["ORDERER_GENERAL_LOCALMSPDIR"]).To(Equal(ordererMSPPath))
			Expect(ordererEnv["ORDERER_CHANNELPARTICIPATION_ENABLED"]).To(Equal("true"))
			Expect(ordererEnv["ORDERER_ADMIN_LISTENADDRESS"]).To(Equal("0.0.0.0:9443"))
			Expect(ordererEnv["ORDERER_OPERATIONS_LISTENADDRESS"]).To(Equal("0.0.0.0:8443"))
			Expect(ordererEnv["ORDERER_OPERATIONS_TLS_ENABLED"]).To(Equal("false"))
			Expect(ordererEnv["ORDERER_METRICS_PROVIDER"]).To(Equal("prometheus"))
			Expect(ordererEnv["ORDERER_GENERAL_TLS_ENABLED"]).To(Equal("true"))
			Expect(ordererEnv["ORDERER_GENERAL_TLS_PRIVATEKEY"]).To(Equal(ordererTLSPath + "/server.key"))
			Expect(ordererEnv["ORDERER_GENERAL_TLS_CERTIFICATE"]).To(Equal(ordererTLSPath + "/server.crt"))
			Expect(ordererEnv["ORDERER_GENERAL_TLS_ROOTCAS"]).To(Equal("[" + ordererTLSPath + "/ca.crt]"))
			Expect(ordererEnv["ORDERER_GENERAL_CLUSTER_CLIENTCERTIFICATE"]).To(Equal(ordererTLSPath + "/server.crt"))
			Expect(ordererEnv["ORDERER_GENERAL_CLUSTER_CLIENTPRIVATEKEY"]).To(Equal(ordererTLSPath + "/server.key"))
			Expect(ordererEnv["ORDERER_GENERAL_CLUSTER_ROOTCAS"]).To(Equal("[" + ordererTLSPath + "/ca.crt]"))
			Expect(ordererEnv["ORDERER_ADMIN_TLS_ENABLED"]).To(Equal("true"))
			Expect(ordererEnv["ORDERER_ADMIN_TLS_PRIVATEKEY"]).To(Equal(ordererTLSPath + "/server.key"))
			Expect(ordererEnv["ORDERER_ADMIN_TLS_CERTIFICATE"]).To(Equal(ordererTLSPath + "/server.crt"))
			Expect(ordererEnv["ORDERER_ADMIN_TLS_ROOTCAS"]).To(Equal("[" + ordererTLSPath + "/ca.crt]"))
			Expect(ordererEnv["ORDERER_ADMIN_TLS_CLIENTAUTHREQUIRED"]).To(Equal("true"))
			Expect(ordererEnv["ORDERER_ADMIN_TLS_CLIENTROOTCAS"]).To(Equal("[" + ordererTLSPath + "/ca.crt]"))
			Expect(ordererDeploy.Labels[labelWorkload]).To(Equal("orderer0"))
			Expect(ordererDeploy.Annotations[annotationOrg]).To(Equal("Orderer"))
			Expect(ordererDeploy.Spec.Template.Annotations[annotationFabricNetwork]).To(Equal(resourceName))
			Expect(containerPorts(ordererContainer)).To(ContainElements(int32(7050), int32(9443), int32(8443)))
			expectOperationsProbe(ordererContainer.ReadinessProbe)
			expectTCPProbe(ordererContainer.LivenessProbe, ordererPort)
			expectContainerResources(ordererContainer, defaultOrdererRequestCPU, defaultOrdererRequestMem, defaultOrdererLimitCPU, defaultOrdererLimitMem)
			Expect(secretVolumeNames(ordererDeploy.Spec.Template.Spec)).To(HaveKeyWithValue(secretKindMSP, "orderer0-msp"))
			Expect(secretVolumeNames(ordererDeploy.Spec.Template.Spec)).To(HaveKeyWithValue(secretKindTLS, "orderer0-tls"))
			Expect(pvcVolumeNames(ordererDeploy.Spec.Template.Spec)).To(HaveKeyWithValue(dataVolumeName, "orderer0-data"))
			Expect(secretVolumeItemKeys(ordererDeploy.Spec.Template.Spec, secretKindMSP)).To(ContainElements(mspConfigKey, mspCACertKey, mspTLSCACertKey, mspSignCertKey, mspKeyStoreKey))
			Expect(secretVolumeItemKeys(ordererDeploy.Spec.Template.Spec, secretKindTLS)).To(ContainElements(tlsCACertKey, tlsServerCertKey, tlsServerKeyKey))
			Expect(volumeMountPaths(ordererContainer)).To(HaveKeyWithValue(secretKindMSP, ordererMSPPath))
			Expect(volumeMountPaths(ordererContainer)).To(HaveKeyWithValue(secretKindTLS, ordererTLSPath))
			Expect(volumeMountPaths(ordererContainer)).To(HaveKeyWithValue(dataVolumeName, fabricProductionPath))
			expectPersistentVolumeClaim(ctx, ordererNamespace, "orderer0-data", "8Gi", "fabricops-orderer")

			var ordererSvc corev1.Service
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: ordererNamespace,
				Name:      "orderer0",
			}, &ordererSvc)).To(Succeed())
			Expect(servicePorts(ordererSvc)).To(ContainElements(int32(7050), int32(9443)))

			var ordererOpsSvc corev1.Service
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: ordererNamespace,
				Name:      "orderer0-operations",
			}, &ordererOpsSvc)).To(Succeed())
			Expect(ordererOpsSvc.Labels[labelEndpoint]).To(Equal(endpointOperations))
			Expect(ordererOpsSvc.Labels[labelAppComponent]).To(Equal("orderer-operations"))
			Expect(ordererOpsSvc.Spec.Selector[labelComponent]).To(Equal(componentOrderer))
			Expect(servicePorts(ordererOpsSvc)).To(ContainElement(int32(8443)))

			var peerDeploy appsv1.Deployment
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "peer0",
			}, &peerDeploy)).To(Succeed())
			Expect(peerDeploy.Spec.Strategy.Type).To(Equal(appsv1.RecreateDeploymentStrategyType))
			peerContainer := peerDeploy.Spec.Template.Spec.Containers[0]
			peerEnv := envMap(peerContainer)
			Expect(peerEnv["CORE_PEER_ID"]).To(Equal("peer0"))
			Expect(peerEnv["CORE_PEER_ADDRESS"]).To(Equal("peer0.fo-test-banka.svc.cluster.local:7051"))
			Expect(peerEnv["CORE_PEER_LISTENADDRESS"]).To(Equal("0.0.0.0:7051"))
			Expect(peerEnv["CORE_PEER_CHAINCODEADDRESS"]).To(Equal("peer0.fo-test-banka.svc.cluster.local:7052"))
			Expect(peerEnv["CORE_PEER_CHAINCODELISTENADDRESS"]).To(Equal("0.0.0.0:7052"))
			Expect(peerEnv["CORE_PEER_GOSSIP_ENDPOINT"]).To(Equal("peer0.fo-test-banka.svc.cluster.local:7051"))
			Expect(peerEnv["CORE_PEER_GOSSIP_EXTERNALENDPOINT"]).To(Equal("peer0.fo-test-banka.svc.cluster.local:7051"))
			Expect(peerEnv["CORE_PEER_LOCALMSPID"]).To(Equal("BankAMSP"))
			Expect(peerEnv["CORE_PEER_MSPCONFIGPATH"]).To(Equal(peerMSPPath))
			Expect(peerEnv["CORE_VM_ENDPOINT"]).To(Equal(""))
			Expect(peerEnv[ccaasBuilderEnvVar]).To(Equal(`{"peer_hostname":"peer0"}`))
			Expect(peerEnv["CORE_OPERATIONS_LISTENADDRESS"]).To(Equal("0.0.0.0:9443"))
			Expect(peerEnv["CORE_OPERATIONS_TLS_ENABLED"]).To(Equal("false"))
			Expect(peerEnv["CORE_METRICS_PROVIDER"]).To(Equal("prometheus"))
			Expect(peerEnv["CORE_PEER_TLS_ENABLED"]).To(Equal("true"))
			Expect(peerEnv["CORE_PEER_TLS_CERT_FILE"]).To(Equal(peerTLSPath + "/server.crt"))
			Expect(peerEnv["CORE_PEER_TLS_KEY_FILE"]).To(Equal(peerTLSPath + "/server.key"))
			Expect(peerEnv["CORE_PEER_TLS_ROOTCERT_FILE"]).To(Equal(peerTLSPath + "/ca.crt"))
			Expect(peerDeploy.Labels[labelWorkload]).To(Equal("peer0"))
			Expect(peerDeploy.Annotations[annotationOrg]).To(Equal("BankA"))
			Expect(peerDeploy.Spec.Template.Annotations[annotationFabricNetwork]).To(Equal(resourceName))
			Expect(containerPorts(peerContainer)).To(ContainElements(int32(7051), int32(7052), int32(9443)))
			expectTCPProbe(peerContainer.ReadinessProbe, peerPort)
			expectTCPProbe(peerContainer.LivenessProbe, peerPort)
			expectContainerResources(peerContainer, defaultPeerRequestCPU, defaultPeerRequestMem, defaultPeerLimitCPU, defaultPeerLimitMem)
			Expect(secretVolumeNames(peerDeploy.Spec.Template.Spec)).To(HaveKeyWithValue(secretKindMSP, "peer0-msp"))
			Expect(secretVolumeNames(peerDeploy.Spec.Template.Spec)).To(HaveKeyWithValue(secretKindTLS, "peer0-tls"))
			Expect(pvcVolumeNames(peerDeploy.Spec.Template.Spec)).To(HaveKeyWithValue(dataVolumeName, "peer0-data"))
			Expect(secretVolumeItemKeys(peerDeploy.Spec.Template.Spec, secretKindMSP)).To(ContainElements(mspConfigKey, mspCACertKey, mspTLSCACertKey, mspSignCertKey, mspKeyStoreKey))
			Expect(secretVolumeItemKeys(peerDeploy.Spec.Template.Spec, secretKindTLS)).To(ContainElements(tlsCACertKey, tlsServerCertKey, tlsServerKeyKey))
			Expect(volumeMountPaths(peerContainer)).To(HaveKeyWithValue(secretKindMSP, peerMSPPath))
			Expect(volumeMountPaths(peerContainer)).To(HaveKeyWithValue(secretKindTLS, peerTLSPath))
			Expect(volumeMountPaths(peerContainer)).To(HaveKeyWithValue(dataVolumeName, fabricProductionPath))
			expectPersistentVolumeClaim(ctx, bankNamespace, "peer0-data", "12Gi", "fabricops-peer")

			var peerSvc corev1.Service
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "peer0",
			}, &peerSvc)).To(Succeed())
			Expect(servicePorts(peerSvc)).To(ContainElements(int32(7051), int32(7052)))

			var peerOpsSvc corev1.Service
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "peer0-operations",
			}, &peerOpsSvc)).To(Succeed())
			Expect(peerOpsSvc.Labels[labelEndpoint]).To(Equal(endpointOperations))
			Expect(peerOpsSvc.Labels[labelAppComponent]).To(Equal("peer-operations"))
			Expect(peerOpsSvc.Spec.Selector[labelComponent]).To(Equal(componentPeer))
			Expect(servicePorts(peerOpsSvc)).To(ContainElement(int32(9443)))

			markDeploymentReady(ctx, ordererNamespace, "orderer0")
			markDeploymentReady(ctx, bankNamespace, "peer0")

			By("Reconciling after workloads report readiness")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			Expect(network.Status.Phase).To(Equal(fabricopsv1alpha1.PhaseReady))
			Expect(network.Status.Message).To(Equal("All Fabric components are ready"))
			Expect(network.Status.OrgStatus).To(HaveLen(2))
			Expect(network.Status.OrgStatus[0].Ready).To(BeTrue())
			Expect(network.Status.OrgStatus[0].IdentityReady).To(BeTrue())
			Expect(network.Status.OrgStatus[0].CAReady).To(BeTrue())
			Expect(network.Status.OrgStatus[0].OrdererEndpoints[0].ClientAddress).To(Equal("orderer0.fo-test-orderer.svc.cluster.local:7050"))
			Expect(network.Status.OrgStatus[0].Orderers.Ready).To(Equal(int32(1)))
			Expect(network.Status.OrgStatus[0].OrderersReady).To(BeTrue())
			Expect(network.Status.OrgStatus[1].Ready).To(BeTrue())
			Expect(network.Status.OrgStatus[1].IdentityReady).To(BeTrue())
			Expect(network.Status.OrgStatus[1].CAReady).To(BeTrue())
			Expect(network.Status.OrgStatus[1].PeerEndpoints[0].Address).To(Equal("peer0.fo-test-banka.svc.cluster.local:7051"))
			Expect(network.Status.OrgStatus[1].Peers.Ready).To(Equal(int32(1)))
			Expect(network.Status.OrgStatus[1].PeersReady).To(BeTrue())

			ready := apiMeta.FindStatusCondition(network.Status.Conditions, conditionReady)
			Expect(ready).NotTo(BeNil())
			Expect(ready.Status).To(Equal(metav1.ConditionTrue))
			Expect(ready.Reason).To(Equal("ComponentsReady"))

			identity := apiMeta.FindStatusCondition(network.Status.Conditions, conditionIdentityMaterialReady)
			Expect(identity).NotTo(BeNil())
			Expect(identity.Status).To(Equal(metav1.ConditionTrue))
			Expect(identity.Reason).To(Equal("IdentityMaterialPresent"))

			channels := apiMeta.FindStatusCondition(network.Status.Conditions, conditionChannelsReady)
			Expect(channels).NotTo(BeNil())
			Expect(channels.Status).To(Equal(metav1.ConditionTrue))
			Expect(channels.Reason).To(Equal("NoChannelsDeclared"))

			observability := apiMeta.FindStatusCondition(network.Status.Conditions, conditionObservabilityReady)
			Expect(observability).NotTo(BeNil())
			Expect(observability.Status).To(Equal(metav1.ConditionTrue))
			Expect(observability.Reason).To(Equal("OperationsEndpointsReady"))
		})

		It("should map declared channel memberships into status", func() {
			var network fabricopsv1alpha1.FabricNetwork
			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			network.Spec.Channels = []fabricopsv1alpha1.Channel{
				{
					Name: "settlement",
					Orgs: []fabricopsv1alpha1.ChannelOrg{
						{
							Name:  "BankA",
							Peers: []string{"peer0"},
						},
					},
				},
			}
			Expect(k8sClient.Update(ctx, &network)).To(Succeed())

			controllerReconciler := &FabricNetworkReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			Expect(network.Status.ChannelStatus).To(HaveLen(1))
			channel := network.Status.ChannelStatus[0]
			Expect(channel.Name).To(Equal("settlement"))
			Expect(channel.Ready).To(BeFalse())
			Expect(channel.ConfigReady).To(BeFalse())
			Expect(channel.BlockReady).To(BeFalse())
			Expect(channel.Message).To(Equal("Waiting for Fabric components before channel bootstrap"))
			Expect(channel.ConfigMapName).To(Equal("settlement-configtx"))
			Expect(channel.BlockConfigMapName).To(Equal("settlement-channel-block"))
			Expect(channel.Orderers.Desired).To(Equal(int32(1)))
			Expect(channel.Orderers.Ready).To(Equal(int32(0)))
			Expect(channel.Peers.Desired).To(Equal(int32(1)))
			Expect(channel.Peers.Ready).To(Equal(int32(0)))
			Expect(channel.Orgs).To(HaveLen(1))
			Expect(channel.Orgs[0].Name).To(Equal("BankA"))
			Expect(channel.Orgs[0].Namespace).To(Equal("fo-test-banka"))
			Expect(channel.Orgs[0].MSPName).To(Equal("BankAMSP"))
			Expect(channel.Orgs[0].PeerNames).To(Equal([]string{"peer0"}))
			Expect(channel.Orgs[0].Peers.Desired).To(Equal(int32(1)))
			Expect(channel.Orgs[0].Peers.Ready).To(Equal(int32(0)))
			Expect(channel.Orgs[0].Ready).To(BeFalse())

			channels := apiMeta.FindStatusCondition(network.Status.Conditions, conditionChannelsReady)
			Expect(channels).NotTo(BeNil())
			Expect(channels.Status).To(Equal(metav1.ConditionFalse))
			Expect(channels.Reason).To(Equal("ChannelBootstrapPending"))
			Expect(channels.Message).To(Equal("settlement: Waiting for Fabric components before channel bootstrap"))
		})

		It("should report invalid Fabric topology before reconciling child resources", func() {
			var network fabricopsv1alpha1.FabricNetwork
			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			network.Spec.Orgs = []fabricopsv1alpha1.Org{
				{
					Organization: fabricopsv1alpha1.OrgMeta{
						Name:    "BankA",
						Domain:  "banka.example.com",
						MSPName: "BankAMSP",
					},
					CA: fabricopsv1alpha1.CAConfig{DB: "sqlite"},
					Peer: &fabricopsv1alpha1.PeerConfig{
						Instances: 1,
						DB:        "CouchDB",
						Prefix:    componentPeer,
						ExternalEndpoints: []fabricopsv1alpha1.ExternalEndpoint{
							{
								Name:     "peer9",
								Address:  "peer9.banka.example.com",
								TLSHosts: []string{""},
							},
							{
								Name:    "peer9",
								Address: "peer9.banka.example.com:7051",
							},
						},
					},
				},
				{
					Organization: fabricopsv1alpha1.OrgMeta{
						Name:    "BankC",
						Domain:  "bankc.example.com",
						MSPName: "BankCMSP",
					},
					CA: fabricopsv1alpha1.CAConfig{DB: "sqlite"},
					Peer: &fabricopsv1alpha1.PeerConfig{
						Instances: 1,
						DB:        "CouchDB",
						Prefix:    componentPeer,
					},
				},
			}
			network.Spec.Channels = []fabricopsv1alpha1.Channel{
				{
					Name: "settlement",
					Orgs: []fabricopsv1alpha1.ChannelOrg{
						{
							Name:  "BankA",
							Peers: []string{"peer9"},
						},
						{
							Name:  "MissingOrg",
							Peers: []string{"peer0"},
						},
					},
				},
			}
			network.Spec.Chaincodes = []fabricopsv1alpha1.Chaincode{
				{
					Name:    "settlement",
					Version: "0.0.1",
					Channel: "payments",
					Image:   "ghcr.io/dpereowei/fabricops-node-settlement:0.1.0",
				},
				{
					Name:         "audit",
					Version:      "0.0.1",
					Channel:      "settlement",
					Image:        "ghcr.io/dpereowei/fabricops-node-audit:0.1.0",
					PackageLabel: "shared-package",
					EndorsementPolicy: "AND(" +
						"'BankAMSP.member'," +
						"'BankCMSP.member'," +
						"'MissingMSP.member'," +
						"'BrokenPrincipal'" +
						")",
					PrivateData: []fabricopsv1alpha1.PrivateDataCollection{
						{
							Name:              "bad-collection",
							OrgNames:          []string{"MissingOrg"},
							RequiredPeerCount: int32Ptr(2),
							MaxPeerCount:      int32Ptr(1),
							EndorsementPolicy: &fabricopsv1alpha1.PrivateDataEndorsementPolicy{
								SignaturePolicy:     "OR('BankAMSP.member')",
								ChannelConfigPolicy: "Admins",
							},
						},
					},
					CouchDBIndexes: []fabricopsv1alpha1.CouchDBIndex{
						{
							Name:   "by-owner",
							Fields: []string{"docType", "owner"},
						},
						{
							Name:   "by-owner",
							Fields: []string{""},
						},
						{
							Name:       "by-private-owner",
							Fields:     []string{"owner"},
							Collection: "missing-collection",
						},
					},
				},
				{
					Name:         "risk",
					Version:      "0.0.1",
					Channel:      "settlement",
					Image:        "ghcr.io/dpereowei/fabricops-node-risk:0.1.0",
					PackageLabel: "shared-package",
				},
			}
			Expect(k8sClient.Update(ctx, &network)).To(Succeed())

			controllerReconciler := &FabricNetworkReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			Expect(network.Status.Phase).To(Equal(fabricopsv1alpha1.PhaseFailed))
			Expect(network.Status.Message).To(ContainSubstring("Invalid Fabric topology"))
			Expect(network.Status.Message).To(ContainSubstring("at least one orderer instance is required"))
			Expect(network.Status.Message).To(ContainSubstring(`org "BankA" peer.externalEndpoints[0] references unknown workload "peer9"`))
			Expect(network.Status.Message).To(ContainSubstring(`org "BankA" peer.externalEndpoints[0].address is invalid`))
			Expect(network.Status.Message).To(ContainSubstring(`org "BankA" peer.externalEndpoints[0].tlsHosts[0] is required`))
			Expect(network.Status.Message).To(ContainSubstring(`org "BankA" peer.externalEndpoints workload "peer9" is declared more than once`))
			Expect(network.Status.Message).To(ContainSubstring(`channel "settlement" org "BankA" references unknown peers: peer9`))
			Expect(network.Status.Message).To(ContainSubstring(`channel "settlement" references unknown org "MissingOrg"`))
			Expect(network.Status.Message).To(ContainSubstring(`chaincode "settlement" references unknown channel "payments"`))
			Expect(network.Status.Message).To(ContainSubstring(`chaincode package label "shared-package" is used by both "settlement/audit" and "settlement/risk"`))
			Expect(network.Status.Message).To(ContainSubstring(`chaincode "audit" endorsementPolicy references MSP "BankCMSP" outside channel "settlement"`))
			Expect(network.Status.Message).To(ContainSubstring(`chaincode "audit" endorsementPolicy references unknown MSP "MissingMSP"`))
			Expect(network.Status.Message).To(ContainSubstring(`chaincode "audit" endorsementPolicy principal "BrokenPrincipal" must use MSP.role format`))
			Expect(network.Status.Message).To(ContainSubstring(`chaincode "audit" private data collection "bad-collection" references unknown org "MissingOrg"`))
			Expect(network.Status.Message).To(ContainSubstring(`chaincode "audit" private data collection "bad-collection" maxPeerCount 1 exceeds available authorized peers 0`))
			Expect(network.Status.Message).To(ContainSubstring(`chaincode "audit" private data collection "bad-collection" requiredPeerCount 2 exceeds maxPeerCount 1`))
			Expect(network.Status.Message).To(ContainSubstring(`chaincode "audit" private data collection "bad-collection" must use only one endorsementPolicy field`))
			Expect(network.Status.Message).To(ContainSubstring(`chaincode "audit" CouchDB index "by-owner" has an empty field`))
			Expect(network.Status.Message).To(ContainSubstring(`chaincode "audit" CouchDB index "by-private-owner" references unknown private data collection "missing-collection"`))
			Expect(network.Status.Message).To(ContainSubstring(`chaincode "audit" CouchDB index package path "metadata/META-INF/statedb/couchdb/indexes/by-owner.json" is declared more than once`))
			Expect(network.Status.OrgStatus).To(BeEmpty())
			Expect(network.Status.ChannelStatus).To(BeEmpty())
			Expect(network.Status.ChaincodeStatus).To(BeEmpty())

			ready := apiMeta.FindStatusCondition(network.Status.Conditions, conditionReady)
			Expect(ready).NotTo(BeNil())
			Expect(ready.Status).To(Equal(metav1.ConditionFalse))
			Expect(ready.Reason).To(Equal("TopologyInvalid"))
			Expect(ready.Message).To(Equal(network.Status.Message))

			channels := apiMeta.FindStatusCondition(network.Status.Conditions, conditionChannelsReady)
			Expect(channels).NotTo(BeNil())
			Expect(channels.Status).To(Equal(metav1.ConditionUnknown))
			Expect(channels.Reason).To(Equal("TopologyInvalid"))
		})

		It("should build ServiceMonitor output for org operations services when enabled", func() {
			var network fabricopsv1alpha1.FabricNetwork
			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			network.Spec.Global.Observability = &fabricopsv1alpha1.ObservabilityConfig{
				ServiceMonitor: &fabricopsv1alpha1.ServiceMonitorConfig{
					Enabled:       true,
					Interval:      "15s",
					ScrapeTimeout: "5s",
					Labels: map[string]string{
						"release": "prometheus",
					},
				},
			}
			org := network.Spec.Orgs[1]

			serviceMonitor := buildOrgServiceMonitor(&network, org, "fo-test-banka")
			Expect(serviceMonitor.GetAPIVersion()).To(Equal("monitoring.coreos.com/v1"))
			Expect(serviceMonitor.GetKind()).To(Equal("ServiceMonitor"))
			Expect(serviceMonitor.GetName()).To(Equal("test-banka-operations"))
			Expect(serviceMonitor.GetNamespace()).To(Equal("fo-test-banka"))
			Expect(serviceMonitor.GetLabels()).To(HaveKeyWithValue("release", "prometheus"))
			Expect(serviceMonitor.GetLabels()).To(HaveKeyWithValue(labelAppComponent, componentMonitor))
			Expect(serviceMonitor.GetAnnotations()).To(HaveKeyWithValue(annotationOrg, "BankA"))

			selector, found, err := unstructured.NestedStringMap(serviceMonitor.Object, "spec", "selector", "matchLabels")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(selector).To(HaveKeyWithValue(labelFabricNetwork, resourceName))
			Expect(selector).To(HaveKeyWithValue(labelFabricNetworkNamespace, resourceNamespace))
			Expect(selector).To(HaveKeyWithValue(labelOrg, "banka"))
			Expect(selector).To(HaveKeyWithValue(labelEndpoint, endpointOperations))

			endpoints, found, err := unstructured.NestedSlice(serviceMonitor.Object, "spec", "endpoints")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(endpoints).To(HaveLen(1))
			endpoint, ok := endpoints[0].(map[string]any)
			Expect(ok).To(BeTrue())
			Expect(endpoint).To(HaveKeyWithValue("port", endpointOperations))
			Expect(endpoint).To(HaveKeyWithValue("path", "/metrics"))
			Expect(endpoint).To(HaveKeyWithValue("scheme", "http"))
			Expect(endpoint).To(HaveKeyWithValue("interval", "15s"))
			Expect(endpoint).To(HaveKeyWithValue("scrapeTimeout", "5s"))
			Expect(endpoint).To(HaveKeyWithValue("honorLabels", true))
		})

		It("should generate CCaaS package metadata for declared chaincodes", func() {
			var network fabricopsv1alpha1.FabricNetwork
			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			network.Spec.Channels = []fabricopsv1alpha1.Channel{
				{
					Name: "settlement",
					Orgs: []fabricopsv1alpha1.ChannelOrg{
						{
							Name:  "BankA",
							Peers: []string{"peer0"},
						},
					},
				},
			}
			network.Spec.Chaincodes = []fabricopsv1alpha1.Chaincode{
				{
					Name:     "settlement",
					Version:  "0.0.1",
					Channel:  "settlement",
					Image:    "ghcr.io/dpereowei/fabricops-node-settlement:0.1.0",
					Sequence: 1,
					CouchDBIndexes: []fabricopsv1alpha1.CouchDBIndex{
						{
							Name:           "indexOwner",
							Fields:         []string{"docType", "owner"},
							DesignDocument: "indexOwnerDoc",
						},
					},
					CCAAS: &fabricopsv1alpha1.ChaincodeAsAService{
						ServicePort: 7052,
						DialTimeout: "10s",
					},
				},
			}
			Expect(k8sClient.Update(ctx, &network)).To(Succeed())

			controllerReconciler := &FabricNetworkReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			var packageConfig corev1.ConfigMap
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: "fo-test-banka",
				Name:      "settlement-settlement-banka-package",
			}, &packageConfig)).To(Succeed())
			Expect(packageConfig.Labels[labelAppComponent]).To(Equal(componentChaincode))
			Expect(packageConfig.Labels[labelChannel]).To(Equal("settlement"))
			Expect(packageConfig.Labels[labelChaincode]).To(Equal("settlement"))
			Expect(packageConfig.Data[chaincodePackageLabelKey]).To(Equal("settlement_settlement_0.0.1"))
			Expect(packageConfig.Data[chaincodePackageFileKey]).To(Equal("settlement_settlement_0.0.1.tar.gz"))
			Expect(packageConfig.BinaryData).To(HaveKey("settlement_settlement_0.0.1.tar.gz"))
			Expect(packageConfig.BinaryData["settlement_settlement_0.0.1.tar.gz"]).NotTo(BeEmpty())
			packageArchive := readTarGz(packageConfig.BinaryData["settlement_settlement_0.0.1.tar.gz"])
			Expect(packageArchive).To(HaveKey(chaincodeMetadataKey))
			Expect(packageArchive).To(HaveKey("code.tar.gz"))
			codeArchive := readTarGz(packageArchive["code.tar.gz"])
			Expect(codeArchive).To(HaveKey(chaincodeConnectionKey))
			Expect(codeArchive).To(HaveKey("metadata/META-INF/statedb/couchdb/indexes/indexowner.json"))
			Expect(string(codeArchive["metadata/META-INF/statedb/couchdb/indexes/indexowner.json"])).To(ContainSubstring(`"fields": [`))
			Expect(string(codeArchive["metadata/META-INF/statedb/couchdb/indexes/indexowner.json"])).To(ContainSubstring(`"docType"`))
			Expect(string(codeArchive["metadata/META-INF/statedb/couchdb/indexes/indexowner.json"])).To(ContainSubstring(`"owner"`))
			Expect(string(codeArchive["metadata/META-INF/statedb/couchdb/indexes/indexowner.json"])).To(ContainSubstring(`"ddoc": "indexOwnerDoc"`))
			Expect(string(codeArchive["metadata/META-INF/statedb/couchdb/indexes/indexowner.json"])).To(ContainSubstring(`"name": "indexOwner"`))
			Expect(string(codeArchive["metadata/META-INF/statedb/couchdb/indexes/indexowner.json"])).To(ContainSubstring(`"type": "json"`))
			Expect(packageConfig.Data[chaincodeConnectionAddrKey]).To(Equal("settlement-settlement-banka-{{.peer_hostname}}-ccaas.fo-test-banka.svc.cluster.local:7052"))
			Expect(packageConfig.Data[chaincodeMetadataKey]).To(ContainSubstring(`"type": "ccaas"`))
			Expect(packageConfig.Data[chaincodeMetadataKey]).To(ContainSubstring(`"label": "settlement_settlement_0.0.1"`))
			Expect(packageConfig.Data[chaincodeConnectionKey]).To(ContainSubstring(`"address": "settlement-settlement-banka-{{.peer_hostname}}-ccaas.fo-test-banka.svc.cluster.local:7052"`))
			Expect(packageConfig.Data[chaincodeConnectionKey]).To(ContainSubstring(`"dial_timeout": "10s"`))
			Expect(packageConfig.Data[chaincodeConnectionKey]).To(ContainSubstring(`"tls_required": false`))

			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			Expect(network.Status.ChaincodeStatus).To(HaveLen(1))
			chaincode := network.Status.ChaincodeStatus[0]
			Expect(chaincode.Name).To(Equal("settlement"))
			Expect(chaincode.Channel).To(Equal("settlement"))
			Expect(chaincode.Version).To(Equal("0.0.1"))
			Expect(chaincode.PackageLabel).To(Equal("settlement_settlement_0.0.1"))
			Expect(chaincode.Sequence).To(Equal(int32(1)))
			Expect(chaincode.PackageMetadata.Desired).To(Equal(int32(1)))
			Expect(chaincode.PackageMetadata.Ready).To(Equal(int32(1)))
			Expect(chaincode.PackageMetadataReady).To(BeTrue())
			Expect(chaincode.Installed.Desired).To(Equal(int32(1)))
			Expect(chaincode.Installed.Ready).To(Equal(int32(0)))
			Expect(chaincode.InstalledReady).To(BeFalse())
			Expect(chaincode.Workloads.Desired).To(Equal(int32(1)))
			Expect(chaincode.Workloads.Ready).To(Equal(int32(0)))
			Expect(chaincode.WorkloadsReady).To(BeFalse())
			Expect(chaincode.Message).To(Equal("Package metadata generated; waiting for channel bootstrap before lifecycle install"))
			Expect(chaincode.Targets).To(HaveLen(1))
			Expect(chaincode.Targets[0].OrgName).To(Equal("BankA"))
			Expect(chaincode.Targets[0].Namespace).To(Equal("fo-test-banka"))
			Expect(chaincode.Targets[0].PeerName).To(Equal("peer0"))
			Expect(chaincode.Targets[0].WorkloadName).To(Equal("settlement-settlement-banka-peer0-ccaas"))
			Expect(chaincode.Targets[0].Workload.Desired).To(Equal(int32(1)))
			Expect(chaincode.Targets[0].Workload.Ready).To(Equal(int32(0)))
			Expect(chaincode.Targets[0].WorkloadReady).To(BeFalse())
			Expect(chaincode.Targets[0].ServiceName).To(Equal("settlement-settlement-banka-peer0-ccaas"))
			Expect(chaincode.Targets[0].PackageConfigMapName).To(Equal("settlement-settlement-banka-package"))
			Expect(chaincode.Targets[0].PackageIDConfigMapName).To(Equal("settlement-settlement-0-0-1-banka-peer0-package-id"))
			Expect(chaincode.Targets[0].InstallJobName).To(Equal("settlement-settlement-0-0-1-banka-peer0-install"))
			Expect(chaincode.Targets[0].PackageMetadataReady).To(BeTrue())
			Expect(chaincode.Targets[0].Installed).To(BeFalse())
			expectDeploymentNotFound(ctx, "fo-test-banka", "settlement-settlement-banka-peer0-ccaas")
			expectServiceNotFound(ctx, "fo-test-banka", "settlement-settlement-banka-peer0-ccaas")
		})

		It("should reuse one CCaaS package across multiple peers in the same org", func() {
			var network fabricopsv1alpha1.FabricNetwork
			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			network.Spec.Orgs[1].Peer.Instances = 2
			network.Spec.Channels = []fabricopsv1alpha1.Channel{
				{
					Name: "settlement",
					Orgs: []fabricopsv1alpha1.ChannelOrg{
						{
							Name:  "BankA",
							Peers: []string{"peer0", "peer1"},
						},
					},
				},
			}
			network.Spec.Chaincodes = []fabricopsv1alpha1.Chaincode{
				{
					Name:     "settlement",
					Version:  "0.0.1",
					Channel:  "settlement",
					Image:    "ghcr.io/dpereowei/fabricops-node-settlement:0.1.0",
					Sequence: 1,
					CCAAS: &fabricopsv1alpha1.ChaincodeAsAService{
						ServicePort: 7052,
					},
				},
				{
					Name:    "audit",
					Version: "0.0.1",
					Channel: "settlement",
					Image:   "ghcr.io/dpereowei/fabricops-node-audit:0.1.0",
				},
			}
			Expect(k8sClient.Update(ctx, &network)).To(Succeed())

			controllerReconciler := &FabricNetworkReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			var settlementPackage corev1.ConfigMap
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: "fo-test-banka",
				Name:      "settlement-settlement-banka-package",
			}, &settlementPackage)).To(Succeed())
			Expect(settlementPackage.Data[chaincodeConnectionAddrKey]).To(Equal("settlement-settlement-banka-{{.peer_hostname}}-ccaas.fo-test-banka.svc.cluster.local:7052"))
			Expect(settlementPackage.Data[chaincodeConnectionKey]).To(ContainSubstring(`"tls_required": false`))

			var auditPackage corev1.ConfigMap
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: "fo-test-banka",
				Name:      "settlement-audit-banka-package",
			}, &auditPackage)).To(Succeed())
			Expect(auditPackage.Data[chaincodePackageLabelKey]).To(Equal("settlement_audit_0.0.1"))
			Expect(auditPackage.Data[chaincodeConnectionAddrKey]).To(Equal("settlement-audit-banka-{{.peer_hostname}}-ccaas.fo-test-banka.svc.cluster.local:7052"))

			var peerSpecificPackage corev1.ConfigMap
			err = k8sClient.Get(ctx, types.NamespacedName{
				Namespace: "fo-test-banka",
				Name:      "settlement-settlement-banka-peer0-package",
			}, &peerSpecificPackage)
			Expect(errors.IsNotFound(err)).To(BeTrue())

			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			Expect(network.Status.ChaincodeStatus).To(HaveLen(2))
			settlementStatus := network.Status.ChaincodeStatus[0]
			Expect(settlementStatus.Name).To(Equal("settlement"))
			Expect(settlementStatus.PackageMetadata.Desired).To(Equal(int32(2)))
			Expect(settlementStatus.PackageMetadata.Ready).To(Equal(int32(2)))
			Expect(settlementStatus.Workloads.Desired).To(Equal(int32(2)))
			Expect(settlementStatus.Targets).To(HaveLen(2))
			Expect(settlementStatus.Targets[0].PeerName).To(Equal("peer0"))
			Expect(settlementStatus.Targets[0].Address).To(Equal("settlement-settlement-banka-peer0-ccaas.fo-test-banka.svc.cluster.local:7052"))
			Expect(settlementStatus.Targets[0].PackageConfigMapName).To(Equal("settlement-settlement-banka-package"))
			Expect(settlementStatus.Targets[0].PackageIDConfigMapName).To(Equal("settlement-settlement-0-0-1-banka-peer0-package-id"))
			Expect(settlementStatus.Targets[1].PeerName).To(Equal("peer1"))
			Expect(settlementStatus.Targets[1].Address).To(Equal("settlement-settlement-banka-peer1-ccaas.fo-test-banka.svc.cluster.local:7052"))
			Expect(settlementStatus.Targets[1].PackageConfigMapName).To(Equal("settlement-settlement-banka-package"))
			Expect(settlementStatus.Targets[1].PackageIDConfigMapName).To(Equal("settlement-settlement-0-0-1-banka-peer1-package-id"))

			bankOrg := network.Spec.Orgs[1]
			settlementChaincode := network.Spec.Chaincodes[0]
			peer0InstallJob := buildChaincodeInstallJob(&network, settlementChaincode, bankOrg, "peer0")
			peer1InstallJob := buildChaincodeInstallJob(&network, settlementChaincode, bankOrg, "peer1")
			Expect(configMapVolumeNames(peer0InstallJob.Spec.Template.Spec)).To(HaveKeyWithValue(chaincodePackageVolumeName, "settlement-settlement-banka-package"))
			Expect(configMapVolumeNames(peer1InstallJob.Spec.Template.Spec)).To(HaveKeyWithValue(chaincodePackageVolumeName, "settlement-settlement-banka-package"))
		})

		It("should prepare chaincode lifecycle for multi-org endorsement", func() {
			var network fabricopsv1alpha1.FabricNetwork
			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			network.Spec.Orgs[1].Peer.Instances = 2
			network.Spec.Orgs = append(network.Spec.Orgs, fabricopsv1alpha1.Org{
				Organization: fabricopsv1alpha1.OrgMeta{
					Name:    "BankB",
					Domain:  "bankb.example.com",
					MSPName: "BankBMSP",
				},
				CA: fabricopsv1alpha1.CAConfig{DB: "sqlite"},
				Peer: &fabricopsv1alpha1.PeerConfig{
					Instances: 1,
					DB:        "CouchDB",
					Prefix:    componentPeer,
				},
			})
			channel := fabricopsv1alpha1.Channel{
				Name: "settlement",
				Orgs: []fabricopsv1alpha1.ChannelOrg{
					{
						Name:  "BankA",
						Peers: []string{"peer0", "peer1"},
					},
					{
						Name:  "BankB",
						Peers: []string{"peer0"},
					},
				},
			}
			chaincode := fabricopsv1alpha1.Chaincode{
				Name:              "settlement",
				Version:           "0.0.1",
				Channel:           "settlement",
				Image:             "ghcr.io/dpereowei/fabricops-node-settlement:0.1.0",
				Sequence:          1,
				EndorsementPolicy: "AND('BankAMSP.member','BankBMSP.member')",
				CCAAS: &fabricopsv1alpha1.ChaincodeAsAService{
					ServicePort: 7052,
				},
			}
			bankAOrg := network.Spec.Orgs[1]
			bankBOrg := network.Spec.Orgs[2]
			definitionHash := chaincodeDefinitionNameHash(chaincode)
			Expect(definitionHash).NotTo(BeEmpty())
			upgradedChaincode := chaincode
			upgradedChaincode.Sequence = 2
			Expect(chaincodeDefinitionNameHash(upgradedChaincode)).NotTo(Equal(definitionHash))

			approvalPeers := chaincodeApprovalPeers(channel)
			Expect(approvalPeers).To(HaveKeyWithValue("BankA", "peer0"))
			Expect(approvalPeers).To(HaveKeyWithValue("BankB", "peer0"))

			peers := chaincodeLifecyclePeers(&network, channel)
			Expect(peers).To(HaveLen(3))
			Expect(peers[0].org.Organization.Name).To(Equal("BankA"))
			Expect(peers[0].peerName).To(Equal("peer0"))
			Expect(peers[0].namespace).To(Equal("fo-test-banka"))
			Expect(peers[1].org.Organization.Name).To(Equal("BankA"))
			Expect(peers[1].peerName).To(Equal("peer1"))
			Expect(peers[1].namespace).To(Equal("fo-test-banka"))
			Expect(peers[2].org.Organization.Name).To(Equal("BankB"))
			Expect(peers[2].peerName).To(Equal("peer0"))
			Expect(peers[2].namespace).To(Equal("fo-test-bankb"))

			bankAPackage, err := buildChaincodePackageConfigMap(&network, chaincode, bankAOrg)
			Expect(err).NotTo(HaveOccurred())
			Expect(bankAPackage.Namespace).To(Equal("fo-test-banka"))
			Expect(bankAPackage.Name).To(Equal("settlement-settlement-banka-package"))
			Expect(bankAPackage.Data[chaincodeConnectionAddrKey]).To(Equal("settlement-settlement-banka-{{.peer_hostname}}-ccaas.fo-test-banka.svc.cluster.local:7052"))

			bankBPackage, err := buildChaincodePackageConfigMap(&network, chaincode, bankBOrg)
			Expect(err).NotTo(HaveOccurred())
			Expect(bankBPackage.Namespace).To(Equal("fo-test-bankb"))
			Expect(bankBPackage.Name).To(Equal("settlement-settlement-bankb-package"))
			Expect(bankBPackage.Data[chaincodeConnectionAddrKey]).To(Equal("settlement-settlement-bankb-{{.peer_hostname}}-ccaas.fo-test-bankb.svc.cluster.local:7052"))

			orderer, ok := chaincodeLifecycleOrderer(&network)
			Expect(ok).To(BeTrue())
			Expect(orderer.namespace).To(Equal("fo-test-orderer"))
			packageID := "settlement_settlement_0.0.1:abc123"

			bankAApproveJob := buildChaincodeApproveJob(&network, channel, chaincode, bankAOrg, "peer0", packageID, orderer)
			Expect(bankAApproveJob.Namespace).To(Equal("fo-test-banka"))
			Expect(bankAApproveJob.Name).To(Equal("settlement-settlement-0-0-1-abc123-" + definitionHash + "-banka-approve"))
			Expect(bankAApproveJob.Annotations[annotationSucceededJobCleanup]).To(Equal("true"))
			Expect(bankAApproveJob.Spec.Template.Spec.ServiceAccountName).To(Equal("settlement-settlement-0-0-1-banka-peer0-installer"))
			bankAApproveCommand := bankAApproveJob.Spec.Template.Spec.InitContainers[0].Command[2]
			Expect(bankAApproveCommand).To(ContainSubstring("CORE_PEER_LOCALMSPID=\"BankAMSP\""))
			Expect(bankAApproveCommand).To(ContainSubstring("CORE_PEER_ADDRESS=\"peer0.fo-test-banka.svc.cluster.local:7051\""))
			Expect(bankAApproveCommand).To(ContainSubstring("ENDORSEMENT_POLICY=\"AND('BankAMSP.member','BankBMSP.member')\""))
			Expect(bankAApproveJob.Spec.Template.Spec.Containers[0].Name).To(Equal(publishChaincodeLifecycleResultContainerName(approveChaincodeContainer)))

			bankBApproveJob := buildChaincodeApproveJob(&network, channel, chaincode, bankBOrg, "peer0", packageID, orderer)
			Expect(bankBApproveJob.Namespace).To(Equal("fo-test-bankb"))
			Expect(bankBApproveJob.Name).To(Equal("settlement-settlement-0-0-1-abc123-" + definitionHash + "-bankb-approve"))
			Expect(bankBApproveJob.Spec.Template.Spec.ServiceAccountName).To(Equal("settlement-settlement-0-0-1-bankb-peer0-installer"))
			Expect(secretVolumeNames(bankBApproveJob.Spec.Template.Spec)).To(HaveKeyWithValue(chaincodeAdminMSPVolume, "bankb-admin-msp"))
			Expect(secretVolumeNames(bankBApproveJob.Spec.Template.Spec)).To(HaveKeyWithValue(chaincodeAdminTLSVolume, "bankb-admin-tls"))
			bankBApproveCommand := bankBApproveJob.Spec.Template.Spec.InitContainers[0].Command[2]
			Expect(bankBApproveCommand).To(ContainSubstring("CORE_PEER_LOCALMSPID=\"BankBMSP\""))
			Expect(bankBApproveCommand).To(ContainSubstring("CORE_PEER_ADDRESS=\"peer0.fo-test-bankb.svc.cluster.local:7051\""))
			Expect(bankBApproveCommand).To(ContainSubstring("ENDORSEMENT_POLICY=\"AND('BankAMSP.member','BankBMSP.member')\""))

			commitJob := buildChaincodeCommitJob(&network, channel, chaincode, packageID, peers[0], orderer, peers)
			Expect(commitJob.Namespace).To(Equal("fo-test-banka"))
			Expect(commitJob.Name).To(Equal("settlement-settlement-0-0-1-abc123-" + definitionHash + "-commit"))
			Expect(commitJob.Spec.Template.Spec.ServiceAccountName).To(Equal("settlement-settlement-0-0-1-committer"))
			Expect(secretVolumeNames(commitJob.Spec.Template.Spec)).To(HaveKeyWithValue(chaincodeAdminMSPVolume, "banka-admin-msp"))
			Expect(secretVolumeNames(commitJob.Spec.Template.Spec)).To(HaveKeyWithValue(chaincodePeerTLSVolumeName(bankAOrg, "peer0"), "settlement-settlement-0-0-1-banka-peer0-tls"))
			Expect(secretVolumeNames(commitJob.Spec.Template.Spec)).To(HaveKeyWithValue(chaincodePeerTLSVolumeName(bankAOrg, "peer1"), "settlement-settlement-0-0-1-banka-peer1-tls"))
			Expect(secretVolumeNames(commitJob.Spec.Template.Spec)).To(HaveKeyWithValue(chaincodePeerTLSVolumeName(bankBOrg, "peer0"), "settlement-settlement-0-0-1-bankb-peer0-tls"))

			commitCommand := commitJob.Spec.Template.Spec.InitContainers[0].Command[2]
			Expect(commitCommand).To(ContainSubstring("set -- \"$@\" --peerAddresses \"peer0.fo-test-banka.svc.cluster.local:7051\""))
			Expect(commitCommand).To(ContainSubstring("set -- \"$@\" --peerAddresses \"peer1.fo-test-banka.svc.cluster.local:7051\""))
			Expect(commitCommand).To(ContainSubstring("set -- \"$@\" --peerAddresses \"peer0.fo-test-bankb.svc.cluster.local:7051\""))
			Expect(commitCommand).To(ContainSubstring("--tlsRootCertFiles \"/fabricops/chaincode/crypto/peers/banka/peer0/tls/ca.crt\""))
			Expect(commitCommand).To(ContainSubstring("--tlsRootCertFiles \"/fabricops/chaincode/crypto/peers/banka/peer1/tls/ca.crt\""))
			Expect(commitCommand).To(ContainSubstring("--tlsRootCertFiles \"/fabricops/chaincode/crypto/peers/bankb/peer0/tls/ca.crt\""))
			Expect(commitCommand).To(ContainSubstring("ENDORSEMENT_POLICY=\"AND('BankAMSP.member','BankBMSP.member')\""))
			Expect(volumeMountPaths(commitJob.Spec.Template.Spec.InitContainers[0])).To(HaveKeyWithValue(chaincodePeerTLSVolumeName(bankBOrg, "peer0"), chaincodePeerTLSPath(bankBOrg, "peer0")))
			Expect(commitJob.Spec.Template.Spec.Containers[0].Name).To(Equal(publishChaincodeLifecycleResultContainerName(commitChaincodeContainer)))
		})

		It("should model post-bootstrap peer scale-up as explicit channel membership", func() {
			var network fabricopsv1alpha1.FabricNetwork
			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())

			bankAOrg := network.Spec.Orgs[1]
			channel := fabricopsv1alpha1.Channel{
				Name: "settlement",
				Orgs: []fabricopsv1alpha1.ChannelOrg{
					{
						Name:  "BankA",
						Peers: []string{"peer0"},
					},
				},
			}

			Expect(desiredPeerNames(bankAOrg)).To(HaveKey("peer0"))
			Expect(desiredPeerNames(bankAOrg)).NotTo(HaveKey("peer1"))
			Expect(chaincodeLifecyclePeers(&network, channel)).To(HaveLen(1))

			network.Spec.Orgs[1].Peer.Instances = 2
			bankAOrg = network.Spec.Orgs[1]

			Expect(desiredPeerNames(bankAOrg)).To(HaveKey("peer1"))
			Expect(unknownChannelPeers(bankAOrg, []string{"peer0", "peer1"})).To(BeEmpty())
			Expect(chaincodeLifecyclePeers(&network, channel)).To(HaveLen(1))
			Expect(channelPeerCountsByOrg(channel)).To(HaveKeyWithValue("BankA", 1))

			channel.Orgs[0].Peers = append(channel.Orgs[0].Peers, "peer1")

			peers := chaincodeLifecyclePeers(&network, channel)
			Expect(peers).To(HaveLen(2))
			Expect(peers[0].peerName).To(Equal("peer0"))
			Expect(peers[1].peerName).To(Equal("peer1"))
			Expect(peers[1].namespace).To(Equal("fo-test-banka"))
			Expect(channelPeerCountsByOrg(channel)).To(HaveKeyWithValue("BankA", 2))
			Expect(chaincodeApprovalPeers(channel)).To(HaveKeyWithValue("BankA", "peer0"))

			chaincode := fabricopsv1alpha1.Chaincode{
				Name:    "settlement",
				Version: "0.0.1",
				Channel: "settlement",
				Image:   "ghcr.io/dpereowei/fabricops-node-settlement:0.1.0",
			}
			peer1InstallJob := buildChaincodeInstallJob(&network, chaincode, bankAOrg, "peer1")
			Expect(peer1InstallJob.Name).To(Equal("settlement-settlement-0-0-1-banka-peer1-install"))
			Expect(peer1InstallJob.Spec.Template.Spec.ServiceAccountName).To(Equal("settlement-settlement-0-0-1-banka-peer1-installer"))
			Expect(peer1InstallJob.Spec.Template.Spec.InitContainers[0].Command[2]).To(ContainSubstring("CORE_PEER_ADDRESS=\"peer1.fo-test-banka.svc.cluster.local:7051\""))
		})

		It("should stop scaled-down peer workloads without deleting peer state", func() {
			var network fabricopsv1alpha1.FabricNetwork
			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())

			controllerReconciler := &FabricNetworkReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			bankNamespace := "fo-test-banka"
			bankOrg := network.Spec.Orgs[1]
			bankOrg.Peer.Instances = 2
			Expect(controllerReconciler.ensureNamespace(ctx, buildOrgNamespace(&network, bankOrg))).To(Succeed())

			peer1Deploy := buildPeerDeployment(&network, bankOrg, 1, bankNamespace)
			peer1PVC, err := buildDataPVC(&network, bankOrg, bankNamespace, peer1Deploy.Name, componentPeer)
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Create(ctx, peer1PVC)).To(Succeed())
			Expect(k8sClient.Create(ctx, peer1Deploy)).To(Succeed())
			Expect(k8sClient.Create(ctx, buildPeerService(&network, bankOrg, 1, bankNamespace))).To(Succeed())
			Expect(k8sClient.Create(ctx, buildPeerOperationsService(&network, bankOrg, 1, bankNamespace))).To(Succeed())

			bankOrg.Peer.Instances = 1
			status, err := controllerReconciler.reconcilePeers(ctx, &network, bankOrg, bankNamespace)
			Expect(err).NotTo(HaveOccurred())
			Expect(status.Desired).To(Equal(int32(1)))

			var peer0Deploy appsv1.Deployment
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "peer0",
			}, &peer0Deploy)).To(Succeed())
			var peer0Service corev1.Service
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "peer0",
			}, &peer0Service)).To(Succeed())

			expectDeploymentNotFound(ctx, bankNamespace, "peer1")
			expectServiceNotFound(ctx, bankNamespace, "peer1")
			expectServiceNotFound(ctx, bankNamespace, "peer1-operations")
			expectPersistentVolumeClaim(ctx, bankNamespace, "peer1-data", "12Gi", "fabricops-peer")
		})

		It("should remove stale CCaaS workloads when a peer leaves chaincode targets", func() {
			var network fabricopsv1alpha1.FabricNetwork
			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())

			controllerReconciler := &FabricNetworkReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			bankNamespace := "fo-test-banka"
			bankOrg := network.Spec.Orgs[1]
			Expect(controllerReconciler.ensureNamespace(ctx, buildOrgNamespace(&network, bankOrg))).To(Succeed())

			channel := fabricopsv1alpha1.Channel{
				Name: "settlement",
				Orgs: []fabricopsv1alpha1.ChannelOrg{
					{
						Name:  "BankA",
						Peers: []string{"peer0"},
					},
				},
			}
			chaincode := fabricopsv1alpha1.Chaincode{
				Name:    "settlement",
				Version: "0.0.1",
				Channel: "settlement",
				Image:   "ghcr.io/dpereowei/fabricops-node-settlement:0.1.0",
				CCAAS: &fabricopsv1alpha1.ChaincodeAsAService{
					ServicePort: 7052,
				},
			}
			network.Spec.Channels = []fabricopsv1alpha1.Channel{channel}
			network.Spec.Chaincodes = []fabricopsv1alpha1.Chaincode{chaincode}

			Expect(k8sClient.Create(ctx, buildChaincodeDeployment(&network, chaincode, bankOrg, "peer1", "settlement_settlement_0.0.1:abc123"))).To(Succeed())
			Expect(k8sClient.Create(ctx, buildChaincodeService(&network, chaincode, bankOrg, "peer1"))).To(Succeed())

			statuses, err := controllerReconciler.reconcileChaincodes(ctx, &network, []fabricopsv1alpha1.ChannelStatus{
				{
					Name:  "settlement",
					Ready: true,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(statuses).To(HaveLen(1))
			Expect(statuses[0].Targets).To(HaveLen(1))
			Expect(statuses[0].Targets[0].PeerName).To(Equal("peer0"))

			expectDeploymentNotFound(ctx, bankNamespace, "settlement-settlement-banka-peer1-ccaas")
			expectServiceNotFound(ctx, bankNamespace, "settlement-settlement-banka-peer1-ccaas")
		})

		It("should render private data collections and mount them into lifecycle jobs", func() {
			var network fabricopsv1alpha1.FabricNetwork
			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			network.Spec.Orgs[1].Peer.Instances = 2
			channel := fabricopsv1alpha1.Channel{
				Name: "settlement",
				Orgs: []fabricopsv1alpha1.ChannelOrg{
					{
						Name:  "BankA",
						Peers: []string{"peer0", "peer1"},
					},
				},
			}
			chaincode := fabricopsv1alpha1.Chaincode{
				Name:     "settlement",
				Version:  "0.0.1",
				Channel:  "settlement",
				Image:    "ghcr.io/dpereowei/fabricops-node-settlement:0.1.0",
				Sequence: 1,
				PrivateData: []fabricopsv1alpha1.PrivateDataCollection{
					{
						Name:     "bank-a-collection",
						OrgNames: []string{"BankA"},
					},
					{
						Name:              "bank-a-short-lived",
						OrgNames:          []string{"BankA"},
						RequiredPeerCount: int32Ptr(1),
						MaxPeerCount:      int32Ptr(1),
						BlockToLive:       int64Ptr(5),
						MemberOnlyWrite:   boolPtr(false),
						EndorsementPolicy: &fabricopsv1alpha1.PrivateDataEndorsementPolicy{
							SignaturePolicy: "OR('BankAMSP.member')",
						},
					},
				},
				CouchDBIndexes: []fabricopsv1alpha1.CouchDBIndex{
					{
						Name:       "privateOwner",
						Fields:     []string{"owner", "status"},
						Collection: "bank-a-collection",
					},
				},
				CCAAS: &fabricopsv1alpha1.ChaincodeAsAService{
					ServicePort: 7052,
				},
			}
			network.Spec.Channels = []fabricopsv1alpha1.Channel{channel}
			network.Spec.Chaincodes = []fabricopsv1alpha1.Chaincode{chaincode}
			Expect(k8sClient.Update(ctx, &network)).To(Succeed())

			controllerReconciler := &FabricNetworkReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			collectionConfigJSON, collectionConfigHash, err := renderChaincodeCollectionConfig(&network, channel, chaincode)
			Expect(err).NotTo(HaveOccurred())
			Expect(collectionConfigJSON).To(ContainSubstring(`"name": "bank-a-collection"`))
			Expect(collectionConfigJSON).To(ContainSubstring(`"policy": "OR('BankAMSP.member')"`))
			Expect(collectionConfigJSON).To(ContainSubstring(`"requiredPeerCount": 0`))
			Expect(collectionConfigJSON).To(ContainSubstring(`"maxPeerCount": 1`))
			Expect(collectionConfigJSON).To(ContainSubstring(`"blockToLive": 5`))
			Expect(collectionConfigJSON).To(ContainSubstring(`"memberOnlyWrite": false`))
			Expect(collectionConfigJSON).To(ContainSubstring(`"signaturePolicy": "OR('BankAMSP.member')"`))

			var collectionConfigMap corev1.ConfigMap
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: "fo-test-banka",
				Name:      "settlement-settlement-collections",
			}, &collectionConfigMap)).To(Succeed())
			Expect(collectionConfigMap.Labels[labelAppComponent]).To(Equal(componentChaincode))
			Expect(collectionConfigMap.Labels[labelChannel]).To(Equal("settlement"))
			Expect(collectionConfigMap.Labels[labelChaincode]).To(Equal("settlement"))
			Expect(collectionConfigMap.Annotations[annotationCollectionConfigHash]).To(Equal(collectionConfigHash))
			Expect(collectionConfigMap.Data[chaincodeCollectionsKey]).To(Equal(collectionConfigJSON))

			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			Expect(network.Status.ChaincodeStatus).To(HaveLen(1))
			Expect(network.Status.ChaincodeStatus[0].CollectionConfigMap).To(Equal("settlement-settlement-collections"))
			Expect(network.Status.ChaincodeStatus[0].CollectionConfigHash).To(Equal(collectionConfigHash))

			var packageConfig corev1.ConfigMap
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: "fo-test-banka",
				Name:      "settlement-settlement-banka-package",
			}, &packageConfig)).To(Succeed())
			packageArchive := readTarGz(packageConfig.BinaryData["settlement_settlement_0.0.1.tar.gz"])
			codeArchive := readTarGz(packageArchive["code.tar.gz"])
			indexPath := "metadata/META-INF/statedb/couchdb/collections/bank-a-collection/indexes/privateowner.json"
			Expect(codeArchive).To(HaveKey(indexPath))
			Expect(string(codeArchive[indexPath])).To(ContainSubstring(`"owner"`))
			Expect(string(codeArchive[indexPath])).To(ContainSubstring(`"status"`))
			Expect(string(codeArchive[indexPath])).To(ContainSubstring(`"name": "privateOwner"`))

			bankOrg := network.Spec.Orgs[1]
			orderer, ok := chaincodeLifecycleOrderer(&network)
			Expect(ok).To(BeTrue())
			packageID := "settlement_settlement_0.0.1:abc123"
			definitionHash := chaincodeDefinitionNameHash(chaincode)
			Expect(definitionHash).NotTo(BeEmpty())

			approveJob := buildChaincodeApproveJob(&network, channel, chaincode, bankOrg, "peer0", packageID, orderer)
			Expect(approveJob.Name).To(Equal("settlement-settlement-0-0-1-abc123-" + definitionHash + "-banka-approve"))
			Expect(configMapVolumeNames(approveJob.Spec.Template.Spec)).To(HaveKeyWithValue(chaincodeCollectionsVolume, "settlement-settlement-collections"))
			approveContainer := approveJob.Spec.Template.Spec.InitContainers[0]
			Expect(volumeMountPaths(approveContainer)).To(HaveKeyWithValue(chaincodeCollectionsVolume, chaincodeCollectionsDir))
			Expect(approveContainer.Command[2]).To(ContainSubstring("COLLECTIONS_CONFIG=\"/fabricops/chaincode/collections/collections.json\""))
			Expect(approveContainer.Command[2]).To(ContainSubstring("--collections-config \"$COLLECTIONS_CONFIG\""))

			peers := chaincodeLifecyclePeers(&network, channel)
			commitJob := buildChaincodeCommitJob(&network, channel, chaincode, packageID, peers[0], orderer, peers)
			Expect(commitJob.Name).To(Equal("settlement-settlement-0-0-1-abc123-" + definitionHash + "-commit"))
			Expect(configMapVolumeNames(commitJob.Spec.Template.Spec)).To(HaveKeyWithValue(chaincodeCollectionsVolume, "settlement-settlement-collections"))
			commitContainer := commitJob.Spec.Template.Spec.InitContainers[0]
			Expect(volumeMountPaths(commitContainer)).To(HaveKeyWithValue(chaincodeCollectionsVolume, chaincodeCollectionsDir))
			Expect(commitContainer.Command[2]).To(ContainSubstring("COLLECTIONS_CONFIG=\"/fabricops/chaincode/collections/collections.json\""))
			Expect(commitContainer.Command[2]).To(ContainSubstring("--collections-config \"$COLLECTIONS_CONFIG\""))
		})

		It("should generate Fabric 2 application channel config without legacy consortium groups", func() {
			var network fabricopsv1alpha1.FabricNetwork
			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			network.Spec.Global.FabricVersion = "2.4.7"

			configtx, err := buildConfigtxYAML(&network, fabricopsv1alpha1.Channel{
				Name: "settlement",
				Orgs: []fabricopsv1alpha1.ChannelOrg{
					{
						Name:  "BankA",
						Peers: []string{"peer0"},
					},
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(configtx).To(ContainSubstring("Settlement:"))
			Expect(configtx).To(ContainSubstring("Orderer:"))
			Expect(configtx).To(ContainSubstring("Application:"))
			Expect(configtx).To(ContainSubstring("Host: orderer0.fo-test-orderer.svc.cluster.local"))
			Expect(configtx).To(ContainSubstring("Host: peer0.fo-test-banka.svc.cluster.local"))
			Expect(configtx).NotTo(ContainSubstring("Consortium:"))
			Expect(configtx).NotTo(ContainSubstring("Consortiums:"))
		})

		It("should parse inactive orderer join evidence as not ready", func() {
			found, status := ordererJoinResultChannelStatus(
				`Status: 200
{"channels":[{"name":"settlement","status":"inactive"}]}`,
				"settlement",
			)
			Expect(found).To(BeTrue())
			Expect(status).To(Equal("inactive"))

			found, status = ordererJoinResultChannelStatus(
				`{"channels":[{"name":"settlement","status":"active"}]}`,
				"settlement",
			)
			Expect(found).To(BeTrue())
			Expect(status).To(Equal("active"))
		})

		It("should allow chaincode policies to reference declared external MSPs", func() {
			network := fabricopsv1alpha1.FabricNetwork{
				Spec: fabricopsv1alpha1.FabricNetworkSpec{
					Global: fabricopsv1alpha1.GlobalConfig{
						FabricVersion: "3.1.0",
						TLS:           true,
					},
					Orgs: []fabricopsv1alpha1.Org{
						{
							Organization: fabricopsv1alpha1.OrgMeta{
								Name:    "Orderer",
								Domain:  "orderer.example.com",
								MSPName: "OrdererMSP",
							},
							CA: fabricopsv1alpha1.CAConfig{DB: "sqlite"},
							Orderers: []fabricopsv1alpha1.OrdererGroup{
								{
									GroupName: "group1",
									Type:      "raft",
									Instances: 1,
									Prefix:    componentOrderer,
								},
							},
						},
						{
							Organization: fabricopsv1alpha1.OrgMeta{
								Name:    "BankA",
								Domain:  "banka.example.com",
								MSPName: "BankAMSP",
							},
							CA: fabricopsv1alpha1.CAConfig{DB: "sqlite"},
							Peer: &fabricopsv1alpha1.PeerConfig{
								Instances: 1,
								DB:        "CouchDB",
								Prefix:    componentPeer,
							},
						},
					},
					Channels: []fabricopsv1alpha1.Channel{
						{
							Name: "settlement",
							Orgs: []fabricopsv1alpha1.ChannelOrg{
								{
									Name:  "BankA",
									Peers: []string{"peer0"},
								},
							},
							ExternalOrgs: []fabricopsv1alpha1.ChannelExternalOrg{
								{
									Name:  "BankB",
									MSPID: "BankBMSP",
									ApplicationOrgRef: fabricopsv1alpha1.ChannelArtifactKeyRef{
										ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: "bankb-application-org",
											},
											Key: "org.json",
										},
									},
								},
							},
						},
					},
					Chaincodes: []fabricopsv1alpha1.Chaincode{
						{
							Name:              "settlement",
							Version:           "1.0",
							Channel:           "settlement",
							Image:             "ghcr.io/dpereowei/fabricops-node-settlement:0.1.2",
							EndorsementPolicy: "AND('BankAMSP.member','BankBMSP.member')",
						},
					},
				},
			}

			Expect(validateFabricNetworkTopology(&network)).To(BeEmpty())
		})

		It("should reject external org required signers outside local channel MSPs", func() {
			network := fabricopsv1alpha1.FabricNetwork{
				Spec: fabricopsv1alpha1.FabricNetworkSpec{
					Global: fabricopsv1alpha1.GlobalConfig{
						FabricVersion: "3.1.0",
						TLS:           true,
					},
					Orgs: []fabricopsv1alpha1.Org{
						{
							Organization: fabricopsv1alpha1.OrgMeta{
								Name:    "Orderer",
								Domain:  "orderer.example.com",
								MSPName: "OrdererMSP",
							},
							CA: fabricopsv1alpha1.CAConfig{DB: "sqlite"},
							Orderers: []fabricopsv1alpha1.OrdererGroup{
								{
									GroupName: "group1",
									Type:      "raft",
									Instances: 1,
									Prefix:    componentOrderer,
								},
							},
						},
						{
							Organization: fabricopsv1alpha1.OrgMeta{
								Name:    "BankA",
								Domain:  "banka.example.com",
								MSPName: "BankAMSP",
							},
							CA: fabricopsv1alpha1.CAConfig{DB: "sqlite"},
							Peer: &fabricopsv1alpha1.PeerConfig{
								Instances: 1,
								DB:        "CouchDB",
								Prefix:    componentPeer,
							},
						},
					},
					Channels: []fabricopsv1alpha1.Channel{
						{
							Name: "settlement",
							Orgs: []fabricopsv1alpha1.ChannelOrg{
								{
									Name:  "BankA",
									Peers: []string{"peer0"},
								},
							},
							ExternalOrgs: []fabricopsv1alpha1.ChannelExternalOrg{
								{
									Name:  "BankB",
									MSPID: "BankBMSP",
									ApplicationOrgRef: fabricopsv1alpha1.ChannelArtifactKeyRef{
										ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: "bankb-application-org",
											},
											Key: "org.json",
										},
									},
									RequiredSignerMSPIDs: []string{"BankAMSP", "BankCMSP", "BankAMSP", ""},
								},
							},
						},
					},
				},
			}

			problems := validateFabricNetworkTopology(&network)
			Expect(problems).To(ContainElement(`channel "settlement" externalOrgs[0] required signer MSP "BankCMSP" is not a local channel org MSP`))
			Expect(problems).To(ContainElement(`channel "settlement" externalOrgs[0] required signer MSP "BankAMSP" is declared more than once`))
			Expect(problems).To(ContainElement(`channel "settlement" externalOrgs[0].requiredSignerMSPIDs[3] is required`))
		})

		It("should admit declared external orgs through founder-side channel updates", func() {
			var network fabricopsv1alpha1.FabricNetwork
			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			network.Spec.Channels = []fabricopsv1alpha1.Channel{
				{
					Name: "federated",
					Orgs: []fabricopsv1alpha1.ChannelOrg{
						{
							Name:  "BankA",
							Peers: []string{"peer0"},
						},
					},
					ExternalOrgs: []fabricopsv1alpha1.ChannelExternalOrg{
						{
							Name:  "BankB",
							MSPID: "BankBMSP",
							ApplicationOrgRef: fabricopsv1alpha1.ChannelArtifactKeyRef{
								ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: "bankb-application-org",
									},
									Key: "org.json",
								},
							},
							AnchorPeers: []fabricopsv1alpha1.ChannelExternalAnchorPeer{
								{
									Host: "peer0.bankb.fabricops.io",
									Port: 7051,
								},
							},
						},
					},
				},
			}
			Expect(k8sClient.Update(ctx, &network)).To(Succeed())

			applicationOrg := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "bankb-application-org",
					Namespace: resourceNamespace,
				},
				Data: map[string]string{
					"org.json": `{
  "mod_policy": "Admins",
  "policies": {},
  "values": {
    "MSP": {
      "value": {
        "config": {
          "name": "BankBMSP"
        }
      }
    },
    "AnchorPeers": {
      "value": {
        "anchor_peers": [
          {
            "host": "peer0.bankb.fabricops.io",
            "port": 7051
          }
        ]
      }
    }
  },
  "version": "0"
}`,
				},
			}
			Expect(k8sClient.Create(ctx, applicationOrg)).To(Succeed())
			DeferCleanup(func() {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, applicationOrg))).To(Succeed())
			})

			controllerReconciler := &FabricNetworkReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			request := reconcile.Request{NamespacedName: typeNamespacedName}
			ordererNamespace := "fo-test-orderer"
			bankNamespace := "fo-test-banka"
			network = prepareLocalChannelReady(ctx, controllerReconciler, request, typeNamespacedName, "federated", ordererNamespace, bankNamespace)

			Expect(network.Status.Phase).To(Equal(fabricopsv1alpha1.PhaseCreating))
			Expect(network.Status.Message).To(Equal("Waiting for Fabric channels to become ready"))
			Expect(network.Status.ChannelStatus).To(HaveLen(1))
			Expect(network.Status.ChannelStatus[0].Ready).To(BeFalse())
			Expect(network.Status.ChannelStatus[0].Message).To(Equal("BankB: Waiting for external org channel update Job"))
			Expect(network.Status.ChannelStatus[0].ExternalOrgs).To(HaveLen(1))
			externalStatus := network.Status.ChannelStatus[0].ExternalOrgs[0]
			Expect(externalStatus.Name).To(Equal("BankB"))
			Expect(externalStatus.MSPID).To(Equal("BankBMSP"))
			Expect(externalStatus.AdminOrg).To(Equal("BankA"))
			Expect(externalStatus.Orderer).To(Equal("Orderer/orderer0"))
			Expect(externalStatus.ApplicationOrgConfigMapName).To(Equal("federated-bankb-application-org"))
			Expect(externalStatus.UpdateJobName).To(Equal("federated-bankb-external-org-update"))
			Expect(externalStatus.Ready).To(BeFalse())

			var normalizedOrg corev1.ConfigMap
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "federated-bankb-application-org",
			}, &normalizedOrg)).To(Succeed())
			Expect(normalizedOrg.Labels[labelAppComponent]).To(Equal(componentChannel))
			Expect(normalizedOrg.Labels[labelChannel]).To(Equal("federated"))
			Expect(normalizedOrg.Data[externalOrgApplicationKey]).To(ContainSubstring(`"name": "BankBMSP"`))

			var ordererTLS corev1.Secret
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "federated-orderer0-tls",
			}, &ordererTLS)).To(Succeed())
			Expect(ordererTLS.Labels[labelAppComponent]).To(Equal(componentChannel))

			var updateJob batchv1.Job
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "federated-bankb-external-org-update",
			}, &updateJob)).To(Succeed())
			Expect(updateJob.Labels[labelAppComponent]).To(Equal(componentChannel))
			Expect(updateJob.Labels[labelChannel]).To(Equal("federated"))
			Expect(updateJob.Labels[labelWorkload]).To(Equal("bankb"))
			Expect(updateJob.Annotations[annotationSucceededJobCleanup]).To(Equal("true"))
			Expect(updateJob.Spec.Template.Spec.ServiceAccountName).To(Equal("federated-channel-bootstrapper"))
			Expect(configMapVolumeNames(updateJob.Spec.Template.Spec)).To(HaveKeyWithValue(externalOrgConfigVolumeName, "federated-bankb-application-org"))
			Expect(secretVolumeNames(updateJob.Spec.Template.Spec)).To(HaveKeyWithValue("msp-banka", "banka-admin-msp"))
			Expect(secretVolumeNames(updateJob.Spec.Template.Spec)).To(HaveKeyWithValue("admin-tls-banka", "banka-admin-tls"))
			Expect(secretVolumeNames(updateJob.Spec.Template.Spec)).To(HaveKeyWithValue("tls-orderer0", "federated-orderer0-tls"))
			Expect(updateJob.Spec.Template.Spec.InitContainers).To(HaveLen(1))
			Expect(updateJob.Spec.Template.Spec.Containers).To(HaveLen(1))

			updateContainer := updateJob.Spec.Template.Spec.InitContainers[0]
			Expect(updateContainer.Name).To(Equal(addExternalOrgContainer))
			Expect(updateContainer.Image).To(Equal("hyperledger/fabric-tools:2.5.14"))
			Expect(updateContainer.Command[2]).To(ContainSubstring("ADMIN_MSP_ID=\"BankAMSP\""))
			Expect(updateContainer.Command[2]).To(ContainSubstring("JOIN_MSP_ID=\"BankBMSP\""))
			Expect(updateContainer.Command[2]).To(ContainSubstring("peer0.fo-test-banka.svc.cluster.local:7051"))
			Expect(updateContainer.Command[2]).To(ContainSubstring("orderer0.fo-test-orderer.svc.cluster.local:7050"))
			Expect(updateContainer.Command[2]).To(ContainSubstring("jq --slurpfile org \"$APPLICATION_ORG_JSON\" --arg msp \"$JOIN_MSP_ID\""))
			Expect(updateContainer.Command[2]).To(ContainSubstring("peer channel update"))
			Expect(volumeMountPaths(updateContainer)).To(HaveKeyWithValue(externalOrgConfigVolumeName, channelExternalOrgConfigDir))
			Expect(volumeMountPaths(updateContainer)).To(HaveKeyWithValue(channelOutputVolumeName, channelOutputDir))

			publisher := updateJob.Spec.Template.Spec.Containers[0]
			Expect(publisher.Name).To(Equal(publishExternalOrgContainer))
			Expect(publisher.Image).To(Equal(kubectlImage()))
			Expect(envMap(publisher)[envExternalOrgResultConfigMap]).To(Equal("federated-bankb-external-org-update-result"))
			Expect(envMap(publisher)[envExternalOrgResultKey]).To(Equal(externalOrgUpdateResultKey))

			markJobComplete(ctx, bankNamespace, "federated-bankb-external-org-update")
			createExternalOrgUpdateResultConfigMap(
				ctx,
				bankNamespace,
				"federated-bankb-external-org-update-result",
				"federated",
				"BankBMSP",
			)

			_, err := controllerReconciler.Reconcile(ctx, request)
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			Expect(network.Status.Phase).To(Equal(fabricopsv1alpha1.PhaseReady))
			Expect(network.Status.Message).To(Equal("All Fabric components and channels are ready"))
			Expect(network.Status.ChannelStatus[0].Ready).To(BeTrue())
			Expect(network.Status.ChannelStatus[0].Message).To(BeEmpty())
			Expect(network.Status.ChannelStatus[0].ExternalOrgs[0].Ready).To(BeTrue())

			channels := apiMeta.FindStatusCondition(network.Status.Conditions, conditionChannelsReady)
			Expect(channels).NotTo(BeNil())
			Expect(channels.Status).To(Equal(metav1.ConditionTrue))
			Expect(channels.Reason).To(Equal("ChannelsReady"))
		})

		It("should wait for manual signatures before multi-admin external org admission", func() {
			var network fabricopsv1alpha1.FabricNetwork
			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			bankC := fabricopsv1alpha1.Org{
				Organization: fabricopsv1alpha1.OrgMeta{
					Name:    "BankC",
					Domain:  "bankc.example.com",
					MSPName: "BankCMSP",
				},
				CA: fabricopsv1alpha1.CAConfig{DB: "sqlite"},
				Peer: &fabricopsv1alpha1.PeerConfig{
					Instances: 1,
					DB:        "CouchDB",
					Prefix:    componentPeer,
				},
			}
			network.Spec.Orgs = append(network.Spec.Orgs, bankC)
			network.Spec.Channels = []fabricopsv1alpha1.Channel{
				{
					Name: "federated-manual",
					Orgs: []fabricopsv1alpha1.ChannelOrg{
						{Name: "BankA", Peers: []string{"peer0"}},
						{Name: "BankC", Peers: []string{"peer0"}},
					},
					ExternalOrgs: []fabricopsv1alpha1.ChannelExternalOrg{
						{
							Name:  "BankB",
							MSPID: "BankBMSP",
							ApplicationOrgRef: fabricopsv1alpha1.ChannelArtifactKeyRef{
								ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: "bankb-application-org",
									},
									Key: "org.json",
								},
							},
							RequiredSignerMSPIDs: []string{"BankAMSP", "BankCMSP"},
						},
					},
				},
			}
			Expect(validateFabricNetworkTopology(&network)).To(BeEmpty())

			applicationOrg := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "bankb-application-org",
					Namespace: resourceNamespace,
				},
				Data: map[string]string{
					"org.json": `{
  "mod_policy": "Admins",
  "policies": {},
  "values": {
    "MSP": {
      "value": {
        "config": {
          "name": "BankBMSP"
        }
      }
    }
  },
  "version": "0"
}`,
				},
			}
			Expect(k8sClient.Create(ctx, applicationOrg)).To(Succeed())
			DeferCleanup(func() {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, applicationOrg))).To(Succeed())
			})

			controllerReconciler := &FabricNetworkReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			ordererOrg := network.Spec.Orgs[0]
			adminOrg := network.Spec.Orgs[1]
			ordererNamespace := orgNamespaceName(&network, ordererOrg)
			adminNamespace := orgNamespaceName(&network, adminOrg)
			Expect(controllerReconciler.ensureNamespace(ctx, buildOrgNamespace(&network, ordererOrg))).To(Succeed())
			Expect(controllerReconciler.ensureNamespace(ctx, buildOrgNamespace(&network, adminOrg))).To(Succeed())

			ordererTLS := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      identitySecretName("orderer0", secretKindTLS),
					Namespace: ordererNamespace,
				},
				Type: corev1.SecretTypeOpaque,
				Data: map[string][]byte{
					tlsCACertKey:     []byte("ORDERER_TLS_CA"),
					tlsServerCertKey: []byte("ORDERER_TLS_CERT"),
					tlsServerKeyKey:  []byte("ORDERER_TLS_KEY"),
				},
			}
			Expect(k8sClient.Create(ctx, ordererTLS)).To(Succeed())
			DeferCleanup(func() {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, ordererTLS))).To(Succeed())
			})

			channel := network.Spec.Channels[0]
			externalOrg := channel.ExternalOrgs[0]
			status, err := controllerReconciler.reconcileExternalOrgUpdate(ctx, &network, channel, externalOrg)
			Expect(err).NotTo(HaveOccurred())
			Expect(status.Ready).To(BeFalse())
			Expect(status.AdminOrg).To(Equal("BankA"))
			Expect(status.RequiredSignerMSPIDs).To(Equal([]string{"BankAMSP", "BankCMSP"}))
			Expect(status.Message).To(Equal("BankB: Waiting for manual channel config update signatures from BankAMSP, BankCMSP"))

			var normalizedOrg corev1.ConfigMap
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: adminNamespace,
				Name:      "federated-manual-bankb-application-org",
			}, &normalizedOrg)).To(Succeed())
			Expect(normalizedOrg.Data[externalOrgApplicationKey]).To(ContainSubstring(`"name": "BankBMSP"`))

			var updateJob batchv1.Job
			err = k8sClient.Get(ctx, types.NamespacedName{
				Namespace: adminNamespace,
				Name:      "federated-manual-bankb-external-org-update",
			}, &updateJob)
			Expect(errors.IsNotFound(err)).To(BeTrue())
		})

		It("should generate channel config and a channel block Job after components are ready", func() {
			var network fabricopsv1alpha1.FabricNetwork
			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			network.Spec.Orgs[0].Orderers[0].ExternalEndpoints = []fabricopsv1alpha1.ExternalEndpoint{
				{
					Name:                "orderer0",
					Address:             "orderer0.bank-a.fabricops.io:8050",
					TLSHosts:            []string{"orderer0.fabricops.io"},
					TLSHostnameOverride: "localhost",
				},
			}
			network.Spec.Orgs[1].Peer.ExternalEndpoints = []fabricopsv1alpha1.ExternalEndpoint{
				{
					Name:                "peer0",
					Address:             "peer0.banka.fabricops.io:8051",
					TLSHosts:            []string{"peer0.fabricops.io"},
					TLSHostnameOverride: "localhost",
				},
			}
			network.Spec.Channels = []fabricopsv1alpha1.Channel{
				{
					Name: "settlement",
					Orgs: []fabricopsv1alpha1.ChannelOrg{
						{
							Name:  "BankA",
							Peers: []string{"peer0"},
						},
					},
				},
			}
			network.Spec.Chaincodes = []fabricopsv1alpha1.Chaincode{
				{
					Name:    "settlement",
					Version: "0.0.1",
					Channel: "settlement",
					Image:   "ghcr.io/dpereowei/fabricops-node-settlement:0.1.0",
					CCAAS: &fabricopsv1alpha1.ChaincodeAsAService{
						ServicePort: 7052,
					},
				},
			}
			Expect(k8sClient.Update(ctx, &network)).To(Succeed())

			controllerReconciler := &FabricNetworkReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			ordererNamespace := "fo-test-orderer"
			bankNamespace := "fo-test-banka"
			chaincode := network.Spec.Chaincodes[0]
			definitionHash := chaincodeDefinitionNameHash(chaincode)
			Expect(definitionHash).NotTo(BeEmpty())
			packageID := "settlement_settlement_0.0.1:abc123"
			approveJobName := "settlement-settlement-0-0-1-abc123-" + definitionHash + "-banka-approve"
			commitJobName := "settlement-settlement-0-0-1-abc123-" + definitionHash + "-commit"

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(ordererWorkloadCSRHosts(network.Spec.Orgs[0].Orderers[0], "orderer0", ordererNamespace)).To(ContainElements(
				"orderer0.bank-a.fabricops.io",
				"orderer0.fabricops.io",
				"localhost",
			))
			Expect(peerWorkloadCSRHosts(network.Spec.Orgs[1], "peer0", bankNamespace)).To(ContainElements(
				"peer0.banka.fabricops.io",
				"peer0.fabricops.io",
				"localhost",
			))

			markDeploymentReady(ctx, ordererNamespace, "orderer-ca")
			markDeploymentReady(ctx, bankNamespace, "banka-ca")

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			writeEnrolledOrgIdentitySecrets(ctx, &network, network.Spec.Orgs[0], ordererNamespace)
			writeEnrolledOrgIdentitySecrets(ctx, &network, network.Spec.Orgs[1], bankNamespace)

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			markDeploymentReady(ctx, ordererNamespace, "orderer0")
			markDeploymentReady(ctx, bankNamespace, "peer0")

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			var configtx corev1.ConfigMap
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: ordererNamespace,
				Name:      "settlement-configtx",
			}, &configtx)).To(Succeed())
			Expect(configtx.Labels[labelAppComponent]).To(Equal(componentChannel))
			Expect(configtx.Labels[labelChannel]).To(Equal("settlement"))
			Expect(configtx.Data["configtx.yaml"]).To(ContainSubstring("Settlement:"))
			Expect(configtx.Data["configtx.yaml"]).To(ContainSubstring("V2_0: true"))
			Expect(configtx.Data["configtx.yaml"]).To(ContainSubstring("Name: OrdererMSP"))
			Expect(configtx.Data["configtx.yaml"]).To(ContainSubstring("Name: BankAMSP"))
			Expect(configtx.Data["configtx.yaml"]).To(ContainSubstring("MSPDir: /fabricops/channel/crypto/orgs/banka/msp"))
			Expect(configtx.Data["configtx.yaml"]).To(ContainSubstring("Host: orderer0.bank-a.fabricops.io"))
			Expect(configtx.Data["configtx.yaml"]).To(ContainSubstring("AnchorPeers:"))
			Expect(configtx.Data["configtx.yaml"]).To(ContainSubstring("Host: peer0.banka.fabricops.io"))
			Expect(configtx.Data["configtx.yaml"]).To(ContainSubstring("Port: 8050"))
			Expect(configtx.Data["configtx.yaml"]).To(ContainSubstring("Port: 8051"))
			Expect(configtx.Data["configtx.yaml"]).To(ContainSubstring("ClientTLSCert: /fabricops/channel/crypto/orderers/orderer0/tls/server.crt"))
			Expect(configtx.Data["configtx.yaml"]).NotTo(ContainSubstring("Consortium:"))
			Expect(configtx.Data["configtx.yaml"]).NotTo(ContainSubstring("Consortiums:"))

			var connectionProfile corev1.ConfigMap
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      connectionProfileConfigMapName(&network),
			}, &connectionProfile)).To(Succeed())
			Expect(connectionProfile.Labels[labelAppComponent]).To(Equal(componentConnectionProfile))
			Expect(connectionProfile.Data).To(HaveKey(connectionProfileJSONKey))
			Expect(connectionProfile.Data).To(HaveKey(connectionProfileYAMLKey))
			Expect(connectionProfile.Data[connectionProfileJSONKey]).To(ContainSubstring(`"organization": "BankA"`))
			Expect(connectionProfile.Data[connectionProfileJSONKey]).To(ContainSubstring(`"peer0.banka"`))
			Expect(connectionProfile.Data[connectionProfileJSONKey]).To(ContainSubstring(`"orderer0.orderer"`))
			Expect(connectionProfile.Data[connectionProfileJSONKey]).To(ContainSubstring(`"grpcs://peer0.banka.fabricops.io:8051"`))
			Expect(connectionProfile.Data[connectionProfileJSONKey]).To(ContainSubstring(`"grpcs://orderer0.bank-a.fabricops.io:8050"`))
			Expect(connectionProfile.Data[connectionProfileJSONKey]).To(ContainSubstring(`"http://banka-ca.fo-test-banka.svc.cluster.local:7054"`))
			Expect(connectionProfile.Data[connectionProfileJSONKey]).To(ContainSubstring(`"ssl-target-name-override": "localhost"`))
			Expect(connectionProfile.Data[connectionProfileJSONKey]).To(ContainSubstring("BEGIN CERTIFICATE"))
			Expect(connectionProfile.Data[connectionProfileYAMLKey]).To(ContainSubstring("client:"))
			Expect(connectionProfile.Data[connectionProfileYAMLKey]).To(ContainSubstring("organization: BankA"))

			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			Expect(network.Status.OrgStatus[1].ConnectionProfileConfigMapName).To(Equal(connectionProfileConfigMapName(&network)))
			Expect(network.Status.OrgStatus[0].OrdererEndpoints[0].ClientAddress).To(Equal("orderer0.bank-a.fabricops.io:8050"))
			Expect(network.Status.OrgStatus[0].OrdererEndpoints[0].Namespace).To(Equal(ordererNamespace))
			Expect(network.Status.OrgStatus[0].OrdererEndpoints[0].TLSHostnameOverride).To(Equal("localhost"))
			Expect(network.Status.OrgStatus[1].PeerEndpoints[0].Address).To(Equal("peer0.banka.fabricops.io:8051"))
			Expect(network.Status.OrgStatus[1].PeerEndpoints[0].TLSHostnameOverride).To(Equal("localhost"))

			var sourceBankAdminMSP corev1.Secret
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "banka-admin-msp",
			}, &sourceBankAdminMSP)).To(Succeed())

			var channelBankAdminMSP corev1.Secret
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: ordererNamespace,
				Name:      "settlement-banka-admin-msp",
			}, &channelBankAdminMSP)).To(Succeed())
			Expect(channelBankAdminMSP.Labels[labelAppComponent]).To(Equal(componentChannel))
			Expect(channelBankAdminMSP.Labels[labelChannel]).To(Equal("settlement"))
			Expect(channelBankAdminMSP.Data).To(Equal(sourceBankAdminMSP.Data))

			var channelOrdererTLS corev1.Secret
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: ordererNamespace,
				Name:      "settlement-orderer0-tls",
			}, &channelOrdererTLS)).To(Succeed())
			Expect(channelOrdererTLS.Labels[labelAppComponent]).To(Equal(componentChannel))
			Expect(channelOrdererTLS.Data).To(HaveKey(tlsServerCertKey))

			var channelOrdererAdminTLS corev1.Secret
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: ordererNamespace,
				Name:      "settlement-orderer-admin-tls",
			}, &channelOrdererAdminTLS)).To(Succeed())
			Expect(channelOrdererAdminTLS.Labels[labelAppComponent]).To(Equal(componentChannel))
			Expect(channelOrdererAdminTLS.Labels[labelChannel]).To(Equal("settlement"))
			Expect(channelOrdererAdminTLS.Data).To(HaveKey(tlsClientCertKey))
			Expect(channelOrdererAdminTLS.Data).To(HaveKey(tlsClientKeyKey))

			var serviceAccount corev1.ServiceAccount
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: ordererNamespace,
				Name:      "settlement-channel-bootstrapper",
			}, &serviceAccount)).To(Succeed())
			Expect(serviceAccount.Labels[labelAppComponent]).To(Equal(componentChannel))

			var role rbacv1.Role
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: ordererNamespace,
				Name:      "settlement-channel-bootstrapper",
			}, &role)).To(Succeed())
			Expect(role.Rules).To(ContainElement(rbacv1.PolicyRule{
				APIGroups: []string{""},
				Resources: []string{"configmaps"},
				Verbs:     []string{"get", "create", "update", "patch"},
			}))

			var job batchv1.Job
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: ordererNamespace,
				Name:      "settlement-channel-block",
			}, &job)).To(Succeed())
			Expect(job.Labels[labelAppComponent]).To(Equal(componentChannel))
			Expect(job.Labels[labelChannel]).To(Equal("settlement"))
			Expect(job.Annotations[annotationSucceededJobCleanup]).To(Equal("true"))
			Expect(job.Spec.Template.Spec.ServiceAccountName).To(Equal("settlement-channel-bootstrapper"))
			Expect(job.Spec.Template.Spec.RestartPolicy).To(Equal(corev1.RestartPolicyNever))
			Expect(job.Spec.Template.Spec.InitContainers).To(HaveLen(1))
			Expect(job.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(configMapVolumeNames(job.Spec.Template.Spec)).To(HaveKeyWithValue(channelConfigVolumeName, "settlement-configtx"))
			Expect(secretVolumeNames(job.Spec.Template.Spec)).To(HaveKeyWithValue("msp-orderer", "settlement-orderer-admin-msp"))
			Expect(secretVolumeNames(job.Spec.Template.Spec)).To(HaveKeyWithValue("msp-banka", "settlement-banka-admin-msp"))
			Expect(secretVolumeNames(job.Spec.Template.Spec)).To(HaveKeyWithValue("tls-orderer0", "settlement-orderer0-tls"))

			generateContainer := job.Spec.Template.Spec.InitContainers[0]
			Expect(generateContainer.Name).To(Equal(generateChannelBlockContainer))
			Expect(generateContainer.Image).To(Equal("hyperledger/fabric-tools:2.5.14"))
			Expect(generateContainer.Command[2]).To(ContainSubstring("configtxgen"))
			Expect(generateContainer.Command[2]).To(ContainSubstring("-profile Settlement"))
			Expect(generateContainer.Command[2]).To(ContainSubstring("-outputBlock /fabricops/channel/output/settlement.block"))
			Expect(volumeMountPaths(generateContainer)).To(HaveKeyWithValue(channelConfigVolumeName, channelConfigDir))
			Expect(volumeMountPaths(generateContainer)).To(HaveKeyWithValue(channelOutputVolumeName, channelOutputDir))
			Expect(volumeMountPaths(generateContainer)).To(HaveKeyWithValue("msp-banka", "/fabricops/channel/crypto/orgs/banka/msp"))
			Expect(volumeMountPaths(generateContainer)).To(HaveKeyWithValue("tls-orderer0", "/fabricops/channel/crypto/orderers/orderer0/tls"))

			publishContainer := job.Spec.Template.Spec.Containers[0]
			Expect(publishContainer.Name).To(Equal(publishChannelBlockContainer))
			Expect(publishContainer.Image).To(Equal(kubectlImage()))
			Expect(envMap(publishContainer)[envChannelBlockConfigMap]).To(Equal("settlement-channel-block"))
			Expect(envMap(publishContainer)[envChannelBlockFile]).To(Equal("settlement.block"))
			Expect(publishContainer.Command[2]).To(ContainSubstring("create configmap \"$FABRICOPS_CHANNEL_BLOCK_CONFIGMAP\""))
			Expect(volumeMountPaths(publishContainer)).To(HaveKeyWithValue(channelOutputVolumeName, channelOutputDir))

			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			Expect(network.Status.Phase).To(Equal(fabricopsv1alpha1.PhaseCreating))
			Expect(network.Status.ChannelStatus).To(HaveLen(1))
			Expect(network.Status.ChannelStatus[0].ConfigReady).To(BeTrue())
			Expect(network.Status.ChannelStatus[0].BlockReady).To(BeFalse())
			Expect(network.Status.ChannelStatus[0].Message).To(Equal("Waiting for channel block generation Job"))

			blockConfigMap := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "settlement-channel-block",
					Namespace: ordererNamespace,
				},
				BinaryData: map[string][]byte{
					"settlement.block": []byte("block"),
				},
			}
			Expect(k8sClient.Create(ctx, blockConfigMap)).To(Succeed())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			Expect(network.Status.ChannelStatus[0].ConfigReady).To(BeTrue())
			Expect(network.Status.ChannelStatus[0].BlockReady).To(BeTrue())
			Expect(network.Status.ChannelStatus[0].Ready).To(BeFalse())
			Expect(network.Status.ChannelStatus[0].Orderers.Ready).To(Equal(int32(0)))
			Expect(network.Status.ChannelStatus[0].Message).To(Equal("Waiting for orderer join Jobs"))

			var ordererJoinJob batchv1.Job
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: ordererNamespace,
				Name:      "settlement-orderer-orderer0-orderer-join",
			}, &ordererJoinJob)).To(Succeed())
			Expect(ordererJoinJob.Labels[labelAppComponent]).To(Equal(componentChannel))
			Expect(ordererJoinJob.Labels[labelChannel]).To(Equal("settlement"))
			Expect(ordererJoinJob.Labels[labelWorkload]).To(Equal("orderer0"))
			Expect(ordererJoinJob.Annotations[annotationSucceededJobCleanup]).To(Equal("true"))
			Expect(ordererJoinJob.Spec.Template.Spec.ServiceAccountName).To(Equal("settlement-channel-bootstrapper"))
			Expect(ordererJoinJob.Spec.Template.Spec.RestartPolicy).To(Equal(corev1.RestartPolicyNever))
			Expect(ordererJoinJob.Spec.Template.Spec.InitContainers).To(HaveLen(1))
			Expect(ordererJoinJob.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(configMapVolumeNames(ordererJoinJob.Spec.Template.Spec)).To(HaveKeyWithValue(channelBlockVolumeName, "settlement-channel-block"))
			Expect(secretVolumeNames(ordererJoinJob.Spec.Template.Spec)).To(HaveKeyWithValue("admin-tls-orderer", "settlement-orderer-admin-tls"))

			joinContainer := ordererJoinJob.Spec.Template.Spec.InitContainers[0]
			Expect(joinContainer.Name).To(Equal(joinOrdererContainer))
			Expect(joinContainer.Image).To(Equal("hyperledger/fabric-tools:2.5.14"))
			Expect(joinContainer.Command[2]).To(ContainSubstring("osnadmin channel join"))
			Expect(joinContainer.Command[2]).To(ContainSubstring("osnadmin channel list"))
			Expect(joinContainer.Command[2]).To(ContainSubstring("--channelID \"$CHANNEL_ID\""))
			Expect(joinContainer.Command[2]).To(ContainSubstring("--no-status"))
			Expect(joinContainer.Command[2]).To(ContainSubstring("sed -n '/^[[:space:]]*[{[]/,$p'"))
			Expect(joinContainer.Command[2]).To(ContainSubstring("Orderer channel $CHANNEL_ID did not become active"))
			Expect(joinContainer.Command[2]).To(ContainSubstring("orderer0.fo-test-orderer.svc.cluster.local:9443"))
			Expect(joinContainer.Command[2]).To(ContainSubstring("--client-cert \"$ADMIN_TLS_DIR/client.crt\""))
			Expect(volumeMountPaths(joinContainer)).To(HaveKeyWithValue(channelOutputVolumeName, channelOutputDir))
			Expect(volumeMountPaths(joinContainer)).To(HaveKeyWithValue(channelBlockVolumeName, channelBlockDir))
			Expect(volumeMountPaths(joinContainer)).To(HaveKeyWithValue("admin-tls-orderer", channelOrdererAdminTLSPath(network.Spec.Orgs[0])))

			joinPublisher := ordererJoinJob.Spec.Template.Spec.Containers[0]
			Expect(joinPublisher.Name).To(Equal(publishOrdererJoinContainer))
			Expect(joinPublisher.Image).To(Equal(kubectlImage()))
			Expect(envMap(joinPublisher)[envOrdererJoinResultConfigMap]).To(Equal("settlement-orderer-orderer0-orderer-join-result"))
			Expect(envMap(joinPublisher)[envOrdererJoinResultKey]).To(Equal(channelOrdererJoinResultKey))
			Expect(volumeMountPaths(joinPublisher)).To(HaveKeyWithValue(channelOutputVolumeName, channelOutputDir))

			markJobComplete(ctx, ordererNamespace, "settlement-orderer-orderer0-orderer-join")
			createOrdererJoinResultConfigMap(ctx, ordererNamespace, "settlement-orderer-orderer0-orderer-join-result", "settlement")

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			Expect(network.Status.ChannelStatus[0].ConfigReady).To(BeTrue())
			Expect(network.Status.ChannelStatus[0].BlockReady).To(BeTrue())
			Expect(network.Status.ChannelStatus[0].Orderers.Ready).To(Equal(int32(1)))
			Expect(network.Status.ChannelStatus[0].Ready).To(BeFalse())
			Expect(network.Status.ChannelStatus[0].Message).To(Equal("Waiting for peer join Jobs"))

			By("Deleting the cleanup-eligible orderer join Job after durable result evidence exists")
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: ordererNamespace,
				Name:      "settlement-orderer-orderer0-orderer-join",
			}, &ordererJoinJob)).To(Succeed())
			propagation := metav1.DeletePropagationBackground
			Expect(k8sClient.Delete(ctx, &ordererJoinJob, &client.DeleteOptions{PropagationPolicy: &propagation})).To(Succeed())
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Namespace: ordererNamespace,
					Name:      "settlement-orderer-orderer0-orderer-join",
				}, &ordererJoinJob)
				return errors.IsNotFound(err)
			}).Should(BeTrue())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			err = k8sClient.Get(ctx, types.NamespacedName{
				Namespace: ordererNamespace,
				Name:      "settlement-orderer-orderer0-orderer-join",
			}, &ordererJoinJob)
			Expect(errors.IsNotFound(err)).To(BeTrue())

			var peerBlockConfigMap corev1.ConfigMap
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "settlement-channel-block",
			}, &peerBlockConfigMap)).To(Succeed())
			Expect(peerBlockConfigMap.Labels[labelAppComponent]).To(Equal(componentChannel))
			Expect(peerBlockConfigMap.Labels[labelChannel]).To(Equal("settlement"))
			Expect(peerBlockConfigMap.BinaryData).To(HaveKeyWithValue("settlement.block", []byte("block")))

			var peerServiceAccount corev1.ServiceAccount
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "settlement-channel-bootstrapper",
			}, &peerServiceAccount)).To(Succeed())
			Expect(peerServiceAccount.Labels[labelAppComponent]).To(Equal(componentChannel))

			var peerRole rbacv1.Role
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "settlement-channel-bootstrapper",
			}, &peerRole)).To(Succeed())
			Expect(peerRole.Rules).To(ContainElement(rbacv1.PolicyRule{
				APIGroups: []string{""},
				Resources: []string{"configmaps"},
				Verbs:     []string{"get", "create", "update", "patch"},
			}))

			var peerRoleBinding rbacv1.RoleBinding
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "settlement-channel-bootstrapper",
			}, &peerRoleBinding)).To(Succeed())
			Expect(peerRoleBinding.Subjects).To(ContainElement(rbacv1.Subject{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      "settlement-channel-bootstrapper",
				Namespace: bankNamespace,
			}))

			var peerJoinJob batchv1.Job
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "settlement-banka-peer0-peer-join",
			}, &peerJoinJob)).To(Succeed())
			Expect(peerJoinJob.Labels[labelAppComponent]).To(Equal(componentChannel))
			Expect(peerJoinJob.Labels[labelChannel]).To(Equal("settlement"))
			Expect(peerJoinJob.Labels[labelWorkload]).To(Equal("peer0"))
			Expect(peerJoinJob.Annotations[annotationSucceededJobCleanup]).To(Equal("true"))
			Expect(peerJoinJob.Spec.Template.Spec.ServiceAccountName).To(Equal("settlement-channel-bootstrapper"))
			Expect(peerJoinJob.Spec.Template.Spec.RestartPolicy).To(Equal(corev1.RestartPolicyNever))
			Expect(peerJoinJob.Spec.Template.Spec.InitContainers).To(HaveLen(1))
			Expect(peerJoinJob.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(configMapVolumeNames(peerJoinJob.Spec.Template.Spec)).To(HaveKeyWithValue(channelBlockVolumeName, "settlement-channel-block"))
			Expect(secretVolumeNames(peerJoinJob.Spec.Template.Spec)).To(HaveKeyWithValue("msp-banka", "banka-admin-msp"))
			Expect(secretVolumeNames(peerJoinJob.Spec.Template.Spec)).To(HaveKeyWithValue("admin-tls-banka", "banka-admin-tls"))

			peerJoinContainer := peerJoinJob.Spec.Template.Spec.InitContainers[0]
			Expect(peerJoinContainer.Name).To(Equal(joinPeerContainer))
			Expect(peerJoinContainer.Image).To(Equal("hyperledger/fabric-tools:2.5.14"))
			Expect(peerJoinContainer.Command[2]).To(ContainSubstring("peer channel join"))
			Expect(peerJoinContainer.Command[2]).To(ContainSubstring("peer channel list"))
			Expect(peerJoinContainer.Command[2]).To(ContainSubstring("CORE_PEER_LOCALMSPID=\"BankAMSP\""))
			Expect(peerJoinContainer.Command[2]).To(ContainSubstring("peer0.fo-test-banka.svc.cluster.local:7051"))
			Expect(peerJoinContainer.Command[2]).To(ContainSubstring("--cafile \"$CORE_PEER_TLS_ROOTCERT_FILE\""))
			Expect(volumeMountPaths(peerJoinContainer)).To(HaveKeyWithValue(channelOutputVolumeName, channelOutputDir))
			Expect(volumeMountPaths(peerJoinContainer)).To(HaveKeyWithValue(channelBlockVolumeName, channelBlockDir))
			Expect(volumeMountPaths(peerJoinContainer)).To(HaveKeyWithValue("msp-banka", channelOrgMSPPath(network.Spec.Orgs[1])))
			Expect(volumeMountPaths(peerJoinContainer)).To(HaveKeyWithValue("admin-tls-banka", channelOrdererAdminTLSPath(network.Spec.Orgs[1])))

			peerJoinPublisher := peerJoinJob.Spec.Template.Spec.Containers[0]
			Expect(peerJoinPublisher.Name).To(Equal(publishPeerJoinContainer))
			Expect(peerJoinPublisher.Image).To(Equal(kubectlImage()))
			Expect(envMap(peerJoinPublisher)[envPeerJoinResultConfigMap]).To(Equal("settlement-banka-peer0-peer-join-result"))
			Expect(envMap(peerJoinPublisher)[envPeerJoinResultKey]).To(Equal(channelPeerJoinResultKey))
			Expect(volumeMountPaths(peerJoinPublisher)).To(HaveKeyWithValue(channelOutputVolumeName, channelOutputDir))

			markJobComplete(ctx, bankNamespace, "settlement-banka-peer0-peer-join")
			createPeerJoinResultConfigMap(ctx, bankNamespace, "settlement-banka-peer0-peer-join-result", "settlement")

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			Expect(network.Status.Phase).To(Equal(fabricopsv1alpha1.PhaseCreating))
			Expect(network.Status.ChannelStatus[0].ConfigReady).To(BeTrue())
			Expect(network.Status.ChannelStatus[0].BlockReady).To(BeTrue())
			Expect(network.Status.ChannelStatus[0].Orderers.Ready).To(Equal(int32(1)))
			Expect(network.Status.ChannelStatus[0].Peers.Ready).To(Equal(int32(1)))
			Expect(network.Status.ChannelStatus[0].Orgs[0].Ready).To(BeFalse())
			Expect(network.Status.ChannelStatus[0].Orgs[0].Message).To(Equal("Waiting for anchor peer update Job"))
			Expect(network.Status.ChannelStatus[0].Ready).To(BeFalse())
			Expect(network.Status.ChannelStatus[0].Message).To(Equal("Waiting for anchor peer update Jobs"))

			channels := apiMeta.FindStatusCondition(network.Status.Conditions, conditionChannelsReady)
			Expect(channels).NotTo(BeNil())
			Expect(channels.Status).To(Equal(metav1.ConditionFalse))
			Expect(channels.Reason).To(Equal("ChannelBootstrapPending"))
			Expect(channels.Message).To(Equal("settlement: Waiting for anchor peer update Jobs"))

			By("Deleting the cleanup-eligible peer join Job after durable result evidence exists")
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "settlement-banka-peer0-peer-join",
			}, &peerJoinJob)).To(Succeed())
			Expect(k8sClient.Delete(ctx, &peerJoinJob, &client.DeleteOptions{PropagationPolicy: &propagation})).To(Succeed())
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Namespace: bankNamespace,
					Name:      "settlement-banka-peer0-peer-join",
				}, &peerJoinJob)
				return errors.IsNotFound(err)
			}).Should(BeTrue())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			err = k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "settlement-banka-peer0-peer-join",
			}, &peerJoinJob)
			Expect(errors.IsNotFound(err)).To(BeTrue())

			var peerOrdererTLS corev1.Secret
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "settlement-orderer0-tls",
			}, &peerOrdererTLS)).To(Succeed())
			Expect(peerOrdererTLS.Labels[labelAppComponent]).To(Equal(componentChannel))
			Expect(peerOrdererTLS.Labels[labelChannel]).To(Equal("settlement"))
			Expect(peerOrdererTLS.Data).To(Equal(channelOrdererTLS.Data))

			var anchorPeerJob batchv1.Job
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "settlement-banka-anchor-peer-update",
			}, &anchorPeerJob)).To(Succeed())
			Expect(anchorPeerJob.Labels[labelAppComponent]).To(Equal(componentChannel))
			Expect(anchorPeerJob.Labels[labelChannel]).To(Equal("settlement"))
			Expect(anchorPeerJob.Labels[labelWorkload]).To(Equal("peer0"))
			Expect(anchorPeerJob.Annotations[annotationSucceededJobCleanup]).To(Equal("true"))
			Expect(anchorPeerJob.Spec.Template.Spec.ServiceAccountName).To(Equal("settlement-channel-bootstrapper"))
			Expect(anchorPeerJob.Spec.Template.Spec.RestartPolicy).To(Equal(corev1.RestartPolicyNever))
			Expect(anchorPeerJob.Spec.Template.Spec.InitContainers).To(HaveLen(1))
			Expect(anchorPeerJob.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(secretVolumeNames(anchorPeerJob.Spec.Template.Spec)).To(HaveKeyWithValue("msp-banka", "banka-admin-msp"))
			Expect(secretVolumeNames(anchorPeerJob.Spec.Template.Spec)).To(HaveKeyWithValue("admin-tls-banka", "banka-admin-tls"))
			Expect(secretVolumeNames(anchorPeerJob.Spec.Template.Spec)).To(HaveKeyWithValue("tls-orderer0", "settlement-orderer0-tls"))

			anchorContainer := anchorPeerJob.Spec.Template.Spec.InitContainers[0]
			Expect(anchorContainer.Name).To(Equal(updateAnchorPeerContainer))
			Expect(anchorContainer.Image).To(Equal("hyperledger/fabric-tools:2.5.14"))
			Expect(anchorContainer.Command[2]).To(ContainSubstring("MSP_ID=\"BankAMSP\""))
			Expect(anchorContainer.Command[2]).To(ContainSubstring("ANCHOR_HOST=\"peer0.banka.fabricops.io\""))
			Expect(anchorContainer.Command[2]).To(ContainSubstring("ANCHOR_PORT=8051"))
			Expect(anchorContainer.Command[2]).To(ContainSubstring("ORDERER_ADDRESS=\"orderer0.fo-test-orderer.svc.cluster.local:7050\""))
			Expect(anchorContainer.Command[2]).To(ContainSubstring("peer channel fetch config"))
			Expect(anchorContainer.Command[2]).To(ContainSubstring("retry 30 5 peer channel fetch config"))
			Expect(anchorContainer.Command[2]).To(ContainSubstring("configtxlator compute_update"))
			Expect(anchorContainer.Command[2]).To(ContainSubstring("jq -n"))
			Expect(anchorContainer.Command[2]).To(ContainSubstring("AnchorPeers"))
			Expect(anchorContainer.Command[2]).To(ContainSubstring("peer channel update"))
			Expect(anchorContainer.Command[2]).To(ContainSubstring("retry 30 5 peer channel update"))
			Expect(anchorContainer.Command[2]).To(ContainSubstring("--cafile \"$ORDERER_TLS_DIR/ca.crt\""))
			Expect(volumeMountPaths(anchorContainer)).To(HaveKeyWithValue(channelOutputVolumeName, channelOutputDir))
			Expect(volumeMountPaths(anchorContainer)).To(HaveKeyWithValue("msp-banka", channelOrgMSPPath(network.Spec.Orgs[1])))
			Expect(volumeMountPaths(anchorContainer)).To(HaveKeyWithValue("admin-tls-banka", channelOrdererAdminTLSPath(network.Spec.Orgs[1])))
			Expect(volumeMountPaths(anchorContainer)).To(HaveKeyWithValue("tls-orderer0", channelOrdererTLSPath("orderer0")))

			anchorPublisher := anchorPeerJob.Spec.Template.Spec.Containers[0]
			Expect(anchorPublisher.Name).To(Equal(publishAnchorPeerContainer))
			Expect(anchorPublisher.Image).To(Equal(kubectlImage()))
			Expect(envMap(anchorPublisher)[envAnchorPeerResultConfigMap]).To(Equal("settlement-banka-anchor-peer-update-result"))
			Expect(envMap(anchorPublisher)[envAnchorPeerResultKey]).To(Equal(channelAnchorPeerResultKey))
			Expect(volumeMountPaths(anchorPublisher)).To(HaveKeyWithValue(channelOutputVolumeName, channelOutputDir))

			markJobComplete(ctx, bankNamespace, "settlement-banka-anchor-peer-update")
			createAnchorPeerUpdateResultConfigMap(
				ctx,
				bankNamespace,
				"settlement-banka-anchor-peer-update-result",
				"settlement",
				"BankAMSP",
				"peer0.banka.fabricops.io",
				8051,
			)

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			Expect(network.Status.Phase).To(Equal(fabricopsv1alpha1.PhaseCreating))
			Expect(network.Status.Message).To(Equal("Waiting for Fabric chaincodes to become ready"))
			Expect(network.Status.ChannelStatus[0].ConfigReady).To(BeTrue())
			Expect(network.Status.ChannelStatus[0].BlockReady).To(BeTrue())
			Expect(network.Status.ChannelStatus[0].Orderers.Ready).To(Equal(int32(1)))
			Expect(network.Status.ChannelStatus[0].Peers.Ready).To(Equal(int32(1)))
			Expect(network.Status.ChannelStatus[0].Orgs[0].Ready).To(BeTrue())
			Expect(network.Status.ChannelStatus[0].Ready).To(BeTrue())
			Expect(network.Status.ChannelStatus[0].Message).To(BeEmpty())

			channels = apiMeta.FindStatusCondition(network.Status.Conditions, conditionChannelsReady)
			Expect(channels).NotTo(BeNil())
			Expect(channels.Status).To(Equal(metav1.ConditionTrue))
			Expect(channels.Reason).To(Equal("ChannelsReady"))

			By("Deleting the cleanup-eligible anchor peer update Job after durable result evidence exists")
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "settlement-banka-anchor-peer-update",
			}, &anchorPeerJob)).To(Succeed())
			Expect(k8sClient.Delete(ctx, &anchorPeerJob, &client.DeleteOptions{PropagationPolicy: &propagation})).To(Succeed())
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Namespace: bankNamespace,
					Name:      "settlement-banka-anchor-peer-update",
				}, &anchorPeerJob)
				return errors.IsNotFound(err)
			}).Should(BeTrue())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			err = k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "settlement-banka-anchor-peer-update",
			}, &anchorPeerJob)
			Expect(errors.IsNotFound(err)).To(BeTrue())

			var chaincodeServiceAccount corev1.ServiceAccount
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "settlement-settlement-0-0-1-banka-peer0-installer",
			}, &chaincodeServiceAccount)).To(Succeed())
			Expect(chaincodeServiceAccount.Labels[labelAppComponent]).To(Equal(componentChaincode))
			Expect(chaincodeServiceAccount.Labels[labelChannel]).To(Equal("settlement"))
			Expect(chaincodeServiceAccount.Labels[labelChaincode]).To(Equal("settlement"))
			Expect(chaincodeServiceAccount.Labels[labelWorkload]).To(Equal("peer0"))

			var chaincodeRole rbacv1.Role
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "settlement-settlement-0-0-1-banka-peer0-installer",
			}, &chaincodeRole)).To(Succeed())
			Expect(chaincodeRole.Rules).To(ContainElement(rbacv1.PolicyRule{
				APIGroups: []string{""},
				Resources: []string{"configmaps"},
				Verbs:     []string{"get", "create", "update", "patch"},
			}))

			var chaincodeInstallJob batchv1.Job
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "settlement-settlement-0-0-1-banka-peer0-install",
			}, &chaincodeInstallJob)).To(Succeed())
			Expect(chaincodeInstallJob.Labels[labelAppComponent]).To(Equal(componentChaincode))
			Expect(chaincodeInstallJob.Labels[labelChannel]).To(Equal("settlement"))
			Expect(chaincodeInstallJob.Labels[labelChaincode]).To(Equal("settlement"))
			Expect(chaincodeInstallJob.Labels[labelWorkload]).To(Equal("peer0"))
			Expect(chaincodeInstallJob.Annotations[annotationSucceededJobCleanup]).To(Equal("true"))
			Expect(chaincodeInstallJob.Spec.Template.Spec.ServiceAccountName).To(Equal("settlement-settlement-0-0-1-banka-peer0-installer"))
			Expect(chaincodeInstallJob.Spec.Template.Spec.RestartPolicy).To(Equal(corev1.RestartPolicyNever))
			Expect(chaincodeInstallJob.Spec.Template.Spec.InitContainers).To(HaveLen(1))
			Expect(chaincodeInstallJob.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(configMapVolumeNames(chaincodeInstallJob.Spec.Template.Spec)).To(HaveKeyWithValue(chaincodePackageVolumeName, "settlement-settlement-banka-package"))
			Expect(configMapVolumeItems(chaincodeInstallJob.Spec.Template.Spec, chaincodePackageVolumeName)).To(ContainElement(corev1.KeyToPath{
				Key:  "settlement_settlement_0.0.1.tar.gz",
				Path: "settlement_settlement_0.0.1.tar.gz",
			}))
			Expect(secretVolumeNames(chaincodeInstallJob.Spec.Template.Spec)).To(HaveKeyWithValue(chaincodeAdminMSPVolume, "banka-admin-msp"))
			Expect(secretVolumeNames(chaincodeInstallJob.Spec.Template.Spec)).To(HaveKeyWithValue(chaincodeAdminTLSVolume, "banka-admin-tls"))

			installContainer := chaincodeInstallJob.Spec.Template.Spec.InitContainers[0]
			Expect(installContainer.Name).To(Equal(installChaincodeContainer))
			Expect(installContainer.Image).To(Equal("hyperledger/fabric-tools:2.5.14"))
			Expect(installContainer.Command[2]).To(ContainSubstring("peer lifecycle chaincode install \"$PACKAGE_FILE\""))
			Expect(installContainer.Command[2]).To(ContainSubstring("peer lifecycle chaincode queryinstalled --output json"))
			Expect(installContainer.Command[2]).To(ContainSubstring("PACKAGE_FILE=\"$PACKAGE_INPUT_DIR/$PACKAGE_ARCHIVE\""))
			Expect(installContainer.Command[2]).To(ContainSubstring("test -f \"$PACKAGE_FILE\""))
			Expect(installContainer.Command[2]).NotTo(ContainSubstring("tar -czf"))
			Expect(installContainer.Command[2]).To(ContainSubstring(".installed_chaincodes[]? | select(.label == $package_label) | .package_id"))
			Expect(installContainer.Command[2]).To(ContainSubstring("CORE_PEER_LOCALMSPID=\"BankAMSP\""))
			Expect(installContainer.Command[2]).To(ContainSubstring("CORE_PEER_ADDRESS=\"peer0.fo-test-banka.svc.cluster.local:7051\""))
			Expect(volumeMountPaths(installContainer)).To(HaveKeyWithValue(chaincodePackageVolumeName, chaincodePackageInputDir))
			Expect(volumeMountPaths(installContainer)).To(HaveKeyWithValue(chaincodeOutputVolumeName, chaincodeOutputDir))
			Expect(volumeMountPaths(installContainer)).To(HaveKeyWithValue(chaincodeAdminMSPVolume, chaincodeAdminMSPPath))
			Expect(volumeMountPaths(installContainer)).To(HaveKeyWithValue(chaincodeAdminTLSVolume, chaincodeAdminTLSPath))

			publishChaincodeContainer := chaincodeInstallJob.Spec.Template.Spec.Containers[0]
			Expect(publishChaincodeContainer.Name).To(Equal(publishChaincodeInstallContainer))
			Expect(publishChaincodeContainer.Image).To(Equal(kubectlImage()))
			Expect(envMap(publishChaincodeContainer)[envChaincodePackageIDConfigMap]).To(Equal("settlement-settlement-0-0-1-banka-peer0-package-id"))
			Expect(envMap(publishChaincodeContainer)[envChaincodeChannel]).To(Equal("settlement"))
			Expect(envMap(publishChaincodeContainer)[envChaincodeName]).To(Equal("settlement"))
			Expect(envMap(publishChaincodeContainer)[envChaincodePeer]).To(Equal("peer0"))
			Expect(publishChaincodeContainer.Command[2]).To(ContainSubstring("create configmap \"$FABRICOPS_CHAINCODE_PACKAGE_ID_CONFIGMAP\""))
			Expect(publishChaincodeContainer.Command[2]).To(ContainSubstring("--from-file=\"packageID=/fabricops/chaincode/output/package-id\""))
			Expect(publishChaincodeContainer.Command[2]).To(ContainSubstring("--from-file=\"chaincodeID=/fabricops/chaincode/output/chaincode-id\""))
			Expect(volumeMountPaths(publishChaincodeContainer)).To(HaveKeyWithValue(chaincodeOutputVolumeName, chaincodeOutputDir))

			Expect(network.Status.ChaincodeStatus).To(HaveLen(1))
			chaincodeStatus := network.Status.ChaincodeStatus[0]
			Expect(chaincodeStatus.PackageMetadataReady).To(BeTrue())
			Expect(chaincodeStatus.Installed.Desired).To(Equal(int32(1)))
			Expect(chaincodeStatus.Installed.Ready).To(Equal(int32(0)))
			Expect(chaincodeStatus.InstalledReady).To(BeFalse())
			Expect(chaincodeStatus.Workloads.Desired).To(Equal(int32(1)))
			Expect(chaincodeStatus.Workloads.Ready).To(Equal(int32(0)))
			Expect(chaincodeStatus.WorkloadsReady).To(BeFalse())
			Expect(chaincodeStatus.Message).To(Equal("Package metadata generated; waiting for chaincode install Jobs"))
			Expect(chaincodeStatus.Targets).To(HaveLen(1))
			Expect(chaincodeStatus.Targets[0].WorkloadName).To(Equal("settlement-settlement-banka-peer0-ccaas"))
			Expect(chaincodeStatus.Targets[0].Workload.Desired).To(Equal(int32(1)))
			Expect(chaincodeStatus.Targets[0].Workload.Ready).To(Equal(int32(0)))
			Expect(chaincodeStatus.Targets[0].WorkloadReady).To(BeFalse())
			Expect(chaincodeStatus.Targets[0].PackageIDConfigMapName).To(Equal("settlement-settlement-0-0-1-banka-peer0-package-id"))
			Expect(chaincodeStatus.Targets[0].InstallJobName).To(Equal("settlement-settlement-0-0-1-banka-peer0-install"))
			Expect(chaincodeStatus.Targets[0].Installed).To(BeFalse())
			expectDeploymentNotFound(ctx, bankNamespace, "settlement-settlement-banka-peer0-ccaas")
			expectServiceNotFound(ctx, bankNamespace, "settlement-settlement-banka-peer0-ccaas")

			packageIDConfigMap := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "settlement-settlement-0-0-1-banka-peer0-package-id",
					Namespace: bankNamespace,
				},
				Data: map[string]string{
					chaincodePackageIDKey:      packageID,
					chaincodeChaincodeIDKey:    packageID,
					chaincodePackageHashKey:    "abc123",
					chaincodeQueryInstalledKey: `{"installed_chaincodes":[{"label":"settlement_settlement_0.0.1","package_id":"settlement_settlement_0.0.1:abc123"}]}`,
				},
			}
			Expect(k8sClient.Create(ctx, packageIDConfigMap)).To(Succeed())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			Expect(network.Status.ChaincodeStatus).To(HaveLen(1))
			chaincodeStatus = network.Status.ChaincodeStatus[0]
			Expect(chaincodeStatus.Installed.Desired).To(Equal(int32(1)))
			Expect(chaincodeStatus.Installed.Ready).To(Equal(int32(1)))
			Expect(chaincodeStatus.InstalledReady).To(BeTrue())
			Expect(chaincodeStatus.Workloads.Desired).To(Equal(int32(1)))
			Expect(chaincodeStatus.Workloads.Ready).To(Equal(int32(0)))
			Expect(chaincodeStatus.WorkloadsReady).To(BeFalse())
			Expect(chaincodeStatus.Approved.Desired).To(Equal(int32(1)))
			Expect(chaincodeStatus.Approved.Ready).To(Equal(int32(0)))
			Expect(chaincodeStatus.ApprovedReady).To(BeFalse())
			Expect(chaincodeStatus.Message).To(Equal("Chaincode package installed; waiting for chaincode workloads and lifecycle approval Jobs"))
			Expect(chaincodeStatus.Targets[0].WorkloadName).To(Equal("settlement-settlement-banka-peer0-ccaas"))
			Expect(chaincodeStatus.Targets[0].Workload.Desired).To(Equal(int32(1)))
			Expect(chaincodeStatus.Targets[0].Workload.Ready).To(Equal(int32(0)))
			Expect(chaincodeStatus.Targets[0].WorkloadReady).To(BeFalse())
			Expect(chaincodeStatus.Targets[0].Installed).To(BeTrue())
			Expect(chaincodeStatus.Targets[0].Approved).To(BeFalse())
			Expect(chaincodeStatus.Targets[0].PackageID).To(Equal("settlement_settlement_0.0.1:abc123"))
			Expect(chaincodeStatus.Targets[0].ChaincodeID).To(Equal("settlement_settlement_0.0.1:abc123"))
			Expect(chaincodeStatus.Targets[0].ApproveJobName).To(Equal(approveJobName))

			var chaincodeService corev1.Service
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "settlement-settlement-banka-peer0-ccaas",
			}, &chaincodeService)).To(Succeed())
			Expect(chaincodeService.Labels[labelAppComponent]).To(Equal(componentChaincode))
			Expect(chaincodeService.Labels[labelChannel]).To(Equal("settlement"))
			Expect(chaincodeService.Labels[labelChaincode]).To(Equal("settlement"))
			Expect(chaincodeService.Labels[labelWorkload]).To(Equal("peer0"))
			Expect(chaincodeService.Spec.Selector[labelComponent]).To(Equal(componentChaincode))
			Expect(chaincodeService.Spec.Selector[labelChannel]).To(Equal("settlement"))
			Expect(chaincodeService.Spec.Selector[labelChaincode]).To(Equal("settlement"))
			Expect(chaincodeService.Spec.Selector[labelWorkload]).To(Equal("peer0"))
			Expect(servicePorts(chaincodeService)).To(ContainElement(int32(7052)))
			Expect(chaincodeService.Spec.Ports[0].TargetPort.IntVal).To(Equal(int32(7052)))

			var chaincodeDeployment appsv1.Deployment
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "settlement-settlement-banka-peer0-ccaas",
			}, &chaincodeDeployment)).To(Succeed())
			Expect(chaincodeDeployment.Labels[labelAppComponent]).To(Equal(componentChaincode))
			Expect(chaincodeDeployment.Labels[labelChannel]).To(Equal("settlement"))
			Expect(chaincodeDeployment.Labels[labelChaincode]).To(Equal("settlement"))
			Expect(chaincodeDeployment.Labels[labelWorkload]).To(Equal("peer0"))
			Expect(chaincodeDeployment.Spec.Replicas).NotTo(BeNil())
			Expect(*chaincodeDeployment.Spec.Replicas).To(Equal(int32(1)))
			Expect(chaincodeDeployment.Spec.Selector.MatchLabels[labelComponent]).To(Equal(componentChaincode))
			Expect(chaincodeDeployment.Spec.Selector.MatchLabels[labelChannel]).To(Equal("settlement"))
			Expect(chaincodeDeployment.Spec.Selector.MatchLabels[labelChaincode]).To(Equal("settlement"))
			Expect(chaincodeDeployment.Spec.Selector.MatchLabels[labelWorkload]).To(Equal("peer0"))
			Expect(chaincodeDeployment.Spec.Template.Labels[labelWorkload]).To(Equal("peer0"))
			Expect(chaincodeDeployment.Spec.Template.Annotations[annotationFabricNetwork]).To(Equal(resourceName))

			chaincodeContainer := chaincodeDeployment.Spec.Template.Spec.Containers[0]
			Expect(chaincodeContainer.Name).To(Equal(chaincodeServerContainer))
			Expect(chaincodeContainer.Image).To(Equal("ghcr.io/dpereowei/fabricops-node-settlement:0.1.0"))
			Expect(chaincodeContainer.ImagePullPolicy).To(Equal(corev1.PullIfNotPresent))
			Expect(containerPorts(chaincodeContainer)).To(ContainElement(int32(7052)))
			expectTCPProbe(chaincodeContainer.ReadinessProbe, 7052)
			expectTCPProbe(chaincodeContainer.LivenessProbe, 7052)
			Expect(envMap(chaincodeContainer)[envCCAASChaincodeID]).To(Equal("settlement_settlement_0.0.1:abc123"))
			Expect(envMap(chaincodeContainer)[envCCAASCoreChaincodeIDName]).To(Equal("settlement_settlement_0.0.1:abc123"))
			Expect(envMap(chaincodeContainer)[envCCAASChaincodeServerAddress]).To(Equal("0.0.0.0:7052"))
			Expect(envMap(chaincodeContainer)[envCCAASCoreChaincodeAddress]).To(Equal("0.0.0.0:7052"))
			expectContainerResources(chaincodeContainer, defaultPeerRequestCPU, defaultPeerRequestMem, defaultPeerLimitCPU, defaultPeerLimitMem)

			var approveJob batchv1.Job
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      approveJobName,
			}, &approveJob)).To(Succeed())
			Expect(approveJob.Labels[labelAppComponent]).To(Equal(componentChaincode))
			Expect(approveJob.Labels[labelChannel]).To(Equal("settlement"))
			Expect(approveJob.Labels[labelChaincode]).To(Equal("settlement"))
			Expect(approveJob.Annotations[annotationSucceededJobCleanup]).To(Equal("true"))
			Expect(approveJob.Spec.Template.Spec.ServiceAccountName).To(Equal("settlement-settlement-0-0-1-banka-peer0-installer"))
			Expect(secretVolumeNames(approveJob.Spec.Template.Spec)).To(HaveKeyWithValue(chaincodeAdminMSPVolume, "banka-admin-msp"))
			Expect(secretVolumeNames(approveJob.Spec.Template.Spec)).To(HaveKeyWithValue(chaincodeAdminTLSVolume, "banka-admin-tls"))
			Expect(secretVolumeNames(approveJob.Spec.Template.Spec)).To(HaveKeyWithValue(channelOrdererTLSVolumeName("orderer0"), "settlement-orderer0-tls"))

			Expect(approveJob.Spec.Template.Spec.InitContainers).To(HaveLen(1))
			Expect(approveJob.Spec.Template.Spec.Containers).To(HaveLen(1))
			approveContainer := approveJob.Spec.Template.Spec.InitContainers[0]
			Expect(approveContainer.Name).To(Equal(approveChaincodeContainer))
			Expect(approveContainer.Command[2]).To(ContainSubstring("peer lifecycle chaincode approveformyorg"))
			Expect(approveContainer.Command[2]).To(ContainSubstring("peer lifecycle chaincode queryapproved"))
			Expect(approveContainer.Command[2]).To(ContainSubstring("--package-id \"$PACKAGE_ID\""))
			Expect(approveContainer.Command[2]).To(ContainSubstring("ENDORSEMENT_POLICY=\"OR('BankAMSP.member')\""))
			Expect(approveContainer.Command[2]).To(ContainSubstring("CORE_PEER_LOCALMSPID=\"BankAMSP\""))
			Expect(approveContainer.Command[2]).To(ContainSubstring("ORDERER_ADDRESS=\"orderer0.fo-test-orderer.svc.cluster.local:7050\""))
			Expect(volumeMountPaths(approveContainer)).To(HaveKeyWithValue(chaincodeAdminMSPVolume, chaincodeAdminMSPPath))
			Expect(volumeMountPaths(approveContainer)).To(HaveKeyWithValue(chaincodeAdminTLSVolume, chaincodeAdminTLSPath))
			Expect(volumeMountPaths(approveContainer)).To(HaveKeyWithValue(channelOrdererTLSVolumeName("orderer0"), chaincodeOrdererTLSPath("orderer0")))

			approvePublisher := approveJob.Spec.Template.Spec.Containers[0]
			Expect(approvePublisher.Name).To(Equal(publishChaincodeLifecycleResultContainerName(approveChaincodeContainer)))
			Expect(envMap(approvePublisher)[envChaincodeLifecycleResultConfigMap]).To(Equal(chaincodeApproveResultConfigMapName(chaincode, network.Spec.Orgs[1], packageID)))
			Expect(envMap(approvePublisher)[envChaincodeLifecycleResultKey]).To(Equal(chaincodeQueryApprovedKey))
			Expect(volumeMountPaths(approvePublisher)).To(HaveKeyWithValue(chaincodeOutputVolumeName, chaincodeOutputDir))

			markJobComplete(ctx, bankNamespace, approveJobName)
			createChaincodeLifecycleResultConfigMap(ctx, &network, network.Spec.Orgs[1], chaincode, chaincodeApproveResultConfigMapName(chaincode, network.Spec.Orgs[1], packageID), chaincodeQueryApprovedKey, chaincodeSequence(chaincode))

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			Expect(network.Status.ChaincodeStatus).To(HaveLen(1))
			chaincodeStatus = network.Status.ChaincodeStatus[0]
			Expect(chaincodeStatus.Approved.Desired).To(Equal(int32(1)))
			Expect(chaincodeStatus.Approved.Ready).To(Equal(int32(1)))
			Expect(chaincodeStatus.ApprovedReady).To(BeTrue())
			Expect(chaincodeStatus.Workloads.Desired).To(Equal(int32(1)))
			Expect(chaincodeStatus.Workloads.Ready).To(Equal(int32(0)))
			Expect(chaincodeStatus.WorkloadsReady).To(BeFalse())
			Expect(chaincodeStatus.CommitJobName).To(Equal(commitJobName))
			Expect(chaincodeStatus.Committed).To(BeFalse())
			Expect(chaincodeStatus.Ready).To(BeFalse())
			Expect(chaincodeStatus.Message).To(Equal("Waiting for chaincode commit Job"))
			Expect(chaincodeStatus.Targets[0].Approved).To(BeTrue())

			var peerTLSCopy corev1.Secret
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "settlement-settlement-0-0-1-banka-peer0-tls",
			}, &peerTLSCopy)).To(Succeed())
			Expect(peerTLSCopy.Labels[labelAppComponent]).To(Equal(componentChaincode))

			var commitServiceAccount corev1.ServiceAccount
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "settlement-settlement-0-0-1-committer",
			}, &commitServiceAccount)).To(Succeed())
			Expect(commitServiceAccount.Labels[labelAppComponent]).To(Equal(componentChaincode))

			var commitRole rbacv1.Role
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "settlement-settlement-0-0-1-committer",
			}, &commitRole)).To(Succeed())
			Expect(commitRole.Rules).To(ContainElement(rbacv1.PolicyRule{
				APIGroups: []string{""},
				Resources: []string{"configmaps"},
				Verbs:     []string{"get", "create", "update", "patch"},
			}))

			var commitJob batchv1.Job
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      commitJobName,
			}, &commitJob)).To(Succeed())
			Expect(commitJob.Labels[labelAppComponent]).To(Equal(componentChaincode))
			Expect(commitJob.Labels[labelChannel]).To(Equal("settlement"))
			Expect(commitJob.Labels[labelChaincode]).To(Equal("settlement"))
			Expect(commitJob.Annotations[annotationSucceededJobCleanup]).To(Equal("true"))
			Expect(commitJob.Spec.Template.Spec.ServiceAccountName).To(Equal("settlement-settlement-0-0-1-committer"))
			Expect(secretVolumeNames(commitJob.Spec.Template.Spec)).To(HaveKeyWithValue(chaincodeAdminMSPVolume, "banka-admin-msp"))
			Expect(secretVolumeNames(commitJob.Spec.Template.Spec)).To(HaveKeyWithValue(chaincodeAdminTLSVolume, "banka-admin-tls"))
			Expect(secretVolumeNames(commitJob.Spec.Template.Spec)).To(HaveKeyWithValue(channelOrdererTLSVolumeName("orderer0"), "settlement-orderer0-tls"))
			Expect(secretVolumeNames(commitJob.Spec.Template.Spec)).To(HaveKeyWithValue(chaincodePeerTLSVolumeName(network.Spec.Orgs[1], "peer0"), "settlement-settlement-0-0-1-banka-peer0-tls"))

			Expect(commitJob.Spec.Template.Spec.InitContainers).To(HaveLen(1))
			Expect(commitJob.Spec.Template.Spec.Containers).To(HaveLen(1))
			commitContainer := commitJob.Spec.Template.Spec.InitContainers[0]
			Expect(commitContainer.Name).To(Equal(commitChaincodeContainer))
			Expect(commitContainer.Command[2]).To(ContainSubstring("peer lifecycle chaincode commit"))
			Expect(commitContainer.Command[2]).To(ContainSubstring("peer lifecycle chaincode querycommitted"))
			Expect(commitContainer.Command[2]).To(ContainSubstring("set -- \"$@\" --peerAddresses \"peer0.fo-test-banka.svc.cluster.local:7051\""))
			Expect(commitContainer.Command[2]).To(ContainSubstring("--tlsRootCertFiles \"/fabricops/chaincode/crypto/peers/banka/peer0/tls/ca.crt\""))
			Expect(commitContainer.Command[2]).To(ContainSubstring("ENDORSEMENT_POLICY=\"OR('BankAMSP.member')\""))
			Expect(volumeMountPaths(commitContainer)).To(HaveKeyWithValue(chaincodeAdminMSPVolume, chaincodeAdminMSPPath))
			Expect(volumeMountPaths(commitContainer)).To(HaveKeyWithValue(chaincodeAdminTLSVolume, chaincodeAdminTLSPath))
			Expect(volumeMountPaths(commitContainer)).To(HaveKeyWithValue(channelOrdererTLSVolumeName("orderer0"), chaincodeOrdererTLSPath("orderer0")))
			Expect(volumeMountPaths(commitContainer)).To(HaveKeyWithValue(chaincodePeerTLSVolumeName(network.Spec.Orgs[1], "peer0"), chaincodePeerTLSPath(network.Spec.Orgs[1], "peer0")))

			commitPublisher := commitJob.Spec.Template.Spec.Containers[0]
			Expect(commitPublisher.Name).To(Equal(publishChaincodeLifecycleResultContainerName(commitChaincodeContainer)))
			Expect(envMap(commitPublisher)[envChaincodeLifecycleResultConfigMap]).To(Equal(chaincodeCommitResultConfigMapName(chaincode, packageID)))
			Expect(envMap(commitPublisher)[envChaincodeLifecycleResultKey]).To(Equal(chaincodeQueryCommittedKey))
			Expect(volumeMountPaths(commitPublisher)).To(HaveKeyWithValue(chaincodeOutputVolumeName, chaincodeOutputDir))

			markJobComplete(ctx, bankNamespace, commitJobName)
			createChaincodeLifecycleResultConfigMap(ctx, &network, network.Spec.Orgs[1], chaincode, chaincodeCommitResultConfigMapName(chaincode, packageID), chaincodeQueryCommittedKey, chaincodeSequence(chaincode))

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			Expect(network.Status.ChaincodeStatus).To(HaveLen(1))
			chaincodeStatus = network.Status.ChaincodeStatus[0]
			Expect(chaincodeStatus.Committed).To(BeTrue())
			Expect(chaincodeStatus.Workloads.Desired).To(Equal(int32(1)))
			Expect(chaincodeStatus.Workloads.Ready).To(Equal(int32(0)))
			Expect(chaincodeStatus.WorkloadsReady).To(BeFalse())
			Expect(chaincodeStatus.Ready).To(BeFalse())
			Expect(chaincodeStatus.Message).To(Equal("Chaincode committed; waiting for chaincode workload Deployment"))
			Expect(network.Status.Phase).To(Equal(fabricopsv1alpha1.PhaseCreating))
			Expect(network.Status.Message).To(Equal("Waiting for Fabric chaincodes to become ready"))

			markDeploymentReady(ctx, bankNamespace, "settlement-settlement-banka-peer0-ccaas")

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			Expect(network.Status.ChaincodeStatus).To(HaveLen(1))
			chaincodeStatus = network.Status.ChaincodeStatus[0]
			Expect(chaincodeStatus.Committed).To(BeTrue())
			Expect(chaincodeStatus.Workloads.Desired).To(Equal(int32(1)))
			Expect(chaincodeStatus.Workloads.Ready).To(Equal(int32(1)))
			Expect(chaincodeStatus.WorkloadsReady).To(BeTrue())
			Expect(chaincodeStatus.Ready).To(BeTrue())
			Expect(chaincodeStatus.Message).To(Equal("Chaincode committed and workload ready"))
			Expect(chaincodeStatus.Targets[0].WorkloadReady).To(BeTrue())
			Expect(network.Status.Phase).To(Equal(fabricopsv1alpha1.PhaseReady))
			Expect(network.Status.Message).To(Equal("All Fabric components, channels, and chaincodes are ready"))

			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			network.Spec.Chaincodes[0].Version = "0.0.2"
			network.Spec.Chaincodes[0].Sequence = 2
			network.Spec.Chaincodes[0].Image = "ghcr.io/dpereowei/fabricops-node-settlement:0.2.0"
			Expect(k8sClient.Update(ctx, &network)).To(Succeed())

			upgradedChaincode := network.Spec.Chaincodes[0]
			upgradeDefinitionHash := chaincodeDefinitionNameHash(upgradedChaincode)
			Expect(upgradeDefinitionHash).NotTo(Equal(definitionHash))
			upgradePackageID := "settlement_settlement_0.0.2:def456"
			upgradeApproveJobName := "settlement-settlement-0-0-2-def456-" + upgradeDefinitionHash + "-banka-approve"
			upgradeCommitJobName := "settlement-settlement-0-0-2-def456-" + upgradeDefinitionHash + "-commit"

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			Expect(network.Status.ChaincodeStatus).To(HaveLen(1))
			chaincodeStatus = network.Status.ChaincodeStatus[0]
			Expect(chaincodeStatus.Version).To(Equal("0.0.2"))
			Expect(chaincodeStatus.Sequence).To(Equal(int32(2)))
			Expect(chaincodeStatus.PackageLabel).To(Equal("settlement_settlement_0.0.2"))
			Expect(chaincodeStatus.PackageMetadataReady).To(BeTrue())
			Expect(chaincodeStatus.Installed.Desired).To(Equal(int32(1)))
			Expect(chaincodeStatus.Installed.Ready).To(Equal(int32(0)))
			Expect(chaincodeStatus.InstalledReady).To(BeFalse())
			Expect(chaincodeStatus.Committed).To(BeFalse())
			Expect(chaincodeStatus.Ready).To(BeFalse())
			Expect(chaincodeStatus.Message).To(Equal("Package metadata generated; waiting for chaincode install Jobs"))
			Expect(chaincodeStatus.Targets).To(HaveLen(1))
			Expect(chaincodeStatus.Targets[0].PackageConfigMapName).To(Equal("settlement-settlement-banka-package"))
			Expect(chaincodeStatus.Targets[0].PackageIDConfigMapName).To(Equal("settlement-settlement-0-0-2-banka-peer0-package-id"))
			Expect(chaincodeStatus.Targets[0].InstallJobName).To(Equal("settlement-settlement-0-0-2-banka-peer0-install"))
			Expect(chaincodeStatus.Targets[0].Installed).To(BeFalse())

			var upgradedPackage corev1.ConfigMap
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "settlement-settlement-banka-package",
			}, &upgradedPackage)).To(Succeed())
			Expect(upgradedPackage.Data[chaincodePackageLabelKey]).To(Equal("settlement_settlement_0.0.2"))
			Expect(upgradedPackage.Data[chaincodePackageFileKey]).To(Equal("settlement_settlement_0.0.2.tar.gz"))
			Expect(upgradedPackage.BinaryData).To(HaveKey("settlement_settlement_0.0.2.tar.gz"))
			Expect(upgradedPackage.BinaryData).NotTo(HaveKey("settlement_settlement_0.0.1.tar.gz"))

			var upgradeInstallJob batchv1.Job
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "settlement-settlement-0-0-2-banka-peer0-install",
			}, &upgradeInstallJob)).To(Succeed())
			Expect(upgradeInstallJob.Spec.Template.Spec.ServiceAccountName).To(Equal("settlement-settlement-0-0-2-banka-peer0-installer"))
			Expect(configMapVolumeItems(upgradeInstallJob.Spec.Template.Spec, chaincodePackageVolumeName)).To(ContainElement(corev1.KeyToPath{
				Key:  "settlement_settlement_0.0.2.tar.gz",
				Path: "settlement_settlement_0.0.2.tar.gz",
			}))

			upgradePackageIDConfigMap := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "settlement-settlement-0-0-2-banka-peer0-package-id",
					Namespace: bankNamespace,
				},
				Data: map[string]string{
					chaincodePackageIDKey:      upgradePackageID,
					chaincodeChaincodeIDKey:    upgradePackageID,
					chaincodePackageHashKey:    "def456",
					chaincodeQueryInstalledKey: `{"installed_chaincodes":[{"label":"settlement_settlement_0.0.2","package_id":"settlement_settlement_0.0.2:def456"}]}`,
				},
			}
			Expect(k8sClient.Create(ctx, upgradePackageIDConfigMap)).To(Succeed())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			Expect(network.Status.ChaincodeStatus).To(HaveLen(1))
			chaincodeStatus = network.Status.ChaincodeStatus[0]
			Expect(chaincodeStatus.InstalledReady).To(BeTrue())
			Expect(chaincodeStatus.Workloads.Desired).To(Equal(int32(1)))
			Expect(chaincodeStatus.Workloads.Ready).To(Equal(int32(0)))
			Expect(chaincodeStatus.WorkloadsReady).To(BeFalse())
			Expect(chaincodeStatus.Targets[0].PackageID).To(Equal(upgradePackageID))
			Expect(chaincodeStatus.Targets[0].ChaincodeID).To(Equal(upgradePackageID))
			Expect(chaincodeStatus.Targets[0].ApproveJobName).To(Equal(upgradeApproveJobName))
			Expect(chaincodeStatus.Targets[0].WorkloadReady).To(BeFalse())
			Expect(chaincodeStatus.Message).To(Equal("Chaincode package installed; waiting for chaincode workloads and lifecycle approval Jobs"))

			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      "settlement-settlement-banka-peer0-ccaas",
			}, &chaincodeDeployment)).To(Succeed())
			chaincodeContainer = chaincodeDeployment.Spec.Template.Spec.Containers[0]
			Expect(chaincodeContainer.Image).To(Equal("ghcr.io/dpereowei/fabricops-node-settlement:0.2.0"))
			Expect(envMap(chaincodeContainer)[envCCAASChaincodeID]).To(Equal(upgradePackageID))
			Expect(envMap(chaincodeContainer)[envCCAASCoreChaincodeIDName]).To(Equal(upgradePackageID))
			Expect(chaincodeDeployment.Status.ReadyReplicas).To(Equal(int32(1)))
			Expect(chaincodeDeployment.Status.ObservedGeneration).To(BeNumerically("<", chaincodeDeployment.Generation))

			var upgradeApproveJob batchv1.Job
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      upgradeApproveJobName,
			}, &upgradeApproveJob)).To(Succeed())
			Expect(upgradeApproveJob.Spec.Template.Spec.ServiceAccountName).To(Equal("settlement-settlement-0-0-2-banka-peer0-installer"))
			Expect(upgradeApproveJob.Spec.Template.Spec.InitContainers[0].Command[2]).To(ContainSubstring("SEQUENCE=2"))
			Expect(upgradeApproveJob.Spec.Template.Spec.InitContainers[0].Command[2]).To(ContainSubstring("--sequence \"$SEQUENCE\""))
			Expect(upgradeApproveJob.Spec.Template.Spec.InitContainers[0].Command[2]).To(ContainSubstring("--package-id \"$PACKAGE_ID\""))

			markJobComplete(ctx, bankNamespace, upgradeApproveJobName)
			createChaincodeLifecycleResultConfigMap(ctx, &network, network.Spec.Orgs[1], upgradedChaincode, chaincodeApproveResultConfigMapName(upgradedChaincode, network.Spec.Orgs[1], upgradePackageID), chaincodeQueryApprovedKey, chaincodeSequence(upgradedChaincode))

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			Expect(network.Status.ChaincodeStatus).To(HaveLen(1))
			chaincodeStatus = network.Status.ChaincodeStatus[0]
			Expect(chaincodeStatus.ApprovedReady).To(BeTrue())
			Expect(chaincodeStatus.CommitJobName).To(Equal(upgradeCommitJobName))
			Expect(chaincodeStatus.Committed).To(BeFalse())
			Expect(chaincodeStatus.Ready).To(BeFalse())
			Expect(chaincodeStatus.Message).To(Equal("Waiting for chaincode commit Job"))

			var upgradeCommitJob batchv1.Job
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: bankNamespace,
				Name:      upgradeCommitJobName,
			}, &upgradeCommitJob)).To(Succeed())
			Expect(upgradeCommitJob.Spec.Template.Spec.ServiceAccountName).To(Equal("settlement-settlement-0-0-2-committer"))
			Expect(upgradeCommitJob.Spec.Template.Spec.InitContainers[0].Command[2]).To(ContainSubstring("SEQUENCE=2"))
			Expect(upgradeCommitJob.Spec.Template.Spec.InitContainers[0].Command[2]).To(ContainSubstring("--sequence \"$SEQUENCE\""))
			Expect(upgradeCommitJob.Spec.Template.Spec.InitContainers[0].Command[2]).To(ContainSubstring("peer lifecycle chaincode querycommitted"))

			markJobComplete(ctx, bankNamespace, upgradeCommitJobName)
			createChaincodeLifecycleResultConfigMap(ctx, &network, network.Spec.Orgs[1], upgradedChaincode, chaincodeCommitResultConfigMapName(upgradedChaincode, upgradePackageID), chaincodeQueryCommittedKey, chaincodeSequence(upgradedChaincode))

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			Expect(network.Status.ChaincodeStatus).To(HaveLen(1))
			chaincodeStatus = network.Status.ChaincodeStatus[0]
			Expect(chaincodeStatus.Committed).To(BeTrue())
			Expect(chaincodeStatus.Workloads.Ready).To(Equal(int32(0)))
			Expect(chaincodeStatus.WorkloadsReady).To(BeFalse())
			Expect(chaincodeStatus.Ready).To(BeFalse())
			Expect(chaincodeStatus.Message).To(Equal("Chaincode committed; waiting for chaincode workload Deployment"))
			Expect(network.Status.Phase).To(Equal(fabricopsv1alpha1.PhaseCreating))

			markDeploymentReady(ctx, bankNamespace, "settlement-settlement-banka-peer0-ccaas")

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			Expect(network.Status.ChaincodeStatus).To(HaveLen(1))
			chaincodeStatus = network.Status.ChaincodeStatus[0]
			Expect(chaincodeStatus.Committed).To(BeTrue())
			Expect(chaincodeStatus.Workloads.Ready).To(Equal(int32(1)))
			Expect(chaincodeStatus.WorkloadsReady).To(BeTrue())
			Expect(chaincodeStatus.Ready).To(BeTrue())
			Expect(chaincodeStatus.Message).To(Equal("Chaincode committed and workload ready"))
			Expect(network.Status.Phase).To(Equal(fabricopsv1alpha1.PhaseReady))
		})

		It("should clean up only eligible succeeded Jobs after the configured history TTL", func() {
			var network fabricopsv1alpha1.FabricNetwork
			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			network.Spec.Global.Jobs = &fabricopsv1alpha1.JobCleanupConfig{
				SucceededHistoryTTLSeconds: int32Ptr(0),
			}

			controllerReconciler := &FabricNetworkReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			bankOrg := network.Spec.Orgs[1]
			namespace := orgNamespaceName(&network, bankOrg)
			Expect(controllerReconciler.ensureNamespace(ctx, buildOrgNamespace(&network, bankOrg))).To(Succeed())

			createCleanupTestJob(ctx, &network, bankOrg, namespace, "eligible-succeeded", true)
			markJobComplete(ctx, namespace, "eligible-succeeded")
			createCleanupTestJob(ctx, &network, bankOrg, namespace, "proof-succeeded", false)
			markJobComplete(ctx, namespace, "proof-succeeded")
			createCleanupTestJob(ctx, &network, bankOrg, namespace, "eligible-failed", true)
			markJobFailed(ctx, namespace, "eligible-failed")

			cleanupAfter, err := controllerReconciler.cleanupSucceededJobs(ctx, &network)
			Expect(err).NotTo(HaveOccurred())
			Expect(cleanupAfter).To(Equal(time.Duration(0)))

			Eventually(func(g Gomega) {
				var job batchv1.Job
				err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "eligible-succeeded"}, &job)
				g.Expect(errors.IsNotFound(err)).To(BeTrue())
			}).Should(Succeed())

			var retained batchv1.Job
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "proof-succeeded"}, &retained)).To(Succeed())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "eligible-failed"}, &retained)).To(Succeed())
		})

		It("should not retroactively mark existing Jobs as cleanup eligible", func() {
			var network fabricopsv1alpha1.FabricNetwork
			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			network.Spec.Global.Jobs = &fabricopsv1alpha1.JobCleanupConfig{
				SucceededHistoryTTLSeconds: int32Ptr(0),
			}

			controllerReconciler := &FabricNetworkReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			bankOrg := network.Spec.Orgs[1]
			namespace := orgNamespaceName(&network, bankOrg)
			Expect(controllerReconciler.ensureNamespace(ctx, buildOrgNamespace(&network, bankOrg))).To(Succeed())

			createCleanupTestJob(ctx, &network, bankOrg, namespace, "legacy-succeeded", false)
			markJobComplete(ctx, namespace, "legacy-succeeded")

			desired := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "legacy-succeeded",
					Namespace:   namespace,
					Labels:      orgLabels(&network, bankOrg, componentChannel),
					Annotations: succeededJobCleanupAnnotations(resourceAnnotations(&network, bankOrg)),
				},
			}
			Expect(controllerReconciler.ensureJob(ctx, desired)).To(Succeed())

			var job batchv1.Job
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "legacy-succeeded"}, &job)).To(Succeed())
			Expect(job.Annotations).NotTo(HaveKey(annotationSucceededJobCleanup))

			cleanupAfter, err := controllerReconciler.cleanupSucceededJobs(ctx, &network)
			Expect(err).NotTo(HaveOccurred())
			Expect(cleanupAfter).To(Equal(time.Duration(0)))
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "legacy-succeeded"}, &job)).To(Succeed())
		})

		It("should requeue cleanup when an eligible succeeded Job is newer than the configured history TTL", func() {
			var network fabricopsv1alpha1.FabricNetwork
			Expect(k8sClient.Get(ctx, typeNamespacedName, &network)).To(Succeed())
			network.Spec.Global.Jobs = &fabricopsv1alpha1.JobCleanupConfig{
				SucceededHistoryTTLSeconds: int32Ptr(60),
			}

			controllerReconciler := &FabricNetworkReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			bankOrg := network.Spec.Orgs[1]
			namespace := orgNamespaceName(&network, bankOrg)
			Expect(controllerReconciler.ensureNamespace(ctx, buildOrgNamespace(&network, bankOrg))).To(Succeed())

			createCleanupTestJob(ctx, &network, bankOrg, namespace, "fresh-succeeded", true)
			markJobCompleteAt(ctx, namespace, "fresh-succeeded", metav1.NewTime(time.Now().Add(-30*time.Second)))

			cleanupAfter, err := controllerReconciler.cleanupSucceededJobs(ctx, &network)
			Expect(err).NotTo(HaveOccurred())
			Expect(cleanupAfter).To(BeNumerically(">", 0))
			Expect(cleanupAfter).To(BeNumerically("<=", 60*time.Second))

			var job batchv1.Job
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "fresh-succeeded"}, &job)).To(Succeed())
		})
	})
})

func deleteFabricNetworkIfExists(ctx context.Context, name types.NamespacedName) {
	network := &fabricopsv1alpha1.FabricNetwork{}
	err := k8sClient.Get(ctx, name, network)
	if errors.IsNotFound(err) {
		return
	}
	Expect(err).NotTo(HaveOccurred())

	if len(network.Finalizers) > 0 {
		network.Finalizers = nil
		Expect(k8sClient.Update(ctx, network)).To(Succeed())
	}

	Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, network))).To(Succeed())
	Eventually(func() bool {
		err := k8sClient.Get(ctx, name, network)
		return errors.IsNotFound(err)
	}).Should(BeTrue())
}

func cleanupOrgNamespaceResources(ctx context.Context, namespace string) {
	deleteAllJobs(ctx, namespace)
	deleteAllNetworkPolicies(ctx, namespace)
	deleteAllConfigMaps(ctx, namespace)
	deleteAllDeployments(ctx, namespace)
	deleteAllPersistentVolumeClaims(ctx, namespace)
	deleteAllServices(ctx, namespace)
	deleteAllSecrets(ctx, namespace)
	deleteAllRoleBindings(ctx, namespace)
	deleteAllRoles(ctx, namespace)
	deleteAllServiceAccounts(ctx, namespace)
}

func deleteAllJobs(ctx context.Context, namespace string) {
	var jobs batchv1.JobList
	err := k8sClient.List(ctx, &jobs, client.InNamespace(namespace))
	if errors.IsNotFound(err) {
		return
	}
	Expect(err).NotTo(HaveOccurred())

	for i := range jobs.Items {
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &jobs.Items[i]))).To(Succeed())
	}
}

func deleteAllNetworkPolicies(ctx context.Context, namespace string) {
	var networkPolicies networkingv1.NetworkPolicyList
	err := k8sClient.List(ctx, &networkPolicies, client.InNamespace(namespace))
	if errors.IsNotFound(err) {
		return
	}
	Expect(err).NotTo(HaveOccurred())

	for i := range networkPolicies.Items {
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &networkPolicies.Items[i]))).To(Succeed())
	}
}

func deleteAllConfigMaps(ctx context.Context, namespace string) {
	var configMaps corev1.ConfigMapList
	err := k8sClient.List(ctx, &configMaps, client.InNamespace(namespace))
	if errors.IsNotFound(err) {
		return
	}
	Expect(err).NotTo(HaveOccurred())

	for i := range configMaps.Items {
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &configMaps.Items[i]))).To(Succeed())
	}
}

func deleteAllDeployments(ctx context.Context, namespace string) {
	var deployments appsv1.DeploymentList
	err := k8sClient.List(ctx, &deployments, client.InNamespace(namespace))
	if errors.IsNotFound(err) {
		return
	}
	Expect(err).NotTo(HaveOccurred())

	for i := range deployments.Items {
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &deployments.Items[i]))).To(Succeed())
	}
}

func deleteAllPersistentVolumeClaims(ctx context.Context, namespace string) {
	var persistentVolumeClaims corev1.PersistentVolumeClaimList
	err := k8sClient.List(ctx, &persistentVolumeClaims, client.InNamespace(namespace))
	if errors.IsNotFound(err) {
		return
	}
	Expect(err).NotTo(HaveOccurred())

	for i := range persistentVolumeClaims.Items {
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &persistentVolumeClaims.Items[i]))).To(Succeed())
	}
}

func deleteAllServices(ctx context.Context, namespace string) {
	var services corev1.ServiceList
	err := k8sClient.List(ctx, &services, client.InNamespace(namespace))
	if errors.IsNotFound(err) {
		return
	}
	Expect(err).NotTo(HaveOccurred())

	for i := range services.Items {
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &services.Items[i]))).To(Succeed())
	}
}

func deleteAllSecrets(ctx context.Context, namespace string) {
	var secrets corev1.SecretList
	err := k8sClient.List(ctx, &secrets, client.InNamespace(namespace))
	if errors.IsNotFound(err) {
		return
	}
	Expect(err).NotTo(HaveOccurred())

	for i := range secrets.Items {
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &secrets.Items[i]))).To(Succeed())
	}
}

func deleteAllServiceAccounts(ctx context.Context, namespace string) {
	var serviceAccounts corev1.ServiceAccountList
	err := k8sClient.List(ctx, &serviceAccounts, client.InNamespace(namespace))
	if errors.IsNotFound(err) {
		return
	}
	Expect(err).NotTo(HaveOccurred())

	for i := range serviceAccounts.Items {
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &serviceAccounts.Items[i]))).To(Succeed())
	}
}

func deleteAllRoles(ctx context.Context, namespace string) {
	var roles rbacv1.RoleList
	err := k8sClient.List(ctx, &roles, client.InNamespace(namespace))
	if errors.IsNotFound(err) {
		return
	}
	Expect(err).NotTo(HaveOccurred())

	for i := range roles.Items {
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &roles.Items[i]))).To(Succeed())
	}
}

func deleteAllRoleBindings(ctx context.Context, namespace string) {
	var roleBindings rbacv1.RoleBindingList
	err := k8sClient.List(ctx, &roleBindings, client.InNamespace(namespace))
	if errors.IsNotFound(err) {
		return
	}
	Expect(err).NotTo(HaveOccurred())

	for i := range roleBindings.Items {
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &roleBindings.Items[i]))).To(Succeed())
	}
}

func markDeploymentReady(ctx context.Context, namespace, name string) {
	var deploy appsv1.Deployment
	Expect(k8sClient.Get(ctx, types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, &deploy)).To(Succeed())
	Expect(deploy.Spec.Replicas).NotTo(BeNil())

	deploy.Status.Replicas = *deploy.Spec.Replicas
	deploy.Status.ReadyReplicas = *deploy.Spec.Replicas
	deploy.Status.AvailableReplicas = *deploy.Spec.Replicas
	deploy.Status.UpdatedReplicas = *deploy.Spec.Replicas
	deploy.Status.ObservedGeneration = deploy.Generation
	Expect(k8sClient.Status().Update(ctx, &deploy)).To(Succeed())
}

func markJobFailed(ctx context.Context, namespace, name string) {
	var job batchv1.Job
	Expect(k8sClient.Get(ctx, types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, &job)).To(Succeed())

	now := metav1.Now()
	job.Status.Failed = 1
	job.Status.StartTime = &now
	job.Status.Conditions = append(job.Status.Conditions,
		batchv1.JobCondition{
			Type:               batchv1.JobFailureTarget,
			Status:             corev1.ConditionTrue,
			Reason:             "BackoffLimitExceeded",
			LastTransitionTime: now,
		},
		batchv1.JobCondition{
			Type:               batchv1.JobFailed,
			Status:             corev1.ConditionTrue,
			Reason:             "BackoffLimitExceeded",
			LastTransitionTime: now,
		},
	)
	Expect(k8sClient.Status().Update(ctx, &job)).To(Succeed())
}

func markJobComplete(ctx context.Context, namespace, name string) {
	markJobCompleteAt(ctx, namespace, name, metav1.Now())
}

func markJobCompleteAt(ctx context.Context, namespace, name string, completedAt metav1.Time) {
	var job batchv1.Job
	Expect(k8sClient.Get(ctx, types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, &job)).To(Succeed())

	job.Status.Succeeded = 1
	job.Status.StartTime = &completedAt
	job.Status.CompletionTime = &completedAt
	job.Status.Conditions = append(job.Status.Conditions,
		batchv1.JobCondition{
			Type:               batchv1.JobSuccessCriteriaMet,
			Status:             corev1.ConditionTrue,
			Reason:             "CompletionsReached",
			LastTransitionTime: completedAt,
		},
		batchv1.JobCondition{
			Type:               batchv1.JobComplete,
			Status:             corev1.ConditionTrue,
			Reason:             "Completed",
			LastTransitionTime: completedAt,
		},
	)
	Expect(k8sClient.Status().Update(ctx, &job)).To(Succeed())
}

func createCleanupTestJob(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	name string,
	eligible bool,
) {
	annotations := resourceAnnotations(net, org)
	if eligible {
		annotations = succeededJobCleanupAnnotations(annotations)
	}
	backoffLimit := int32(0)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      orgLabels(net, org, componentChannel),
			Annotations: annotations,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: orgLabels(net, org, componentChannel),
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:  "test",
							Image: "busybox",
						},
					},
				},
			},
		},
	}
	Expect(k8sClient.Create(ctx, job)).To(Succeed())
}

func createChaincodeLifecycleResultConfigMap(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	chaincode fabricopsv1alpha1.Chaincode,
	name string,
	key string,
	sequence int32,
) {
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   orgNamespaceName(net, org),
			Labels:      chaincodeLabels(net, org, chaincode.Channel, chaincode.Name),
			Annotations: resourceAnnotations(net, org),
		},
		Data: map[string]string{
			key: fmt.Sprintf(`{"sequence":%d}`, sequence),
		},
	}
	Expect(k8sClient.Create(ctx, configMap)).To(Succeed())
}

func createOrdererJoinResultConfigMap(ctx context.Context, namespace, name, channelName string) {
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: map[string]string{
			channelOrdererJoinResultKey: fmt.Sprintf("Status: 200\n{\"channels\":[{\"name\":%q,\"status\":\"active\"}]}", channelName),
		},
	}
	Expect(k8sClient.Create(ctx, configMap)).To(Succeed())
}

func createPeerJoinResultConfigMap(ctx context.Context, namespace, name, channelName string) {
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: map[string]string{
			channelPeerJoinResultKey: fmt.Sprintf("Channels peers has joined:\n%s\n", channelName),
		},
	}
	Expect(k8sClient.Create(ctx, configMap)).To(Succeed())
}

func createAnchorPeerUpdateResultConfigMap(ctx context.Context, namespace, name, channelName, mspID, host string, port int32) {
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: map[string]string{
			channelAnchorPeerResultKey: fmt.Sprintf(
				`{"channel":%q,"mspID":%q,"anchorPeers":[{"host":%q,"port":%d}]}`,
				channelName,
				mspID,
				host,
				port,
			),
		},
	}
	Expect(k8sClient.Create(ctx, configMap)).To(Succeed())
}

func createExternalOrgUpdateResultConfigMap(ctx context.Context, namespace, name, channelName, mspID string) {
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: map[string]string{
			externalOrgUpdateResultKey: fmt.Sprintf(
				`{"channel":%q,"mspID":%q,"anchorPeers":[{"host":"peer0.bankb.fabricops.io","port":7051}]}`,
				channelName,
				mspID,
			),
		},
	}
	Expect(k8sClient.Create(ctx, configMap)).To(Succeed())
}

func prepareLocalChannelReady(
	ctx context.Context,
	controllerReconciler *FabricNetworkReconciler,
	request reconcile.Request,
	key types.NamespacedName,
	channelName string,
	ordererNamespace string,
	peerNamespace string,
) fabricopsv1alpha1.FabricNetwork {
	_, err := controllerReconciler.Reconcile(ctx, request)
	Expect(err).NotTo(HaveOccurred())

	markDeploymentReady(ctx, ordererNamespace, "orderer-ca")
	markDeploymentReady(ctx, peerNamespace, "banka-ca")

	_, err = controllerReconciler.Reconcile(ctx, request)
	Expect(err).NotTo(HaveOccurred())

	var network fabricopsv1alpha1.FabricNetwork
	Expect(k8sClient.Get(ctx, key, &network)).To(Succeed())
	writeEnrolledOrgIdentitySecrets(ctx, &network, network.Spec.Orgs[0], ordererNamespace)
	writeEnrolledOrgIdentitySecrets(ctx, &network, network.Spec.Orgs[1], peerNamespace)

	_, err = controllerReconciler.Reconcile(ctx, request)
	Expect(err).NotTo(HaveOccurred())

	markDeploymentReady(ctx, ordererNamespace, "orderer0")
	markDeploymentReady(ctx, peerNamespace, "peer0")

	_, err = controllerReconciler.Reconcile(ctx, request)
	Expect(err).NotTo(HaveOccurred())

	blockConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      channelBlockConfigMapName(channelName),
			Namespace: ordererNamespace,
		},
		BinaryData: map[string][]byte{
			channelBlockFileName(channelName): []byte("block"),
		},
	}
	Expect(k8sClient.Create(ctx, blockConfigMap)).To(Succeed())

	_, err = controllerReconciler.Reconcile(ctx, request)
	Expect(err).NotTo(HaveOccurred())

	orderer := ordererInstance{
		org:       network.Spec.Orgs[0],
		group:     network.Spec.Orgs[0].Orderers[0],
		name:      "orderer0",
		namespace: ordererNamespace,
	}
	markJobComplete(ctx, ordererNamespace, channelOrdererJoinJobName(channelName, orderer))
	createOrdererJoinResultConfigMap(ctx, ordererNamespace, channelOrdererJoinResultConfigMapName(channelName, orderer), channelName)

	_, err = controllerReconciler.Reconcile(ctx, request)
	Expect(err).NotTo(HaveOccurred())

	peer := peerInstance{
		org:       network.Spec.Orgs[1],
		name:      "peer0",
		namespace: peerNamespace,
	}
	markJobComplete(ctx, peerNamespace, channelPeerJoinJobName(channelName, peer))
	createPeerJoinResultConfigMap(ctx, peerNamespace, channelPeerJoinResultConfigMapName(channelName, peer), channelName)

	_, err = controllerReconciler.Reconcile(ctx, request)
	Expect(err).NotTo(HaveOccurred())

	markJobComplete(ctx, peerNamespace, channelAnchorPeerUpdateJobName(channelName, network.Spec.Orgs[1]))
	createAnchorPeerUpdateResultConfigMap(
		ctx,
		peerNamespace,
		channelAnchorPeerUpdateResultConfigMapName(channelName, network.Spec.Orgs[1]),
		channelName,
		"BankAMSP",
		"peer0.fo-test-banka.svc.cluster.local",
		peerPort,
	)

	_, err = controllerReconciler.Reconcile(ctx, request)
	Expect(err).NotTo(HaveOccurred())

	Expect(k8sClient.Get(ctx, key, &network)).To(Succeed())
	return network
}

func writeEnrolledOrgIdentitySecrets(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
) {
	authority, err := generateTestIdentityAuthority(org)
	Expect(err).NotTo(HaveOccurred())

	adminName := adminIdentityName(org)
	adminMSP, err := buildTestMSPSecret(net, org, namespace, adminName, componentAdmin, secretKindAdminMSP, authority)
	Expect(err).NotTo(HaveOccurred())
	adminMSP.Labels[labelIdentitySource] = identitySourceFabricCA
	upsertSecret(ctx, adminMSP)

	if net.Spec.Global.TLS {
		adminTLS, err := buildTestAdminTLSSecret(net, org, namespace, adminName, authority)
		Expect(err).NotTo(HaveOccurred())
		adminTLS.Labels[labelIdentitySource] = identitySourceFabricCA
		upsertSecret(ctx, adminTLS)
	}

	for _, group := range org.Orderers {
		for i := 0; i < group.Instances; i++ {
			name := sanitizeName(fmt.Sprintf("%s%d", group.Prefix, i))
			writeEnrolledWorkloadIdentitySecrets(ctx, net, org, namespace, name, componentOrderer, authority)
		}
	}

	if org.Peer == nil {
		return
	}

	for i := 0; i < org.Peer.Instances; i++ {
		name := sanitizeName(fmt.Sprintf("%s%d", org.Peer.Prefix, i))
		writeEnrolledWorkloadIdentitySecrets(ctx, net, org, namespace, name, componentPeer, authority)
	}
}

func writeEnrolledWorkloadIdentitySecrets(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	workloadName string,
	component string,
	authority *testIdentityAuthority,
) {
	msp, err := buildTestMSPSecret(net, org, namespace, workloadName, component, secretKindMSP, authority)
	Expect(err).NotTo(HaveOccurred())
	msp.Labels[labelIdentitySource] = identitySourceFabricCA
	upsertSecret(ctx, msp)

	if !net.Spec.Global.TLS {
		return
	}

	tls, err := buildTestWorkloadTLSSecret(net, org, namespace, workloadName, component, workloadDNSNames(workloadName, namespace), authority)
	Expect(err).NotTo(HaveOccurred())
	tls.Labels[labelIdentitySource] = identitySourceFabricCA
	upsertSecret(ctx, tls)
}

type testIdentityAuthority struct {
	mspCACertPEM []byte
	mspCAKey     *ecdsa.PrivateKey
	tlsCACertPEM []byte
	tlsCAKey     *ecdsa.PrivateKey
}

func generateTestIdentityAuthority(org fabricopsv1alpha1.Org) (*testIdentityAuthority, error) {
	mspCert, mspKey, err := generateTestCA("ca."+org.Organization.Domain, org.Organization.MSPName)
	if err != nil {
		return nil, err
	}

	tlsCert, tlsKey, err := generateTestCA("tlsca."+org.Organization.Domain, org.Organization.MSPName)
	if err != nil {
		return nil, err
	}

	return &testIdentityAuthority{
		mspCACertPEM: mspCert,
		mspCAKey:     mspKey,
		tlsCACertPEM: tlsCert,
		tlsCAKey:     tlsKey,
	}, nil
}

func buildTestMSPSecret(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	workloadName string,
	component string,
	kind string,
	authority *testIdentityAuthority,
) (*corev1.Secret, error) {
	certPEM, keyPEM, err := issueTestCertificate(
		workloadName,
		component,
		nil,
		authority.mspCACertPEM,
		authority.mspCAKey,
		x509.KeyUsageDigitalSignature,
		nil,
	)
	if err != nil {
		return nil, err
	}

	data := map[string][]byte{
		mspConfigKey:   []byte(mspConfigYAML()),
		mspCACertKey:   authority.mspCACertPEM,
		mspSignCertKey: certPEM,
		mspKeyStoreKey: keyPEM,
	}
	if net.Spec.Global.TLS {
		data[mspTLSCACertKey] = authority.tlsCACertPEM
	}

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      identitySecretName(workloadName, secretKindMSP),
			Namespace: namespace,
			Labels: identityLabels(net, org, component, workloadName, map[string]string{
				labelIdentityKind: kind,
				labelWorkload:     workloadName,
			}),
		},
		Type: corev1.SecretTypeOpaque,
		Data: data,
	}, nil
}

func buildTestAdminTLSSecret(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	adminName string,
	authority *testIdentityAuthority,
) (*corev1.Secret, error) {
	certPEM, keyPEM, err := issueTestCertificate(
		adminName,
		componentAdmin,
		nil,
		authority.tlsCACertPEM,
		authority.tlsCAKey,
		x509.KeyUsageDigitalSignature,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	)
	if err != nil {
		return nil, err
	}

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      identitySecretName(adminName, secretKindTLS),
			Namespace: namespace,
			Labels: identityLabels(net, org, componentAdmin, adminName, map[string]string{
				labelIdentityKind: secretKindAdminTLS,
				labelWorkload:     adminName,
			}),
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			tlsCACertKey:     authority.tlsCACertPEM,
			tlsClientCertKey: certPEM,
			tlsClientKeyKey:  keyPEM,
		},
	}, nil
}

func buildTestWorkloadTLSSecret(
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
	namespace string,
	workloadName string,
	component string,
	dnsNames []string,
	authority *testIdentityAuthority,
) (*corev1.Secret, error) {
	certPEM, keyPEM, err := issueTestCertificate(
		workloadName,
		component,
		dnsNames,
		authority.tlsCACertPEM,
		authority.tlsCAKey,
		x509.KeyUsageDigitalSignature|x509.KeyUsageKeyEncipherment,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	)
	if err != nil {
		return nil, err
	}

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      identitySecretName(workloadName, secretKindTLS),
			Namespace: namespace,
			Labels: identityLabels(net, org, component, workloadName, map[string]string{
				labelIdentityKind: secretKindTLS,
				labelWorkload:     workloadName,
			}),
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			tlsCACertKey:     authority.tlsCACertPEM,
			tlsServerCertKey: certPEM,
			tlsServerKeyKey:  keyPEM,
		},
	}, nil
}

func generateTestCA(commonName, organization string) ([]byte, *ecdsa.PrivateKey, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	serial, err := randomTestSerial()
	if err != nil {
		return nil, nil, err
	}

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   commonName,
			Organization: []string{organization},
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}

	return pemEncodeTestCertificate(certDER), key, nil
}

func issueTestCertificate(
	commonName string,
	organizationalUnit string,
	dnsNames []string,
	caCertPEM []byte,
	caKey *ecdsa.PrivateKey,
	keyUsage x509.KeyUsage,
	usages []x509.ExtKeyUsage,
) ([]byte, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	caCert, err := parsePEMCertificate(caCertPEM)
	if err != nil {
		return nil, nil, err
	}

	serial, err := randomTestSerial()
	if err != nil {
		return nil, nil, err
	}

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:         commonName,
			OrganizationalUnit: []string{organizationalUnit},
		},
		DNSNames:              dnsNames,
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().AddDate(1, 0, 0),
		KeyUsage:              keyUsage,
		ExtKeyUsage:           usages,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}

	return pemEncodeTestCertificate(certDER), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), nil
}

func randomTestSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	return rand.Int(rand.Reader, limit)
}

func pemEncodeTestCertificate(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func upsertSecret(ctx context.Context, desired *corev1.Secret) {
	var existing corev1.Secret
	err := k8sClient.Get(ctx, types.NamespacedName{
		Namespace: desired.Namespace,
		Name:      desired.Name,
	}, &existing)
	if errors.IsNotFound(err) {
		Expect(k8sClient.Create(ctx, desired)).To(Succeed())
		return
	}
	Expect(err).NotTo(HaveOccurred())

	existing.Labels = desired.Labels
	existing.Type = desired.Type
	existing.Data = desired.Data
	Expect(k8sClient.Update(ctx, &existing)).To(Succeed())
}

func envMap(container corev1.Container) map[string]string {
	env := map[string]string{}
	for _, item := range container.Env {
		env[item.Name] = item.Value
	}
	return env
}

func envSecretRefs(container corev1.Container) map[string]string {
	env := map[string]string{}
	for _, item := range container.Env {
		if item.ValueFrom == nil || item.ValueFrom.SecretKeyRef == nil {
			continue
		}
		env[item.Name] = item.ValueFrom.SecretKeyRef.Name + "/" + item.ValueFrom.SecretKeyRef.Key
	}
	return env
}

func secretVolumeNames(podSpec corev1.PodSpec) map[string]string {
	secrets := map[string]string{}
	for _, volume := range podSpec.Volumes {
		if volume.Secret == nil {
			continue
		}
		secrets[volume.Name] = volume.Secret.SecretName
	}
	return secrets
}

func secretVolumeItems(podSpec corev1.PodSpec, volumeName string) []corev1.KeyToPath {
	for _, volume := range podSpec.Volumes {
		if volume.Name == volumeName && volume.Secret != nil {
			return volume.Secret.Items
		}
	}
	return nil
}

func configMapVolumeNames(podSpec corev1.PodSpec) map[string]string {
	configMaps := map[string]string{}
	for _, volume := range podSpec.Volumes {
		if volume.ConfigMap == nil {
			continue
		}
		configMaps[volume.Name] = volume.ConfigMap.Name
	}
	return configMaps
}

func configMapVolumeItems(podSpec corev1.PodSpec, volumeName string) []corev1.KeyToPath {
	for _, volume := range podSpec.Volumes {
		if volume.Name == volumeName && volume.ConfigMap != nil {
			return volume.ConfigMap.Items
		}
	}
	return nil
}

func pvcVolumeNames(podSpec corev1.PodSpec) map[string]string {
	persistentVolumeClaims := map[string]string{}
	for _, volume := range podSpec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}
		persistentVolumeClaims[volume.Name] = volume.PersistentVolumeClaim.ClaimName
	}
	return persistentVolumeClaims
}

func secretVolumeItemKeys(podSpec corev1.PodSpec, volumeName string) []string {
	for _, volume := range podSpec.Volumes {
		if volume.Name != volumeName || volume.Secret == nil {
			continue
		}

		keys := make([]string, 0, len(volume.Secret.Items))
		for _, item := range volume.Secret.Items {
			keys = append(keys, item.Key)
		}
		return keys
	}
	return nil
}

func volumeMountPaths(container corev1.Container) map[string]string {
	mounts := map[string]string{}
	for _, mount := range container.VolumeMounts {
		mounts[mount.Name] = mount.MountPath
	}
	return mounts
}

func containerPorts(container corev1.Container) []int32 {
	ports := make([]int32, 0, len(container.Ports))
	for _, port := range container.Ports {
		ports = append(ports, port.ContainerPort)
	}
	return ports
}

func expectTCPProbe(probe *corev1.Probe, port int32) {
	Expect(probe).NotTo(BeNil())
	Expect(probe.TCPSocket).NotTo(BeNil())
	Expect(probe.TCPSocket.Port.IntVal).To(Equal(port))
}

func expectOperationsProbe(probe *corev1.Probe) {
	Expect(probe).NotTo(BeNil())
	Expect(probe.HTTPGet).NotTo(BeNil())
	Expect(probe.HTTPGet.Path).To(Equal("/healthz"))
	Expect(probe.HTTPGet.Port.StrVal).To(Equal(endpointOperations))
	Expect(probe.HTTPGet.Scheme).To(Equal(corev1.URISchemeHTTP))
}

func expectContainerResources(container corev1.Container, requestCPU, requestMemory, limitCPU, limitMemory string) {
	requestedCPU := container.Resources.Requests[corev1.ResourceCPU]
	requestedMemory := container.Resources.Requests[corev1.ResourceMemory]
	limitedCPU := container.Resources.Limits[corev1.ResourceCPU]
	limitedMemory := container.Resources.Limits[corev1.ResourceMemory]

	Expect(requestedCPU.Cmp(resource.MustParse(requestCPU))).To(Equal(0))
	Expect(requestedMemory.Cmp(resource.MustParse(requestMemory))).To(Equal(0))
	Expect(limitedCPU.Cmp(resource.MustParse(limitCPU))).To(Equal(0))
	Expect(limitedMemory.Cmp(resource.MustParse(limitMemory))).To(Equal(0))
}

func readTarGz(contents []byte) map[string][]byte {
	GinkgoHelper()

	gzipReader, err := gzip.NewReader(bytes.NewReader(contents))
	Expect(err).NotTo(HaveOccurred())
	defer func() {
		Expect(gzipReader.Close()).To(Succeed())
	}()

	tarReader := tar.NewReader(gzipReader)
	files := map[string][]byte{}
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		Expect(err).NotTo(HaveOccurred())
		if header.Typeflag == tar.TypeDir {
			continue
		}
		data, err := io.ReadAll(tarReader)
		Expect(err).NotTo(HaveOccurred())
		files[header.Name] = data
	}

	return files
}

func expectPersistentVolumeClaim(ctx context.Context, namespace, name, size, storageClassName string) {
	var persistentVolumeClaim corev1.PersistentVolumeClaim
	Expect(k8sClient.Get(ctx, types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, &persistentVolumeClaim)).To(Succeed())

	Expect(persistentVolumeClaim.Spec.AccessModes).To(ContainElement(corev1.ReadWriteOnce))
	request := persistentVolumeClaim.Spec.Resources.Requests[corev1.ResourceStorage]
	Expect(request.Cmp(resource.MustParse(size))).To(Equal(0))
	Expect(persistentVolumeClaim.Spec.StorageClassName).NotTo(BeNil())
	Expect(*persistentVolumeClaim.Spec.StorageClassName).To(Equal(storageClassName))
	Expect(persistentVolumeClaim.Labels[labelAppManagedBy]).To(Equal(managedByValue))
	Expect(persistentVolumeClaim.Annotations[annotationManagedBy]).To(Equal(controllerName))
}

func expectIdentitySecret(ctx context.Context, namespace, name, kind string) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
	}
	Expect(k8sClient.Get(ctx, types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, secret)).To(Succeed())

	Expect(identitySecretValidationError(*secret, kind, true)).To(BeEmpty())
	Expect(secret.Labels[labelIdentityKind]).To(Equal(kind))
}

func expectSecretNotFound(ctx context.Context, namespace, name string) {
	var secret corev1.Secret
	err := k8sClient.Get(ctx, types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, &secret)
	Expect(errors.IsNotFound(err)).To(BeTrue())
}

func expectDeploymentNotFound(ctx context.Context, namespace, name string) {
	var deploy appsv1.Deployment
	err := k8sClient.Get(ctx, types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, &deploy)
	Expect(errors.IsNotFound(err)).To(BeTrue())
}

func expectServiceNotFound(ctx context.Context, namespace, name string) {
	var service corev1.Service
	err := k8sClient.Get(ctx, types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, &service)
	Expect(errors.IsNotFound(err)).To(BeTrue())
}

func servicePorts(service corev1.Service) []int32 {
	ports := make([]int32, 0, len(service.Spec.Ports))
	for _, port := range service.Spec.Ports {
		ports = append(ports, port.Port)
	}
	return ports
}

func stringPtr(value string) *string {
	return &value
}

func int32Ptr(value int32) *int32 {
	return &value
}

func int64Ptr(value int64) *int64 {
	return &value
}

func boolPtr(value bool) *bool {
	return &value
}
