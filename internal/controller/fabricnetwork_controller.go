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
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

// FabricNetworkReconciler reconciles a FabricNetwork object
type FabricNetworkReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

const (
	conditionReady                     = "Ready"
	conditionIdentityMaterialReady     = "IdentityMaterialReady"
	conditionCertificateLifecycleReady = "CertificateLifecycleReady"
	conditionChannelsReady             = "ChannelsReady"
	conditionObservabilityReady        = "ObservabilityReady"
	fabricNetworkFinalizer             = "fabricops.io/finalizer"
)

func orgNamespaceName(net *fabricopsv1alpha1.FabricNetwork, org fabricopsv1alpha1.Org) string {
	return sanitizeName("fo-" + networkNamespaceSlug(net) + "-" + org.Organization.Name)
}

func buildOrgNamespace(net *fabricopsv1alpha1.FabricNetwork, org fabricopsv1alpha1.Org) *corev1.Namespace {
	name := orgNamespaceName(net, org)

	return &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Labels:      orgLabels(net, org, "namespace"),
			Annotations: resourceAnnotations(net, org),
		},
	}
}

func (r *FabricNetworkReconciler) ensureNamespace(ctx context.Context, desired *corev1.Namespace) error {
	var existing corev1.Namespace

	err := r.Get(ctx, client.ObjectKey{Name: desired.Name}, &existing)
	if err == nil {
		return r.updateObjectWithRetry(ctx, desired, func(object client.Object) (bool, error) {
			existing := object.(*corev1.Namespace)
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
			log.Info("Updating Namespace metadata", "namespace", desired.Name)
			return true, nil
		})
	}

	if !apierrors.IsNotFound(err) {
		return err
	}

	log := logf.FromContext(ctx)
	log.Info("Creating Namespace", "namespace", desired.Name)
	return r.Create(ctx, desired)
}

func (r *FabricNetworkReconciler) cleanupFabricNetwork(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
) error {
	log := logf.FromContext(ctx)

	for _, org := range net.Spec.Orgs {
		namespaceName := orgNamespaceName(net, org)
		var namespace corev1.Namespace
		err := r.Get(ctx, client.ObjectKey{Name: namespaceName}, &namespace)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return err
		}

		if !namespaceOwnedByFabricNetwork(namespace, net, org) {
			log.Info("Skipping Namespace cleanup because labels do not match", "namespace", namespaceName)
			continue
		}

		log.Info("Deleting Namespace", "namespace", namespaceName)
		if err := r.Delete(ctx, &namespace); client.IgnoreNotFound(err) != nil {
			return err
		}
	}

	return nil
}

func (r *FabricNetworkReconciler) updateFabricNetworkFinalizer(
	ctx context.Context,
	key types.NamespacedName,
	update func(*fabricopsv1alpha1.FabricNetwork) bool,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var network fabricopsv1alpha1.FabricNetwork
		if err := r.Get(ctx, key, &network); err != nil {
			return err
		}
		if !update(&network) {
			return nil
		}

		return r.Update(ctx, &network)
	})
}

func (r *FabricNetworkReconciler) ensureFabricNetworkFinalizer(
	ctx context.Context,
	key types.NamespacedName,
) error {
	return r.updateFabricNetworkFinalizer(ctx, key, func(network *fabricopsv1alpha1.FabricNetwork) bool {
		if !network.DeletionTimestamp.IsZero() {
			return false
		}
		if controllerutil.ContainsFinalizer(network, fabricNetworkFinalizer) {
			return false
		}

		log := logf.FromContext(ctx)
		log.Info("Adding FabricNetwork finalizer", "name", key.Name, "namespace", key.Namespace)
		controllerutil.AddFinalizer(network, fabricNetworkFinalizer)
		return true
	})
}

func (r *FabricNetworkReconciler) removeFabricNetworkFinalizer(
	ctx context.Context,
	key types.NamespacedName,
) error {
	return r.updateFabricNetworkFinalizer(ctx, key, func(network *fabricopsv1alpha1.FabricNetwork) bool {
		if !controllerutil.ContainsFinalizer(network, fabricNetworkFinalizer) {
			return false
		}

		log := logf.FromContext(ctx)
		log.Info("Removing FabricNetwork finalizer", "name", key.Name, "namespace", key.Namespace)
		controllerutil.RemoveFinalizer(network, fabricNetworkFinalizer)
		return true
	})
}

func (r *FabricNetworkReconciler) updateObjectWithRetry(
	ctx context.Context,
	desired client.Object,
	mutate func(client.Object) (bool, error),
) error {
	key := client.ObjectKeyFromObject(desired)

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		existing, ok := desired.DeepCopyObject().(client.Object)
		if !ok {
			return nil
		}
		if err := r.Get(ctx, key, existing); err != nil {
			return err
		}
		if err := ensureSameFabricNetworkOwner(existing, desired); err != nil {
			return err
		}

		changed, err := mutate(existing)
		if err != nil || !changed {
			return err
		}

		return r.Update(ctx, existing)
	})
}

type fabricNetworkOwner struct {
	name      string
	namespace string
}

func ensureSameFabricNetworkOwner(existing client.Object, desired client.Object) error {
	desiredAnnotations := desired.GetAnnotations()
	desiredAnnotationOwner := fabricNetworkOwner{
		name:      desiredAnnotations[annotationFabricNetwork],
		namespace: desiredAnnotations[annotationFabricNetworkNamespace],
	}
	if existingAnnotationOwner, ok := fabricNetworkOwnerFromAnnotations(existing); ok {
		return ensureMatchingFabricNetworkOwner(existing, existingAnnotationOwner, desiredAnnotationOwner)
	}

	desiredLabels := desired.GetLabels()
	desiredLabelOwner := fabricNetworkOwner{
		name:      desiredLabels[labelFabricNetwork],
		namespace: desiredLabels[labelFabricNetworkNamespace],
	}
	if existingLabelOwner, ok := fabricNetworkOwnerFromLabels(existing); ok {
		return ensureMatchingFabricNetworkOwner(existing, existingLabelOwner, desiredLabelOwner)
	}

	return nil
}

func fabricNetworkOwnerFromAnnotations(object client.Object) (fabricNetworkOwner, bool) {
	annotations := object.GetAnnotations()
	owner := fabricNetworkOwner{
		name:      annotations[annotationFabricNetwork],
		namespace: annotations[annotationFabricNetworkNamespace],
	}

	return owner, owner.name != "" || owner.namespace != ""
}

func fabricNetworkOwnerFromLabels(object client.Object) (fabricNetworkOwner, bool) {
	labels := object.GetLabels()
	owner := fabricNetworkOwner{
		name:      labels[labelFabricNetwork],
		namespace: labels[labelFabricNetworkNamespace],
	}

	return owner, owner.name != "" || owner.namespace != ""
}

func ensureMatchingFabricNetworkOwner(
	object client.Object,
	existing fabricNetworkOwner,
	desired fabricNetworkOwner,
) error {
	if existing.name == desired.name && existing.namespace == desired.namespace {
		return nil
	}

	return fmt.Errorf(
		"%s is owned by FabricNetwork %s/%s, refusing to update for FabricNetwork %s/%s",
		objectDescription(object),
		existing.namespace,
		existing.name,
		desired.namespace,
		desired.name,
	)
}

func objectDescription(object client.Object) string {
	kind := object.GetObjectKind().GroupVersionKind().Kind
	if kind == "" {
		objectType := reflect.TypeOf(object)
		if objectType.Kind() == reflect.Pointer {
			objectType = objectType.Elem()
		}
		kind = objectType.Name()
	}

	if object.GetNamespace() == "" {
		return kind + " " + object.GetName()
	}

	return kind + " " + object.GetNamespace() + "/" + object.GetName()
}

func namespaceOwnedByFabricNetwork(
	namespace corev1.Namespace,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
) bool {
	return namespace.Labels[labelFabricNetwork] == sanitizeName(net.Name) &&
		namespace.Labels[labelFabricNetworkNamespace] == sanitizeName(net.Namespace) &&
		namespace.Labels[labelOrg] == sanitizeName(org.Organization.Name) &&
		namespace.Labels[labelAppManagedBy] == managedByValue
}

func (r *FabricNetworkReconciler) reconcileOrg(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	org fabricopsv1alpha1.Org,
) (fabricopsv1alpha1.OrgStatus, error) {
	log := logf.FromContext(ctx)
	namespace := orgNamespaceName(net, org)

	log.Info("Reconciling org",
		"name", org.Organization.Name,
		"domain", org.Organization.Domain,
		"namespace", namespace,
	)

	status := fabricopsv1alpha1.OrgStatus{
		Name:      org.Organization.Name,
		Namespace: namespace,
	}
	status.CAEndpoint, status.OrdererEndpoints, status.PeerEndpoints = orgEndpointStatus(org, namespace)

	if err := r.ensureNamespace(ctx, buildOrgNamespace(net, org)); err != nil {
		return status, err
	}

	if networkPolicyEnabled(net) {
		if err := r.ensureNetworkPolicy(ctx, buildOrgNetworkPolicy(net, org, namespace)); err != nil {
			return status, err
		}
	} else {
		if err := r.ensureNetworkPolicyAbsent(ctx, buildOrgNetworkPolicy(net, org, namespace)); err != nil {
			return status, err
		}
	}

	if serviceMonitorEnabled(net) {
		if err := r.ensureServiceMonitor(ctx, buildOrgServiceMonitor(net, org, namespace)); err != nil {
			return status, err
		}
	}

	if err := r.reconcileIdentityMaterial(ctx, net, org, namespace); err != nil {
		return status, err
	}

	caReady, err := r.reconcileCA(ctx, net, org, namespace)
	if err != nil {
		return status, err
	}
	status.CAReady = caReady

	registrarRotation, err := r.reconcileCARegistrarRotation(ctx, net, org, namespace, status.CAReady)
	if err != nil {
		return status, err
	}
	status.CARegistrarRotation = registrarRotation
	registrarRotationBlocksWork := caRegistrarRotationBlocksWork(registrarRotation)

	if caReady && !registrarRotationBlocksWork {
		if err := r.reconcileAdminEnrollment(ctx, net, org, namespace); err != nil {
			return status, err
		}
		if err := r.reconcileWorkloadEnrollments(ctx, net, org, namespace); err != nil {
			return status, err
		}
	}

	identityReady, identityError, err := r.identityMaterialStatus(ctx, net, org, namespace)
	if err != nil {
		return status, err
	}
	status.IdentityReady = identityReady
	status.IdentityError = identityError

	certificates, renewalRequired, renewalError, err := r.reconcileCertificateLifecycle(
		ctx,
		net,
		org,
		namespace,
		status.CAReady,
		status.IdentityReady,
		registrarRotationBlocksWork,
	)
	if err != nil {
		return status, err
	}
	status.Certificates = certificates
	status.CertificateRenewalRequired = renewalRequired
	status.CertificateRenewalError = renewalError

	if !status.IdentityReady {
		status.Orderers = desiredOrdererStatus(org)
		status.OrderersReady = workloadReady(status.Orderers)
		status.Peers = desiredPeerStatus(org)
		status.PeersReady = workloadReady(status.Peers)
		status.Ready = false
		return status, nil
	}

	orderers, err := r.reconcileOrderers(ctx, net, org, namespace)
	if err != nil {
		return status, err
	}
	status.Orderers = orderers
	status.OrderersReady = workloadReady(orderers)

	peers, err := r.reconcilePeers(ctx, net, org, namespace)
	if err != nil {
		return status, err
	}
	status.Peers = peers
	status.PeersReady = workloadReady(peers)
	status.Ready = status.IdentityReady && status.CAReady && status.OrderersReady && status.PeersReady

	return status, nil
}

func (r *FabricNetworkReconciler) reconcileOrgs(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
) ([]fabricopsv1alpha1.OrgStatus, error) {
	orgStatuses := make([]fabricopsv1alpha1.OrgStatus, 0, len(net.Spec.Orgs))

	for _, org := range net.Spec.Orgs {
		status, err := r.reconcileOrg(ctx, net, org)
		if err != nil {
			return orgStatuses, err
		}
		orgStatuses = append(orgStatuses, status)
	}

	return orgStatuses, nil
}

func orgStatusesEqual(a, b []fabricopsv1alpha1.OrgStatus) bool {
	return reflect.DeepEqual(a, b)
}

func workloadReady(status fabricopsv1alpha1.WorkloadStatus) bool {
	return status.Ready >= status.Desired
}

func desiredOrdererStatus(org fabricopsv1alpha1.Org) fabricopsv1alpha1.WorkloadStatus {
	status := fabricopsv1alpha1.WorkloadStatus{}
	for _, group := range org.Orderers {
		status.Desired += int32(group.Instances)
	}
	return status
}

func desiredPeerStatus(org fabricopsv1alpha1.Org) fabricopsv1alpha1.WorkloadStatus {
	if org.Peer == nil {
		return fabricopsv1alpha1.WorkloadStatus{}
	}

	return fabricopsv1alpha1.WorkloadStatus{
		Desired: int32(org.Peer.Instances),
	}
}

func allOrgsReady(statuses []fabricopsv1alpha1.OrgStatus) bool {
	if len(statuses) == 0 {
		return false
	}

	for _, status := range statuses {
		if !status.Ready {
			return false
		}
	}

	return true
}

func identityMaterialReady(statuses []fabricopsv1alpha1.OrgStatus) bool {
	for _, status := range statuses {
		if !status.IdentityReady {
			return false
		}
	}

	return true
}

func identityMaterialMessage(statuses []fabricopsv1alpha1.OrgStatus) string {
	messages := []string{}
	for _, status := range statuses {
		if status.IdentityError != "" {
			messages = append(messages, status.Name+": "+status.IdentityError)
		}
	}

	if len(messages) == 0 {
		return "All required identity material is present"
	}

	return strings.Join(messages, "; ")
}

func readyCondition(
	net *fabricopsv1alpha1.FabricNetwork,
	status metav1.ConditionStatus,
	reason string,
	message string,
) []metav1.Condition {
	conditions := append([]metav1.Condition(nil), net.Status.Conditions...)
	apiMeta.SetStatusCondition(&conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: net.Generation,
	})

	return conditions
}

func identityMaterialCondition(
	net *fabricopsv1alpha1.FabricNetwork,
	conditions []metav1.Condition,
	status metav1.ConditionStatus,
	reason string,
	message string,
) []metav1.Condition {
	conditions = append([]metav1.Condition(nil), conditions...)
	apiMeta.SetStatusCondition(&conditions, metav1.Condition{
		Type:               conditionIdentityMaterialReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: net.Generation,
	})

	return conditions
}

func certificateLifecycleCondition(
	net *fabricopsv1alpha1.FabricNetwork,
	conditions []metav1.Condition,
	status metav1.ConditionStatus,
	reason string,
	message string,
) []metav1.Condition {
	conditions = append([]metav1.Condition(nil), conditions...)
	apiMeta.SetStatusCondition(&conditions, metav1.Condition{
		Type:               conditionCertificateLifecycleReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: net.Generation,
	})

	return conditions
}

func channelsReadyCondition(
	net *fabricopsv1alpha1.FabricNetwork,
	conditions []metav1.Condition,
	status metav1.ConditionStatus,
	reason string,
	message string,
) []metav1.Condition {
	conditions = append([]metav1.Condition(nil), conditions...)
	apiMeta.SetStatusCondition(&conditions, metav1.Condition{
		Type:               conditionChannelsReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: net.Generation,
	})

	return conditions
}

func observabilityReady(statuses []fabricopsv1alpha1.OrgStatus) bool {
	return allOrgsReady(statuses)
}

func observabilityStatusMessage(statuses []fabricopsv1alpha1.OrgStatus) string {
	if observabilityReady(statuses) {
		return "All Fabric operations endpoints are exposed"
	}

	messages := []string{}
	for _, status := range statuses {
		orgMessages := []string{}
		if !status.CAReady {
			orgMessages = append(orgMessages, "CA not ready")
		}
		if !status.OrderersReady {
			orgMessages = append(orgMessages, "orderers not ready")
		}
		if !status.PeersReady {
			orgMessages = append(orgMessages, "peers not ready")
		}
		if len(orgMessages) > 0 {
			messages = append(messages, status.Name+": "+strings.Join(orgMessages, ", "))
		}
	}

	if len(messages) == 0 {
		return "Waiting for Fabric operations endpoints"
	}

	return strings.Join(messages, "; ")
}

func observabilityReadyCondition(
	net *fabricopsv1alpha1.FabricNetwork,
	conditions []metav1.Condition,
	status metav1.ConditionStatus,
	reason string,
	message string,
) []metav1.Condition {
	conditions = append([]metav1.Condition(nil), conditions...)
	apiMeta.SetStatusCondition(&conditions, metav1.Condition{
		Type:               conditionObservabilityReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: net.Generation,
	})

	return conditions
}

func topologyInvalidConditions(net *fabricopsv1alpha1.FabricNetwork, message string) []metav1.Condition {
	return observabilityReadyCondition(
		net,
		channelsReadyCondition(
			net,
			certificateLifecycleCondition(
				net,
				identityMaterialCondition(
					net,
					readyCondition(net, metav1.ConditionFalse, "TopologyInvalid", message),
					metav1.ConditionUnknown,
					"TopologyInvalid",
					"Topology validation failed before identity material check: "+message,
				),
				metav1.ConditionUnknown,
				"TopologyInvalid",
				"Topology validation failed before certificate lifecycle check: "+message,
			),
			metav1.ConditionUnknown,
			"TopologyInvalid",
			message,
		),
		metav1.ConditionUnknown,
		"TopologyInvalid",
		"Topology validation failed before observability check: "+message,
	)
}

func (r *FabricNetworkReconciler) updateStatus(
	ctx context.Context,
	net *fabricopsv1alpha1.FabricNetwork,
	newPhase fabricopsv1alpha1.Phase,
	newMessage string,
	orgStatus []fabricopsv1alpha1.OrgStatus,
	channelStatus []fabricopsv1alpha1.ChannelStatus,
	chaincodeStatus []fabricopsv1alpha1.ChaincodeStatus,
	conditions []metav1.Condition,
) error {
	if net.Status.Phase == newPhase &&
		net.Status.Message == newMessage &&
		orgStatusesEqual(net.Status.OrgStatus, orgStatus) &&
		channelStatusesEqual(net.Status.ChannelStatus, channelStatus) &&
		chaincodeStatusesEqual(net.Status.ChaincodeStatus, chaincodeStatus) &&
		reflect.DeepEqual(net.Status.Conditions, conditions) {
		return nil
	}

	base := net.DeepCopy()
	base.Status.Phase = newPhase
	base.Status.Message = newMessage
	base.Status.OrgStatus = orgStatus
	base.Status.ChannelStatus = channelStatus
	base.Status.ChaincodeStatus = chaincodeStatus
	base.Status.Conditions = conditions

	log := logf.FromContext(ctx)

	log.Info(
		"Updating status",
		"oldPhase", net.Status.Phase,
		"newPhase", newPhase,
		"oldMessage", net.Status.Message,
		"newMessage", newMessage,
	)

	if err := r.Status().Patch(ctx, base, client.MergeFrom(net)); err != nil {
		return err
	}

	if r.Recorder != nil && (net.Status.Phase != newPhase || net.Status.Message != newMessage) {
		switch newPhase {
		case fabricopsv1alpha1.PhaseReady:
			r.Recorder.Event(base, corev1.EventTypeNormal, "FabricNetworkReady", newMessage)
		case fabricopsv1alpha1.PhaseFailed:
			r.Recorder.Event(base, corev1.EventTypeWarning, "FabricNetworkReconcileFailed", newMessage)
		}
	}

	return nil
}

// +kubebuilder:rbac:groups=fabricops.io,resources=fabricnetworks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=fabricops.io,resources=fabricnetworks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=fabricops.io,resources=fabricnetworks/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=servicemonitors,verbs=get;list;watch;create;update;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *FabricNetworkReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	log.Info(
		"Reconciling FabricNetwork",
		"name", req.Name,
		"namespace", req.Namespace,
	)

	var network fabricopsv1alpha1.FabricNetwork

	if err := r.Get(ctx, req.NamespacedName, &network); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !network.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&network, fabricNetworkFinalizer) {
			if err := r.cleanupFabricNetwork(ctx, &network); err != nil {
				return ctrl.Result{}, err
			}
			if err := r.removeFabricNetworkFinalizer(ctx, req.NamespacedName); err != nil {
				return ctrl.Result{}, err
			}
		}

		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&network, fabricNetworkFinalizer) {
		if err := r.ensureFabricNetworkFinalizer(ctx, req.NamespacedName); err != nil {
			return ctrl.Result{}, err
		}
	}

	if problems := validateFabricNetworkTopology(&network); len(problems) > 0 {
		message := "Invalid Fabric topology: " + strings.Join(problems, "; ")
		log.Info("FabricNetwork topology is invalid", "problems", problems)
		if err := r.updateStatus(
			ctx,
			&network,
			fabricopsv1alpha1.PhaseFailed,
			message,
			nil,
			nil,
			nil,
			topologyInvalidConditions(&network, message),
		); err != nil {
			return ctrl.Result{}, err
		}

		return ctrl.Result{}, nil
	}

	orgStatuses, err := r.reconcileOrgs(ctx, &network)
	if err != nil {
		log.Error(err, "Failed to reconcile orgs")

		_ = r.updateStatus(
			ctx,
			&network,
			fabricopsv1alpha1.PhaseFailed,
			"Failed to reconcile orgs: "+err.Error(),
			orgStatuses,
			nil,
			nil,
			observabilityReadyCondition(
				&network,
				channelsReadyCondition(
					&network,
					certificateLifecycleCondition(
						&network,
						identityMaterialCondition(
							&network,
							readyCondition(&network, metav1.ConditionFalse, "ReconcileError", "Failed to reconcile orgs: "+err.Error()),
							metav1.ConditionUnknown,
							"ReconcileError",
							"Failed to check identity material: "+err.Error(),
						),
						metav1.ConditionUnknown,
						"ReconcileError",
						"Failed to check certificate lifecycle: "+err.Error(),
					),
					metav1.ConditionUnknown,
					"ReconcileError",
					"Failed to check channels: "+err.Error(),
				),
				metav1.ConditionUnknown,
				"ReconcileError",
				"Failed to check observability: "+err.Error(),
			),
		)

		return ctrl.Result{}, err
	}

	channelStatuses, err := r.reconcileChannels(ctx, &network, orgStatuses)
	if err != nil {
		log.Error(err, "Failed to reconcile channels")

		_ = r.updateStatus(
			ctx,
			&network,
			fabricopsv1alpha1.PhaseFailed,
			"Failed to reconcile channels: "+err.Error(),
			orgStatuses,
			channelStatuses,
			nil,
			observabilityReadyCondition(
				&network,
				channelsReadyCondition(
					&network,
					certificateLifecycleCondition(
						&network,
						identityMaterialCondition(
							&network,
							readyCondition(&network, metav1.ConditionFalse, "ReconcileError", "Failed to reconcile channels: "+err.Error()),
							metav1.ConditionUnknown,
							"ReconcileError",
							"Failed to check identity material: "+err.Error(),
						),
						metav1.ConditionUnknown,
						"ReconcileError",
						"Failed to check certificate lifecycle: "+err.Error(),
					),
					metav1.ConditionUnknown,
					"ReconcileError",
					"Failed to reconcile channels: "+err.Error(),
				),
				metav1.ConditionUnknown,
				"ReconcileError",
				"Failed to check observability: "+err.Error(),
			),
		)

		return ctrl.Result{}, err
	}

	channelsReady := allChannelsReady(channelStatuses)
	channelsStatus := metav1.ConditionFalse
	channelsReason := "ChannelBootstrapPending"
	channelsMessage := channelStatusMessage(channelStatuses)
	if len(channelStatuses) == 0 {
		channelsStatus = metav1.ConditionTrue
		channelsReason = "NoChannelsDeclared"
	} else if channelsReady {
		channelsStatus = metav1.ConditionTrue
		channelsReason = "ChannelsReady"
	}

	chaincodeStatuses, err := r.reconcileChaincodes(ctx, &network, channelStatuses)
	if err != nil {
		log.Error(err, "Failed to reconcile chaincodes")

		_ = r.updateStatus(
			ctx,
			&network,
			fabricopsv1alpha1.PhaseFailed,
			"Failed to reconcile chaincodes: "+err.Error(),
			orgStatuses,
			channelStatuses,
			chaincodeStatuses,
			observabilityReadyCondition(
				&network,
				channelsReadyCondition(
					&network,
					certificateLifecycleCondition(
						&network,
						identityMaterialCondition(
							&network,
							readyCondition(&network, metav1.ConditionFalse, "ReconcileError", "Failed to reconcile chaincodes: "+err.Error()),
							metav1.ConditionUnknown,
							"ReconcileError",
							"Failed to check identity material: "+err.Error(),
						),
						metav1.ConditionUnknown,
						"ReconcileError",
						"Failed to check certificate lifecycle: "+err.Error(),
					),
					channelsStatus,
					channelsReason,
					channelsMessage,
				),
				metav1.ConditionUnknown,
				"ReconcileError",
				"Failed to reconcile chaincodes: "+err.Error(),
			),
		)

		return ctrl.Result{}, err
	}

	orgStatuses, err = r.reconcileConnectionProfiles(ctx, &network, orgStatuses)
	if err != nil {
		log.Error(err, "Failed to reconcile connection profiles")

		_ = r.updateStatus(
			ctx,
			&network,
			fabricopsv1alpha1.PhaseFailed,
			"Failed to reconcile connection profiles: "+err.Error(),
			orgStatuses,
			channelStatuses,
			chaincodeStatuses,
			observabilityReadyCondition(
				&network,
				channelsReadyCondition(
					&network,
					certificateLifecycleCondition(
						&network,
						identityMaterialCondition(
							&network,
							readyCondition(&network, metav1.ConditionFalse, "ReconcileError", "Failed to reconcile connection profiles: "+err.Error()),
							metav1.ConditionUnknown,
							"ReconcileError",
							"Failed to check identity material: "+err.Error(),
						),
						metav1.ConditionUnknown,
						"ReconcileError",
						"Failed to check certificate lifecycle: "+err.Error(),
					),
					channelsStatus,
					channelsReason,
					channelsMessage,
				),
				metav1.ConditionUnknown,
				"ReconcileError",
				"Failed to reconcile connection profiles: "+err.Error(),
			),
		)

		return ctrl.Result{}, err
	}

	identityReady := identityMaterialReady(orgStatuses)
	chaincodesReady := allChaincodesReady(chaincodeStatuses)
	identityStatus := metav1.ConditionFalse
	identityReason := "IdentityMaterialMissing"
	if identityReady {
		identityStatus = metav1.ConditionTrue
		identityReason = "IdentityMaterialPresent"
	}
	identityMessage := identityMaterialMessage(orgStatuses)
	certificateStatus, certificateReason, certificateMessage := certificateLifecycleConditionStatus(orgStatuses)

	observabilityStatus := metav1.ConditionFalse
	observabilityReason := "OperationsEndpointsPending"
	observabilityMessage := observabilityStatusMessage(orgStatuses)
	if observabilityReady(orgStatuses) {
		observabilityStatus = metav1.ConditionTrue
		observabilityReason = "OperationsEndpointsReady"
	}

	if allOrgsReady(orgStatuses) && channelsReady && chaincodesReady && certificateStatus == metav1.ConditionTrue {
		readyReason := "ComponentsReady"
		readyMessage := "All Fabric components are ready"
		if len(channelStatuses) > 0 && len(chaincodeStatuses) > 0 {
			readyReason = "FabricNetworkReady"
			readyMessage = "All Fabric components, channels, and chaincodes are ready"
		} else if len(channelStatuses) > 0 {
			readyReason = "FabricNetworkReady"
			readyMessage = "All Fabric components and channels are ready"
		} else if len(chaincodeStatuses) > 0 {
			readyReason = "FabricNetworkReady"
			readyMessage = "All Fabric components and chaincodes are ready"
		}
		if err := r.updateStatus(
			ctx,
			&network,
			fabricopsv1alpha1.PhaseReady,
			readyMessage,
			orgStatuses,
			channelStatuses,
			chaincodeStatuses,
			observabilityReadyCondition(
				&network,
				channelsReadyCondition(
					&network,
					certificateLifecycleCondition(
						&network,
						identityMaterialCondition(
							&network,
							readyCondition(&network, metav1.ConditionTrue, readyReason, readyMessage),
							identityStatus,
							identityReason,
							identityMessage,
						),
						certificateStatus,
						certificateReason,
						certificateMessage,
					),
					channelsStatus,
					channelsReason,
					channelsMessage,
				),
				observabilityStatus,
				observabilityReason,
				observabilityMessage,
			),
		); err != nil {
			return ctrl.Result{}, err
		}

		cleanupAfter, err := r.cleanupSucceededJobs(ctx, &network)
		if err != nil {
			return ctrl.Result{}, err
		}

		return resultWithCleanupRequeue(ctrl.Result{}, cleanupAfter), nil
	}

	readyReason := "ComponentsNotReady"
	readyMessage := "Waiting for Fabric components to become ready"
	if !identityReady {
		readyReason = "IdentityMaterialMissing"
		readyMessage = "Waiting for required Fabric identity material"
	} else if allOrgsReady(orgStatuses) && certificateStatus != metav1.ConditionTrue {
		readyReason = "CertificateLifecyclePending"
		readyMessage = "Waiting for Fabric certificate lifecycle to become ready"
	} else if allOrgsReady(orgStatuses) && !channelsReady {
		readyReason = "ChannelsNotReady"
		readyMessage = "Waiting for Fabric channels to become ready"
	} else if allOrgsReady(orgStatuses) && channelsReady && !chaincodesReady {
		readyReason = "ChaincodesNotReady"
		readyMessage = "Waiting for Fabric chaincodes to become ready"
	}

	if err := r.updateStatus(
		ctx,
		&network,
		fabricopsv1alpha1.PhaseCreating,
		readyMessage,
		orgStatuses,
		channelStatuses,
		chaincodeStatuses,
		observabilityReadyCondition(
			&network,
			channelsReadyCondition(
				&network,
				certificateLifecycleCondition(
					&network,
					identityMaterialCondition(
						&network,
						readyCondition(&network, metav1.ConditionFalse, readyReason, readyMessage),
						identityStatus,
						identityReason,
						identityMessage,
					),
					certificateStatus,
					certificateReason,
					certificateMessage,
				),
				channelsStatus,
				channelsReason,
				channelsMessage,
			),
			observabilityStatus,
			observabilityReason,
			observabilityMessage,
		),
	); err != nil {
		return ctrl.Result{}, err
	}

	cleanupAfter, err := r.cleanupSucceededJobs(ctx, &network)
	if err != nil {
		return ctrl.Result{}, err
	}

	return resultWithCleanupRequeue(ctrl.Result{RequeueAfter: 10 * time.Second}, cleanupAfter), nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *FabricNetworkReconciler) SetupWithManager(mgr ctrl.Manager) error {
	enqueueOwningFabricNetwork := handler.EnqueueRequestsFromMapFunc(fabricNetworkRequestsForObject)

	return ctrl.NewControllerManagedBy(mgr).
		For(&fabricopsv1alpha1.FabricNetwork{}).
		Watches(&corev1.Namespace{}, enqueueOwningFabricNetwork).
		Watches(&appsv1.Deployment{}, enqueueOwningFabricNetwork).
		Watches(&corev1.Service{}, enqueueOwningFabricNetwork).
		Watches(&corev1.ConfigMap{}, enqueueOwningFabricNetwork).
		Watches(&corev1.Secret{}, enqueueOwningFabricNetwork).
		Watches(&corev1.ServiceAccount{}, enqueueOwningFabricNetwork).
		Watches(&corev1.PersistentVolumeClaim{}, enqueueOwningFabricNetwork).
		Watches(&networkingv1.NetworkPolicy{}, enqueueOwningFabricNetwork).
		Watches(&batchv1.Job{}, enqueueOwningFabricNetwork).
		Watches(&rbacv1.Role{}, enqueueOwningFabricNetwork).
		Watches(&rbacv1.RoleBinding{}, enqueueOwningFabricNetwork).
		Named("fabricnetwork").
		Complete(r)
}

func fabricNetworkRequestsForObject(_ context.Context, object client.Object) []reconcile.Request {
	if object == nil {
		return nil
	}

	annotations := object.GetAnnotations()
	name := annotations[annotationFabricNetwork]
	namespace := annotations[annotationFabricNetworkNamespace]

	labels := object.GetLabels()
	if name == "" {
		name = labels[labelFabricNetwork]
	}
	if namespace == "" {
		namespace = labels[labelFabricNetworkNamespace]
	}

	if name == "" || namespace == "" {
		return nil
	}

	return []reconcile.Request{
		{
			NamespacedName: types.NamespacedName{
				Name:      name,
				Namespace: namespace,
			},
		},
	}
}
