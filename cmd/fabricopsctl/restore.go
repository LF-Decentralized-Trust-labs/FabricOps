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

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

const maxRestoreArchiveBytes = 64 << 20

type restorePlan struct {
	APIVersion     string   `json:"apiVersion"`
	Kind           string   `json:"kind"`
	Context        string   `json:"context"`
	ArchiveSHA256  string   `json:"archiveSHA256"`
	Network        string   `json:"network"`
	Manager        string   `json:"manager"`
	Namespaces     []string `json:"namespaces"`
	ApplySupported bool     `json:"applySupported"`
	MetadataOnly   bool     `json:"metadataOnly"`
	Resources      []string `json:"resources"`
	Blockers       []string `json:"blockers"`
	ManualChecks   []string `json:"manualChecks"`
	Steps          []string `json:"steps"`
}

func runRestorePlan(args []string, stdout, stderr io.Writer) error {
	var kube kubeOptions
	var archive, out, operatorNamespace, operatorDeployment string
	var metadataOnly bool
	flags := flag.NewFlagSet("backup plan-restore", flag.ContinueOnError)
	flags.SetOutput(stderr)
	bindKubeFlags(flags, &kube)
	flags.StringVar(&archive, "file", "", "Metadata archive JSON")
	flags.StringVar(&out, "out", "", "New private JSON plan file")
	flags.BoolVar(&metadataOnly, "metadata-only", false, "Plan held metadata staging without restoring volume data")
	flags.StringVar(&operatorNamespace, "operator-namespace", "fabricops-system", "Operator namespace")
	flags.StringVar(&operatorDeployment, "operator-deployment", "fabricops-controller-manager", "Manager Deployment")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || archive == "" || out == "" || out == "-" || kube.context == "" {
		return fmt.Errorf("%w: plan-restore requires --file, --out and explicit --context; no positional arguments", errUsage)
	}
	bundle, data, err := loadRestoreArchive(archive)
	if err != nil {
		return err
	}
	if kube.namespace != bundle.Network.Namespace {
		return fmt.Errorf("namespace remapping is unsupported; use --namespace %s", bundle.Network.Namespace)
	}
	client, err := newClient(kube)
	if err != nil {
		return err
	}
	planner := buildRestorePlan
	if metadataOnly {
		planner = buildMetadataRestorePlan
	}
	plan, err := planner(context.Background(), client, bundle, kube.context,
		ctrlclient.ObjectKey{Namespace: operatorNamespace, Name: operatorDeployment})
	if err != nil {
		return err
	}
	plan.ArchiveSHA256 = restoreArchiveDigest(data)
	encoded, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	if err := writeBackupFile(out, append(encoded, '\n')); err != nil {
		return err
	}
	printf(stdout, "Wrote read-only restore plan to %s (%d blockers)\n", out, len(plan.Blockers))
	if len(plan.Blockers) > 0 {
		return fmt.Errorf("restore preflight blocked; review the plan before proceeding")
	}
	printLine(stderr, "No cluster resources changed. Manual checks remain; this plan does not authorize an apply.")
	return nil
}

func loadRestoreArchive(path string) (*backupBundle, []byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(file, maxRestoreArchiveBytes+1))
	closeErr := file.Close()
	if readErr != nil {
		return nil, nil, readErr
	}
	if closeErr != nil {
		return nil, nil, closeErr
	}
	if len(data) > maxRestoreArchiveBytes {
		return nil, nil, fmt.Errorf("archive exceeds 64 MiB limit")
	}
	bundle, err := decodeRestoreArchive(data)
	return bundle, data, err
}

func decodeRestoreArchive(data []byte) (*backupBundle, error) {
	var bundle backupBundle
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&bundle); err != nil {
		return nil, fmt.Errorf("invalid metadata archive JSON")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("archive must contain exactly one JSON document")
	}
	if err := validateRestoreArchive(&bundle); err != nil {
		return nil, err
	}
	return &bundle, nil
}

func validateRestoreArchive(bundle *backupBundle) error {
	if bundle == nil || bundle.APIVersion != "fabricops.io/v1alpha1" || bundle.Kind != "FabricOpsMetadataBackup" ||
		bundle.Network == nil || bundle.SourceUID == "" {
		return fmt.Errorf("unsupported or incomplete metadata archive")
	}
	net := bundle.Network.DeepCopy()
	if net.APIVersion != "fabricops.io/v1alpha1" || net.Kind != "FabricNetwork" ||
		net.UID != "" || len(net.Finalizers) != 0 || len(net.OwnerReferences) != 0 ||
		net.DeletionTimestamp != nil || net.ResourceVersion != "" ||
		len(validation.IsDNS1123Subdomain(net.Name)) != 0 || len(validation.IsDNS1123Label(net.Namespace)) != 0 {
		return fmt.Errorf("invalid archived network metadata")
	}
	if bundle.StartedAt.IsZero() || bundle.CompletedAt.IsZero() || bundle.CompletedAt.Before(&bundle.StartedAt) {
		return fmt.Errorf("archive collection timestamps are missing or invalid")
	}
	if !bundle.SecretsIncluded && len(bundle.Secrets) != 0 {
		return fmt.Errorf("archive contains Secrets despite secretsIncluded=false")
	}
	namespaces := map[string]bool{}
	for _, ns := range bundle.Namespaces {
		if namespaces[ns] || len(validation.IsDNS1123Label(ns)) != 0 {
			return fmt.Errorf("invalid archive namespaces")
		}
		namespaces[ns] = true
	}
	if !namespaces[net.Namespace] {
		return fmt.Errorf("archive omits the network namespace")
	}
	provenance := map[string]bool{}
	for _, ref := range bundle.NamespaceScopedArtifacts {
		if provenance[ref] {
			return fmt.Errorf("duplicate namespace-scoped provenance")
		}
		provenance[ref] = true
	}
	net.UID = types.UID(bundle.SourceUID)
	seen := map[string]bool{}
	for _, object := range restoreArchiveObjects(bundle) {
		ref := restoreObjectRef(object)
		if seen[ref] || !namespaces[object.GetNamespace()] || len(validation.IsDNS1123Subdomain(object.GetName())) != 0 ||
			len(object.GetFinalizers()) != 0 || len(object.GetOwnerReferences()) != 0 || object.GetUID() != "" ||
			object.GetDeletionTimestamp() != nil || object.GetResourceVersion() != "" {
			return fmt.Errorf("invalid or duplicate archived resource metadata")
		}
		seen[ref] = true
		kinds, _, err := cliScheme.ObjectKinds(object)
		if err != nil {
			return fmt.Errorf("unsupported archived resource type")
		}
		if gvk := object.GetObjectKind().GroupVersionKind(); !gvk.Empty() && gvk != kinds[0] {
			return fmt.Errorf("archived resource type metadata does not match its archive section")
		}
		if secret, ok := object.(*corev1.Secret); ok && secret.Type == corev1.SecretTypeServiceAccountToken {
			return fmt.Errorf("ServiceAccount tokens cannot be restored")
		}
		if !backupOwnedBy(object, net) {
			if !provenance[ref] || object.GetNamespace() == net.Namespace || !backupJobArtifact(object) {
				return fmt.Errorf("archived resource has invalid ownership provenance")
			}
			delete(provenance, ref)
		}
	}
	if len(provenance) != 0 {
		return fmt.Errorf("unmatched namespace-scoped provenance")
	}
	return nil
}

func restoreArchiveObjects(bundle *backupBundle) []ctrlclient.Object {
	objects := make([]ctrlclient.Object, 0, len(bundle.ConfigMaps)+len(bundle.Secrets)+len(bundle.Volumes))
	for i := range bundle.ConfigMaps {
		objects = append(objects, &bundle.ConfigMaps[i])
	}
	for i := range bundle.Secrets {
		objects = append(objects, &bundle.Secrets[i])
	}
	for i := range bundle.Volumes {
		objects = append(objects, &bundle.Volumes[i])
	}
	return objects
}

func restoreObjectRef(obj ctrlclient.Object) string {
	var kind string
	switch obj.(type) {
	case *corev1.ConfigMap:
		kind = "ConfigMap"
	case *corev1.Secret:
		kind = "Secret"
	case *corev1.PersistentVolumeClaim:
		kind = "PersistentVolumeClaim"
	}
	return kind + "/" + obj.GetNamespace() + "/" + obj.GetName()
}

func buildRestorePlan(
	ctx context.Context, client ctrlclient.Reader, bundle *backupBundle,
	targetContext string, managerKey ctrlclient.ObjectKey,
) (*restorePlan, error) {
	if err := validateRestoreArchive(bundle); err != nil {
		return nil, err
	}
	plan := &restorePlan{APIVersion: "fabricops.io/v1alpha1", Kind: "FabricOpsRestorePlan", Context: targetContext,
		Network: bundle.Network.Namespace + "/" + bundle.Network.Name, Blockers: []string{}, Resources: []string{},
		Manager: managerKey.String(), Namespaces: append([]string{}, bundle.Namespaces...),
		ManualChecks: []string{
			"Confirm target cluster identity and isolation from the original network; context names are not cluster identities.",
			"Verify all controllers and automation are stopped; checks cover the named manager and target namespace activity.",
			"Verify matching volume recovery, external dependencies, software versions, image digests and certificate validity.",
			"Review namespace-scoped provenance and pending channel operations; never blindly replay signed updates/receipts.",
			"This is a sequential preflight, not a lock or full-network recovery authorization.",
		},
		Steps: []string{
			"Keep reconciliation stopped and verify the coordinated recovery set.",
			"Create the target CR without starting reconciliation, then bind restored ownership to its new UID.",
			"Restore namespaces, provider-managed storage, identities and reviewed configuration/evidence.",
			"Verify storage/identity continuity before starting CA, ordering, peers and chaincode in controlled order.",
			"Review pending operations, resume reconciliation, and verify quorum, ledger state and invoke/query results.",
		},
	}
	if err := checkRestoreManager(ctx, client, managerKey, plan); err != nil {
		return nil, err
	}
	net := &fabricopsv1alpha1.FabricNetwork{}
	err := client.Get(ctx, ctrlclient.ObjectKeyFromObject(bundle.Network), net)
	if err == nil {
		if string(net.UID) == bundle.SourceUID {
			plan.Blockers = append(plan.Blockers, "Target network has the source UID; refusing in-place source recovery")
		} else {
			plan.Blockers = append(plan.Blockers, "Target FabricNetwork already exists; overwrite/adoption is unsupported")
		}
	} else if !apierrors.IsNotFound(err) {
		return nil, err
	}
	for _, ns := range bundle.Namespaces {
		namespace := &corev1.Namespace{}
		err := client.Get(ctx, ctrlclient.ObjectKey{Name: ns}, namespace)
		if err == nil && (ns != bundle.Network.Namespace || namespace.DeletionTimestamp != nil) {
			plan.Blockers = append(plan.Blockers, "Namespace/"+ns+" already exists or is terminating; adoption is unsupported")
		} else if err != nil && !apierrors.IsNotFound(err) {
			return nil, err
		}
	}
	for _, object := range restoreArchiveObjects(bundle) {
		ref := restoreObjectRef(object)
		plan.Resources = append(plan.Resources, ref)
		if _, volume := object.(*corev1.PersistentVolumeClaim); volume {
			plan.Blockers = append(plan.Blockers, ref+": volume data recovery is not verified by metadata inventory")
		}
		existing := object.DeepCopyObject().(ctrlclient.Object)
		err := client.Get(ctx, ctrlclient.ObjectKeyFromObject(object), existing)
		if err == nil {
			plan.Blockers = append(plan.Blockers, ref+" already exists; overwrite is unsupported")
		} else if !apierrors.IsNotFound(err) {
			return nil, err
		}
	}
	if !bundle.SecretsIncluded {
		plan.Blockers = append(plan.Blockers,
			"Archive omits Secrets; identity/credential recovery needs a separate verified source")
	}
	namespaces := append([]string{managerKey.Namespace}, bundle.Namespaces...)
	checked := map[string]bool{}
	for _, ns := range namespaces {
		if checked[ns] {
			continue
		}
		checked[ns] = true
		if err := checkRestoreActivity(ctx, client, ns, plan); err != nil {
			return nil, err
		}
	}
	return plan, nil
}

func checkRestoreManager(
	ctx context.Context, client ctrlclient.Reader, key ctrlclient.ObjectKey, plan *restorePlan,
) error {
	manager := &appsv1.Deployment{}
	err := client.Get(ctx, key, manager)
	if apierrors.IsNotFound(err) {
		plan.Blockers = append(plan.Blockers, "Named manager Deployment is missing; stopped state cannot be verified")
	} else if err != nil {
		return err
	} else if manager.Spec.Replicas == nil || *manager.Spec.Replicas != 0 || manager.Status.Replicas != 0 ||
		manager.Status.ReadyReplicas != 0 || manager.Status.AvailableReplicas != 0 ||
		manager.Status.ObservedGeneration < manager.Generation || manager.DeletionTimestamp != nil {
		plan.Blockers = append(plan.Blockers, "Named manager Deployment is not fully stopped")
	}
	return nil
}

func buildMetadataRestorePlan(ctx context.Context, client ctrlclient.Reader, bundle *backupBundle,
	targetContext string, managerKey ctrlclient.ObjectKey) (*restorePlan, error) {
	if err := validateRestoreArchive(bundle); err != nil {
		return nil, err
	}
	copy := *bundle
	copy.Volumes = nil
	plan, err := buildRestorePlan(ctx, client, &copy, targetContext, managerKey)
	if err != nil {
		return nil, err
	}
	plan.MetadataOnly = true
	plan.ApplySupported = len(plan.Blockers) == 0
	plan.ManualChecks = append(plan.ManualChecks,
		"Volume inventory is excluded from metadata staging; keep restore hold until recovery is verified.")
	plan.Steps = []string{
		"Keep a compatible operator stopped; confirm archive digest, network identity and incomplete recovery.",
		"Create the held FabricNetwork, then create namespaces and metadata bound to its new UID; never overwrite or adopt.",
		"Preserve the private receipt and all partial state on failure; do not automatically delete or retry.",
		"Recover matching volumes and external dependencies separately; review pending operations before any resume.",
		"Remove restore hold only after recovery verification; this command never resumes reconciliation or workloads.",
	}
	return plan, nil
}

func restoreArchiveDigest(data []byte) string {
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

func checkRestoreActivity(ctx context.Context, client ctrlclient.Reader, namespace string, plan *restorePlan) error {
	pods := &corev1.PodList{}
	if err := client.List(ctx, pods, ctrlclient.InNamespace(namespace)); err != nil {
		return err
	}
	for _, pod := range pods.Items {
		if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			plan.Blockers = append(plan.Blockers, "Pod/"+namespace+"/"+pod.Name+" is not terminal")
		}
	}
	jobs := &batchv1.JobList{}
	if err := client.List(ctx, jobs, ctrlclient.InNamespace(namespace)); err != nil {
		return err
	}
	for _, job := range jobs.Items {
		finished := false
		for _, condition := range job.Status.Conditions {
			if condition.Status == corev1.ConditionTrue &&
				(condition.Type == batchv1.JobComplete || condition.Type == batchv1.JobFailed) {
				finished = true
			}
		}
		if !finished {
			plan.Blockers = append(plan.Blockers, "Job/"+namespace+"/"+job.Name+" is not terminal")
		}
	}
	return nil
}
