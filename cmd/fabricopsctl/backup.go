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
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

// backupBundle is an archive contract, not a Kubernetes resource or an apply-ready restore plan.
type backupBundle struct {
	APIVersion               string                           `json:"apiVersion"`
	Kind                     string                           `json:"kind"`
	StartedAt                metav1.Time                      `json:"startedAt"`
	CompletedAt              metav1.Time                      `json:"completedAt"`
	CLIVersion               string                           `json:"cliVersion"`
	SourceUID                string                           `json:"sourceUID"`
	SecretsIncluded          bool                             `json:"secretsIncluded"`
	Warnings                 []string                         `json:"warnings"`
	Network                  *fabricopsv1alpha1.FabricNetwork `json:"network"`
	Namespaces               []string                         `json:"namespaces"`
	ConfigMaps               []corev1.ConfigMap               `json:"configMaps"`
	Secrets                  []corev1.Secret                  `json:"secrets,omitempty"`
	Volumes                  []corev1.PersistentVolumeClaim   `json:"volumeInventory"`
	NamespaceScopedArtifacts []string                         `json:"namespaceScopedArtifacts,omitempty"`
}

func runBackup(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] != "export" {
		printLine(stderr, "Usage: fabricopsctl backup export --out <file.json> [--include-secrets] [flags] <fabricnetwork>")
		return errUsage
	}
	var kube kubeOptions
	var path string
	var includeSecrets bool
	flags := flag.NewFlagSet("backup export", flag.ContinueOnError)
	flags.SetOutput(stderr)
	bindKubeFlags(flags, &kube)
	flags.StringVar(&path, "out", "", "New private JSON archive file (required; never overwritten)")
	flags.BoolVar(&includeSecrets, "include-secrets", false, "Include owned Secrets with private keys and credentials")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 1 || path == "" || path == "-" {
		return fmt.Errorf("%w: supply one FabricNetwork and --out <file.json>; stdout export is not supported", errUsage)
	}
	client, err := newClient(kube)
	if err != nil {
		return err
	}
	ctx := context.Background()
	net := &fabricopsv1alpha1.FabricNetwork{}
	if err := client.Get(ctx, ctrlclient.ObjectKey{Namespace: kube.namespace, Name: flags.Arg(0)}, net); err != nil {
		return err
	}
	bundle, err := collectBackup(ctx, client, net, includeSecrets)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		return err
	}
	if err := writeBackupFile(path, append(data, '\n')); err != nil {
		return err
	}
	printf(stdout, "Exported metadata to %s (Secrets included: %t)\n", path, includeSecrets)
	printLine(stderr, "No volume data copied. This is not a full backup or a consistent point-in-time snapshot.")
	return nil
}

func collectBackup(
	ctx context.Context, client ctrlclient.Client, net *fabricopsv1alpha1.FabricNetwork, includeSecrets bool,
) (*backupBundle, error) {
	if net.UID == "" || net.DeletionTimestamp != nil {
		return nil, fmt.Errorf("backup requires a live FabricNetwork with a UID, not a deleting resource")
	}
	bundle := &backupBundle{
		APIVersion: "fabricops.io/v1alpha1", Kind: "FabricOpsMetadataBackup",
		StartedAt: metav1.NewTime(time.Now().UTC()), CLIVersion: version, SourceUID: string(net.UID),
		SecretsIncluded: includeSecrets, Network: net.DeepCopy(), ConfigMaps: []corev1.ConfigMap{},
		Volumes: []corev1.PersistentVolumeClaim{},
		Warnings: []string{
			"Metadata only: PVC contents, CA databases, ledgers and state databases are not copied.",
			"Sequential API reads are not an application-consistent snapshot; quiesce changes for coordinated backups.",
			"UID ownership or verified namespace plus Job labels select resources; external references need separate backups.",
			"Archives can contain sensitive configuration even without Secrets; encrypt and store outside the repository.",
			"Not apply-ready: restore requires storage recovery and explicit ownership rebinding before reconciliation.",
		},
	}
	if !includeSecrets {
		bundle.Warnings = append(bundle.Warnings,
			"Secrets omitted: identity and credential recovery is not possible from this archive alone.")
	}
	bundle.Network.TypeMeta = metav1.TypeMeta{APIVersion: "fabricops.io/v1alpha1", Kind: "FabricNetwork"}
	bundle.Network.ObjectMeta = backupMetadata(net.ObjectMeta)
	bundle.Network.Status = fabricopsv1alpha1.FabricNetworkStatus{}
	namespaces := &corev1.NamespaceList{}
	if err := client.List(ctx, namespaces); err != nil {
		return nil, fmt.Errorf("discover owned namespaces: %w", err)
	}
	bundle.Namespaces = []string{net.Namespace}
	ownedNamespaces := map[string]bool{}
	for _, ns := range namespaces.Items {
		if backupOwnedBy(&ns, net) {
			ownedNamespaces[ns.Name] = true
			if ns.Name != net.Namespace {
				bundle.Namespaces = append(bundle.Namespaces, ns.Name)
			}
		}
	}
	slices.Sort(bundle.Namespaces)
	for _, namespace := range bundle.Namespaces {
		if err := collectBackupNamespace(ctx, client, net, namespace, ownedNamespaces[namespace], bundle); err != nil {
			return nil, fmt.Errorf("export namespace %s: %w", namespace, err)
		}
	}
	latest := &fabricopsv1alpha1.FabricNetwork{}
	if err := client.Get(ctx, ctrlclient.ObjectKeyFromObject(net), latest); err != nil {
		return nil, err
	}
	if latest.UID != net.UID || latest.Generation != net.Generation || latest.DeletionTimestamp != nil {
		return nil, fmt.Errorf("FabricNetwork changed during export; retry with topology changes paused")
	}
	bundle.CompletedAt = metav1.NewTime(time.Now().UTC())
	return bundle, nil
}

func collectBackupNamespace(
	ctx context.Context, client ctrlclient.Client, net *fabricopsv1alpha1.FabricNetwork,
	namespace string, ownedNamespace bool, bundle *backupBundle,
) error {
	cms := &corev1.ConfigMapList{}
	if err := client.List(ctx, cms, ctrlclient.InNamespace(namespace)); err != nil {
		return err
	}
	slices.SortFunc(cms.Items, func(a, b corev1.ConfigMap) int { return strings.Compare(a.Name, b.Name) })
	for _, cm := range cms.Items {
		jobOutput := ownedNamespace && backupJobArtifact(&cm)
		if backupOwnedBy(&cm, net) || jobOutput {
			if jobOutput {
				bundle.NamespaceScopedArtifacts = append(bundle.NamespaceScopedArtifacts, "ConfigMap/"+namespace+"/"+cm.Name)
			}
			cm.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}
			cm.ObjectMeta = backupMetadata(cm.ObjectMeta)
			bundle.ConfigMaps = append(bundle.ConfigMaps, cm)
		}
	}
	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := client.List(ctx, pvcs, ctrlclient.InNamespace(namespace)); err != nil {
		return err
	}
	slices.SortFunc(pvcs.Items, func(a, b corev1.PersistentVolumeClaim) int { return strings.Compare(a.Name, b.Name) })
	for _, pvc := range pvcs.Items {
		if backupOwnedBy(&pvc, net) {
			pvc.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"}
			pvc.ObjectMeta = backupMetadata(pvc.ObjectMeta)
			bundle.Volumes = append(bundle.Volumes, pvc)
		}
	}
	if !bundle.SecretsIncluded {
		return nil
	}
	secrets := &corev1.SecretList{}
	if err := client.List(ctx, secrets, ctrlclient.InNamespace(namespace)); err != nil {
		return err
	}
	slices.SortFunc(secrets.Items, func(a, b corev1.Secret) int { return strings.Compare(a.Name, b.Name) })
	for _, secret := range secrets.Items {
		jobOutput := ownedNamespace && backupJobArtifact(&secret)
		if (backupOwnedBy(&secret, net) || jobOutput) && secret.Type != corev1.SecretTypeServiceAccountToken {
			if jobOutput {
				bundle.NamespaceScopedArtifacts = append(bundle.NamespaceScopedArtifacts, "Secret/"+namespace+"/"+secret.Name)
			}
			secret.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"}
			secret.ObjectMeta = backupMetadata(secret.ObjectMeta)
			bundle.Secrets = append(bundle.Secrets, secret)
		}
	}
	return nil
}

// Publisher Jobs label outputs without network annotations. This fallback requires a UID-verified namespace.
func backupJobArtifact(obj ctrlclient.Object) bool {
	if len(obj.GetOwnerReferences()) != 0 {
		return false
	}
	a := obj.GetAnnotations()
	for _, key := range []string{"fabricops.io/fabricnetwork", "fabricops.io/fabricnetwork-namespace",
		"fabricops.io/fabricnetwork-uid"} {
		if a[key] != "" {
			return false
		}
	}
	labels := obj.GetLabels()
	if labels["fabricops.io/fabricnetwork"] != "" || labels["fabricops.io/fabricnetwork-namespace"] != "" {
		return false
	}
	switch obj.(type) {
	case *corev1.ConfigMap:
		return labels["fabricops.io/component"] == "channel" || labels["fabricops.io/component"] == "chaincode"
	case *corev1.Secret:
		kind := labels["fabricops.io/identity-kind"]
		return labels["fabricops.io/identity-source"] == "fabric-ca" && labels["fabricops.io/workload"] != "" &&
			(kind == "msp" || kind == "tls" || kind == "admin-msp" || kind == "admin-tls")
	default:
		return false
	}
}

func backupOwnedBy(obj metav1.Object, net *fabricopsv1alpha1.FabricNetwork) bool {
	a := obj.GetAnnotations()
	return a["fabricops.io/fabricnetwork"] == net.Name &&
		a["fabricops.io/fabricnetwork-namespace"] == net.Namespace &&
		a["fabricops.io/fabricnetwork-uid"] == string(net.UID)
}

func backupMetadata(meta metav1.ObjectMeta) metav1.ObjectMeta {
	copy := meta.DeepCopy()
	delete(copy.Annotations, "kubectl.kubernetes.io/last-applied-configuration")
	return metav1.ObjectMeta{
		Name: copy.Name, Namespace: copy.Namespace, Labels: copy.Labels, Annotations: copy.Annotations,
	}
}

func writeBackupFile(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(data)
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		return fmt.Errorf("incomplete archive at %s; remove it before retrying: %w", path, errors.Join(writeErr, closeErr))
	}
	return nil
}
