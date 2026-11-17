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
	"flag"
	"fmt"
	"io"
	"os"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

type metadataRestoreReceipt struct {
	ArchiveSHA256 string   `json:"archiveSHA256"`
	NetworkUID    string   `json:"networkUID,omitempty"`
	Created       []string `json:"created"`
	Complete      bool     `json:"metadataStaged"`
}

func runMetadataRestore(args []string, stdout, stderr io.Writer) error {
	var kube kubeOptions
	var archive, out, digest, network, operatorNamespace, operatorDeployment string
	var acknowledge bool
	flags := flag.NewFlagSet("backup restore-metadata", flag.ContinueOnError)
	flags.SetOutput(stderr)
	bindKubeFlags(flags, &kube)
	flags.StringVar(&archive, "file", "", "Metadata archive JSON")
	flags.StringVar(&out, "out", "", "New private receipt file (required)")
	flags.StringVar(&digest, "confirm-sha256", "", "Confirm the archive SHA-256 from the reviewed plan")
	flags.StringVar(&network, "confirm-network", "", "Confirm namespace/name of the archived network")
	flags.BoolVar(&acknowledge, "acknowledge-incomplete-recovery", false, "Acknowledge staging does not recover storage")
	flags.StringVar(&operatorNamespace, "operator-namespace", "fabricops-system", "Operator namespace")
	flags.StringVar(&operatorDeployment, "operator-deployment", "fabricops-controller-manager", "Manager Deployment")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || archive == "" || out == "" || out == "-" || kube.context == "" ||
		digest == "" || network == "" || !acknowledge {
		return fmt.Errorf("%w: require file, out, context, archive/network confirmation and acknowledgement", errUsage)
	}
	bundle, data, err := loadRestoreArchive(archive)
	if err != nil {
		return err
	}
	if digest != restoreArchiveDigest(data) || network != bundle.Network.Namespace+"/"+bundle.Network.Name ||
		kube.namespace != bundle.Network.Namespace {
		return fmt.Errorf("archive digest or target network/namespace does not match confirmation")
	}
	c, err := newClient(kube)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	receipt, restoreErr := restoreMetadata(context.Background(), c, bundle, digest, kube.context,
		ctrlclient.ObjectKey{Namespace: operatorNamespace, Name: operatorDeployment})
	writeErr := json.NewEncoder(file).Encode(receipt)
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		return fmt.Errorf("receipt write failed; inspect held resources before retrying, no rollback was attempted")
	}
	if restoreErr != nil {
		return restoreErr
	}
	printf(stdout, "Staged metadata for %s; receipt: %s\n", network, out)
	printLine(stderr, "Restore hold remains set. No volumes or workloads were created; reconciliation was not resumed.")
	return nil
}

func restoreMetadata(ctx context.Context, c ctrlclient.Client, bundle *backupBundle, digest, targetContext string,
	managerKey ctrlclient.ObjectKey) (*metadataRestoreReceipt, error) {
	receipt := &metadataRestoreReceipt{ArchiveSHA256: digest, Created: []string{}}
	plan, err := buildMetadataRestorePlan(ctx, c, bundle, targetContext, managerKey)
	if err != nil {
		return receipt, err
	}
	if len(plan.Blockers) != 0 {
		return receipt, fmt.Errorf("metadata restore preflight blocked; generate a fresh plan")
	}
	net := bundle.Network.DeepCopy()
	net.ObjectMeta = backupMetadata(net.ObjectMeta)
	if net.Annotations == nil {
		net.Annotations = map[string]string{}
	}
	net.Annotations[fabricopsv1alpha1.RestoreHoldAnnotation] = digest
	net.Finalizers = []string{"fabricops.io/finalizer"}
	net.Status = fabricopsv1alpha1.FabricNetworkStatus{}
	namespaceUIDs := map[string]types.UID{}
	guard := func() error {
		p := &restorePlan{}
		if err := checkRestoreManager(ctx, c, managerKey, p); err != nil {
			return err
		}
		for _, ns := range append([]string{managerKey.Namespace}, bundle.Namespaces...) {
			if err := checkRestoreActivity(ctx, c, ns, p); err != nil {
				return err
			}
		}
		if len(p.Blockers) != 0 {
			return fmt.Errorf("restore guard changed; partial metadata remains held")
		}
		if receipt.NetworkUID != "" {
			current := &fabricopsv1alpha1.FabricNetwork{}
			if err := c.Get(ctx, ctrlclient.ObjectKeyFromObject(net), current); err != nil {
				return err
			}
			if current.UID != net.UID || current.Generation != net.Generation || current.DeletionTimestamp != nil ||
				current.Annotations[fabricopsv1alpha1.RestoreHoldAnnotation] != digest {
				return fmt.Errorf("target network or restore hold changed; partial metadata retained")
			}
		}
		for name, uid := range namespaceUIDs {
			ns := &corev1.Namespace{}
			if err := c.Get(ctx, ctrlclient.ObjectKey{Name: name}, ns); err != nil {
				return err
			}
			if ns.UID != uid || ns.DeletionTimestamp != nil || (name != net.Namespace && !backupOwnedBy(ns, net)) {
				return fmt.Errorf("target namespace changed; partial metadata retained")
			}
		}
		return nil
	}
	create := func(object ctrlclient.Object, ref string) error {
		if err := guard(); err != nil {
			return err
		}
		if err := c.Create(ctx, object); err != nil {
			return fmt.Errorf("could not create %s (%s); partial metadata retained, no rollback attempted",
				ref, apierrors.ReasonForError(err))
		}
		receipt.Created = append(receipt.Created, ref)
		return nil
	}
	rootNamespace := &corev1.Namespace{}
	err = c.Get(ctx, ctrlclient.ObjectKey{Name: net.Namespace}, rootNamespace)
	if apierrors.IsNotFound(err) {
		rootNamespace = &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: net.Namespace}}
		if err := create(rootNamespace, "Namespace/"+net.Namespace); err != nil {
			return receipt, err
		}
	} else if err != nil {
		return receipt, err
	}
	namespaceUIDs[net.Namespace] = rootNamespace.UID
	if err := create(net, "FabricNetwork/"+net.Namespace+"/"+net.Name); err != nil {
		return receipt, err
	}
	receipt.NetworkUID = string(net.UID)
	if net.UID == "" || string(net.UID) == bundle.SourceUID {
		return receipt, fmt.Errorf("target API did not assign a fresh network UID; partial metadata retained")
	}
	for _, name := range bundle.Namespaces {
		if name == net.Namespace {
			continue
		}
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
		bindRestoredOwner(ns, net)
		if err := create(ns, "Namespace/"+name); err != nil {
			return receipt, err
		}
		namespaceUIDs[name] = ns.UID
	}
	for _, object := range restoreArchiveObjects(bundle) {
		if _, volume := object.(*corev1.PersistentVolumeClaim); volume {
			continue
		}
		copy := object.DeepCopyObject().(ctrlclient.Object)
		bindRestoredOwner(copy, net)
		if err := create(copy, restoreObjectRef(copy)); err != nil {
			return receipt, err
		}
	}
	if err := guard(); err != nil {
		return receipt, err
	}
	receipt.Complete = true
	return receipt, nil
}

func bindRestoredOwner(object ctrlclient.Object, net *fabricopsv1alpha1.FabricNetwork) {
	a := object.GetAnnotations()
	if a == nil {
		a = map[string]string{}
	}
	a["fabricops.io/fabricnetwork"] = net.Name
	a["fabricops.io/fabricnetwork-namespace"] = net.Namespace
	a["fabricops.io/fabricnetwork-uid"] = string(net.UID)
	object.SetAnnotations(a)
}
