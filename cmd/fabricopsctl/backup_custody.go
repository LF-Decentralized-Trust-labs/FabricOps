// Copyright contributors to FabricOps. SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"slices"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const backupCustodyRetention = "fabricops.io/pkcs11-state-retention"
const externalCustodyRestoreBlocker = "Archive requires external custody recovery; " +
	"automated restore and UID/state rebinding are unsupported"
const externalCustodyExportError = "secret-inclusive backup is unsupported for external custody; " +
	"export metadata without --include-secrets"

// Presence is recovery evidence, not permission to replay a retention receipt
// into a different network/namespace UID. Public references remain in Network.
type backupExternalCustody struct {
	Version            int      `json:"version"`
	RetainedNamespaces []string `json:"retainedNamespaces,omitempty"`
}

func networkHasExternalCustody(net *fabricopsv1alpha1.FabricNetwork) bool {
	if net == nil {
		return false
	}
	for _, org := range net.Spec.Orgs {
		if org.Peer == nil || org.Peer.MSPSigning == nil {
			continue
		}
		signing := org.Peer.MSPSigning
		if signing.Provider != "Secret" || signing.PKCS11 != nil {
			return true
		}
	}
	return false
}

func backupCustodyMetadata(object metav1.Object) bool {
	_, retention := object.GetAnnotations()[backupCustodyRetention]
	_, token := object.GetAnnotations()["fabricops.io/pkcs11-peer"]
	return retention || token || object.GetLabels()["fabricops.io/identity-kind"] == "peer-pkcs11-msp"
}

func recordBackupExternalCustody(bundle *backupBundle) {
	if bundle.ExternalCustody == nil {
		bundle.ExternalCustody = &backupExternalCustody{Version: 1}
		bundle.Warnings = append(bundle.Warnings,
			"External custody: provider credentials, token state and retained UID bindings require separate verified recovery; "+
				"automated restore is unsupported.")
	}
}

func backupHasExternalCustody(bundle *backupBundle) bool {
	if bundle.ExternalCustody != nil || networkHasExternalCustody(bundle.Network) ||
		backupCustodyMetadata(bundle.Network) {
		return true
	}
	for _, object := range restoreArchiveObjects(bundle) {
		if backupCustodyMetadata(object) {
			return true
		}
	}
	return false
}

func blockExternalCustodyRestore(plan *restorePlan, bundle *backupBundle) {
	if backupHasExternalCustody(bundle) && !slices.Contains(plan.Blockers, externalCustodyRestoreBlocker) {
		plan.Blockers = append(plan.Blockers, externalCustodyRestoreBlocker)
	}
}

func validateBackupCustody(bundle *backupBundle, namespaces map[string]bool) error {
	if bundle.ExternalCustody == nil {
		return nil
	}
	if bundle.ExternalCustody.Version != 1 {
		return fmt.Errorf("unsupported external custody inventory version")
	}
	seen := map[string]bool{}
	for _, namespace := range bundle.ExternalCustody.RetainedNamespaces {
		if !namespaces[namespace] || seen[namespace] {
			return fmt.Errorf("invalid external custody namespace inventory")
		}
		seen[namespace] = true
	}
	return nil
}
