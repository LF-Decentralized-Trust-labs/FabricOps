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
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const annotationPKCS11StateRetention = "fabricops.io/pkcs11-state-retention"

// Only public references belong here. Token contents and provider credentials
// must never enter namespace metadata or controller logs.
type pkcs11StateRetention struct {
	Version       int                           `json:"version"`
	NetworkUID    types.UID                     `json:"networkUID"`
	NamespaceUID  types.UID                     `json:"namespaceUID"`
	Claims        []string                      `json:"claims"`
	ClaimBindings map[string]pkcs11ClaimBinding `json:"claimBindings,omitempty"`
}

type pkcs11ClaimBinding struct {
	UID  types.UID `json:"uid"`
	Peer string    `json:"peer"`
}

func (r *FabricNetworkReconciler) recordPKCS11StateRetention(ctx context.Context, net *fabricopsv1alpha1.FabricNetwork) error {
	for _, org := range net.Spec.Orgs {
		if err := r.recordOrgPKCS11StateRetention(ctx, net, org); err != nil {
			return err
		}
	}
	return nil
}

func (r *FabricNetworkReconciler) recordOrgPKCS11StateRetention(ctx context.Context, net *fabricopsv1alpha1.FabricNetwork, org fabricopsv1alpha1.Org) error {
	return r.updateOrgPKCS11StateRetention(ctx, net, org, nil)
}

func (r *FabricNetworkReconciler) updateOrgPKCS11StateRetention(ctx context.Context, net *fabricopsv1alpha1.FabricNetwork, org fabricopsv1alpha1.Org, bindings map[string]pkcs11ClaimBinding) error {
	if org.Peer == nil || org.Peer.MSPSigning == nil || org.Peer.MSPSigning.PKCS11 == nil || org.Peer.MSPSigning.PKCS11.Runtime == nil {
		return nil
	}
	states := org.Peer.MSPSigning.PKCS11.Runtime.StateClaims
	if len(states) == 0 {
		return nil
	}
	var namespace corev1.Namespace
	if err := r.Get(ctx, client.ObjectKey{Name: orgNamespaceName(net, org)}, &namespace); err != nil {
		if len(bindings) > 0 {
			return err
		}
		return client.IgnoreNotFound(err)
	}
	return r.updateObjectWithRetry(ctx, &namespace, func(object client.Object) (bool, error) {
		current := object.(*corev1.Namespace)
		if net.UID == "" || current.UID == "" || current.UID != namespace.UID ||
			!namespaceOwnedByFabricNetwork(*current, net, org) || current.Annotations[annotationFabricNetworkUID] != string(net.UID) {
			return false, fmt.Errorf("PKCS11 state retention requires matching network and Namespace ownership UIDs")
		}
		record := pkcs11StateRetention{Version: 1, NetworkUID: net.UID, NamespaceUID: current.UID}
		previous, exists := current.Annotations[annotationPKCS11StateRetention]
		if exists {
			record = pkcs11StateRetention{}
			decoder := json.NewDecoder(strings.NewReader(previous))
			decoder.DisallowUnknownFields()
			var trailing any
			if err := decoder.Decode(&record); err != nil || decoder.Decode(&trailing) != io.EOF ||
				record.Version != 1 || record.NetworkUID != net.UID || record.NamespaceUID != current.UID || len(record.Claims) == 0 {
				return false, fmt.Errorf("PKCS11 state retention record requires manual review; refusing to overwrite it")
			}
		}
		for name, binding := range record.ClaimBindings {
			if !slices.Contains(record.Claims, name) || binding.UID == "" || len(validation.IsDNS1123Label(binding.Peer)) != 0 {
				return false, fmt.Errorf("PKCS11 claim retention binding requires manual review; refusing to overwrite it")
			}
		}
		for _, state := range states {
			record.Claims = append(record.Claims, state.ClaimName)
		}
		for _, claim := range record.Claims {
			if len(validation.IsDNS1123Subdomain(claim)) != 0 {
				return false, fmt.Errorf("PKCS11 state retention requires valid claim names; refusing to change the record")
			}
		}
		for name, binding := range bindings {
			if previous, exists := record.ClaimBindings[name]; exists && previous != binding {
				return false, fmt.Errorf("PKCS11 token claim UID or peer assignment changed; explicit recovery review is required")
			}
			if record.ClaimBindings == nil {
				record.ClaimBindings = map[string]pkcs11ClaimBinding{}
			}
			record.ClaimBindings[name] = binding
		}
		slices.Sort(record.Claims)
		record.Claims = slices.Compact(record.Claims)
		encoded, err := json.Marshal(record)
		if err != nil {
			return false, err
		}
		if exists && previous == string(encoded) {
			return false, nil
		}
		current.Annotations[annotationPKCS11StateRetention] = string(encoded)
		return true, nil
	})
}
