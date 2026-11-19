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
	"fmt"
	"path"
	"strings"
	"unicode"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
	"k8s.io/apimachinery/pkg/util/validation"
)

// This selects artifact shape only; it does not prove provider availability.
func workloadMSPSecretKind(org fabricopsv1alpha1.Org, component string) string {
	if component == componentPeer && org.Peer != nil && org.Peer.MSPSigning != nil && org.Peer.MSPSigning.Provider == "PKCS11" {
		return secretKindPeerPKCS11MSP
	}
	return secretKindMSP
}

func validatePeerMSPSigning(org fabricopsv1alpha1.Org) []string {
	if org.Peer == nil || org.Peer.MSPSigning == nil {
		return nil
	}
	config := org.Peer.MSPSigning
	prefix := fmt.Sprintf("org %q peer.mspSigning", org.Organization.Name)
	var problems []string
	if config.Provider != "Secret" && config.Provider != "PKCS11" {
		problems = append(problems, prefix+".provider must be Secret or PKCS11")
	}
	if config.Provider != "PKCS11" {
		if config.PKCS11 != nil {
			problems = append(problems, prefix+".pkcs11 must be omitted unless provider is PKCS11")
		}
		return problems
	}
	// No image tag or user assertion proves provider capability. Keep this gate
	// closed until enrollment and runtime wiring have passed integration tests.
	problems = append(problems, prefix+".provider PKCS11 is unavailable: PKCS#11 enrollment and peer runtime support are not enabled; software fallback is disabled")
	provider := config.PKCS11
	if provider == nil {
		return append(problems, prefix+".pkcs11 is required")
	}
	if !path.IsAbs(provider.Library) || path.Clean(provider.Library) != provider.Library ||
		provider.Library == "/" || len(provider.Library) > 1024 || strings.ContainsFunc(provider.Library, unicode.IsControl) {
		problems = append(problems, prefix+".pkcs11.library must be a clean absolute module path")
	}
	if strings.TrimSpace(provider.TokenLabel) == "" || len(provider.TokenLabel) > 32 || strings.ContainsFunc(provider.TokenLabel, unicode.IsControl) {
		problems = append(problems, prefix+".pkcs11.tokenLabel must be a nonempty token label of at most 32 bytes without control characters")
	}
	selector := provider.PINSecretKeyRef
	if len(validation.IsDNS1123Subdomain(selector.Name)) != 0 || len(validation.IsConfigMapKey(selector.Key)) != 0 ||
		(selector.Optional != nil && *selector.Optional) {
		problems = append(problems, prefix+".pkcs11.pinSecretKeyRef must name a required org-local Secret key")
	}
	for _, problem := range validatePKCS11Runtime(org) {
		problems = append(problems, prefix+".pkcs11 "+problem)
	}
	return problems
}
