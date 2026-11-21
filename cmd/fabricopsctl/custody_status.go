// Copyright contributors to FabricOps. SPDX-License-Identifier: Apache-2.0

package main

import (
	"io"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

// This is requested configuration, not observed runtime evidence. Never inspect
// provider fields or interpolate unknown provider values into diagnostics.
func printPeerMSPCustody(out io.Writer, orgs []fabricopsv1alpha1.Org) {
	header := false
	for _, org := range orgs {
		if org.Peer == nil || org.Peer.MSPSigning == nil {
			continue
		}
		signing := org.Peer.MSPSigning
		if signing.Provider == "Secret" && signing.PKCS11 == nil {
			continue
		}
		if !header {
			printLine(out)
			printLine(out, "Peer MSP custody (requested configuration):")
			header = true
		}
		name := org.Organization.Name
		if len(name) > 63 {
			name = name[:63] + "..."
		}
		if signing.Provider == "PKCS11" {
			printf(out, "- org=%q provider=PKCS11 state=Gated\n", name)
		} else {
			printf(out, "- org=%q provider=Invalid state=Blocked\n", name)
		}
	}
	if header {
		printLine(out, "  PKCS11 is not enabled in either public API; software fallback is disabled.")
		printLine(out, "  Action: Review peer.mspSigning and controller conditions/events; activation remains gated.")
		printLine(out, "  Do not delete token claims or switch providers to bypass the gate; "+
			"preserve local credentials/state.")
		printLine(out, "  This is configuration guidance, not runtime health or hardware attestation.")
	}
}
