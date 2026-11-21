//go:build ignore

// Copyright contributors to FabricOps. SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"
)

func TestCapabilityContract(t *testing.T) {
	if err := validateRequest("peer", "1", "3.1.0", fabricRevision, caRevision); err != nil {
		t.Fatal(err)
	}
	for _, input := range [][5]string{
		{"peer", "", "3.1.0", fabricRevision, caRevision},
		{"peer", "2", "3.1.0", fabricRevision, caRevision},
		{"peer", "1", "2.5.14", fabricRevision, caRevision},
		{"peer", "1", "3.1.1", fabricRevision, caRevision},
		{"peer", "1", "v3.1.0", fabricRevision, caRevision},
		{"peer", "1", "3.1.0", "different-source", caRevision},
		{"enrollment", "1", "3.1.0", fabricRevision, "different-source"},
		{"orderer", "1", "3.1.0", fabricRevision, caRevision},
	} {
		if err := validateRequest(input[0], input[1], input[2], input[3], input[4]); err == nil {
			t.Fatal("accepted unsupported runtime contract")
		}
	}
}

func TestActualBinaryCapabilities(t *testing.T) {
	info := &debug.BuildInfo{Path: "github.com/hyperledger/fabric/cmd/peer", Settings: []debug.BuildSetting{{Key: "-tags", Value: "pkcs11"}, {Key: "CGO_ENABLED", Value: "1"}}}
	if err := validateBuild(info, info.Path); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*debug.BuildInfo){
		"software build":  func(i *debug.BuildInfo) { i.Settings[0].Value = "other" },
		"substring tag":   func(i *debug.BuildInfo) { i.Settings[0].Value = "notpkcs11" },
		"CGO disabled":    func(i *debug.BuildInfo) { i.Settings[1].Value = "0" },
		"other component": func(i *debug.BuildInfo) { i.Path = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			copy := *info
			copy.Settings = append([]debug.BuildSetting(nil), info.Settings...)
			change(&copy)
			if err := validateBuild(&copy, info.Path); err == nil {
				t.Fatal("accepted incompatible binary")
			}
		})
	}
	path := filepath.Join(t.TempDir(), "component")
	data := []byte("known component")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(data)
	if err := verifyArtifact(path, hex.EncodeToString(hash[:])); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("replaced component"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := verifyArtifact(path, hex.EncodeToString(hash[:])); err == nil {
		t.Fatal("accepted a replaced artifact")
	}
	if err := verifyArtifact(path, ""); err == nil {
		t.Fatal("accepted missing image build binding")
	}
}

func TestVersionAndOutputBounds(t *testing.T) {
	if err := validateVersion("peer", "peer:\n Version: 3.1.0\n Commit SHA: "+fabricRevision+"\n"); err != nil {
		t.Fatal(err)
	}
	if err := validateVersion("enrollment", "fabric-ca-client:\n Version: 1.5.15\n"); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"Version: latest", "Version: 2.5.14", "Version: 3.1.0\nCommit SHA: wrong", "Version: 3.1.0\nVersion: 3.1.0\nCommit SHA: " + fabricRevision} {
		if err := validateVersion("peer", value); err == nil {
			t.Fatal("accepted version skew")
		}
	}
	buffer := &boundedVersionOutput{}
	if _, err := buffer.Write(make([]byte, 8193)); err == nil || buffer.Len() != 0 {
		t.Fatal("accepted unbounded version output")
	}
	if _, err := io.Copy(buffer, io.LimitReader(strings.NewReader(strings.Repeat("x", 8193)), 8193)); err == nil || buffer.Len() > 8192 {
		t.Fatal("stream copying bypassed output bounds")
	}
}
