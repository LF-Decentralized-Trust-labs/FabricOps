//go:build ignore

// Copyright contributors to FabricOps. SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime/debug"
	"strings"
	"time"
	"unicode"
)

const (
	fabricRevision = "8f08391d840c1b105c6b62e8917687e81937695e"
	caRevision     = "256fdbeecf05c19b5cdf30e083d0e61d5ebd1c57"
)

// Build-stage bindings detect accidental binary/helper replacement. They are
// not image signatures, provider attestation or a malicious-image boundary.
var builtFabricRevision, builtCARevision string
var peerDigest, caDigest, rendererDigest, readinessDigest, enrollerDigest string

func main() {
	if len(os.Args) != 2 || checkCapability(os.Args[1]) != nil {
		fmt.Fprintln(os.Stderr, "PKCS11 runtime capability check failed")
		os.Exit(1)
	}
}

func validateRequest(role, abi, version, fabricSource, caSource string) error {
	if (role != "peer" && role != "enrollment") || abi != "1" || version != "3.1.0" || fabricSource != fabricRevision || caSource != caRevision {
		return errors.New("unsupported PKCS11 runtime contract")
	}
	return nil
}

func validateBuild(info *debug.BuildInfo, path string) error {
	if info == nil || info.Path != path {
		return errors.New("unexpected component binary")
	}
	pkcs11, cgo := false, false
	for _, setting := range info.Settings {
		if setting.Key == "CGO_ENABLED" {
			cgo = setting.Value == "1"
		}
		if setting.Key == "-tags" {
			for _, tag := range strings.FieldsFunc(setting.Value, func(r rune) bool { return r == ',' || unicode.IsSpace(r) }) {
				if tag == "pkcs11" {
					pkcs11 = true
				}
			}
		}
	}
	if !pkcs11 || !cgo {
		return errors.New("component lacks native PKCS11 support")
	}
	return nil
}

func verifyArtifact(path, expected string) error {
	if len(expected) != 64 {
		return errors.New("missing artifact binding")
	}
	file, err := os.Open(path)
	if err != nil {
		return errors.New("runtime artifact is unavailable")
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil || hex.EncodeToString(hash.Sum(nil)) != expected {
		return errors.New("runtime artifact does not match image build")
	}
	return nil
}

func checkCapability(role string) error {
	if err := validateRequest(role, os.Getenv("FABRICOPS_PKCS11_RUNTIME_ABI"), os.Getenv("FABRICOPS_PKCS11_FABRIC_VERSION"), builtFabricRevision, builtCARevision); err != nil {
		return err
	}
	component, module, digest := "/usr/local/bin/peer", "github.com/hyperledger/fabric/cmd/peer", peerDigest
	artifacts := map[string]string{"/usr/local/bin/fabricops-pkcs11-config": rendererDigest}
	if role == "enrollment" {
		component, module, digest = "/usr/local/bin/fabric-ca-client", "github.com/hyperledger/fabric-ca/cmd/fabric-ca-client", caDigest
		artifacts["/usr/local/bin/fabricops-enroll-peer"] = enrollerDigest
	} else {
		artifacts["/usr/local/bin/fabricops-pkcs11-ready"] = readinessDigest
	}
	artifacts[component] = digest
	for path, expected := range artifacts {
		if err := verifyArtifact(path, expected); err != nil {
			return err
		}
	}
	info, err := buildinfo.ReadFile(component)
	if err != nil {
		return errors.New("component build information is unavailable")
	}
	if err := validateBuild(info, module); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, component, "version")
	// Version inspection has no reason to inherit PINs, enrollment credentials,
	// provider configuration, or caller-controlled Fabric debug settings.
	command.Env = []string{"PATH=/usr/local/bin:/usr/bin:/bin", "HOME=/nonexistent", "FABRIC_LOGGING_SPEC=error"}
	command.Dir = "/"
	output := &boundedVersionOutput{}
	command.Stdout, command.Stderr = output, output
	if err := command.Run(); err != nil {
		return errors.New("component version inspection failed")
	}
	return validateVersion(role, output.String())
}

type boundedVersionOutput struct{ buffer bytes.Buffer }

func (b *boundedVersionOutput) Len() int       { return b.buffer.Len() }
func (b *boundedVersionOutput) String() string { return b.buffer.String() }

func (b *boundedVersionOutput) Write(data []byte) (int, error) {
	if b.Len()+len(data) > 8192 {
		return 0, errors.New("version output exceeds limit")
	}
	return b.buffer.Write(data)
}

func validateVersion(role, output string) error {
	fields := map[string]string{}
	for _, line := range strings.Split(output, "\n") {
		key, value, ok := strings.Cut(line, ":")
		key = strings.TrimSpace(key)
		if !ok || (key != "Version" && key != "Commit SHA") {
			continue
		}
		if _, duplicate := fields[key]; duplicate {
			return errors.New("ambiguous version output")
		}
		fields[key] = strings.TrimSpace(value)
	}
	if role == "peer" && fields["Version"] == "3.1.0" && fields["Commit SHA"] == fabricRevision {
		return nil
	}
	if role == "enrollment" && fields["Version"] == "1.5.15" {
		return nil
	}
	return errors.New("component version does not match runtime contract")
}
