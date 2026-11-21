//go:build ignore

// Copyright contributors to FabricOps. SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

func main() {
	if len(os.Args) < 3 {
		panic("expected peer input/output paths or client output path")
	}
	library, label, pin := os.Getenv("FABRICOPS_PKCS11_LIBRARY"), os.Getenv("FABRICOPS_PKCS11_TOKEN_LABEL"), os.Getenv("FABRICOPS_PKCS11_PIN")
	if library == "" || label == "" || pin == "" {
		panic("required PKCS11 configuration is missing")
	}
	bccsp := map[string]any{"Default": "PKCS11", "PKCS11": map[string]any{
		"Library": library, "Label": label, "Pin": pin,
		"Hash": "SHA2", "Security": 256, "SoftwareVerify": true, "Immutable": false,
	}}
	var config map[string]any
	var output string
	switch {
	case os.Args[1] == "peer" && len(os.Args) == 4:
		data, err := os.ReadFile(os.Args[2])
		if err != nil {
			panic(err)
		}
		if err := yaml.Unmarshal(data, &config); err != nil {
			panic(err)
		}
		peer, ok := config["peer"].(map[string]any)
		if !ok {
			panic("peer configuration is missing")
		}
		peer["BCCSP"] = bccsp
		output = os.Args[3]
	case os.Args[1] == "client" && len(os.Args) == 3:
		config = map[string]any{"bccsp": bccsp, "debug": false, "loglevel": "warning"}
		output = os.Args[2]
	default:
		panic("expected peer input/output paths or client output path")
	}
	data, err := yaml.Marshal(config)
	if err != nil {
		panic(err)
	}
	file, err := os.CreateTemp(filepath.Dir(output), ".pkcs11-*.yaml")
	if err != nil {
		panic(err)
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(data); err != nil {
		file.Close()
		panic(err)
	}
	if err := file.Close(); err != nil {
		panic(err)
	}
	if err := os.Rename(file.Name(), output); err != nil {
		panic(err)
	}
}
