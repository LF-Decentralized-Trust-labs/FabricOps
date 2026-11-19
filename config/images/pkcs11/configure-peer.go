//go:build ignore

// Copyright contributors to FabricOps. SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"

	"gopkg.in/yaml.v3"
)

func main() {
	if len(os.Args) != 3 {
		panic("expected input and output core configuration paths")
	}
	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	var config map[string]any
	if err := yaml.Unmarshal(data, &config); err != nil {
		panic(err)
	}
	peer := config["peer"].(map[string]any)
	bccsp := peer["BCCSP"].(map[string]any)
	bccsp["Default"] = "PKCS11"
	library, label, pin := os.Getenv("CORE_PEER_BCCSP_PKCS11_LIBRARY"), os.Getenv("CORE_PEER_BCCSP_PKCS11_LABEL"), os.Getenv("CORE_PEER_BCCSP_PKCS11_PIN")
	if library == "" || label == "" || pin == "" {
		panic("required PKCS11 configuration is missing")
	}
	bccsp["PKCS11"] = map[string]any{
		"Library": library, "Label": label, "Pin": pin,
		"Hash": "SHA2", "Security": 256, "SoftwareVerify": true, "Immutable": false,
	}
	data, err = yaml.Marshal(config)
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(os.Args[2], data, 0o600); err != nil {
		panic(err)
	}
}
