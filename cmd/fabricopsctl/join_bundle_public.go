// Copyright contributors to FabricOps. SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"

	corev1 "k8s.io/api/core/v1"
	kubeyaml "k8s.io/apimachinery/pkg/util/yaml"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

// Membership artifacts describe public trust, not a local signing provider.
// Do not return parser errors: unknown YAML keys may themselves be credentials.
func validateJoinBundlePublicValue(value string, config bool) error {
	if strings.Contains(strings.ToUpper(value), "PRIVATE KEY-----") {
		return fmt.Errorf("join bundle public artifacts must not contain private key PEM")
	}
	if config {
		reader := kubeyaml.NewYAMLReader(bufio.NewReader(strings.NewReader(value)))
		document, err := reader.Read()
		if err != nil && err != io.EOF {
			return fmt.Errorf("join bundle MSP config is not a single public YAML document")
		}
		if _, err := reader.Read(); err != io.EOF {
			return fmt.Errorf("join bundle MSP config is not a single public YAML document")
		}
		var public joinBundleMSPConfigFile
		if err := yaml.UnmarshalStrict(document, &public); err != nil {
			return fmt.Errorf("join bundle MSP config must contain only public NodeOUs settings; " +
				"local signer/provider configuration is unsupported")
		}
	}
	return nil
}

func validateJoinBundlePublicArtifacts(bundle joinBundle) error {
	if err := validateJoinBundlePublicValue(bundle.Exported.AdminMSP.ConfigYAML, true); err != nil {
		return err
	}
	values := []string{bundle.Exported.AdminMSP.CACertPEM, bundle.Exported.AdminMSP.TLSCACertPEM}
	roots := []*joinBundleTLSRoot{bundle.Exported.AdminTLS}
	for _, peer := range bundle.Exported.Peers {
		roots = append(roots, peer.TLSRoot)
	}
	for _, orderer := range bundle.Orderers {
		roots = append(roots, orderer.TLSRoot)
	}
	for _, root := range roots {
		if root != nil {
			values = append(values, root.CACertPEM)
		}
	}
	for _, value := range values {
		if err := validateJoinBundlePublicValue(value, false); err != nil {
			return err
		}
	}
	return nil
}

func loadJoinBundlePublicSecret(ctx context.Context, client ctrlclient.Client,
	ref joinBundleObjectRef, key string, config bool) (string, error) {
	var secret corev1.Secret
	if err := client.Get(ctx, ctrlclient.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, &secret); err != nil {
		return "", err
	}
	if backupCustodyMetadata(&secret) {
		return "", fmt.Errorf("join bundle admin/TLS sources must not use external-custody signing artifacts")
	}
	value := strings.TrimSpace(string(secret.Data[key]))
	if value == "" {
		return "", fmt.Errorf("secret %s/%s is missing %s", ref.Namespace, ref.Name, key)
	}
	if err := validateJoinBundlePublicValue(value, config); err != nil {
		return "", err
	}
	return value, nil
}
