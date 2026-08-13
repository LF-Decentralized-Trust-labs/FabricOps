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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	stdlibnet "net"
	"os"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

const (
	joinBundleAPIVersion         = "fabricops.io/v1alpha1"
	joinBundleKind               = "FabricNetworkJoinBundle"
	joinBundlePlanKind           = "FabricNetworkJoinPlan"
	joinBundleUpdateRecipeKind   = "FabricNetworkJoinConfigUpdateRecipe"
	joinBundleOutputJSON         = "json"
	joinBundleOutputScript       = "script"
	defaultJoinBundleUpdateDir   = "fabricops-join-update"
	joinBundleUpdateScriptHeader = `#!/usr/bin/env bash
set -euo pipefail
`
)

type joinBundleOptions struct {
	kube       kubeOptions
	org        string
	output     string
	outputPath string
}

type joinBundleParticipantOptions struct {
	kube       kubeOptions
	output     string
	outputPath string
}

type joinBundleValidateOptions struct {
	output string
}

type joinBundlePlanOptions struct {
	channels   stringListFlag
	output     string
	outputPath string
}

type joinBundleRenderOrgOptions struct {
	channel    string
	output     string
	outputPath string
}

type joinBundleRenderUpdateOptions struct {
	channel    string
	orderer    string
	output     string
	outputPath string
	workDir    string
}

type joinBundleValidationResult struct {
	Valid      bool   `json:"valid"`
	Network    string `json:"network"`
	Namespace  string `json:"namespace"`
	Org        string `json:"org"`
	Peers      int    `json:"peers"`
	Orderers   int    `json:"orderers"`
	Channels   int    `json:"channels"`
	Chaincodes int    `json:"chaincodes"`
}

type joinBundlePlan struct {
	APIVersion string                  `json:"apiVersion"`
	Kind       string                  `json:"kind"`
	Network    joinBundleNetwork       `json:"network"`
	Org        joinBundlePlanOrg       `json:"org"`
	Channels   []joinBundlePlanChannel `json:"channels"`
}

type joinBundlePlanOrg struct {
	Name      string            `json:"name"`
	MSPID     string            `json:"mspID"`
	Domain    string            `json:"domain,omitempty"`
	Namespace string            `json:"namespace"`
	MSP       joinBundlePlanMSP `json:"msp"`
	Peers     int               `json:"peers"`
}

type joinBundlePlanMSP struct {
	ConfigYAML   bool `json:"configYAML"`
	CACertPEM    bool `json:"caCertPEM"`
	TLSCACertPEM bool `json:"tlsCACertPEM"`
}

type joinBundlePlanChannel struct {
	Name               string                       `json:"name"`
	Peers              []joinBundlePeerRef          `json:"peers,omitempty"`
	AnchorPeers        []joinBundleAnchorPeer       `json:"anchorPeers,omitempty"`
	Membership         *joinBundleChannelMembership `json:"membership,omitempty"`
	Chaincodes         []joinBundlePlanChaincode    `json:"chaincodes,omitempty"`
	FounderActions     []joinBundlePlanAction       `json:"founderActions"`
	ParticipantActions []joinBundlePlanAction       `json:"participantActions"`
}

type joinBundlePlanChaincode struct {
	Name              string `json:"name"`
	Version           string `json:"version"`
	PackageLabel      string `json:"packageLabel"`
	Sequence          int32  `json:"sequence"`
	EndorsementPolicy string `json:"endorsementPolicy,omitempty"`
	InitRequired      bool   `json:"initRequired,omitempty"`
}

type joinBundlePlanAction struct {
	Name    string `json:"name"`
	Summary string `json:"summary"`
}

type joinBundleConfigUpdateRecipe struct {
	APIVersion              string                        `json:"apiVersion"`
	Kind                    string                        `json:"kind"`
	Network                 joinBundleNetwork             `json:"network"`
	Channel                 string                        `json:"channel"`
	Org                     joinBundleConfigUpdateOrg     `json:"org"`
	Orderer                 joinBundleConfigUpdateOrderer `json:"orderer"`
	Membership              *joinBundleChannelMembership  `json:"membership,omitempty"`
	WorkDir                 string                        `json:"workDir"`
	Files                   joinBundleConfigUpdateFiles   `json:"files"`
	RequiredTools           []string                      `json:"requiredTools"`
	ApplicationOrg          map[string]any                `json:"applicationOrg"`
	OrdererTLSCACertPEM     string                        `json:"ordererTLSCACertPEM,omitempty"`
	FounderAdminEnv         []string                      `json:"founderAdminEnv"`
	InspectBeforeSubmitting []string                      `json:"inspectBeforeSubmitting"`
}

type joinBundleConfigUpdateOrg struct {
	Name  string `json:"name"`
	MSPID string `json:"mspID"`
}

type joinBundleConfigUpdateOrderer struct {
	Org                 string `json:"org"`
	Name                string `json:"name"`
	Address             string `json:"address"`
	TLSHostnameOverride string `json:"tlsHostnameOverride,omitempty"`
}

type joinBundleConfigUpdateFiles struct {
	ApplicationOrgJSON       string `json:"applicationOrgJSON"`
	OrdererTLSCACert         string `json:"ordererTLSCACert,omitempty"`
	ConfigBlockPB            string `json:"configBlockPB"`
	ConfigBlockJSON          string `json:"configBlockJSON"`
	ConfigJSON               string `json:"configJSON"`
	ModifiedConfigJSON       string `json:"modifiedConfigJSON"`
	ConfigPB                 string `json:"configPB"`
	ModifiedConfigPB         string `json:"modifiedConfigPB"`
	ConfigUpdatePB           string `json:"configUpdatePB"`
	ConfigUpdateJSON         string `json:"configUpdateJSON"`
	ConfigUpdateEnvelopeJSON string `json:"configUpdateEnvelopeJSON"`
	ConfigUpdateEnvelopePB   string `json:"configUpdateEnvelopePB"`
}

type joinBundleMSPConfigFile struct {
	NodeOUs joinBundleNodeOUs `json:"NodeOUs"`
}

type joinBundleNodeOUs struct {
	Enable              bool                       `json:"Enable"`
	ClientOUIdentifier  joinBundleOUIdentifierYAML `json:"ClientOUIdentifier"`
	PeerOUIdentifier    joinBundleOUIdentifierYAML `json:"PeerOUIdentifier"`
	AdminOUIdentifier   joinBundleOUIdentifierYAML `json:"AdminOUIdentifier"`
	OrdererOUIdentifier joinBundleOUIdentifierYAML `json:"OrdererOUIdentifier"`
}

type joinBundleOUIdentifierYAML struct {
	Certificate                  string `json:"Certificate"`
	OrganizationalUnitIdentifier string `json:"OrganizationalUnitIdentifier"`
}

type joinBundle struct {
	APIVersion string                `json:"apiVersion"`
	Kind       string                `json:"kind"`
	Network    joinBundleNetwork     `json:"network"`
	Exported   joinBundleExportedOrg `json:"exported"`
	Orderers   []joinBundleOrderer   `json:"orderers,omitempty"`
	Channels   []joinBundleChannel   `json:"channels,omitempty"`
	Chaincodes []joinBundleChaincode `json:"chaincodes,omitempty"`
}

type joinBundleNetwork struct {
	Name          string `json:"name"`
	Namespace     string `json:"namespace"`
	FabricVersion string `json:"fabricVersion"`
	TLS           bool   `json:"tls"`
}

type joinBundleExportedOrg struct {
	Name              string                 `json:"name"`
	MSPID             string                 `json:"mspID"`
	Domain            string                 `json:"domain,omitempty"`
	Namespace         string                 `json:"namespace"`
	CAEndpoint        string                 `json:"caEndpoint,omitempty"`
	ConnectionProfile *joinBundleObjectRef   `json:"connectionProfile,omitempty"`
	AdminMSP          joinBundlePublicMSP    `json:"adminMSP"`
	AdminTLS          *joinBundleTLSRoot     `json:"adminTLS,omitempty"`
	Peers             []joinBundlePeer       `json:"peers,omitempty"`
	AnchorPeers       []joinBundleAnchorPeer `json:"anchorPeers,omitempty"`
	Channels          []joinBundleOrgChannel `json:"channels,omitempty"`
}

type joinBundleObjectRef struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

type joinBundlePublicMSP struct {
	SecretRef    joinBundleObjectRef `json:"secretRef"`
	ConfigYAML   string              `json:"configYAML,omitempty"`
	CACertPEM    string              `json:"caCertPEM"`
	TLSCACertPEM string              `json:"tlsCACertPEM,omitempty"`
}

type joinBundleTLSRoot struct {
	SecretRef    *joinBundleObjectRef `json:"secretRef,omitempty"`
	ConfigMapRef *joinBundleObjectRef `json:"configMapRef,omitempty"`
	CACertPEM    string               `json:"caCertPEM"`
}

type joinBundlePeer struct {
	Name                string             `json:"name"`
	Address             string             `json:"address,omitempty"`
	TLSHostnameOverride string             `json:"tlsHostnameOverride,omitempty"`
	ChaincodeAddress    string             `json:"chaincodeAddress,omitempty"`
	OperationsAddress   string             `json:"operationsAddress,omitempty"`
	TLSRoot             *joinBundleTLSRoot `json:"tlsRoot,omitempty"`
}

type joinBundleOrderer struct {
	Org                 string             `json:"org"`
	Name                string             `json:"name"`
	ClientAddress       string             `json:"clientAddress,omitempty"`
	TLSHostnameOverride string             `json:"tlsHostnameOverride,omitempty"`
	AdminAddress        string             `json:"adminAddress,omitempty"`
	OperationsAddress   string             `json:"operationsAddress,omitempty"`
	TLSRoot             *joinBundleTLSRoot `json:"tlsRoot,omitempty"`
}

type joinBundleChannel struct {
	Name        string                       `json:"name"`
	Peers       []joinBundlePeerRef          `json:"peers,omitempty"`
	AnchorPeers []joinBundleAnchorPeer       `json:"anchorPeers,omitempty"`
	Membership  *joinBundleChannelMembership `json:"membership,omitempty"`
}

type joinBundleChannelMembership struct {
	ApplicationPolicy    string   `json:"applicationPolicy,omitempty"`
	RequiredSignerMSPIDs []string `json:"requiredSignerMSPIDs,omitempty"`
}

type joinBundleOrgChannel struct {
	Name        string                 `json:"name"`
	Peers       []string               `json:"peers,omitempty"`
	AnchorPeers []joinBundleAnchorPeer `json:"anchorPeers,omitempty"`
}

type joinBundlePeerRef struct {
	Org     string `json:"org"`
	Name    string `json:"name"`
	Address string `json:"address,omitempty"`
}

type joinBundleAnchorPeer struct {
	Org  string `json:"org"`
	Name string `json:"name"`
	Host string `json:"host"`
	Port int32  `json:"port"`
}

type joinBundleChaincode struct {
	Name                 string `json:"name"`
	Channel              string `json:"channel"`
	Version              string `json:"version"`
	Image                string `json:"image,omitempty"`
	PackageLabel         string `json:"packageLabel"`
	Sequence             int32  `json:"sequence"`
	EndorsementPolicy    string `json:"endorsementPolicy,omitempty"`
	InitRequired         bool   `json:"initRequired,omitempty"`
	CollectionConfigHash string `json:"collectionConfigHash,omitempty"`
	Committed            bool   `json:"committed"`
	Ready                bool   `json:"ready"`
}

func runJoinBundle(args []string, stdout, stderr io.Writer) error {
	if len(args) > 0 {
		switch args[0] {
		case "validate":
			return runJoinBundleValidate(args[1:], stdout, stderr)
		case "participant":
			return runJoinBundleParticipant(args[1:], stdout, stderr)
		case "plan":
			return runJoinBundlePlan(args[1:], stdout, stderr)
		case "render-org":
			return runJoinBundleRenderOrg(args[1:], stdout, stderr)
		case "render-update":
			return runJoinBundleRenderUpdate(args[1:], stdout, stderr)
		}
	}

	var options joinBundleOptions
	flags := flag.NewFlagSet("join-bundle", flag.ContinueOnError)
	flags.SetOutput(stderr)
	bindKubeFlags(flags, &options.kube)
	flags.StringVar(&options.org, "org", "", "Organization name to export")
	flags.StringVar(&options.output, "o", joinBundleOutputJSON, "Output format: json")
	flags.StringVar(&options.output, "output", joinBundleOutputJSON, "Output format: json")
	flags.StringVar(&options.outputPath, "out", "", "Write bundle to this file instead of stdout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		printLine(stderr, "Usage: fabricopsctl join-bundle --org <org> [flags] <fabricnetwork>")
		return errUsage
	}
	if strings.TrimSpace(options.org) == "" {
		return fmt.Errorf("--org is required")
	}
	if options.output != joinBundleOutputJSON {
		return fmt.Errorf("unsupported output format %q", options.output)
	}

	ctx := context.Background()
	network, err := getFabricNetwork(ctx, options.kube, flags.Arg(0))
	if err != nil {
		return err
	}
	client, err := newClient(options.kube)
	if err != nil {
		return err
	}
	bundle, err := buildJoinBundle(ctx, client, network, options.org)
	if err != nil {
		return err
	}
	contents, err := marshalJoinBundle(bundle)
	if err != nil {
		return err
	}

	return writeProfileOutput(stdout, options.outputPath, contents)
}

func runJoinBundleParticipant(args []string, stdout, stderr io.Writer) error {
	var options joinBundleParticipantOptions
	flags := flag.NewFlagSet("join-bundle participant", flag.ContinueOnError)
	flags.SetOutput(stderr)
	bindKubeFlags(flags, &options.kube)
	flags.StringVar(&options.output, "o", joinBundleOutputJSON, "Output format: json")
	flags.StringVar(&options.output, "output", joinBundleOutputJSON, "Output format: json")
	flags.StringVar(&options.outputPath, "out", "", "Write bundle to this file instead of stdout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		printLine(stderr, "Usage: fabricopsctl join-bundle participant [flags] <fabricparticipant>")
		return errUsage
	}
	if options.output != joinBundleOutputJSON {
		return fmt.Errorf("unsupported output format %q", options.output)
	}

	ctx := context.Background()
	client, err := newClient(options.kube)
	if err != nil {
		return err
	}
	participant := &fabricopsv1alpha1.FabricParticipant{}
	if err := client.Get(ctx, ctrlclient.ObjectKey{
		Namespace: options.kube.namespace,
		Name:      flags.Arg(0),
	}, participant); err != nil {
		return err
	}
	bundle, err := buildParticipantJoinBundle(ctx, client, participant)
	if err != nil {
		return err
	}
	contents, err := marshalJoinBundle(bundle)
	if err != nil {
		return err
	}

	return writeProfileOutput(stdout, options.outputPath, contents)
}

func runJoinBundleRenderOrg(args []string, stdout, stderr io.Writer) error {
	var options joinBundleRenderOrgOptions
	flags := flag.NewFlagSet("join-bundle render-org", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&options.channel, "channel", "", "Channel whose anchor peer entries should be rendered")
	flags.StringVar(&options.output, "o", joinBundleOutputJSON, "Output format: json")
	flags.StringVar(&options.outputPath, "out", "", "Write rendered org JSON to this file instead of stdout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		printLine(stderr, "Usage: fabricopsctl join-bundle render-org [flags] <bundle.json>")
		return errUsage
	}
	if options.output != joinBundleOutputJSON {
		return fmt.Errorf("unsupported output format %q", options.output)
	}

	contents, err := os.ReadFile(flags.Arg(0))
	if err != nil {
		return err
	}
	bundle, err := decodeJoinBundle(contents)
	if err != nil {
		return err
	}
	channel, err := selectJoinBundleRenderOrgChannel(bundle, options.channel)
	if err != nil {
		return err
	}
	rendered, err := buildJoinBundleApplicationOrgGroup(bundle, channel)
	if err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(rendered, "", "  ")
	if err != nil {
		return err
	}

	return writeProfileOutput(stdout, options.outputPath, string(encoded)+"\n")
}

func runJoinBundleRenderUpdate(args []string, stdout, stderr io.Writer) error {
	var options joinBundleRenderUpdateOptions
	flags := flag.NewFlagSet("join-bundle render-update", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&options.channel, "channel", "", "Channel to prepare a config update for")
	flags.StringVar(&options.orderer, "orderer", "", "Orderer name or org/name to target; defaults to the first orderer")
	flags.StringVar(&options.output, "o", joinBundleOutputScript, "Output format: script or json")
	flags.StringVar(&options.outputPath, "out", "", "Write rendered output to this file instead of stdout")
	flags.StringVar(&options.workDir, "workdir", "", "Working directory used by the generated script")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		printLine(stderr, "Usage: fabricopsctl join-bundle render-update [flags] <bundle.json>")
		return errUsage
	}

	contents, err := os.ReadFile(flags.Arg(0))
	if err != nil {
		return err
	}
	bundle, err := decodeJoinBundle(contents)
	if err != nil {
		return err
	}
	recipe, err := buildJoinBundleConfigUpdateRecipe(bundle, options)
	if err != nil {
		return err
	}

	switch options.output {
	case joinBundleOutputScript:
		script, err := renderJoinBundleConfigUpdateScript(recipe)
		if err != nil {
			return err
		}
		return writeProfileOutput(stdout, options.outputPath, script)
	case joinBundleOutputJSON:
		encoded, err := json.MarshalIndent(recipe, "", "  ")
		if err != nil {
			return err
		}
		return writeProfileOutput(stdout, options.outputPath, string(encoded)+"\n")
	default:
		return fmt.Errorf("unsupported output format %q", options.output)
	}
}

func runJoinBundlePlan(args []string, stdout, stderr io.Writer) error {
	var options joinBundlePlanOptions
	flags := flag.NewFlagSet("join-bundle plan", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Var(&options.channels, "channel", "Channel to include; repeat for multiple channels; defaults to all")
	flags.StringVar(&options.output, "o", operationOutputText, "Output format: text or json")
	flags.StringVar(&options.outputPath, "out", "", "Write plan to this file instead of stdout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		printLine(stderr, "Usage: fabricopsctl join-bundle plan [flags] <bundle.json>")
		return errUsage
	}

	contents, err := os.ReadFile(flags.Arg(0))
	if err != nil {
		return err
	}
	bundle, err := decodeJoinBundle(contents)
	if err != nil {
		return err
	}
	plan, err := buildJoinBundlePlan(bundle, options.channels)
	if err != nil {
		return err
	}

	switch options.output {
	case operationOutputText:
		return writeProfileOutput(stdout, options.outputPath, renderJoinBundlePlanText(plan))
	case operationOutputJSON:
		contents, err := marshalJoinBundlePlan(plan)
		if err != nil {
			return err
		}
		return writeProfileOutput(stdout, options.outputPath, contents)
	default:
		return fmt.Errorf("unsupported output format %q", options.output)
	}
}

func runJoinBundleValidate(args []string, stdout, stderr io.Writer) error {
	var options joinBundleValidateOptions
	flags := flag.NewFlagSet("join-bundle validate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&options.output, "o", operationOutputText, "Output format: text or json")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		printLine(stderr, "Usage: fabricopsctl join-bundle validate [flags] <bundle.json>")
		return errUsage
	}

	contents, err := os.ReadFile(flags.Arg(0))
	if err != nil {
		return err
	}
	bundle, err := decodeJoinBundle(contents)
	if err != nil {
		return err
	}
	result, err := validateJoinBundle(bundle)
	if err != nil {
		return err
	}

	switch options.output {
	case operationOutputText:
		printf(stdout, "Join bundle valid: %s/%s org %s\n", result.Namespace, result.Network, result.Org)
		printf(
			stdout,
			"Peers: %d, orderers: %d, channels: %d, chaincodes: %d\n",
			result.Peers,
			result.Orderers,
			result.Channels,
			result.Chaincodes,
		)
		return nil
	case operationOutputJSON:
		return writeJSON(stdout, result)
	default:
		return fmt.Errorf("unsupported output format %q", options.output)
	}
}

func buildJoinBundle(
	ctx context.Context,
	client ctrlclient.Client,
	network *fabricopsv1alpha1.FabricNetwork,
	orgName string,
) (joinBundle, error) {
	specOrg, orgStatus, err := selectJoinBundleOrg(network, orgName)
	if err != nil {
		return joinBundle{}, err
	}

	exported, err := buildJoinBundleExportedOrg(ctx, client, network, specOrg, orgStatus)
	if err != nil {
		return joinBundle{}, err
	}

	channelSet := map[string]struct{}{}
	channels, err := buildJoinBundleChannels(network, specOrg, orgStatus, channelSet)
	if err != nil {
		return joinBundle{}, err
	}
	exported.Channels = joinBundleOrgChannels(channels)
	exported.AnchorPeers = joinBundleOrgAnchorPeers(channels)

	orderers, err := buildJoinBundleOrderers(ctx, client, network)
	if err != nil {
		return joinBundle{}, err
	}

	return joinBundle{
		APIVersion: joinBundleAPIVersion,
		Kind:       joinBundleKind,
		Network: joinBundleNetwork{
			Name:          network.Name,
			Namespace:     network.Namespace,
			FabricVersion: network.Spec.Global.FabricVersion,
			TLS:           network.Spec.Global.TLS,
		},
		Exported:   exported,
		Orderers:   orderers,
		Channels:   channels,
		Chaincodes: buildJoinBundleChaincodes(network, channelSet),
	}, nil
}

func buildParticipantJoinBundle(
	ctx context.Context,
	client ctrlclient.Client,
	participant *fabricopsv1alpha1.FabricParticipant,
) (joinBundle, error) {
	specOrg, orgStatus, err := selectParticipantJoinBundleOrg(participant)
	if err != nil {
		return joinBundle{}, err
	}

	exported, err := buildParticipantJoinBundleExportedOrg(ctx, client, participant, specOrg, orgStatus)
	if err != nil {
		return joinBundle{}, err
	}

	channelSet := map[string]struct{}{}
	channels, err := buildParticipantJoinBundleChannels(participant, specOrg, orgStatus, channelSet)
	if err != nil {
		return joinBundle{}, err
	}
	exported.Channels = joinBundleOrgChannels(channels)
	exported.AnchorPeers = joinBundleOrgAnchorPeers(channels)

	orderers, err := buildParticipantJoinBundleOrderers(ctx, client, participant)
	if err != nil {
		return joinBundle{}, err
	}

	return joinBundle{
		APIVersion: joinBundleAPIVersion,
		Kind:       joinBundleKind,
		Network: joinBundleNetwork{
			Name:          participant.Spec.Network.Name,
			Namespace:     participant.Namespace,
			FabricVersion: participant.Spec.Global.FabricVersion,
			TLS:           participant.Spec.Global.TLS,
		},
		Exported:   exported,
		Orderers:   orderers,
		Channels:   channels,
		Chaincodes: buildParticipantJoinBundleChaincodes(participant, channelSet),
	}, nil
}

func marshalJoinBundle(bundle joinBundle) (string, error) {
	encoded, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		return "", err
	}
	return string(encoded) + "\n", nil
}

func marshalJoinBundlePlan(plan joinBundlePlan) (string, error) {
	encoded, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return "", err
	}
	return string(encoded) + "\n", nil
}

func decodeJoinBundle(contents []byte) (joinBundle, error) {
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()

	var bundle joinBundle
	if err := decoder.Decode(&bundle); err != nil {
		return joinBundle{}, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return joinBundle{}, fmt.Errorf("join bundle contains multiple JSON values")
	}
	return bundle, nil
}

func buildJoinBundlePlan(bundle joinBundle, channelFilters []string) (joinBundlePlan, error) {
	if _, err := validateJoinBundle(bundle); err != nil {
		return joinBundlePlan{}, err
	}

	channels, err := selectJoinBundlePlanChannels(bundle, channelFilters)
	if err != nil {
		return joinBundlePlan{}, err
	}
	if len(channels) == 0 {
		return joinBundlePlan{}, fmt.Errorf("join bundle does not contain channels to plan")
	}

	planChannels := make([]joinBundlePlanChannel, 0, len(channels))
	for _, channel := range channels {
		chaincodes := joinBundlePlanChaincodesForChannel(bundle.Chaincodes, channel.Name)
		planChannel := joinBundlePlanChannel{
			Name:               channel.Name,
			Peers:              channel.Peers,
			AnchorPeers:        channel.AnchorPeers,
			Membership:         copyJoinBundleChannelMembership(channel.Membership),
			Chaincodes:         chaincodes,
			FounderActions:     joinBundleFounderActions(bundle, channel),
			ParticipantActions: joinBundleParticipantActions(bundle, channel, chaincodes),
		}
		planChannels = append(planChannels, planChannel)
	}

	return joinBundlePlan{
		APIVersion: joinBundleAPIVersion,
		Kind:       joinBundlePlanKind,
		Network:    bundle.Network,
		Org: joinBundlePlanOrg{
			Name:      bundle.Exported.Name,
			MSPID:     bundle.Exported.MSPID,
			Domain:    bundle.Exported.Domain,
			Namespace: bundle.Exported.Namespace,
			MSP: joinBundlePlanMSP{
				ConfigYAML:   strings.TrimSpace(bundle.Exported.AdminMSP.ConfigYAML) != "",
				CACertPEM:    strings.TrimSpace(bundle.Exported.AdminMSP.CACertPEM) != "",
				TLSCACertPEM: strings.TrimSpace(bundle.Exported.AdminMSP.TLSCACertPEM) != "",
			},
			Peers: len(bundle.Exported.Peers),
		},
		Channels: planChannels,
	}, nil
}

func buildJoinBundleConfigUpdateRecipe(
	bundle joinBundle,
	options joinBundleRenderUpdateOptions,
) (joinBundleConfigUpdateRecipe, error) {
	channel, err := selectJoinBundleRenderOrgChannel(bundle, options.channel)
	if err != nil {
		return joinBundleConfigUpdateRecipe{}, err
	}
	orderer, err := selectJoinBundleConfigUpdateOrderer(bundle, options.orderer)
	if err != nil {
		return joinBundleConfigUpdateRecipe{}, err
	}
	applicationOrg, err := buildJoinBundleApplicationOrgGroup(bundle, channel)
	if err != nil {
		return joinBundleConfigUpdateRecipe{}, err
	}

	workDir := strings.TrimSpace(options.workDir)
	if workDir == "" {
		workDir = defaultJoinBundleUpdateWorkDir(channel.Name, bundle.Exported.Name)
	}
	files := joinBundleConfigUpdateFilesForChannel(channel.Name)
	ordererTLSRoot := ""
	tlsHostnameOverride := ""
	if bundle.Network.TLS {
		if orderer.TLSRoot == nil {
			return joinBundleConfigUpdateRecipe{}, fmt.Errorf(
				"orderer %s/%s is missing TLS root material",
				orderer.Org,
				orderer.Name,
			)
		}
		ordererTLSRoot = orderer.TLSRoot.CACertPEM
		tlsHostnameOverride = strings.TrimSpace(orderer.TLSHostnameOverride)
		if tlsHostnameOverride == "" {
			tlsHostnameOverride, err = joinBundleEndpointHost(orderer.ClientAddress)
			if err != nil {
				return joinBundleConfigUpdateRecipe{}, err
			}
		}
	} else {
		files.OrdererTLSCACert = ""
	}

	return joinBundleConfigUpdateRecipe{
		APIVersion: joinBundleAPIVersion,
		Kind:       joinBundleUpdateRecipeKind,
		Network:    bundle.Network,
		Channel:    channel.Name,
		Org: joinBundleConfigUpdateOrg{
			Name:  bundle.Exported.Name,
			MSPID: bundle.Exported.MSPID,
		},
		Orderer: joinBundleConfigUpdateOrderer{
			Org:                 orderer.Org,
			Name:                orderer.Name,
			Address:             orderer.ClientAddress,
			TLSHostnameOverride: tlsHostnameOverride,
		},
		Membership:          copyJoinBundleChannelMembership(channel.Membership),
		WorkDir:             workDir,
		Files:               files,
		RequiredTools:       []string{"peer", "configtxlator", "jq"},
		ApplicationOrg:      applicationOrg,
		OrdererTLSCACertPEM: ordererTLSRoot,
		FounderAdminEnv:     joinBundleFounderAdminEnv(bundle.Network.TLS),
		InspectBeforeSubmitting: []string{
			files.ApplicationOrgJSON,
			files.ModifiedConfigJSON,
			files.ConfigUpdateJSON,
			files.ConfigUpdateEnvelopeJSON,
		},
	}, nil
}

func selectJoinBundleConfigUpdateOrderer(
	bundle joinBundle,
	ordererName string,
) (joinBundleOrderer, error) {
	if strings.TrimSpace(ordererName) == "" {
		if len(bundle.Orderers) == 0 {
			return joinBundleOrderer{}, fmt.Errorf("join bundle does not contain orderer endpoints")
		}
		return bundle.Orderers[0], nil
	}
	for _, orderer := range bundle.Orderers {
		if strings.EqualFold(orderer.Name, ordererName) ||
			strings.EqualFold(orderer.Org+"/"+orderer.Name, ordererName) {
			return orderer, nil
		}
	}
	return joinBundleOrderer{}, fmt.Errorf("join bundle does not contain orderer %q", ordererName)
}

func renderJoinBundleConfigUpdateScript(recipe joinBundleConfigUpdateRecipe) (string, error) {
	applicationOrgJSON, err := json.MarshalIndent(recipe.ApplicationOrg, "", "  ")
	if err != nil {
		return "", err
	}

	var b strings.Builder
	b.WriteString(joinBundleUpdateScriptHeader)
	fmt.Fprintf(&b, "\nCHANNEL_NAME=%s\n", joinBundleShellQuote(recipe.Channel))
	fmt.Fprintf(&b, "JOIN_ORG_NAME=%s\n", joinBundleShellQuote(recipe.Org.Name))
	fmt.Fprintf(&b, "JOIN_MSP_ID=%s\n", joinBundleShellQuote(recipe.Org.MSPID))
	fmt.Fprintf(&b, "ORDERER_ADDRESS=%s\n", joinBundleShellQuote(recipe.Orderer.Address))
	if recipe.Network.TLS {
		fmt.Fprintf(&b, "ORDERER_TLS_HOSTNAME_OVERRIDE=%s\n", joinBundleShellQuote(recipe.Orderer.TLSHostnameOverride))
	}
	fmt.Fprintf(&b, "WORKDIR=${WORKDIR:-%s}\n\n", joinBundleShellQuote(recipe.WorkDir))

	b.WriteString(`require_tool() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required tool: $1" >&2
    exit 1
  }
}

`)
	for _, tool := range recipe.RequiredTools {
		fmt.Fprintf(&b, "require_tool %s\n", joinBundleShellQuote(tool))
	}
	b.WriteByte('\n')

	for _, envName := range recipe.FounderAdminEnv {
		fmt.Fprintf(&b, ": \"${%s:?set %s for the founder admin identity}\"\n", envName, envName)
	}
	if recipe.Network.TLS {
		b.WriteString("export CORE_PEER_TLS_ENABLED=${CORE_PEER_TLS_ENABLED:-true}\n")
	} else {
		b.WriteString("export CORE_PEER_TLS_ENABLED=${CORE_PEER_TLS_ENABLED:-false}\n")
	}
	b.WriteString("\nmkdir -p \"$WORKDIR\"\n\n")

	renderJoinBundleScriptFileVariables(&b, recipe.Files)
	fmt.Fprintf(
		&b,
		"\ncat > \"$APPLICATION_ORG_JSON\" <<'FABRICOPS_APPLICATION_ORG_JSON'\n%s\nFABRICOPS_APPLICATION_ORG_JSON\n",
		string(applicationOrgJSON),
	)
	if recipe.Network.TLS {
		fmt.Fprintf(
			&b,
			"\ncat > \"$ORDERER_TLS_CA\" <<'FABRICOPS_ORDERER_TLS_CA'\n%s\nFABRICOPS_ORDERER_TLS_CA\n",
			strings.TrimSpace(recipe.OrdererTLSCACertPEM),
		)
	}

	b.WriteString("\n")
	renderJoinBundleConfigUpdateCommands(&b, recipe)
	renderJoinBundleConfigUpdateNextSteps(&b, recipe)

	return b.String(), nil
}

func renderJoinBundleScriptFileVariables(out *strings.Builder, files joinBundleConfigUpdateFiles) {
	fmt.Fprintf(out, "APPLICATION_ORG_JSON=\"$WORKDIR/%s\"\n", files.ApplicationOrgJSON)
	if files.OrdererTLSCACert != "" {
		fmt.Fprintf(out, "ORDERER_TLS_CA=\"$WORKDIR/%s\"\n", files.OrdererTLSCACert)
	}
	fmt.Fprintf(out, "CONFIG_BLOCK_PB=\"$WORKDIR/%s\"\n", files.ConfigBlockPB)
	fmt.Fprintf(out, "CONFIG_BLOCK_JSON=\"$WORKDIR/%s\"\n", files.ConfigBlockJSON)
	fmt.Fprintf(out, "CONFIG_JSON=\"$WORKDIR/%s\"\n", files.ConfigJSON)
	fmt.Fprintf(out, "MODIFIED_CONFIG_JSON=\"$WORKDIR/%s\"\n", files.ModifiedConfigJSON)
	fmt.Fprintf(out, "CONFIG_PB=\"$WORKDIR/%s\"\n", files.ConfigPB)
	fmt.Fprintf(out, "MODIFIED_CONFIG_PB=\"$WORKDIR/%s\"\n", files.ModifiedConfigPB)
	fmt.Fprintf(out, "CONFIG_UPDATE_PB=\"$WORKDIR/%s\"\n", files.ConfigUpdatePB)
	fmt.Fprintf(out, "CONFIG_UPDATE_JSON=\"$WORKDIR/%s\"\n", files.ConfigUpdateJSON)
	fmt.Fprintf(out, "CONFIG_UPDATE_ENVELOPE_JSON=\"$WORKDIR/%s\"\n", files.ConfigUpdateEnvelopeJSON)
	fmt.Fprintf(out, "CONFIG_UPDATE_ENVELOPE_PB=\"$WORKDIR/%s\"\n", files.ConfigUpdateEnvelopePB)
}

func renderJoinBundleConfigUpdateCommands(out *strings.Builder, recipe joinBundleConfigUpdateRecipe) {
	fetchTLSFlags := joinBundleFetchConfigTLSFlags(recipe)
	fmt.Fprintf(
		out,
		"peer channel fetch config \"$CONFIG_BLOCK_PB\" -o \"$ORDERER_ADDRESS\" -c \"$CHANNEL_NAME\"%s\n\n",
		fetchTLSFlags,
	)
	out.WriteString("configtxlator proto_decode \\\n")
	out.WriteString("  --input \"$CONFIG_BLOCK_PB\" \\\n")
	out.WriteString("  --type common.Block \\\n")
	out.WriteString("  --output \"$CONFIG_BLOCK_JSON\"\n\n")
	out.WriteString("jq '.data.data[0].payload.data.config' \"$CONFIG_BLOCK_JSON\" > \"$CONFIG_JSON\"\n\n")
	out.WriteString("if jq -e --arg msp \"$JOIN_MSP_ID\" \\\n")
	out.WriteString("  '.channel_group.groups.Application.groups[$msp] != null' \\\n")
	out.WriteString("  \"$CONFIG_JSON\" >/dev/null; then\n")
	out.WriteString("  echo \"Application org $JOIN_MSP_ID already exists on channel $CHANNEL_NAME\" >&2\n")
	out.WriteString("  exit 1\n")
	out.WriteString("fi\n\n")
	out.WriteString("jq --slurpfile org \"$APPLICATION_ORG_JSON\" --arg msp \"$JOIN_MSP_ID\" \\\n")
	out.WriteString("  '.channel_group.groups.Application.groups[$msp] = $org[0]' \\\n")
	out.WriteString("  \"$CONFIG_JSON\" > \"$MODIFIED_CONFIG_JSON\"\n\n")
	out.WriteString("configtxlator proto_encode \\\n")
	out.WriteString("  --input \"$CONFIG_JSON\" \\\n")
	out.WriteString("  --type common.Config \\\n")
	out.WriteString("  --output \"$CONFIG_PB\"\n\n")
	out.WriteString("configtxlator proto_encode \\\n")
	out.WriteString("  --input \"$MODIFIED_CONFIG_JSON\" \\\n")
	out.WriteString("  --type common.Config \\\n")
	out.WriteString("  --output \"$MODIFIED_CONFIG_PB\"\n\n")
	out.WriteString("configtxlator compute_update \\\n")
	out.WriteString("  --channel_id \"$CHANNEL_NAME\" \\\n")
	out.WriteString("  --original \"$CONFIG_PB\" \\\n")
	out.WriteString("  --updated \"$MODIFIED_CONFIG_PB\" \\\n")
	out.WriteString("  --output \"$CONFIG_UPDATE_PB\"\n\n")
	out.WriteString("configtxlator proto_decode \\\n")
	out.WriteString("  --input \"$CONFIG_UPDATE_PB\" \\\n")
	out.WriteString("  --type common.ConfigUpdate \\\n")
	out.WriteString("  --output \"$CONFIG_UPDATE_JSON\"\n\n")
	out.WriteString("jq --arg channel \"$CHANNEL_NAME\" \\\n")
	out.WriteString("  '")
	out.WriteString("{\"payload\":{\"header\":{\"channel_header\":{\"channel_id\":$channel,\"type\":2}},")
	out.WriteString("\"data\":{\"config_update\":.}}}' \\\n")
	out.WriteString("  \"$CONFIG_UPDATE_JSON\" > \"$CONFIG_UPDATE_ENVELOPE_JSON\"\n\n")
	out.WriteString("configtxlator proto_encode \\\n")
	out.WriteString("  --input \"$CONFIG_UPDATE_ENVELOPE_JSON\" \\\n")
	out.WriteString("  --type common.Envelope \\\n")
	out.WriteString("  --output \"$CONFIG_UPDATE_ENVELOPE_PB\"\n")
}

func renderJoinBundleConfigUpdateNextSteps(out *strings.Builder, recipe joinBundleConfigUpdateRecipe) {
	updateTLSFlags := joinBundleUpdateConfigTLSFlags(recipe)
	out.WriteString("\ncat <<FABRICOPS_NEXT_STEPS\n")
	out.WriteString("Created unsigned channel update envelope: $CONFIG_UPDATE_ENVELOPE_PB\n\n")
	out.WriteString("Inspect before submitting:\n")
	for _, file := range recipe.InspectBeforeSubmitting {
		fmt.Fprintf(out, "  - $WORKDIR/%s\n", file)
	}
	if recipe.Membership != nil {
		if policy := strings.TrimSpace(recipe.Membership.ApplicationPolicy); policy != "" {
			fmt.Fprintf(out, "\nApplication policy hint: %s\n", policy)
		}
		if signers := joinBundleTrimmedStrings(recipe.Membership.RequiredSignerMSPIDs); len(signers) > 0 {
			fmt.Fprintf(out, "\nExpected founder signer MSP IDs: %s\n", strings.Join(signers, ", "))
		}
	}
	out.WriteString("\nCollect required founder-admin signatures according to the channel policy:\n")
	out.WriteString("  peer channel signconfigtx -f \"$CONFIG_UPDATE_ENVELOPE_PB\"\n\n")
	out.WriteString("Submit with an authorized founder admin after signatures are complete:\n")
	fmt.Fprintf(
		out,
		"  peer channel update -f \"$CONFIG_UPDATE_ENVELOPE_PB\" -c \"$CHANNEL_NAME\" -o \"$ORDERER_ADDRESS\"%s\n",
		updateTLSFlags,
	)
	out.WriteString("FABRICOPS_NEXT_STEPS\n")
}

func joinBundleFetchConfigTLSFlags(recipe joinBundleConfigUpdateRecipe) string {
	if !recipe.Network.TLS {
		return ""
	}
	flags := " --tls --cafile \"$ORDERER_TLS_CA\""
	if strings.TrimSpace(recipe.Orderer.TLSHostnameOverride) != "" {
		flags += " --ordererTLSHostnameOverride \"$ORDERER_TLS_HOSTNAME_OVERRIDE\""
	}
	return flags
}

func joinBundleUpdateConfigTLSFlags(recipe joinBundleConfigUpdateRecipe) string {
	return joinBundleFetchConfigTLSFlags(recipe)
}

func joinBundleConfigUpdateFilesForChannel(channelName string) joinBundleConfigUpdateFiles {
	prefix := sanitizeName(channelName)
	return joinBundleConfigUpdateFiles{
		ApplicationOrgJSON:       prefix + "-application-org.json",
		OrdererTLSCACert:         prefix + "-orderer-tls-ca.pem",
		ConfigBlockPB:            prefix + "-config-block.pb",
		ConfigBlockJSON:          prefix + "-config-block.json",
		ConfigJSON:               prefix + "-config.json",
		ModifiedConfigJSON:       prefix + "-modified-config.json",
		ConfigPB:                 prefix + "-config.pb",
		ModifiedConfigPB:         prefix + "-modified-config.pb",
		ConfigUpdatePB:           prefix + "-join-update.pb",
		ConfigUpdateJSON:         prefix + "-join-update.json",
		ConfigUpdateEnvelopeJSON: prefix + "-join-update-envelope.json",
		ConfigUpdateEnvelopePB:   prefix + "-join-update-envelope.pb",
	}
}

func joinBundleFounderAdminEnv(tls bool) []string {
	env := []string{
		"CORE_PEER_LOCALMSPID",
		"CORE_PEER_MSPCONFIGPATH",
		"CORE_PEER_ADDRESS",
	}
	if tls {
		env = append(env, "CORE_PEER_TLS_ROOTCERT_FILE")
	}
	return env
}

func defaultJoinBundleUpdateWorkDir(channelName string, orgName string) string {
	return sanitizeName(defaultJoinBundleUpdateDir + "-" + channelName + "-" + orgName)
}

func joinBundleEndpointHost(address string) (string, error) {
	host, _, err := joinBundleHostPort(address, 0)
	if err != nil {
		return "", err
	}
	return host, nil
}

func joinBundleShellQuote(value string) string {
	if value == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(value, "'", `'"'"'`) + "'"
}

func selectJoinBundleRenderOrgChannel(bundle joinBundle, channelName string) (joinBundleChannel, error) {
	if _, err := validateJoinBundle(bundle); err != nil {
		return joinBundleChannel{}, err
	}
	if strings.TrimSpace(channelName) != "" {
		for _, channel := range bundle.Channels {
			if strings.EqualFold(channel.Name, channelName) {
				return channel, nil
			}
		}
		return joinBundleChannel{}, fmt.Errorf("join bundle does not contain channel %q", channelName)
	}
	if len(bundle.Channels) == 1 {
		return bundle.Channels[0], nil
	}
	return joinBundleChannel{}, fmt.Errorf("--channel is required when the join bundle contains multiple channels")
}

func buildJoinBundleApplicationOrgGroup(
	bundle joinBundle,
	channel joinBundleChannel,
) (map[string]any, error) {
	mspConfig, err := buildJoinBundleMSPConfig(bundle)
	if err != nil {
		return nil, err
	}

	values := map[string]any{
		"MSP": joinBundleConfigValue(map[string]any{
			"config": mspConfig,
			"type":   0,
		}),
	}
	if len(channel.AnchorPeers) > 0 {
		values["AnchorPeers"] = joinBundleConfigValue(map[string]any{
			"anchor_peers": joinBundleRenderedAnchorPeers(channel.AnchorPeers),
		})
	}

	return map[string]any{
		"mod_policy": "Admins",
		"policies": map[string]any{
			"Admins":      joinBundleSignaturePolicy(bundle.Exported.MSPID, "ADMIN"),
			"Endorsement": joinBundleSignaturePolicy(bundle.Exported.MSPID, "MEMBER"),
			"Readers":     joinBundleSignaturePolicy(bundle.Exported.MSPID, "MEMBER"),
			"Writers":     joinBundleSignaturePolicy(bundle.Exported.MSPID, "MEMBER"),
		},
		"values":  values,
		"version": "0",
	}, nil
}

func buildJoinBundleMSPConfig(bundle joinBundle) (map[string]any, error) {
	nodeOUs, err := buildJoinBundleFabricNodeOUs(bundle.Exported.AdminMSP)
	if err != nil {
		return nil, err
	}

	config := map[string]any{
		"admins":                          []string{},
		"crypto_config":                   joinBundleCryptoConfig(),
		"fabric_node_ous":                 nodeOUs,
		"intermediate_certs":              []string{},
		"name":                            bundle.Exported.MSPID,
		"organizational_unit_identifiers": []string{},
		"revocation_list":                 []string{},
		"root_certs": []string{
			joinBundleBase64PEM(bundle.Exported.AdminMSP.CACertPEM),
		},
	}
	if strings.TrimSpace(bundle.Exported.AdminMSP.TLSCACertPEM) != "" {
		config["tls_root_certs"] = []string{
			joinBundleBase64PEM(bundle.Exported.AdminMSP.TLSCACertPEM),
		}
	}
	return config, nil
}

func buildJoinBundleFabricNodeOUs(msp joinBundlePublicMSP) (map[string]any, error) {
	var config joinBundleMSPConfigFile
	if err := yaml.Unmarshal([]byte(msp.ConfigYAML), &config); err != nil {
		return nil, fmt.Errorf("could not parse exported MSP configYAML: %w", err)
	}
	if !config.NodeOUs.Enable {
		return map[string]any{"enable": false}, nil
	}

	cert := joinBundleBase64PEM(msp.CACertPEM)
	nodeOUs := map[string]any{"enable": true}
	for _, item := range []struct {
		name       string
		identifier joinBundleOUIdentifierYAML
	}{
		{name: "client_ou_identifier", identifier: config.NodeOUs.ClientOUIdentifier},
		{name: "peer_ou_identifier", identifier: config.NodeOUs.PeerOUIdentifier},
		{name: "admin_ou_identifier", identifier: config.NodeOUs.AdminOUIdentifier},
		{name: "orderer_ou_identifier", identifier: config.NodeOUs.OrdererOUIdentifier},
	} {
		ou := strings.TrimSpace(item.identifier.OrganizationalUnitIdentifier)
		if ou == "" {
			return nil, fmt.Errorf("exported MSP configYAML is missing %s.OrganizationalUnitIdentifier", item.name)
		}
		nodeOUs[item.name] = map[string]any{
			"certificate":                    cert,
			"organizational_unit_identifier": ou,
		}
	}
	return nodeOUs, nil
}

func joinBundleConfigValue(value any) map[string]any {
	return map[string]any{
		"mod_policy": "Admins",
		"value":      value,
		"version":    "0",
	}
}

func joinBundleSignaturePolicy(mspID string, role string) map[string]any {
	return map[string]any{
		"mod_policy": "Admins",
		"policy": map[string]any{
			"type": 1,
			"value": map[string]any{
				"identities": []map[string]any{
					{
						"principal": map[string]any{
							"msp_identifier": mspID,
							"role":           role,
						},
						"principal_classification": "ROLE",
					},
				},
				"rule": map[string]any{
					"n_out_of": map[string]any{
						"n": 1,
						"rules": []map[string]any{
							{"signed_by": 0},
						},
					},
				},
				"version": 0,
			},
		},
		"version": "0",
	}
}

func joinBundleCryptoConfig() map[string]string {
	return map[string]string{
		"identity_identifier_hash_function": "SHA256",
		"signature_hash_family":             "SHA2",
	}
}

func joinBundleRenderedAnchorPeers(anchors []joinBundleAnchorPeer) []map[string]any {
	rendered := make([]map[string]any, 0, len(anchors))
	for _, anchor := range anchors {
		rendered = append(rendered, map[string]any{
			"host": anchor.Host,
			"port": anchor.Port,
		})
	}
	return rendered
}

func joinBundleBase64PEM(value string) string {
	return base64.StdEncoding.EncodeToString([]byte(strings.TrimSpace(value)))
}

func selectJoinBundlePlanChannels(
	bundle joinBundle,
	channelFilters []string,
) ([]joinBundleChannel, error) {
	if len(channelFilters) == 0 {
		return bundle.Channels, nil
	}

	channels := map[string]joinBundleChannel{}
	for _, channel := range bundle.Channels {
		channels[joinBundleLookupKey(channel.Name)] = channel
	}

	selected := make([]joinBundleChannel, 0, len(channelFilters))
	for _, channelName := range channelFilters {
		channel, ok := channels[joinBundleLookupKey(channelName)]
		if !ok {
			return nil, fmt.Errorf("join bundle does not contain channel %q", channelName)
		}
		selected = append(selected, channel)
	}
	return selected, nil
}

func joinBundlePlanChaincodesForChannel(
	chaincodes []joinBundleChaincode,
	channelName string,
) []joinBundlePlanChaincode {
	selected := []joinBundlePlanChaincode{}
	for _, chaincode := range chaincodes {
		if !strings.EqualFold(chaincode.Channel, channelName) {
			continue
		}
		selected = append(selected, joinBundlePlanChaincode{
			Name:              chaincode.Name,
			Version:           chaincode.Version,
			PackageLabel:      chaincode.PackageLabel,
			Sequence:          chaincode.Sequence,
			EndorsementPolicy: chaincode.EndorsementPolicy,
			InitRequired:      chaincode.InitRequired,
		})
	}
	return selected
}

func joinBundleFounderActions(bundle joinBundle, channel joinBundleChannel) []joinBundlePlanAction {
	signatureSummary := fmt.Sprintf(
		"Collect required signatures and submit the channel config update for %s",
		channel.Name,
	)
	if channel.Membership != nil {
		if signers := joinBundleTrimmedStrings(channel.Membership.RequiredSignerMSPIDs); len(signers) > 0 {
			signatureSummary = fmt.Sprintf(
				"Collect founder signatures from %s and submit the channel config update for %s",
				strings.Join(signers, ", "),
				channel.Name,
			)
		}
	}
	actions := []joinBundlePlanAction{
		{
			Name: "add-org-msp",
			Summary: fmt.Sprintf(
				"Add %s MSP definition to channel %s using the exported MSP config and root certificates",
				bundle.Exported.MSPID,
				channel.Name,
			),
		},
	}
	if len(channel.AnchorPeers) > 0 {
		actions = append(actions, joinBundlePlanAction{
			Name: "update-anchor-peers",
			Summary: fmt.Sprintf(
				"Set %s anchor peer entries for org %s on channel %s",
				strconv.Itoa(len(channel.AnchorPeers)),
				bundle.Exported.Name,
				channel.Name,
			),
		})
	}
	actions = append(actions, joinBundlePlanAction{
		Name:    "submit-channel-config-update",
		Summary: signatureSummary,
	})
	return actions
}

func joinBundleParticipantActions(
	bundle joinBundle,
	channel joinBundleChannel,
	chaincodes []joinBundlePlanChaincode,
) []joinBundlePlanAction {
	actions := []joinBundlePlanAction{
		{
			Name: "receive-channel-block",
			Summary: fmt.Sprintf(
				"Receive the updated %s channel block or fetch it from a trusted orderer",
				channel.Name,
			),
		},
	}
	if len(channel.Peers) > 0 {
		actions = append(actions, joinBundlePlanAction{
			Name: "join-peers",
			Summary: fmt.Sprintf(
				"Join %s to channel %s for org %s",
				joinBundlePeerNamesSummary(channel.Peers),
				channel.Name,
				bundle.Exported.Name,
			),
		})
	}
	for _, chaincode := range chaincodes {
		actions = append(actions, joinBundlePlanAction{
			Name: "approve-chaincode-definition",
			Summary: fmt.Sprintf(
				"Install package %s and approve chaincode %s sequence %d for %s",
				chaincode.PackageLabel,
				chaincode.Name,
				chaincode.Sequence,
				bundle.Exported.MSPID,
			),
		})
	}
	return actions
}

func renderJoinBundlePlanText(plan joinBundlePlan) string {
	var b strings.Builder
	fmt.Fprintf(
		&b,
		"Join plan: %s/%s org %s (%s)\n",
		plan.Network.Namespace,
		plan.Network.Name,
		plan.Org.Name,
		plan.Org.MSPID,
	)
	for _, channel := range plan.Channels {
		fmt.Fprintf(&b, "\nChannel %s\n", channel.Name)
		printJoinBundlePlanMembership(&b, channel.Membership)
		printJoinBundlePlanActions(&b, "Founder", channel.FounderActions)
		printJoinBundlePlanActions(&b, "Participant", channel.ParticipantActions)
	}
	return b.String()
}

func printJoinBundlePlanMembership(out *strings.Builder, membership *joinBundleChannelMembership) {
	if membership == nil {
		return
	}
	if policy := strings.TrimSpace(membership.ApplicationPolicy); policy != "" {
		fmt.Fprintf(out, "Membership policy: %s\n", policy)
	}
	if signers := joinBundleTrimmedStrings(membership.RequiredSignerMSPIDs); len(signers) > 0 {
		fmt.Fprintf(out, "Required founder signer MSP IDs: %s\n", strings.Join(signers, ", "))
	}
}

func printJoinBundlePlanActions(out *strings.Builder, title string, actions []joinBundlePlanAction) {
	fmt.Fprintf(out, "%s:\n", title)
	for _, action := range actions {
		fmt.Fprintf(out, "  - %s: %s\n", action.Name, action.Summary)
	}
}

func joinBundlePeerNamesSummary(peers []joinBundlePeerRef) string {
	names := make([]string, 0, len(peers))
	for _, peer := range peers {
		names = append(names, peer.Name)
	}
	return strings.Join(names, ", ")
}

func joinBundleTrimmedStrings(values []string) []string {
	trimmed := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		trimmed = append(trimmed, value)
	}
	return trimmed
}

func validateJoinBundle(bundle joinBundle) (joinBundleValidationResult, error) {
	violations := []string{}
	addViolation := func(format string, args ...any) {
		violations = append(violations, fmt.Sprintf(format, args...))
	}

	if strings.TrimSpace(bundle.APIVersion) != joinBundleAPIVersion {
		addViolation("apiVersion must be %q", joinBundleAPIVersion)
	}
	if strings.TrimSpace(bundle.Kind) != joinBundleKind {
		addViolation("kind must be %q", joinBundleKind)
	}
	if strings.TrimSpace(bundle.Network.Name) == "" {
		addViolation("network.name is required")
	}
	if strings.TrimSpace(bundle.Network.Namespace) == "" {
		addViolation("network.namespace is required")
	}
	if strings.TrimSpace(bundle.Network.FabricVersion) == "" {
		addViolation("network.fabricVersion is required")
	}
	if strings.TrimSpace(bundle.Exported.Name) == "" {
		addViolation("exported.name is required")
	}
	if strings.TrimSpace(bundle.Exported.MSPID) == "" {
		addViolation("exported.mspID is required")
	}
	if strings.TrimSpace(bundle.Exported.Namespace) == "" {
		addViolation("exported.namespace is required")
	}
	validateJoinBundleObjectRef("exported.adminMSP.secretRef", bundle.Exported.AdminMSP.SecretRef, addViolation)
	if strings.TrimSpace(bundle.Exported.AdminMSP.ConfigYAML) == "" {
		addViolation("exported.adminMSP.configYAML is required")
	}
	if strings.TrimSpace(bundle.Exported.AdminMSP.CACertPEM) == "" {
		addViolation("exported.adminMSP.caCertPEM is required")
	}
	if bundle.Network.TLS {
		if strings.TrimSpace(bundle.Exported.AdminMSP.TLSCACertPEM) == "" {
			addViolation("exported.adminMSP.tlsCACertPEM is required when TLS is enabled")
		}
		if bundle.Exported.AdminTLS == nil {
			addViolation("exported.adminTLS is required when TLS is enabled")
		} else {
			validateJoinBundleTLSRoot("exported.adminTLS", *bundle.Exported.AdminTLS, bundle.Network.TLS, addViolation)
		}
	}

	peerNames := map[string]struct{}{}
	for i, peer := range bundle.Exported.Peers {
		path := fmt.Sprintf("exported.peers[%d]", i)
		if strings.TrimSpace(peer.Name) == "" {
			addViolation("%s.name is required", path)
		} else {
			peerNames[joinBundleLookupKey(peer.Name)] = struct{}{}
		}
		validateJoinBundleEndpoint(path+".address", peer.Address, true, addViolation)
		validateJoinBundleEndpoint(path+".chaincodeAddress", peer.ChaincodeAddress, false, addViolation)
		validateJoinBundleEndpoint(path+".operationsAddress", peer.OperationsAddress, false, addViolation)
		if bundle.Network.TLS {
			if peer.TLSRoot == nil {
				addViolation("%s.tlsRoot is required when TLS is enabled", path)
			} else {
				validateJoinBundleTLSRoot(path+".tlsRoot", *peer.TLSRoot, bundle.Network.TLS, addViolation)
			}
		}
	}

	if len(bundle.Orderers) == 0 {
		addViolation("at least one orderer is required")
	}
	for i, orderer := range bundle.Orderers {
		path := fmt.Sprintf("orderers[%d]", i)
		if strings.TrimSpace(orderer.Org) == "" {
			addViolation("%s.org is required", path)
		}
		if strings.TrimSpace(orderer.Name) == "" {
			addViolation("%s.name is required", path)
		}
		validateJoinBundleEndpoint(path+".clientAddress", orderer.ClientAddress, true, addViolation)
		validateJoinBundleEndpoint(path+".adminAddress", orderer.AdminAddress, false, addViolation)
		validateJoinBundleEndpoint(path+".operationsAddress", orderer.OperationsAddress, false, addViolation)
		if bundle.Network.TLS {
			if orderer.TLSRoot == nil {
				addViolation("%s.tlsRoot is required when TLS is enabled", path)
			} else {
				validateJoinBundleTLSRoot(path+".tlsRoot", *orderer.TLSRoot, bundle.Network.TLS, addViolation)
			}
		}
	}

	channelNames := map[string]struct{}{}
	for i, channel := range bundle.Channels {
		path := fmt.Sprintf("channels[%d]", i)
		if strings.TrimSpace(channel.Name) == "" {
			addViolation("%s.name is required", path)
		} else {
			channelNames[joinBundleLookupKey(channel.Name)] = struct{}{}
		}
		for j, peer := range channel.Peers {
			peerPath := fmt.Sprintf("%s.peers[%d]", path, j)
			if strings.TrimSpace(peer.Org) == "" {
				addViolation("%s.org is required", peerPath)
			}
			if strings.TrimSpace(peer.Name) == "" {
				addViolation("%s.name is required", peerPath)
			} else if _, ok := peerNames[joinBundleLookupKey(peer.Name)]; !ok {
				addViolation("%s.name references unknown exported peer %q", peerPath, peer.Name)
			}
			validateJoinBundleEndpoint(peerPath+".address", peer.Address, false, addViolation)
		}
		for j, anchor := range channel.AnchorPeers {
			validateJoinBundleAnchorPeer(fmt.Sprintf("%s.anchorPeers[%d]", path, j), anchor, peerNames, addViolation)
		}
		validateJoinBundleChannelMembership(path+".membership", channel.Membership, addViolation)
	}

	for i, chaincode := range bundle.Chaincodes {
		path := fmt.Sprintf("chaincodes[%d]", i)
		if strings.TrimSpace(chaincode.Name) == "" {
			addViolation("%s.name is required", path)
		}
		if strings.TrimSpace(chaincode.Channel) == "" {
			addViolation("%s.channel is required", path)
		} else if _, ok := channelNames[joinBundleLookupKey(chaincode.Channel)]; !ok {
			addViolation("%s.channel references unknown channel %q", path, chaincode.Channel)
		}
		if strings.TrimSpace(chaincode.Version) == "" {
			addViolation("%s.version is required", path)
		}
		if strings.TrimSpace(chaincode.PackageLabel) == "" {
			addViolation("%s.packageLabel is required", path)
		}
		if chaincode.Sequence <= 0 {
			addViolation("%s.sequence must be greater than zero", path)
		}
	}

	if len(violations) > 0 {
		return joinBundleValidationResult{}, fmt.Errorf("join bundle is invalid: %s", strings.Join(violations, "; "))
	}

	return joinBundleValidationResult{
		Valid:      true,
		Network:    bundle.Network.Name,
		Namespace:  bundle.Network.Namespace,
		Org:        bundle.Exported.Name,
		Peers:      len(bundle.Exported.Peers),
		Orderers:   len(bundle.Orderers),
		Channels:   len(bundle.Channels),
		Chaincodes: len(bundle.Chaincodes),
	}, nil
}

func validateJoinBundleObjectRef(path string, ref joinBundleObjectRef, addViolation func(string, ...any)) {
	if strings.TrimSpace(ref.Namespace) == "" {
		addViolation("%s.namespace is required", path)
	}
	if strings.TrimSpace(ref.Name) == "" {
		addViolation("%s.name is required", path)
	}
}

func validateJoinBundleTLSRoot(
	path string,
	root joinBundleTLSRoot,
	tls bool,
	addViolation func(string, ...any),
) {
	hasSecretRef := root.SecretRef != nil
	hasConfigMapRef := root.ConfigMapRef != nil
	if !hasSecretRef && !hasConfigMapRef {
		addViolation("%s.secretRef or %s.configMapRef is required", path, path)
	}
	if hasSecretRef {
		validateJoinBundleObjectRef(path+".secretRef", *root.SecretRef, addViolation)
	}
	if hasConfigMapRef {
		validateJoinBundleObjectRef(path+".configMapRef", *root.ConfigMapRef, addViolation)
	}
	if tls && strings.TrimSpace(root.CACertPEM) == "" {
		addViolation("%s.caCertPEM is required when TLS is enabled", path)
	}
}

func validateJoinBundleEndpoint(
	path string,
	address string,
	required bool,
	addViolation func(string, ...any),
) {
	if strings.TrimSpace(address) == "" {
		if required {
			addViolation("%s is required", path)
		}
		return
	}
	host, port, err := joinBundleHostPort(address, 0)
	if err != nil {
		addViolation("%s is invalid: %v", path, err)
		return
	}
	if strings.TrimSpace(host) == "" {
		addViolation("%s host is required", path)
	}
	if port <= 0 || port > 65535 {
		addViolation("%s port must be between 1 and 65535", path)
	}
}

func validateJoinBundleAnchorPeer(
	path string,
	anchor joinBundleAnchorPeer,
	peerNames map[string]struct{},
	addViolation func(string, ...any),
) {
	if strings.TrimSpace(anchor.Org) == "" {
		addViolation("%s.org is required", path)
	}
	if strings.TrimSpace(anchor.Name) == "" {
		addViolation("%s.name is required", path)
	} else if _, ok := peerNames[joinBundleLookupKey(anchor.Name)]; !ok {
		addViolation("%s.name references unknown exported peer %q", path, anchor.Name)
	}
	if strings.TrimSpace(anchor.Host) == "" {
		addViolation("%s.host is required", path)
	}
	if anchor.Port <= 0 || anchor.Port > 65535 {
		addViolation("%s.port must be between 1 and 65535", path)
	}
}

func validateJoinBundleChannelMembership(
	path string,
	membership *joinBundleChannelMembership,
	addViolation func(string, ...any),
) {
	if membership == nil {
		return
	}
	if len(strings.TrimSpace(membership.ApplicationPolicy)) > 256 {
		addViolation("%s.applicationPolicy must be at most 256 characters", path)
	}
	seenSigners := map[string]struct{}{}
	for i, mspID := range membership.RequiredSignerMSPIDs {
		mspID = strings.TrimSpace(mspID)
		signerPath := fmt.Sprintf("%s.requiredSignerMSPIDs[%d]", path, i)
		if mspID == "" {
			addViolation("%s is required", signerPath)
			continue
		}
		if _, ok := seenSigners[mspID]; ok {
			addViolation("%s required signer MSP %q is declared more than once", path, mspID)
			continue
		}
		seenSigners[mspID] = struct{}{}
	}
}

func selectJoinBundleOrg(
	network *fabricopsv1alpha1.FabricNetwork,
	orgName string,
) (fabricopsv1alpha1.Org, fabricopsv1alpha1.OrgStatus, error) {
	var specOrg fabricopsv1alpha1.Org
	specFound := false
	for _, org := range network.Spec.Orgs {
		if strings.EqualFold(org.Organization.Name, orgName) {
			specOrg = org
			specFound = true
			break
		}
	}
	if !specFound {
		return fabricopsv1alpha1.Org{}, fabricopsv1alpha1.OrgStatus{}, fmt.Errorf(
			"org %q was not found in FabricNetwork spec",
			orgName,
		)
	}

	var status fabricopsv1alpha1.OrgStatus
	statusFound := false
	for _, item := range network.Status.OrgStatus {
		if strings.EqualFold(item.Name, specOrg.Organization.Name) {
			status = item
			statusFound = true
			break
		}
	}
	if !statusFound {
		return fabricopsv1alpha1.Org{}, fabricopsv1alpha1.OrgStatus{}, fmt.Errorf(
			"org %q does not have status yet",
			specOrg.Organization.Name,
		)
	}
	if status.Namespace == "" {
		return fabricopsv1alpha1.Org{}, fabricopsv1alpha1.OrgStatus{}, fmt.Errorf(
			"org %q does not have a namespace in status yet",
			specOrg.Organization.Name,
		)
	}
	if !status.IdentityReady {
		return fabricopsv1alpha1.Org{}, fabricopsv1alpha1.OrgStatus{}, fmt.Errorf(
			"org %q identity material is not ready",
			specOrg.Organization.Name,
		)
	}

	return specOrg, status, nil
}

func selectParticipantJoinBundleOrg(
	participant *fabricopsv1alpha1.FabricParticipant,
) (fabricopsv1alpha1.Org, fabricopsv1alpha1.OrgStatus, error) {
	status := participant.Status.LocalOrgStatus
	specOrg := participant.Spec.Org
	if strings.TrimSpace(status.Name) == "" {
		return fabricopsv1alpha1.Org{}, fabricopsv1alpha1.OrgStatus{}, fmt.Errorf(
			"participant %q does not have local org status yet",
			participant.Name,
		)
	}
	if !strings.EqualFold(status.Name, specOrg.Organization.Name) {
		return fabricopsv1alpha1.Org{}, fabricopsv1alpha1.OrgStatus{}, fmt.Errorf(
			"participant local org status %q does not match spec org %q",
			status.Name,
			specOrg.Organization.Name,
		)
	}
	if status.Namespace == "" {
		return fabricopsv1alpha1.Org{}, fabricopsv1alpha1.OrgStatus{}, fmt.Errorf(
			"participant org %q does not have a namespace in status yet",
			specOrg.Organization.Name,
		)
	}
	if !status.IdentityReady {
		return fabricopsv1alpha1.Org{}, fabricopsv1alpha1.OrgStatus{}, fmt.Errorf(
			"participant org %q identity material is not ready",
			specOrg.Organization.Name,
		)
	}
	if len(status.PeerEndpoints) == 0 {
		return fabricopsv1alpha1.Org{}, fabricopsv1alpha1.OrgStatus{}, fmt.Errorf(
			"participant org %q does not have peer endpoints in status yet",
			specOrg.Organization.Name,
		)
	}

	return specOrg, status, nil
}

func buildJoinBundleExportedOrg(
	ctx context.Context,
	client ctrlclient.Client,
	network *fabricopsv1alpha1.FabricNetwork,
	specOrg fabricopsv1alpha1.Org,
	status fabricopsv1alpha1.OrgStatus,
) (joinBundleExportedOrg, error) {
	adminName := sanitizeName(specOrg.Organization.Name + "-admin")
	mspRef := joinBundleObjectRef{
		Namespace: status.Namespace,
		Name:      adminName + "-msp",
	}
	caCert, err := loadJoinBundleSecretKey(ctx, client, mspRef, mspCACertKey)
	if err != nil {
		return joinBundleExportedOrg{}, err
	}
	configYAML, err := loadJoinBundleSecretKey(ctx, client, mspRef, mspConfigKey)
	if err != nil {
		return joinBundleExportedOrg{}, err
	}

	msp := joinBundlePublicMSP{
		SecretRef:  mspRef,
		ConfigYAML: configYAML,
		CACertPEM:  caCert,
	}
	if network.Spec.Global.TLS {
		tlsCACert, err := loadJoinBundleSecretKey(ctx, client, mspRef, mspTLSCACertKey)
		if err != nil {
			return joinBundleExportedOrg{}, err
		}
		msp.TLSCACertPEM = tlsCACert
	}

	peers, err := buildJoinBundlePeers(ctx, client, network, status)
	if err != nil {
		return joinBundleExportedOrg{}, err
	}

	exported := joinBundleExportedOrg{
		Name:       specOrg.Organization.Name,
		MSPID:      specOrg.Organization.MSPName,
		Domain:     specOrg.Organization.Domain,
		Namespace:  status.Namespace,
		CAEndpoint: status.CAEndpoint,
		AdminMSP:   msp,
		Peers:      peers,
	}
	if status.ConnectionProfileConfigMapName != "" {
		exported.ConnectionProfile = &joinBundleObjectRef{
			Namespace: status.Namespace,
			Name:      status.ConnectionProfileConfigMapName,
		}
	}
	if network.Spec.Global.TLS {
		adminTLSRef := joinBundleObjectRef{
			Namespace: status.Namespace,
			Name:      adminName + "-tls",
		}
		cert, err := loadJoinBundleSecretKey(ctx, client, adminTLSRef, tlsCACertKey)
		if err != nil {
			return joinBundleExportedOrg{}, err
		}
		exported.AdminTLS = &joinBundleTLSRoot{
			SecretRef: &adminTLSRef,
			CACertPEM: cert,
		}
	}

	return exported, nil
}

func buildParticipantJoinBundleExportedOrg(
	ctx context.Context,
	client ctrlclient.Client,
	participant *fabricopsv1alpha1.FabricParticipant,
	specOrg fabricopsv1alpha1.Org,
	status fabricopsv1alpha1.OrgStatus,
) (joinBundleExportedOrg, error) {
	adminName := sanitizeName(specOrg.Organization.Name + "-admin")
	mspRef := joinBundleObjectRef{
		Namespace: status.Namespace,
		Name:      adminName + "-msp",
	}
	caCert, err := loadJoinBundleSecretKey(ctx, client, mspRef, mspCACertKey)
	if err != nil {
		return joinBundleExportedOrg{}, err
	}
	configYAML, err := loadJoinBundleSecretKey(ctx, client, mspRef, mspConfigKey)
	if err != nil {
		return joinBundleExportedOrg{}, err
	}

	msp := joinBundlePublicMSP{
		SecretRef:  mspRef,
		ConfigYAML: configYAML,
		CACertPEM:  caCert,
	}
	if participant.Spec.Global.TLS {
		tlsCACert, err := loadJoinBundleSecretKey(ctx, client, mspRef, mspTLSCACertKey)
		if err != nil {
			return joinBundleExportedOrg{}, err
		}
		msp.TLSCACertPEM = tlsCACert
	}

	peers, err := buildJoinBundlePeersFromStatus(ctx, client, participant.Spec.Global.TLS, status)
	if err != nil {
		return joinBundleExportedOrg{}, err
	}

	exported := joinBundleExportedOrg{
		Name:       specOrg.Organization.Name,
		MSPID:      specOrg.Organization.MSPName,
		Domain:     specOrg.Organization.Domain,
		Namespace:  status.Namespace,
		CAEndpoint: status.CAEndpoint,
		AdminMSP:   msp,
		Peers:      peers,
	}
	if participant.Spec.Global.TLS {
		adminTLSRef := joinBundleObjectRef{
			Namespace: status.Namespace,
			Name:      adminName + "-tls",
		}
		cert, err := loadJoinBundleSecretKey(ctx, client, adminTLSRef, tlsCACertKey)
		if err != nil {
			return joinBundleExportedOrg{}, err
		}
		exported.AdminTLS = &joinBundleTLSRoot{
			SecretRef: &adminTLSRef,
			CACertPEM: cert,
		}
	}

	return exported, nil
}

func buildJoinBundlePeers(
	ctx context.Context,
	client ctrlclient.Client,
	network *fabricopsv1alpha1.FabricNetwork,
	status fabricopsv1alpha1.OrgStatus,
) ([]joinBundlePeer, error) {
	return buildJoinBundlePeersFromStatus(ctx, client, network.Spec.Global.TLS, status)
}

func buildJoinBundlePeersFromStatus(
	ctx context.Context,
	client ctrlclient.Client,
	tlsEnabled bool,
	status fabricopsv1alpha1.OrgStatus,
) ([]joinBundlePeer, error) {
	peers := make([]joinBundlePeer, 0, len(status.PeerEndpoints))
	for _, endpoint := range status.PeerEndpoints {
		peer := joinBundlePeer{
			Name:                endpoint.Name,
			Address:             endpoint.Address,
			TLSHostnameOverride: endpoint.TLSHostnameOverride,
			ChaincodeAddress:    endpoint.ChaincodeAddress,
			OperationsAddress:   endpoint.OperationsAddress,
		}
		if tlsEnabled {
			ref := joinBundleObjectRef{
				Namespace: status.Namespace,
				Name:      endpoint.Name + "-tls",
			}
			cert, err := loadJoinBundleSecretKey(ctx, client, ref, tlsCACertKey)
			if err != nil {
				return nil, err
			}
			peer.TLSRoot = &joinBundleTLSRoot{
				SecretRef: &ref,
				CACertPEM: cert,
			}
		}
		peers = append(peers, peer)
	}
	return peers, nil
}

func buildJoinBundleOrderers(
	ctx context.Context,
	client ctrlclient.Client,
	network *fabricopsv1alpha1.FabricNetwork,
) ([]joinBundleOrderer, error) {
	orderers := []joinBundleOrderer{}
	for _, status := range network.Status.OrgStatus {
		for _, endpoint := range status.OrdererEndpoints {
			orderer := joinBundleOrderer{
				Org:                 status.Name,
				Name:                endpoint.Name,
				ClientAddress:       endpoint.ClientAddress,
				TLSHostnameOverride: endpoint.TLSHostnameOverride,
				AdminAddress:        endpoint.AdminAddress,
				OperationsAddress:   endpoint.OperationsAddress,
			}
			if network.Spec.Global.TLS {
				ref := joinBundleObjectRef{
					Namespace: status.Namespace,
					Name:      endpoint.Name + "-tls",
				}
				cert, err := loadJoinBundleSecretKey(ctx, client, ref, tlsCACertKey)
				if err != nil {
					return nil, err
				}
				orderer.TLSRoot = &joinBundleTLSRoot{
					SecretRef: &ref,
					CACertPEM: cert,
				}
			}
			orderers = append(orderers, orderer)
		}
	}
	return orderers, nil
}

func buildParticipantJoinBundleOrderers(
	ctx context.Context,
	client ctrlclient.Client,
	participant *fabricopsv1alpha1.FabricParticipant,
) ([]joinBundleOrderer, error) {
	orderers := make([]joinBundleOrderer, 0, len(participant.Spec.Network.Orderers))
	for _, endpoint := range participant.Spec.Network.Orderers {
		orderer := joinBundleOrderer{
			Org:                 endpoint.Org,
			Name:                endpoint.Name,
			ClientAddress:       endpoint.ClientAddress,
			TLSHostnameOverride: endpoint.TLSHostnameOverride,
			AdminAddress:        endpoint.AdminAddress,
		}
		if participant.Spec.Global.TLS {
			root, err := loadJoinBundleParticipantTLSRoot(
				ctx,
				client,
				participant.Namespace,
				endpoint.TLSRootCARef,
			)
			if err != nil {
				return nil, fmt.Errorf(
					"could not load orderer TLS root for %s/%s: %w",
					endpoint.Org,
					endpoint.Name,
					err,
				)
			}
			orderer.TLSRoot = root
		}
		orderers = append(orderers, orderer)
	}
	return orderers, nil
}

func buildJoinBundleChannels(
	network *fabricopsv1alpha1.FabricNetwork,
	specOrg fabricopsv1alpha1.Org,
	status fabricopsv1alpha1.OrgStatus,
	channelSet map[string]struct{},
) ([]joinBundleChannel, error) {
	peerEndpoints := joinBundlePeerEndpointsByName(status)
	channels := []joinBundleChannel{}
	for _, channel := range network.Spec.Channels {
		for _, channelOrg := range channel.Orgs {
			if !strings.EqualFold(channelOrg.Name, specOrg.Organization.Name) {
				continue
			}
			channelSet[channel.Name] = struct{}{}
			peers, err := joinBundleChannelPeers(channel.Name, specOrg.Organization.Name, channelOrg.Peers, peerEndpoints)
			if err != nil {
				return nil, err
			}
			anchors, err := joinBundleChannelAnchorPeers(
				channel.Name,
				specOrg.Organization.Name,
				channelOrg.Peers,
				peerEndpoints,
			)
			if err != nil {
				return nil, err
			}
			channels = append(channels, joinBundleChannel{
				Name:        channel.Name,
				Peers:       peers,
				AnchorPeers: anchors,
			})
			break
		}
	}
	return channels, nil
}

func buildParticipantJoinBundleChannels(
	participant *fabricopsv1alpha1.FabricParticipant,
	specOrg fabricopsv1alpha1.Org,
	status fabricopsv1alpha1.OrgStatus,
	channelSet map[string]struct{},
) ([]joinBundleChannel, error) {
	peerEndpoints := joinBundlePeerEndpointsByName(status)
	channels := make([]joinBundleChannel, 0, len(participant.Spec.Channels))
	for _, channel := range participant.Spec.Channels {
		channelSet[channel.Name] = struct{}{}
		peers, err := joinBundleChannelPeers(channel.Name, specOrg.Organization.Name, channel.Peers, peerEndpoints)
		if err != nil {
			return nil, err
		}
		anchors, err := participantJoinBundleAnchorPeers(channel, specOrg.Organization.Name, peerEndpoints)
		if err != nil {
			return nil, err
		}
		channels = append(channels, joinBundleChannel{
			Name:        channel.Name,
			Peers:       peers,
			AnchorPeers: anchors,
			Membership:  participantJoinBundleChannelMembership(channel.Membership),
		})
	}
	return channels, nil
}

func participantJoinBundleChannelMembership(
	membership *fabricopsv1alpha1.ParticipantChannelMembership,
) *joinBundleChannelMembership {
	if membership == nil {
		return nil
	}
	rendered := &joinBundleChannelMembership{
		ApplicationPolicy:    strings.TrimSpace(membership.ApplicationPolicy),
		RequiredSignerMSPIDs: joinBundleTrimmedStrings(membership.RequiredSignerMSPIDs),
	}
	if rendered.ApplicationPolicy == "" && len(rendered.RequiredSignerMSPIDs) == 0 {
		return nil
	}
	return rendered
}

func copyJoinBundleChannelMembership(
	membership *joinBundleChannelMembership,
) *joinBundleChannelMembership {
	if membership == nil {
		return nil
	}
	rendered := &joinBundleChannelMembership{
		ApplicationPolicy:    strings.TrimSpace(membership.ApplicationPolicy),
		RequiredSignerMSPIDs: joinBundleTrimmedStrings(membership.RequiredSignerMSPIDs),
	}
	if rendered.ApplicationPolicy == "" && len(rendered.RequiredSignerMSPIDs) == 0 {
		return nil
	}
	return rendered
}

func participantJoinBundleAnchorPeers(
	channel fabricopsv1alpha1.ParticipantChannel,
	orgName string,
	peerEndpoints map[string]fabricopsv1alpha1.PeerEndpointStatus,
) ([]joinBundleAnchorPeer, error) {
	anchors := make([]joinBundleAnchorPeer, 0, len(channel.AnchorPeers))
	for _, anchor := range channel.AnchorPeers {
		if _, ok := peerEndpoints[joinBundleLookupKey(anchor.Name)]; !ok {
			return nil, fmt.Errorf(
				"anchor peer %q for org %q on channel %q is missing from status",
				anchor.Name,
				orgName,
				channel.Name,
			)
		}
		anchors = append(anchors, joinBundleAnchorPeer{
			Org:  orgName,
			Name: anchor.Name,
			Host: anchor.Host,
			Port: anchor.Port,
		})
	}
	return anchors, nil
}

func joinBundlePeerEndpointsByName(
	status fabricopsv1alpha1.OrgStatus,
) map[string]fabricopsv1alpha1.PeerEndpointStatus {
	peers := map[string]fabricopsv1alpha1.PeerEndpointStatus{}
	for _, endpoint := range status.PeerEndpoints {
		peers[joinBundleLookupKey(endpoint.Name)] = endpoint
	}
	return peers
}

func joinBundleChannelPeers(
	channelName string,
	orgName string,
	peerNames []string,
	peerEndpoints map[string]fabricopsv1alpha1.PeerEndpointStatus,
) ([]joinBundlePeerRef, error) {
	peers := make([]joinBundlePeerRef, 0, len(peerNames))
	for _, peerName := range peerNames {
		endpoint, ok := peerEndpoints[joinBundleLookupKey(peerName)]
		if !ok {
			return nil, fmt.Errorf(
				"peer %q for org %q on channel %q is missing from status",
				peerName,
				orgName,
				channelName,
			)
		}
		peers = append(peers, joinBundlePeerRef{
			Org:     orgName,
			Name:    endpoint.Name,
			Address: endpoint.Address,
		})
	}
	return peers, nil
}

func joinBundleChannelAnchorPeers(
	channelName string,
	orgName string,
	peerNames []string,
	peerEndpoints map[string]fabricopsv1alpha1.PeerEndpointStatus,
) ([]joinBundleAnchorPeer, error) {
	if len(peerNames) == 0 {
		return nil, nil
	}
	endpoint, ok := peerEndpoints[joinBundleLookupKey(peerNames[0])]
	if !ok {
		return nil, fmt.Errorf(
			"anchor peer %q for org %q on channel %q is missing from status",
			peerNames[0],
			orgName,
			channelName,
		)
	}
	host, port, err := joinBundleHostPort(endpoint.Address, 7051)
	if err != nil {
		return nil, err
	}
	return []joinBundleAnchorPeer{{
		Org:  orgName,
		Name: endpoint.Name,
		Host: host,
		Port: port,
	}}, nil
}

func joinBundleOrgChannels(channels []joinBundleChannel) []joinBundleOrgChannel {
	orgChannels := make([]joinBundleOrgChannel, 0, len(channels))
	for _, channel := range channels {
		peers := make([]string, 0, len(channel.Peers))
		for _, peer := range channel.Peers {
			peers = append(peers, peer.Name)
		}
		orgChannels = append(orgChannels, joinBundleOrgChannel{
			Name:        channel.Name,
			Peers:       peers,
			AnchorPeers: channel.AnchorPeers,
		})
	}
	return orgChannels
}

func joinBundleOrgAnchorPeers(channels []joinBundleChannel) []joinBundleAnchorPeer {
	seen := map[string]struct{}{}
	anchors := []joinBundleAnchorPeer{}
	for _, channel := range channels {
		for _, anchor := range channel.AnchorPeers {
			key := strings.Join([]string{
				joinBundleLookupKey(anchor.Org),
				joinBundleLookupKey(anchor.Name),
				anchor.Host,
				strconv.Itoa(int(anchor.Port)),
			}, "/")
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			anchors = append(anchors, anchor)
		}
	}
	return anchors
}

func buildJoinBundleChaincodes(
	network *fabricopsv1alpha1.FabricNetwork,
	channelSet map[string]struct{},
) []joinBundleChaincode {
	statuses := map[string]fabricopsv1alpha1.ChaincodeStatus{}
	for _, status := range network.Status.ChaincodeStatus {
		statuses[joinBundleChaincodeKey(status.Channel, status.Name)] = status
	}

	chaincodes := []joinBundleChaincode{}
	for _, chaincode := range network.Spec.Chaincodes {
		if _, ok := channelSet[chaincode.Channel]; !ok {
			continue
		}
		status := statuses[joinBundleChaincodeKey(chaincode.Channel, chaincode.Name)]
		sequence := chaincode.Sequence
		if sequence == 0 {
			sequence = 1
		}
		if status.Sequence > 0 {
			sequence = status.Sequence
		}
		packageLabel := chaincode.PackageLabel
		if packageLabel == "" {
			packageLabel = fmt.Sprintf("%s_%s_%s", chaincode.Channel, chaincode.Name, chaincode.Version)
		}
		if status.PackageLabel != "" {
			packageLabel = status.PackageLabel
		}

		chaincodes = append(chaincodes, joinBundleChaincode{
			Name:                 chaincode.Name,
			Channel:              chaincode.Channel,
			Version:              chaincode.Version,
			Image:                chaincode.Image,
			PackageLabel:         packageLabel,
			Sequence:             sequence,
			EndorsementPolicy:    chaincode.EndorsementPolicy,
			InitRequired:         chaincode.InitRequired,
			CollectionConfigHash: status.CollectionConfigHash,
			Committed:            status.Committed,
			Ready:                status.Ready,
		})
	}
	return chaincodes
}

func buildParticipantJoinBundleChaincodes(
	participant *fabricopsv1alpha1.FabricParticipant,
	channelSet map[string]struct{},
) []joinBundleChaincode {
	statuses := map[string]fabricopsv1alpha1.ParticipantChaincodeStatus{}
	for _, status := range participant.Status.ChaincodeStatus {
		statuses[joinBundleChaincodeKey(status.Channel, status.Name)] = status
	}

	chaincodes := []joinBundleChaincode{}
	for _, chaincode := range participant.Spec.Chaincodes {
		if _, ok := channelSet[chaincode.Channel]; !ok {
			continue
		}
		status := statuses[joinBundleChaincodeKey(chaincode.Channel, chaincode.Name)]
		chaincodes = append(chaincodes, joinBundleChaincode{
			Name:                 chaincode.Name,
			Channel:              chaincode.Channel,
			Version:              chaincode.Version,
			Image:                chaincode.Image,
			PackageLabel:         chaincode.PackageLabel,
			Sequence:             chaincode.Sequence,
			EndorsementPolicy:    chaincode.EndorsementPolicy,
			InitRequired:         chaincode.InitRequired,
			CollectionConfigHash: chaincode.CollectionConfigHash,
			Committed:            false,
			Ready:                status.Ready,
		})
	}
	return chaincodes
}

func loadJoinBundleSecretKey(
	ctx context.Context,
	client ctrlclient.Client,
	ref joinBundleObjectRef,
	key string,
) (string, error) {
	value, err := loadSecretKey(ctx, client, ref.Namespace, ref.Name, key)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(value)), nil
}

func loadJoinBundleParticipantTLSRoot(
	ctx context.Context,
	client ctrlclient.Client,
	namespace string,
	ref *fabricopsv1alpha1.ParticipantArtifactKeyRef,
) (*joinBundleTLSRoot, error) {
	if ref == nil {
		return nil, fmt.Errorf("artifact ref is required")
	}
	if ref.ConfigMapKeyRef != nil {
		name := strings.TrimSpace(ref.ConfigMapKeyRef.Name)
		key := strings.TrimSpace(ref.ConfigMapKeyRef.Key)
		value, err := loadConfigMapKey(ctx, client, namespace, name, key)
		if err != nil {
			return nil, err
		}
		configMapRef := joinBundleObjectRef{Namespace: namespace, Name: name}
		return &joinBundleTLSRoot{
			ConfigMapRef: &configMapRef,
			CACertPEM:    strings.TrimSpace(string(value)),
		}, nil
	}
	if ref.SecretKeyRef != nil {
		name := strings.TrimSpace(ref.SecretKeyRef.Name)
		key := strings.TrimSpace(ref.SecretKeyRef.Key)
		value, err := loadSecretKey(ctx, client, namespace, name, key)
		if err != nil {
			return nil, err
		}
		secretRef := joinBundleObjectRef{Namespace: namespace, Name: name}
		return &joinBundleTLSRoot{
			SecretRef: &secretRef,
			CACertPEM: strings.TrimSpace(string(value)),
		}, nil
	}
	return nil, fmt.Errorf("artifact ref must set configMapKeyRef or secretKeyRef")
}

func loadConfigMapKey(ctx context.Context, client ctrlclient.Client, namespace, name, key string) ([]byte, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("configmap name is required")
	}
	if strings.TrimSpace(key) == "" {
		return nil, fmt.Errorf("configmap key is required")
	}
	var configMap corev1.ConfigMap
	if err := client.Get(ctx, ctrlclient.ObjectKey{Namespace: namespace, Name: name}, &configMap); err != nil {
		return nil, err
	}
	value := configMap.Data[key]
	if strings.TrimSpace(value) == "" {
		return nil, fmt.Errorf("configmap %s/%s is missing %s", namespace, name, key)
	}
	return []byte(value), nil
}

func joinBundleHostPort(address string, defaultPort int32) (string, int32, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return "", 0, fmt.Errorf("endpoint address is empty")
	}
	host, port, err := stdlibnet.SplitHostPort(address)
	if err == nil {
		parsed, err := strconv.ParseInt(port, 10, 32)
		if err != nil {
			return "", 0, fmt.Errorf("endpoint %q has invalid port %q: %w", address, port, err)
		}
		return host, int32(parsed), nil
	}

	index := strings.LastIndex(address, ":")
	if index < 0 {
		return address, defaultPort, nil
	}
	parsed, parseErr := strconv.ParseInt(address[index+1:], 10, 32)
	if parseErr != nil {
		return "", 0, fmt.Errorf("endpoint %q has invalid port: %w", address, parseErr)
	}
	return address[:index], int32(parsed), nil
}

func joinBundleChaincodeKey(channelName, chaincodeName string) string {
	return joinBundleLookupKey(channelName) + "/" + joinBundleLookupKey(chaincodeName)
}

func joinBundleLookupKey(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
