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
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

type recordedInstall struct {
	Org            string          `json:"org"`
	Peer           string          `json:"peer"`
	ConfigMap      string          `json:"configMap,omitempty"`
	QueryInstalled json.RawMessage `json:"queryInstalled,omitempty"`
}

func runChaincodeInspection(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || (args[0] != "list" && args[0] != "inspect") {
		printLine(stderr, "Usage: fabricopsctl chaincode list|inspect [flags] <fabricnetwork|fabricparticipant>")
		return errUsage
	}
	inspect := args[0] == "inspect"
	var kube kubeOptions
	flags := flag.NewFlagSet("chaincode "+args[0], flag.ContinueOnError)
	flags.SetOutput(stderr)
	bindKubeFlags(flags, &kube)
	channel := flags.String("channel", "", "Filter by channel (required for inspect)")
	name := flags.String("name", "", "Chaincode name (required for inspect)")
	output := flags.String("o", "table", "Output format: table or json")
	participant := flags.Bool("participant", false, "Inspect a FabricParticipant instead of a FabricNetwork")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 1 || (inspect && (*channel == "" || *name == "")) {
		return fmt.Errorf("%w: supply a resource name; inspect also requires --channel and --name", errUsage)
	}
	if *output != "table" && *output != "json" {
		return fmt.Errorf("unsupported output format %q", *output)
	}
	ctx := context.Background()
	if *participant {
		resource, err := getFabricParticipant(ctx, kube, flags.Arg(0))
		if err != nil {
			return err
		}
		return printParticipantChaincodeInspection(stdout, resource.Status.ChaincodeStatus, *channel, *name, *output, inspect)
	}
	client, err := newClient(kube)
	if err != nil {
		return err
	}
	var network fabricopsv1alpha1.FabricNetwork
	if err := client.Get(ctx, ctrlclient.ObjectKey{Namespace: kube.namespace, Name: flags.Arg(0)}, &network); err != nil {
		return err
	}
	statuses := make([]fabricopsv1alpha1.ChaincodeStatus, 0)
	for _, status := range network.Status.ChaincodeStatus {
		if (*channel == "" || status.Channel == *channel) && (*name == "" || status.Name == *name) {
			statuses = append(statuses, status)
		}
	}
	if !inspect {
		if *output == "json" {
			return writeJSON(stdout, statuses)
		}
		if len(statuses) == 0 {
			printLine(stdout, "No recorded chaincode status matched.")
			return nil
		}
		printLine(stdout, "Recorded chaincode status (not a live Fabric query):")
		printChaincodeStatuses(stdout, statuses)
		return nil
	}
	if len(statuses) != 1 {
		return fmt.Errorf("expected one recorded definition for %s/%s; found %d", *channel, *name, len(statuses))
	}
	status := statuses[0]
	committed, err := readRecordedChaincodeJSON(ctx, client,
		status.CommitResultNamespace, status.CommitResultConfigMapName, "querycommitted.json")
	if err != nil {
		return err
	}
	installs, err := readRecordedInstalls(ctx, client, statuses[0].Targets)
	if err != nil {
		return err
	}
	if *output == "json" {
		return writeJSON(stdout, struct {
			Status    fabricopsv1alpha1.ChaincodeStatus `json:"recordedStatus"`
			Installs  []recordedInstall                 `json:"recordedInstalls"`
			Committed json.RawMessage                   `json:"recordedQueryCommitted,omitempty"`
		}{status, installs, committed})
	}
	printLine(stdout, "Recorded chaincode status (not a live Fabric query):")
	printChaincodeStatuses(stdout, statuses)
	printChaincodeDiagnostics(stdout, status)
	if len(committed) == 0 {
		printLine(stdout, "Committed definition evidence not observed.")
	} else {
		printf(stdout, "Committed definition (%s/%s): %s\n",
			status.CommitResultNamespace, status.CommitResultConfigMapName, committed)
	}
	for _, install := range installs {
		if len(install.QueryInstalled) == 0 {
			printf(stdout, "%s/%s: install evidence not observed\n", install.Org, install.Peer)
		} else {
			printf(stdout, "%s/%s (%s): %s\n", install.Org, install.Peer, install.ConfigMap, install.QueryInstalled)
		}
	}
	return nil
}

func printChaincodeDiagnostics(out io.Writer, status fabricopsv1alpha1.ChaincodeStatus) {
	for _, target := range status.Targets {
		printf(out, "Target %s/%s: installed=%t workloadReady=%t",
			target.OrgName, target.PeerName, target.Installed, target.WorkloadReady)
		if target.ApproveJobName != "" {
			printf(out, " approved=%t", target.Approved)
		}
		if target.Message != "" {
			printf(out, " message=%q", target.Message)
		}
		printLine(out)
		printChaincodeArtifact(out, "Install Job", target.Namespace, target.InstallJobName)
		printChaincodeArtifact(out, "Install evidence ConfigMap", target.Namespace, target.PackageIDConfigMapName)
		printChaincodeArtifact(out, "Approve Job", target.Namespace, target.ApproveJobName)
		printChaincodeArtifact(out, "Workload Deployment", target.Namespace, target.WorkloadName)
	}
	printChaincodeArtifact(out, "Commit Job", status.CommitResultNamespace, status.CommitJobName)
	printChaincodeArtifact(out, "Commit evidence ConfigMap",
		status.CommitResultNamespace, status.CommitResultConfigMapName)
}

func printChaincodeArtifact(out io.Writer, label, namespace, name string) {
	if name == "" {
		return
	}
	if namespace == "" {
		printf(out, "  %s: %s (namespace not recorded)\n", label, name)
		return
	}
	printf(out, "  %s: %s/%s\n", label, namespace, name)
}

func printParticipantChaincodeInspection(
	out io.Writer, observed []fabricopsv1alpha1.ParticipantChaincodeStatus, channel, name, output string, inspect bool,
) error {
	statuses := make([]fabricopsv1alpha1.ParticipantChaincodeStatus, 0)
	for _, status := range observed {
		if (channel == "" || status.Channel == channel) && (name == "" || status.Name == name) {
			statuses = append(statuses, status)
		}
	}
	if inspect && len(statuses) != 1 {
		return fmt.Errorf("expected one recorded definition for %s/%s; found %d", channel, name, len(statuses))
	}
	if output == "json" {
		if inspect {
			return writeJSON(out, struct {
				Status fabricopsv1alpha1.ParticipantChaincodeStatus `json:"recordedStatus"`
			}{statuses[0]})
		}
		return writeJSON(out, statuses)
	}
	if len(statuses) == 0 {
		printLine(out, "No recorded chaincode status matched.")
		return nil
	}
	printLine(out, "Recorded participant lifecycle status (not a live Fabric query):")
	printParticipantChaincodeStatuses(out, statuses)
	if inspect {
		printLine(out, "Raw peer install and channel commit evidence are not exposed in participant status.")
	}
	return nil
}

func readRecordedInstalls(
	ctx context.Context, client ctrlclient.Client, targets []fabricopsv1alpha1.ChaincodeTargetStatus,
) ([]recordedInstall, error) {
	results := make([]recordedInstall, 0, len(targets))
	for _, target := range targets {
		result := recordedInstall{Org: target.OrgName, Peer: target.PeerName}
		if target.Namespace != "" && target.PackageIDConfigMapName != "" {
			result.ConfigMap = target.Namespace + "/" + target.PackageIDConfigMapName
			raw, err := readRecordedChaincodeJSON(ctx, client,
				target.Namespace, target.PackageIDConfigMapName, "queryinstalled.json")
			if err != nil {
				return nil, fmt.Errorf("read install evidence for %s/%s: %w", result.Org, result.Peer, err)
			}
			result.QueryInstalled = raw
		}
		results = append(results, result)
	}
	return results, nil
}

func readRecordedChaincodeJSON(
	ctx context.Context, client ctrlclient.Client, namespace, name, key string,
) (json.RawMessage, error) {
	if namespace == "" || name == "" {
		return nil, nil
	}
	raw, err := loadConfigMapKey(ctx, client, namespace, name, key)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s from %s/%s: %w", key, namespace, name, err)
	}
	if !json.Valid(raw) {
		return nil, fmt.Errorf("invalid %s in %s/%s", key, namespace, name)
	}
	return raw, nil
}
