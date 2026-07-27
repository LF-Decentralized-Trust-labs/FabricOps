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
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	fabricopsv1alpha1 "github.com/dpereowei/fabricops/api/v1alpha1"
)

const (
	defaultNamespace = "default"
	defaultWaitFor   = "condition=Ready"
	defaultCondition = "Ready"

	connectionProfileJSONKey = "connection.json"
	connectionProfileYAMLKey = "connection.yaml"
)

var (
	errUsage  = errors.New("usage error")
	cliScheme = runtime.NewScheme()
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(cliScheme))
	utilruntime.Must(fabricopsv1alpha1.AddToScheme(cliScheme))
}

type kubeOptions struct {
	namespace  string
	kubeconfig string
	context    string
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if !errors.Is(err, errUsage) {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		printUsage(stderr)
		return errUsage
	}

	switch args[0] {
	case "help", "-h", "--help":
		printUsage(stdout)
		return nil
	case "status":
		return runStatus(args[1:], stdout, stderr)
	case "wait":
		return runWait(args[1:], stdout, stderr)
	case "connection-profile":
		return runConnectionProfile(args[1:], stdout, stderr)
	case "join-bundle":
		return runJoinBundle(args[1:], stdout, stderr)
	case "invoke":
		return runChaincodeOperation(args[1:], stdout, stderr, chaincodeOperationInvoke)
	case "query":
		return runChaincodeOperation(args[1:], stdout, stderr, chaincodeOperationQuery)
	default:
		printUsage(stderr)
		return fmt.Errorf("%w: unknown command %q", errUsage, args[0])
	}
}

func runStatus(args []string, stdout, stderr io.Writer) error {
	var kube kubeOptions
	var output string
	var participant bool
	flags := flag.NewFlagSet("status", flag.ContinueOnError)
	flags.SetOutput(stderr)
	bindKubeFlags(flags, &kube)
	flags.StringVar(&output, "o", "table", "Output format: table or json")
	flags.BoolVar(&participant, "participant", false, "Treat the resource argument as a FabricParticipant")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		printLine(stderr, "Usage: fabricopsctl status [flags] <fabricnetwork|fabricparticipant>")
		return errUsage
	}

	switch output {
	case "json":
	case "table":
	default:
		return fmt.Errorf("unsupported output format %q", output)
	}

	ctx := context.Background()
	if participant {
		participant, err := getFabricParticipant(ctx, kube, flags.Arg(0))
		if err != nil {
			return err
		}
		if output == "json" {
			return writeJSON(stdout, participant.Status)
		}
		printParticipantStatus(stdout, participant)
		return nil
	}

	network, err := getFabricNetwork(ctx, kube, flags.Arg(0))
	if err != nil {
		return err
	}
	if output == "json" {
		return writeJSON(stdout, network.Status)
	}
	printStatus(stdout, network)
	return nil
}

func runWait(args []string, stdout, stderr io.Writer) error {
	var kube kubeOptions
	var waitFor, timeoutValue, pollIntervalValue string
	var participant bool
	flags := flag.NewFlagSet("wait", flag.ContinueOnError)
	flags.SetOutput(stderr)
	bindKubeFlags(flags, &kube)
	flags.StringVar(&waitFor, "for", defaultWaitFor, "Wait target such as condition=Ready")
	flags.StringVar(&timeoutValue, "timeout", "20m", "How long to wait for readiness")
	flags.StringVar(&pollIntervalValue, "poll-interval", "5s", "How often to poll resource status")
	flags.BoolVar(&participant, "participant", false, "Treat the resource argument as a FabricParticipant")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		printLine(stderr, "Usage: fabricopsctl wait [flags] <fabricnetwork|fabricparticipant>")
		return errUsage
	}
	conditionType, err := waitConditionType(waitFor)
	if err != nil {
		return err
	}

	timeout, err := time.ParseDuration(timeoutValue)
	if err != nil {
		return fmt.Errorf("invalid --timeout value %q: %w", timeoutValue, err)
	}
	pollInterval, err := time.ParseDuration(pollIntervalValue)
	if err != nil {
		return fmt.Errorf("invalid --poll-interval value %q: %w", pollIntervalValue, err)
	}

	if participant {
		return waitForFabricParticipantCondition(
			context.Background(),
			kube,
			flags.Arg(0),
			conditionType,
			timeout,
			pollInterval,
			stdout,
			stderr,
			getFabricParticipant,
		)
	}

	return waitForFabricNetworkCondition(
		context.Background(),
		kube,
		flags.Arg(0),
		conditionType,
		timeout,
		pollInterval,
		stdout,
		stderr,
		getFabricNetwork,
	)
}

func runConnectionProfile(args []string, stdout, stderr io.Writer) error {
	var kube kubeOptions
	var orgName, format, outputPath string
	flags := flag.NewFlagSet("connection-profile", flag.ContinueOnError)
	flags.SetOutput(stderr)
	bindKubeFlags(flags, &kube)
	flags.StringVar(&orgName, "org", "", "Peer organization name; optional when exactly one profile exists")
	flags.StringVar(&format, "format", "yaml", "Profile format: yaml or json")
	flags.StringVar(&outputPath, "out", "", "Write profile to this file instead of stdout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		printLine(stderr, "Usage: fabricopsctl connection-profile [flags] <fabricnetwork>")
		return errUsage
	}

	ctx := context.Background()
	network, err := getFabricNetwork(ctx, kube, flags.Arg(0))
	if err != nil {
		return err
	}
	orgStatus, err := selectConnectionProfileOrg(network.Status.OrgStatus, orgName)
	if err != nil {
		return err
	}

	client, err := newClient(kube)
	if err != nil {
		return err
	}
	var configMap corev1.ConfigMap
	if err := client.Get(ctx, ctrlclient.ObjectKey{
		Namespace: orgStatus.Namespace,
		Name:      orgStatus.ConnectionProfileConfigMapName,
	}, &configMap); err != nil {
		return err
	}

	key, err := connectionProfileKey(format)
	if err != nil {
		return err
	}
	contents, ok := configMap.Data[key]
	if !ok {
		return fmt.Errorf("configmap %s/%s does not contain %s", configMap.Namespace, configMap.Name, key)
	}

	return writeProfileOutput(stdout, outputPath, contents)
}

func bindKubeFlags(flags *flag.FlagSet, kube *kubeOptions) {
	flags.StringVar(&kube.namespace, "n", defaultNamespace, "Resource namespace")
	flags.StringVar(&kube.namespace, "namespace", defaultNamespace, "Resource namespace")
	flags.StringVar(&kube.kubeconfig, "kubeconfig", "", "Path to kubeconfig; defaults to KUBECONFIG or ~/.kube/config")
	flags.StringVar(&kube.context, "context", "", "Kubeconfig context override")
}

func getFabricNetwork(ctx context.Context, kube kubeOptions, name string) (*fabricopsv1alpha1.FabricNetwork, error) {
	client, err := newClient(kube)
	if err != nil {
		return nil, err
	}

	network := &fabricopsv1alpha1.FabricNetwork{}
	if err := client.Get(ctx, ctrlclient.ObjectKey{
		Namespace: kube.namespace,
		Name:      name,
	}, network); err != nil {
		return nil, err
	}

	return network, nil
}

func getFabricParticipant(
	ctx context.Context,
	kube kubeOptions,
	name string,
) (*fabricopsv1alpha1.FabricParticipant, error) {
	client, err := newClient(kube)
	if err != nil {
		return nil, err
	}

	participant := &fabricopsv1alpha1.FabricParticipant{}
	if err := client.Get(ctx, ctrlclient.ObjectKey{
		Namespace: kube.namespace,
		Name:      name,
	}, participant); err != nil {
		return nil, err
	}

	return participant, nil
}

func newClient(kube kubeOptions) (ctrlclient.Client, error) {
	config, err := newRESTConfig(kube)
	if err != nil {
		return nil, err
	}

	return ctrlclient.New(config, ctrlclient.Options{Scheme: cliScheme})
}

func newRESTConfig(kube kubeOptions) (*rest.Config, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kube.kubeconfig != "" {
		rules.ExplicitPath = kube.kubeconfig
	}
	overrides := &clientcmd.ConfigOverrides{}
	if kube.context != "" {
		overrides.CurrentContext = kube.context
	}
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
	if err != nil {
		return nil, err
	}

	return config, nil
}

func printStatus(out io.Writer, network *fabricopsv1alpha1.FabricNetwork) {
	printf(out, "FabricNetwork: %s/%s\n", network.Namespace, network.Name)
	printf(out, "Phase: %s\n", network.Status.Phase)
	if ready := readyConditionSummary(network.Status.Conditions); ready != "" {
		printf(out, "Ready: %s\n", ready)
	}
	if network.Status.Message != "" {
		printf(out, "Message: %s\n", network.Status.Message)
	}

	printOrgStatuses(out, network.Status.OrgStatus)
	printChannelStatuses(out, network.Status.ChannelStatus)
	printChaincodeStatuses(out, network.Status.ChaincodeStatus)
}

func printParticipantStatus(out io.Writer, participant *fabricopsv1alpha1.FabricParticipant) {
	printf(out, "FabricParticipant: %s/%s\n", participant.Namespace, participant.Name)
	printf(out, "Phase: %s\n", participant.Status.Phase)
	if ready := readyConditionSummary(participant.Status.Conditions); ready != "" {
		printf(out, "Ready: %s\n", ready)
	}
	if participant.Status.Message != "" {
		printf(out, "Message: %s\n", participant.Status.Message)
	}
	printf(out, "LocalInfrastructureReady: %t\n", participant.Status.LocalInfrastructureReady)
	printf(out, "RemoteArtifactsReady: %t\n", participant.Status.RemoteArtifactsReady)
	printf(out, "ChannelsReady: %t\n", participant.Status.ChannelsReady)
	printf(out, "ChaincodeLifecycleReady: %t\n", participant.Status.ChaincodeLifecycleReady)

	if participant.Status.LocalOrgStatus.Name != "" || participant.Status.LocalOrgStatus.Namespace != "" {
		printOrgStatuses(out, []fabricopsv1alpha1.OrgStatus{participant.Status.LocalOrgStatus})
	}
	printParticipantChannelStatuses(out, participant.Status.ChannelStatus)
	printParticipantChaincodeStatuses(out, participant.Status.ChaincodeStatus)
}

type fabricNetworkGetter func(
	ctx context.Context,
	kube kubeOptions,
	name string,
) (*fabricopsv1alpha1.FabricNetwork, error)

func waitForFabricNetworkReady(
	ctx context.Context,
	kube kubeOptions,
	name string,
	timeout time.Duration,
	pollInterval time.Duration,
	stdout io.Writer,
	stderr io.Writer,
	getter fabricNetworkGetter,
) error {
	return waitForFabricNetworkCondition(ctx, kube, name, defaultCondition, timeout, pollInterval, stdout, stderr, getter)
}

func waitForFabricNetworkCondition(
	ctx context.Context,
	kube kubeOptions,
	name string,
	conditionType string,
	timeout time.Duration,
	pollInterval time.Duration,
	stdout io.Writer,
	stderr io.Writer,
	getter fabricNetworkGetter,
) error {
	return waitForResourceCondition(
		ctx,
		kube,
		name,
		"FabricNetwork",
		conditionType,
		timeout,
		pollInterval,
		stdout,
		stderr,
		fabricNetworkWaitState(getter),
	)
}

type fabricParticipantGetter func(
	ctx context.Context,
	kube kubeOptions,
	name string,
) (*fabricopsv1alpha1.FabricParticipant, error)

func waitForFabricParticipantReady(
	ctx context.Context,
	kube kubeOptions,
	name string,
	timeout time.Duration,
	pollInterval time.Duration,
	stdout io.Writer,
	stderr io.Writer,
	getter fabricParticipantGetter,
) error {
	return waitForFabricParticipantCondition(
		ctx,
		kube,
		name,
		defaultCondition,
		timeout,
		pollInterval,
		stdout,
		stderr,
		getter,
	)
}

func waitForFabricParticipantCondition(
	ctx context.Context,
	kube kubeOptions,
	name string,
	conditionType string,
	timeout time.Duration,
	pollInterval time.Duration,
	stdout io.Writer,
	stderr io.Writer,
	getter fabricParticipantGetter,
) error {
	return waitForResourceCondition(
		ctx,
		kube,
		name,
		"FabricParticipant",
		conditionType,
		timeout,
		pollInterval,
		stdout,
		stderr,
		fabricParticipantWaitState(getter),
	)
}

type waitResourceState struct {
	namespace        string
	name             string
	conditionIsTrue  bool
	printDiagnostics func(io.Writer)
}

type waitResourceGetter func(
	ctx context.Context,
	kube kubeOptions,
	name string,
	conditionType string,
) (waitResourceState, error)

func waitForResourceCondition(
	ctx context.Context,
	kube kubeOptions,
	name string,
	kind string,
	conditionType string,
	timeout time.Duration,
	pollInterval time.Duration,
	stdout io.Writer,
	stderr io.Writer,
	getter waitResourceGetter,
) error {
	if timeout <= 0 {
		return fmt.Errorf("--timeout must be greater than zero")
	}
	if pollInterval <= 0 {
		return fmt.Errorf("--poll-interval must be greater than zero")
	}

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	var lastState *waitResourceState
	var lastErr error
	for {
		state, err := getter(ctx, kube, name, conditionType)
		if err == nil {
			lastState = &state
			lastErr = nil
			if state.conditionIsTrue {
				printWaitSuccess(stdout, kind, state.namespace, state.name, conditionType)
				return nil
			}
		} else {
			lastErr = err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			printResourceWaitDiagnostics(stderr, lastState, lastErr)
			return waitTimeoutError(kind, kube.namespace, name, conditionType)
		case <-time.After(pollInterval):
		}
	}
}

func fabricNetworkWaitState(getter fabricNetworkGetter) waitResourceGetter {
	return func(ctx context.Context, kube kubeOptions, name string, conditionType string) (waitResourceState, error) {
		network, err := getter(ctx, kube, name)
		if err != nil {
			return waitResourceState{}, err
		}
		return waitResourceState{
			namespace:       network.Namespace,
			name:            network.Name,
			conditionIsTrue: conditionTrue(network.Status.Conditions, conditionType),
			printDiagnostics: func(out io.Writer) {
				printStatus(out, network)
			},
		}, nil
	}
}

func fabricParticipantWaitState(getter fabricParticipantGetter) waitResourceGetter {
	return func(ctx context.Context, kube kubeOptions, name string, conditionType string) (waitResourceState, error) {
		participant, err := getter(ctx, kube, name)
		if err != nil {
			return waitResourceState{}, err
		}
		return waitResourceState{
			namespace:       participant.Namespace,
			name:            participant.Name,
			conditionIsTrue: conditionTrue(participant.Status.Conditions, conditionType),
			printDiagnostics: func(out io.Writer) {
				printParticipantStatus(out, participant)
			},
		}, nil
	}
}

func conditionTrue(conditions []metav1.Condition, conditionType string) bool {
	for _, condition := range conditions {
		if condition.Type == conditionType {
			return condition.Status == metav1.ConditionTrue
		}
	}
	return false
}

func waitConditionType(waitFor string) (string, error) {
	const prefix = "condition="
	waitFor = strings.TrimSpace(waitFor)
	if !strings.HasPrefix(waitFor, prefix) {
		return "", fmt.Errorf("unsupported wait target %q; use condition=<type>", waitFor)
	}
	conditionType := strings.TrimSpace(strings.TrimPrefix(waitFor, prefix))
	if conditionType == "" {
		return "", fmt.Errorf("unsupported wait target %q; condition type is required", waitFor)
	}
	return conditionType, nil
}

func printWaitSuccess(out io.Writer, kind, namespace, name, conditionType string) {
	if conditionType == defaultCondition {
		printf(out, "%s %s/%s is Ready\n", kind, namespace, name)
		return
	}
	printf(out, "%s %s/%s condition %s is True\n", kind, namespace, name, conditionType)
}

func waitTimeoutError(kind, namespace, name, conditionType string) error {
	if conditionType == defaultCondition {
		return fmt.Errorf("timed out waiting for %s %s/%s to be Ready", kind, namespace, name)
	}
	return fmt.Errorf("timed out waiting for %s %s/%s condition %s to be True", kind, namespace, name, conditionType)
}

func printResourceWaitDiagnostics(out io.Writer, state *waitResourceState, lastErr error) {
	if lastErr != nil {
		printf(out, "Last error: %v\n", lastErr)
	}
	if state != nil && state.printDiagnostics != nil {
		state.printDiagnostics(out)
	}
}

func readyConditionSummary(conditions []metav1.Condition) string {
	for _, condition := range conditions {
		if condition.Type == "Ready" {
			if condition.Reason == "" {
				return string(condition.Status)
			}
			return fmt.Sprintf("%s (%s)", condition.Status, condition.Reason)
		}
	}
	return ""
}

func printOrgStatuses(out io.Writer, statuses []fabricopsv1alpha1.OrgStatus) {
	if len(statuses) == 0 {
		return
	}

	printLine(out)
	printLine(out, "Orgs:")
	for _, status := range statuses {
		printf(
			out,
			"- %s namespace=%s ready=%t identity=%t ca=%t\n",
			status.Name,
			status.Namespace,
			status.Ready,
			status.IdentityReady,
			status.CAReady,
		)
		if status.CAEndpoint != "" {
			printf(out, "  ca: %s\n", status.CAEndpoint)
		}
		if status.ConnectionProfileConfigMapName != "" {
			printf(out, "  connectionProfile: %s/%s\n", status.Namespace, status.ConnectionProfileConfigMapName)
		}
		for _, endpoint := range status.OrdererEndpoints {
			printf(
				out,
				"  orderer %s: client=%s admin=%s operations=%s\n",
				endpoint.Name,
				endpoint.ClientAddress,
				endpoint.AdminAddress,
				endpoint.OperationsAddress,
			)
		}
		for _, endpoint := range status.PeerEndpoints {
			printf(
				out,
				"  peer %s: client=%s chaincode=%s operations=%s\n",
				endpoint.Name,
				endpoint.Address,
				endpoint.ChaincodeAddress,
				endpoint.OperationsAddress,
			)
		}
	}
}

func printChannelStatuses(out io.Writer, statuses []fabricopsv1alpha1.ChannelStatus) {
	if len(statuses) == 0 {
		return
	}

	printLine(out)
	printLine(out, "Channels:")
	for _, status := range statuses {
		printf(
			out,
			"- %s ready=%t config=%t block=%t orderers=%d/%d peers=%d/%d",
			status.Name,
			status.Ready,
			status.ConfigReady,
			status.BlockReady,
			status.Orderers.Ready,
			status.Orderers.Desired,
			status.Peers.Ready,
			status.Peers.Desired,
		)
		if status.Message != "" {
			printf(out, " message=%q", status.Message)
		}
		printLine(out)
	}
}

func printParticipantChannelStatuses(out io.Writer, statuses []fabricopsv1alpha1.ParticipantChannelStatus) {
	if len(statuses) == 0 {
		return
	}

	printLine(out)
	printLine(out, "Channels:")
	for _, status := range statuses {
		printf(
			out,
			"- %s ready=%t block=%t joined=%t peers=%d/%d",
			status.Name,
			status.Ready,
			status.BlockReady,
			status.Joined,
			status.Peers.Ready,
			status.Peers.Desired,
		)
		if status.Message != "" {
			printf(out, " message=%q", status.Message)
		}
		printLine(out)
	}
}

func printParticipantChaincodeStatuses(out io.Writer, statuses []fabricopsv1alpha1.ParticipantChaincodeStatus) {
	if len(statuses) == 0 {
		return
	}

	printLine(out)
	printLine(out, "Chaincodes:")
	for _, status := range statuses {
		printf(
			out,
			"- %s channel=%s ready=%t package=%t installed=%t approved=%t",
			status.Name,
			status.Channel,
			status.Ready,
			status.PackageReady,
			status.Installed,
			status.Approved,
		)
		if status.Message != "" {
			printf(out, " message=%q", status.Message)
		}
		printLine(out)
	}
}

func printChaincodeStatuses(out io.Writer, statuses []fabricopsv1alpha1.ChaincodeStatus) {
	if len(statuses) == 0 {
		return
	}

	printLine(out)
	printLine(out, "Chaincodes:")
	for _, status := range statuses {
		printf(
			out,
			"- %s channel=%s version=%s sequence=%d ready=%t installed=%d/%d approved=%d/%d workloads=%d/%d committed=%t",
			status.Name,
			status.Channel,
			status.Version,
			status.Sequence,
			status.Ready,
			status.Installed.Ready,
			status.Installed.Desired,
			status.Approved.Ready,
			status.Approved.Desired,
			status.Workloads.Ready,
			status.Workloads.Desired,
			status.Committed,
		)
		if status.Message != "" {
			printf(out, " message=%q", status.Message)
		}
		printLine(out)
	}
}

func selectConnectionProfileOrg(
	statuses []fabricopsv1alpha1.OrgStatus,
	orgName string,
) (fabricopsv1alpha1.OrgStatus, error) {
	if orgName != "" {
		for _, status := range statuses {
			if strings.EqualFold(status.Name, orgName) {
				if status.ConnectionProfileConfigMapName == "" {
					return fabricopsv1alpha1.OrgStatus{}, fmt.Errorf(
						"org %q does not have a generated connection profile",
						status.Name,
					)
				}
				return status, nil
			}
		}
		return fabricopsv1alpha1.OrgStatus{}, fmt.Errorf("org %q was not found in FabricNetwork status", orgName)
	}

	matches := []fabricopsv1alpha1.OrgStatus{}
	for _, status := range statuses {
		if status.ConnectionProfileConfigMapName != "" {
			matches = append(matches, status)
		}
	}
	switch len(matches) {
	case 0:
		return fabricopsv1alpha1.OrgStatus{}, errors.New("no generated connection profiles were found")
	case 1:
		return matches[0], nil
	default:
		names := make([]string, 0, len(matches))
		for _, status := range matches {
			names = append(names, status.Name)
		}
		return fabricopsv1alpha1.OrgStatus{}, fmt.Errorf(
			"multiple connection profiles exist (%s); pass --org",
			strings.Join(names, ", "),
		)
	}
}

func connectionProfileKey(format string) (string, error) {
	switch strings.ToLower(format) {
	case "yaml", "yml":
		return connectionProfileYAMLKey, nil
	case "json":
		return connectionProfileJSONKey, nil
	default:
		return "", fmt.Errorf("unsupported profile format %q", format)
	}
}

func writeJSON(out io.Writer, value any) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, string(encoded))
	return err
}

func writeProfileOutput(out io.Writer, outputPath, contents string) error {
	if outputPath != "" {
		return os.WriteFile(outputPath, []byte(contents), 0644)
	}
	if strings.HasSuffix(contents, "\n") {
		_, err := fmt.Fprint(out, contents)
		return err
	}
	_, err := fmt.Fprintln(out, contents)
	return err
}

func printf(out io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(out, format, args...)
}

func printLine(out io.Writer, args ...any) {
	_, _ = fmt.Fprintln(out, args...)
}

func printUsage(out io.Writer) {
	printLine(out, `Usage:
  fabricopsctl status [flags] <fabricnetwork>
  fabricopsctl status --participant [flags] <fabricparticipant>
  fabricopsctl wait [flags] <fabricnetwork>
  fabricopsctl wait --participant [flags] <fabricparticipant>
  fabricopsctl connection-profile [flags] <fabricnetwork>
  fabricopsctl join-bundle --org <org> [flags] <fabricnetwork>
  fabricopsctl join-bundle participant [flags] <fabricparticipant>
  fabricopsctl join-bundle validate [flags] <bundle.json>
  fabricopsctl join-bundle plan [flags] <bundle.json>
  fabricopsctl join-bundle render-org [flags] <bundle.json>
  fabricopsctl join-bundle render-update [flags] <bundle.json>
  fabricopsctl invoke [flags] <fabricnetwork>
  fabricopsctl invoke --participant [flags] <fabricparticipant>
  fabricopsctl query [flags] <fabricnetwork>
  fabricopsctl query --participant [flags] <fabricparticipant>

Common flags:
  -n, --namespace string   Resource namespace (default "default")
      --kubeconfig string  Path to kubeconfig
      --context string     Kubeconfig context override

Examples:
  fabricopsctl status fabricnetwork-sample
  fabricopsctl status --participant bankb-participant
  fabricopsctl status -n default -o json fabricnetwork-sample
  fabricopsctl wait -n default --timeout 20m fabricnetwork-sample
  fabricopsctl wait --participant -n default --timeout 20m bankb-participant
  fabricopsctl connection-profile --org BankA --format yaml fabricnetwork-sample
  fabricopsctl connection-profile --org BankA --format json --out connection-banka.json fabricnetwork-sample
  fabricopsctl join-bundle --org BankA --out banka-join-bundle.json fabricnetwork-sample
  fabricopsctl join-bundle participant --out bankb-join-bundle.json bankb-participant
  fabricopsctl join-bundle validate banka-join-bundle.json
  fabricopsctl join-bundle plan --channel settlement banka-join-bundle.json
  fabricopsctl join-bundle render-org --channel settlement --out banka-org.json banka-join-bundle.json
  fabricopsctl join-bundle render-update --channel settlement --out settlement-join-update.sh banka-join-bundle.json
  fabricopsctl query --org BankA --channel settlement \
    --chaincode settlement --function readSettlement --args '["id1"]' -o json fabricnetwork-sample
  fabricopsctl invoke --org BankA --peer BankA/peer0 --peer BankB/peer0 \
    --channel settlement --chaincode settlement --function createSettlement \
    --args '["id1","alice","bob","100","USD"]' fabricnetwork-sample
  fabricopsctl query --participant --org BankB --channel settlement \
    --chaincode settlement --function readSettlement --args '["id1"]' bankb-participant`)
}
