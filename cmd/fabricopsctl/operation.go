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
	"hash/fnv"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	fabricopsv1alpha1 "github.com/LF-Decentralized-Trust-labs/FabricOps/api/v1alpha1"
)

const (
	chaincodeOperationInvoke = "invoke"
	chaincodeOperationQuery  = "query"

	operationOutputText = "text"
	operationOutputJSON = "json"

	operationAdminMSPPath = "/fabricops/operation/admin-msp"
	operationAdminTLSPath = "/fabricops/operation/admin-tls"
	operationTLSRootPath  = "/fabricops/operation/tls-roots"

	mspConfigKey    = "config.yaml"
	mspCACertKey    = "cacert.pem"
	mspTLSCACertKey = "tlscacert.pem"
	mspSignCertKey  = "signcert.pem"
	mspKeyStoreKey  = "keystore.pem"

	tlsCACertKey     = "ca.crt"
	tlsClientCertKey = "client.crt"
	tlsClientKeyKey  = "client.key"

	payloadFlagUsage = `Raw Fabric chaincode JSON payload, for example {"Args":["fn","arg"]}`
)

type chaincodeOperationOptions struct {
	kube         kubeOptions
	channel      string
	chaincode    string
	org          string
	peers        stringListFlag
	function     string
	argsJSON     string
	payload      string
	transient    string
	timeout      string
	output       string
	waitForEvent bool
	keepJob      bool
	participant  bool
}

type operationPeerTarget struct {
	orgName      string
	status       fabricopsv1alpha1.OrgStatus
	endpoint     fabricopsv1alpha1.PeerEndpointStatus
	tlsRootCARef *fabricopsv1alpha1.ParticipantArtifactKeyRef
}

type operationResult struct {
	Operation   string                `json:"operation"`
	Network     string                `json:"network"`
	Namespace   string                `json:"namespace"`
	Channel     string                `json:"channel"`
	Chaincode   string                `json:"chaincode"`
	Function    string                `json:"function"`
	Org         string                `json:"org"`
	Job         operationResultJob    `json:"job"`
	Peers       []operationResultPeer `json:"peers"`
	Succeeded   bool                  `json:"succeeded"`
	JobRetained bool                  `json:"jobRetained"`
	Logs        string                `json:"logs,omitempty"`
}

type operationResultJob struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

type operationResultPeer struct {
	Org     string `json:"org"`
	Name    string `json:"name"`
	Address string `json:"address,omitempty"`
}

func runChaincodeOperation(args []string, stdout, stderr io.Writer, operation string) error {
	var options chaincodeOperationOptions
	flags := flag.NewFlagSet(operation, flag.ContinueOnError)
	flags.SetOutput(stderr)
	bindKubeFlags(flags, &options.kube)
	flags.StringVar(&options.channel, "channel", "", "Channel name")
	flags.StringVar(&options.chaincode, "chaincode", "", "Chaincode name")
	flags.StringVar(&options.org, "org", "", "Submitting peer organization")
	flags.Var(&options.peers, "peer", "Target peer as Org/peer or peer; repeat for multi-org endorsement")
	flags.StringVar(&options.function, "function", "", "Chaincode function")
	flags.StringVar(&options.argsJSON, "args", "[]", "JSON array of string arguments")
	flags.StringVar(&options.payload, "payload", "", payloadFlagUsage)
	flags.StringVar(&options.transient, "transient", "", "Raw transient JSON for invoke operations")
	flags.StringVar(&options.timeout, "timeout", "180s", "How long to wait for the operation Job")
	flags.StringVar(&options.output, "o", operationOutputText, "Output format: text or json")
	flags.StringVar(&options.output, "output", operationOutputText, "Output format: text or json")
	flags.BoolVar(&options.waitForEvent, "wait-for-event", true, "Pass --waitForEvent to invoke")
	flags.BoolVar(&options.keepJob, "keep-job", false, "Keep the operation Job and temporary Secret")
	flags.BoolVar(&options.participant, "participant", false, "Treat the resource argument as a FabricParticipant")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		printLine(stderr, "Usage: fabricopsctl "+operation+" [flags] <fabricnetwork|fabricparticipant>")
		return errUsage
	}
	if err := validateOperationOptions(options); err != nil {
		return err
	}

	return runChaincodeOperationWithOptions(context.Background(), flags.Arg(0), operation, options, stdout)
}

func validateOperationOptions(options chaincodeOperationOptions) error {
	missing := []string{}
	if options.channel == "" {
		missing = append(missing, "--channel")
	}
	if options.chaincode == "" {
		missing = append(missing, "--chaincode")
	}
	if options.payload == "" && options.function == "" {
		missing = append(missing, "--function")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required flags: %s", strings.Join(missing, ", "))
	}
	if options.payload != "" && options.function != "" {
		return fmt.Errorf("--payload cannot be combined with --function")
	}
	if options.payload != "" && options.argsJSON != "" && options.argsJSON != "[]" {
		return fmt.Errorf("--payload cannot be combined with --args")
	}
	if err := validateOperationOutput(options.output); err != nil {
		return err
	}
	return nil
}

func runChaincodeOperationWithOptions(
	ctx context.Context,
	networkName string,
	operation string,
	options chaincodeOperationOptions,
	stdout io.Writer,
) error {
	timeout, err := time.ParseDuration(options.timeout)
	if err != nil {
		return err
	}
	if options.participant {
		return runParticipantChaincodeOperationWithOptions(ctx, networkName, operation, options, timeout, stdout)
	}
	network, err := getFabricNetwork(ctx, options.kube, networkName)
	if err != nil {
		return err
	}
	payload, function, err := resolveChaincodePayload(options)
	if err != nil {
		return err
	}
	options.function = function
	targets, submitter, err := selectOperationTargets(network.Status.OrgStatus, options.org, options.peers)
	if err != nil {
		return err
	}
	if err := validateSelectedOperationTargets(operation, submitter, targets); err != nil {
		return err
	}
	mspID, err := mspIDForOrg(network, submitter.Name)
	if err != nil {
		return err
	}
	orderer, err := selectOperationOrderer(network.Status.OrgStatus)
	if err != nil {
		return err
	}

	ctrlClient, err := newClient(options.kube)
	if err != nil {
		return err
	}
	restConfig, err := newRESTConfig(options.kube)
	if err != nil {
		return err
	}
	kubeClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return err
	}

	jobName := operationJobName(network.Name, operation)
	tlsSecretName := jobName + "-tls-roots"
	tlsEnabled := network.Spec.Global.TLS
	if tlsEnabled {
		if err := ensureOperationTLSSecret(ctx, ctrlClient, tlsSecretName, submitter, orderer, targets); err != nil {
			return err
		}
	}

	job := buildOperationJob(
		network,
		operation,
		options,
		payload,
		jobName,
		tlsSecretName,
		mspID,
		submitter,
		orderer,
		targets,
	)
	if err := ctrlClient.Create(ctx, job); err != nil {
		return err
	}

	completed, err := waitForOperationJob(ctx, ctrlClient, job.Namespace, job.Name, timeout)
	logs, logErr := operationLogs(ctx, kubeClient, job.Namespace, job.Name)
	if err != nil {
		return err
	}
	if logErr != nil {
		return logErr
	}
	result := buildOperationResult(operation, network, options, job, submitter, targets, completed, logs)
	if err := writeOperationResult(stdout, options.output, result); err != nil {
		return err
	}
	if !completed {
		return fmt.Errorf("operation job %s/%s failed", job.Namespace, job.Name)
	}
	if !options.keepJob {
		cleanupOperationObjects(ctx, ctrlClient, job, tlsEnabled, tlsSecretName)
	}

	return nil
}

func runParticipantChaincodeOperationWithOptions(
	ctx context.Context,
	participantName string,
	operation string,
	options chaincodeOperationOptions,
	timeout time.Duration,
	stdout io.Writer,
) error {
	participant, err := getFabricParticipant(ctx, options.kube, participantName)
	if err != nil {
		return err
	}
	payload, function, err := resolveChaincodePayload(options)
	if err != nil {
		return err
	}
	options.function = function
	statuses := participantOperationOrgStatuses(participant)
	targets, submitter, err := selectOperationTargets(
		statuses,
		options.org,
		options.peers,
	)
	if err != nil {
		return err
	}
	targets = decorateParticipantOperationTargets(participant, targets)
	if err := validateSelectedOperationTargets(operation, submitter, targets); err != nil {
		return err
	}
	mspID, err := mspIDForParticipantOrg(participant, submitter.Name)
	if err != nil {
		return err
	}
	ordererSpec, err := selectParticipantOperationOrderer(participant)
	if err != nil {
		return err
	}
	orderer := participantOperationOrdererStatus(ordererSpec)

	ctrlClient, err := newClient(options.kube)
	if err != nil {
		return err
	}
	restConfig, err := newRESTConfig(options.kube)
	if err != nil {
		return err
	}
	kubeClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return err
	}

	network := participantOperationNetwork(participant)
	jobName := operationJobName(participant.Name, operation)
	tlsSecretName := jobName + "-tls-roots"
	tlsEnabled := participant.Spec.Global.TLS
	if tlsEnabled {
		if err := ensureParticipantOperationTLSSecret(
			ctx,
			ctrlClient,
			tlsSecretName,
			participant,
			submitter,
			ordererSpec,
			targets,
		); err != nil {
			return err
		}
	}

	job := buildOperationJob(
		network,
		operation,
		options,
		payload,
		jobName,
		tlsSecretName,
		mspID,
		submitter,
		orderer,
		targets,
	)
	if err := ctrlClient.Create(ctx, job); err != nil {
		return err
	}

	completed, err := waitForOperationJob(ctx, ctrlClient, job.Namespace, job.Name, timeout)
	logs, logErr := operationLogs(ctx, kubeClient, job.Namespace, job.Name)
	if err != nil {
		return err
	}
	if logErr != nil {
		return logErr
	}
	result := buildOperationResult(operation, network, options, job, submitter, targets, completed, logs)
	if err := writeOperationResult(stdout, options.output, result); err != nil {
		return err
	}
	if !completed {
		return fmt.Errorf("operation job %s/%s failed", job.Namespace, job.Name)
	}
	if !options.keepJob {
		cleanupOperationObjects(ctx, ctrlClient, job, tlsEnabled, tlsSecretName)
	}

	return nil
}

func validateOperationOutput(output string) error {
	switch output {
	case "", operationOutputText, operationOutputJSON:
		return nil
	default:
		return fmt.Errorf("unsupported output format %q", output)
	}
}

func parseChaincodeArgs(argsJSON string) ([]string, error) {
	var args []string
	if err := json.Unmarshal([]byte(argsJSON), &args); err != nil {
		return nil, fmt.Errorf("--args must be a JSON array of strings: %w", err)
	}
	return args, nil
}

func resolveChaincodePayload(options chaincodeOperationOptions) (string, string, error) {
	if options.payload != "" {
		return normalizeRawChaincodePayload(options.payload)
	}

	args, err := parseChaincodeArgs(options.argsJSON)
	if err != nil {
		return "", "", err
	}
	payload, err := chaincodePayload(options.function, args)
	if err != nil {
		return "", "", err
	}
	return payload, options.function, nil
}

func chaincodePayload(function string, args []string) (string, error) {
	payloadArgs := append([]string{function}, args...)
	payload := map[string][]string{"Args": payloadArgs}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func normalizeRawChaincodePayload(payloadJSON string) (string, string, error) {
	var payload struct {
		Args []string `json:"Args"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return "", "", fmt.Errorf("--payload must be a Fabric chaincode JSON payload: %w", err)
	}
	if len(payload.Args) == 0 {
		return "", "", fmt.Errorf("--payload Args must contain at least the chaincode function")
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", "", err
	}
	return string(encoded), payload.Args[0], nil
}

func selectOperationTargets(
	statuses []fabricopsv1alpha1.OrgStatus,
	orgName string,
	peerSelectors []string,
) ([]operationPeerTarget, fabricopsv1alpha1.OrgStatus, error) {
	submitter, err := selectOperationOrg(statuses, orgName)
	if err != nil {
		return nil, fabricopsv1alpha1.OrgStatus{}, err
	}
	if len(peerSelectors) == 0 {
		if len(submitter.PeerEndpoints) != 1 {
			return nil, submitter, fmt.Errorf("org %q has %d peers; pass --peer", submitter.Name, len(submitter.PeerEndpoints))
		}
		return []operationPeerTarget{{
			orgName:  submitter.Name,
			status:   submitter,
			endpoint: submitter.PeerEndpoints[0],
		}}, submitter, nil
	}

	targets := make([]operationPeerTarget, 0, len(peerSelectors))
	for _, selector := range peerSelectors {
		target, err := selectOperationPeer(statuses, submitter.Name, selector)
		if err != nil {
			return nil, submitter, err
		}
		targets = append(targets, target)
	}
	if !targetsIncludeOrg(targets, submitter.Name) {
		return nil, submitter, fmt.Errorf("at least one --peer must belong to submitting org %q", submitter.Name)
	}
	return targets, submitter, nil
}

func validateSelectedOperationTargets(
	operation string,
	submitter fabricopsv1alpha1.OrgStatus,
	targets []operationPeerTarget,
) error {
	if len(targets) == 0 {
		return errors.New("no peer targets were selected")
	}
	if operation == chaincodeOperationQuery {
		if len(targets) != 1 {
			return errors.New("query supports exactly one --peer")
		}
		if !strings.EqualFold(targets[0].orgName, submitter.Name) {
			return fmt.Errorf("query peer %q must belong to submitting org %q", targets[0].endpoint.Name, submitter.Name)
		}
	}
	return nil
}

func selectOperationOrg(statuses []fabricopsv1alpha1.OrgStatus, orgName string) (fabricopsv1alpha1.OrgStatus, error) {
	if orgName != "" {
		for _, status := range statuses {
			if strings.EqualFold(status.Name, orgName) {
				if len(status.PeerEndpoints) == 0 {
					return fabricopsv1alpha1.OrgStatus{}, fmt.Errorf("org %q has no peer endpoints", status.Name)
				}
				return status, nil
			}
		}
		return fabricopsv1alpha1.OrgStatus{}, fmt.Errorf("org %q was not found in FabricNetwork status", orgName)
	}

	matches := []fabricopsv1alpha1.OrgStatus{}
	for _, status := range statuses {
		if len(status.PeerEndpoints) > 0 {
			matches = append(matches, status)
		}
	}
	if len(matches) != 1 {
		return fabricopsv1alpha1.OrgStatus{}, fmt.Errorf("found %d peer orgs; pass --org", len(matches))
	}
	return matches[0], nil
}

func selectOperationPeer(
	statuses []fabricopsv1alpha1.OrgStatus,
	defaultOrg string,
	selector string,
) (operationPeerTarget, error) {
	orgName := defaultOrg
	peerName := selector
	if strings.Contains(selector, "/") {
		parts := strings.Split(selector, "/")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return operationPeerTarget{}, fmt.Errorf("peer selector %q must use Org/peer", selector)
		}
		orgName = parts[0]
		peerName = parts[1]
	}

	for _, status := range statuses {
		if !strings.EqualFold(status.Name, orgName) {
			continue
		}
		for _, endpoint := range status.PeerEndpoints {
			if endpoint.Name == peerName {
				return operationPeerTarget{orgName: status.Name, status: status, endpoint: endpoint}, nil
			}
		}
		return operationPeerTarget{}, fmt.Errorf("peer %q was not found in org %q", peerName, status.Name)
	}
	return operationPeerTarget{}, fmt.Errorf("org %q was not found in FabricNetwork status", orgName)
}

func targetsIncludeOrg(targets []operationPeerTarget, orgName string) bool {
	for _, target := range targets {
		if strings.EqualFold(target.orgName, orgName) {
			return true
		}
	}
	return false
}

func selectOperationOrderer(
	statuses []fabricopsv1alpha1.OrgStatus,
) (fabricopsv1alpha1.OrdererEndpointStatus, error) {
	for _, status := range statuses {
		if len(status.OrdererEndpoints) > 0 {
			return status.OrdererEndpoints[0], nil
		}
	}
	return fabricopsv1alpha1.OrdererEndpointStatus{}, fmt.Errorf("no orderer endpoint found in FabricNetwork status")
}

func ensureOperationTLSSecret(
	ctx context.Context,
	client ctrlclient.Client,
	name string,
	submitter fabricopsv1alpha1.OrgStatus,
	orderer fabricopsv1alpha1.OrdererEndpointStatus,
	targets []operationPeerTarget,
) error {
	ordererNamespace := strings.TrimSpace(orderer.Namespace)
	if ordererNamespace == "" {
		var err error
		ordererNamespace, err = endpointNamespace(orderer.ClientAddress)
		if err != nil {
			return err
		}
	}
	ordererRoot, err := loadSecretKey(ctx, client, ordererNamespace, orderer.Name+"-tls", tlsCACertKey)
	if err != nil {
		return err
	}
	return ensureOperationTLSSecretWithOrdererRoot(ctx, client, name, submitter, ordererRoot, targets)
}

func ensureParticipantOperationTLSSecret(
	ctx context.Context,
	client ctrlclient.Client,
	name string,
	participant *fabricopsv1alpha1.FabricParticipant,
	submitter fabricopsv1alpha1.OrgStatus,
	orderer fabricopsv1alpha1.ParticipantOrdererEndpoint,
	targets []operationPeerTarget,
) error {
	ordererRoot, err := loadParticipantOperationArtifact(ctx, client, participant.Namespace, orderer.TLSRootCARef)
	if err != nil {
		return err
	}
	data := map[string][]byte{
		"orderer-ca.crt": ordererRoot,
	}
	for i, target := range targets {
		var root []byte
		var err error
		if target.tlsRootCARef != nil {
			root, err = loadParticipantOperationArtifact(ctx, client, participant.Namespace, target.tlsRootCARef)
		} else {
			root, err = loadSecretKey(ctx, client, target.status.Namespace, target.endpoint.Name+"-tls", tlsCACertKey)
		}
		if err != nil {
			return err
		}
		data[fmt.Sprintf("peer-%d-ca.crt", i)] = root
	}
	return createOperationTLSSecret(ctx, client, name, submitter.Namespace, data)
}

func ensureOperationTLSSecretWithOrdererRoot(
	ctx context.Context,
	client ctrlclient.Client,
	name string,
	submitter fabricopsv1alpha1.OrgStatus,
	ordererRoot []byte,
	targets []operationPeerTarget,
) error {
	data := map[string][]byte{
		"orderer-ca.crt": ordererRoot,
	}
	for i, target := range targets {
		root, err := loadSecretKey(ctx, client, target.status.Namespace, target.endpoint.Name+"-tls", tlsCACertKey)
		if err != nil {
			return err
		}
		data[fmt.Sprintf("peer-%d-ca.crt", i)] = root
	}
	return createOperationTLSSecret(ctx, client, name, submitter.Namespace, data)
}

func createOperationTLSSecret(
	ctx context.Context,
	client ctrlclient.Client,
	name string,
	namespace string,
	data map[string][]byte,
) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "fabricops",
				"app.kubernetes.io/component": "operation",
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: data,
	}
	return client.Create(ctx, secret)
}

func loadParticipantOperationArtifact(
	ctx context.Context,
	client ctrlclient.Client,
	namespace string,
	ref *fabricopsv1alpha1.ParticipantArtifactKeyRef,
) ([]byte, error) {
	if ref == nil {
		return nil, fmt.Errorf("participant orderer TLS root ref is required")
	}
	if ref.ConfigMapKeyRef != nil {
		return loadConfigMapKey(ctx, client, namespace, ref.ConfigMapKeyRef.Name, ref.ConfigMapKeyRef.Key)
	}
	if ref.SecretKeyRef != nil {
		return loadSecretKey(ctx, client, namespace, ref.SecretKeyRef.Name, ref.SecretKeyRef.Key)
	}
	return nil, fmt.Errorf("participant artifact ref must set configMapKeyRef or secretKeyRef")
}

func loadSecretKey(ctx context.Context, client ctrlclient.Client, namespace, name, key string) ([]byte, error) {
	var secret corev1.Secret
	if err := client.Get(ctx, ctrlclient.ObjectKey{Namespace: namespace, Name: name}, &secret); err != nil {
		return nil, err
	}
	value := secret.Data[key]
	if len(value) == 0 {
		return nil, fmt.Errorf("secret %s/%s is missing %s", namespace, name, key)
	}
	return value, nil
}

func buildOperationJob(
	network *fabricopsv1alpha1.FabricNetwork,
	operation string,
	options chaincodeOperationOptions,
	payload string,
	jobName string,
	tlsSecretName string,
	mspID string,
	submitter fabricopsv1alpha1.OrgStatus,
	orderer fabricopsv1alpha1.OrdererEndpointStatus,
	targets []operationPeerTarget,
) *batchv1.Job {
	backoffLimit := int32(0)
	script := operationScript(network.Spec.Global.TLS, options.waitForEvent, len(targets))
	labels := map[string]string{
		"app.kubernetes.io/name":      "fabricops",
		"app.kubernetes.io/component": "operation",
		"fabricops.io/fabricnetwork":  network.Name,
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: submitter.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:         operation,
							Image:        fabricToolsImage(network.Spec.Global.FabricVersion),
							Env:          operationEnv(operation, options, payload, mspID, submitter, orderer, targets),
							Command:      []string{"sh", "-ec", script},
							VolumeMounts: operationVolumeMounts(network.Spec.Global.TLS, len(targets)),
						},
					},
					Volumes: operationVolumes(network.Spec.Global.TLS, submitter, tlsSecretName, len(targets)),
				},
			},
		},
	}
}

func operationEnv(
	operation string,
	options chaincodeOperationOptions,
	payload string,
	mspID string,
	submitter fabricopsv1alpha1.OrgStatus,
	orderer fabricopsv1alpha1.OrdererEndpointStatus,
	targets []operationPeerTarget,
) []corev1.EnvVar {
	env := []corev1.EnvVar{
		{Name: "FABRICOPS_OPERATION", Value: operation},
		{Name: "FABRICOPS_CHANNEL", Value: options.channel},
		{Name: "FABRICOPS_CHAINCODE", Value: options.chaincode},
		{Name: "FABRICOPS_PAYLOAD", Value: payload},
		{Name: "FABRICOPS_TRANSIENT", Value: options.transient},
		{Name: "FABRICOPS_MSP_ID", Value: mspID},
		{Name: "FABRICOPS_ORDERER_ADDRESS", Value: orderer.ClientAddress},
		{Name: "FABRICOPS_ORDERER_TLS_HOSTNAME_OVERRIDE", Value: orderer.TLSHostnameOverride},
		{Name: "FABRICOPS_CORE_PEER_ADDRESS", Value: submitterPeerAddress(submitter.Name, targets)},
		{Name: "FABRICOPS_CORE_PEER_TLS_ROOT", Value: submitterPeerTLSRoot(submitter.Name, targets)},
	}
	for i, target := range targets {
		env = append(env, corev1.EnvVar{
			Name:  fmt.Sprintf("FABRICOPS_PEER_ADDRESS_%d", i),
			Value: target.endpoint.Address,
		})
	}
	return env
}

func operationScript(tlsEnabled bool, waitForEvent bool, peerCount int) string {
	tlsSetup := "export CORE_PEER_TLS_ENABLED=false"
	invokeTLSArgs := ""
	if tlsEnabled {
		tlsSetup = fmt.Sprintf(`export CORE_PEER_TLS_ENABLED=true
export CORE_PEER_TLS_CERT_FILE=%s/client.crt
export CORE_PEER_TLS_KEY_FILE=%s/client.key
export CORE_PEER_TLS_ROOTCERT_FILE="$FABRICOPS_CORE_PEER_TLS_ROOT"`, operationAdminTLSPath, operationAdminTLSPath)
		invokeTLSArgs = fmt.Sprintf(`set -- "$@" --tls --cafile %s/orderer-ca.crt
if [ -n "$FABRICOPS_ORDERER_TLS_HOSTNAME_OVERRIDE" ]; then
  set -- "$@" --ordererTLSHostnameOverride "$FABRICOPS_ORDERER_TLS_HOSTNAME_OVERRIDE"
fi`, operationTLSRootPath)
	}

	waitArg := ""
	if waitForEvent {
		waitArg = `set -- "$@" --waitForEvent`
	}

	peerArgs := operationPeerArgs(tlsEnabled, peerCount)

	return fmt.Sprintf(`set -eu

export CORE_PEER_LOCALMSPID="$FABRICOPS_MSP_ID"
export CORE_PEER_ADDRESS="$FABRICOPS_CORE_PEER_ADDRESS"
export CORE_PEER_MSPCONFIGPATH=%s
%s

if [ "$FABRICOPS_OPERATION" = "query" ]; then
  set -- peer chaincode query \
    -C "$FABRICOPS_CHANNEL" \
    -n "$FABRICOPS_CHAINCODE" \
    -c "$FABRICOPS_PAYLOAD"

  if [ -n "$FABRICOPS_TRANSIENT" ]; then
    set -- "$@" --transient "$FABRICOPS_TRANSIENT"
  fi

  "$@"
  exit 0
fi

set -- peer chaincode invoke \
  -o "$FABRICOPS_ORDERER_ADDRESS" \
  -C "$FABRICOPS_CHANNEL" \
  -n "$FABRICOPS_CHAINCODE" \
  -c "$FABRICOPS_PAYLOAD"
%s
%s

if [ -n "$FABRICOPS_TRANSIENT" ]; then
  set -- "$@" --transient "$FABRICOPS_TRANSIENT"
fi

%s

"$@"
`, operationAdminMSPPath, tlsSetup, invokeTLSArgs, waitArg, peerArgs)
}

func operationPeerArgs(tlsEnabled bool, peerCount int) string {
	var builder strings.Builder
	for i := range peerCount {
		fmt.Fprintf(&builder, "set -- \"$@\" --peerAddresses \"$FABRICOPS_PEER_ADDRESS_%d\"\n", i)
		if tlsEnabled {
			fmt.Fprintf(
				&builder,
				"set -- \"$@\" --tlsRootCertFiles \"%s/peer-%d-ca.crt\"\n",
				operationTLSRootPath,
				i,
			)
		}
	}
	return builder.String()
}

func operationVolumeMounts(tlsEnabled bool, peerCount int) []corev1.VolumeMount {
	mounts := []corev1.VolumeMount{
		{Name: "admin-msp", MountPath: operationAdminMSPPath, ReadOnly: true},
	}
	if tlsEnabled {
		mounts = append(mounts,
			corev1.VolumeMount{Name: "admin-tls", MountPath: operationAdminTLSPath, ReadOnly: true},
			corev1.VolumeMount{Name: "tls-roots", MountPath: operationTLSRootPath, ReadOnly: true},
		)
	}
	_ = peerCount
	return mounts
}

func operationVolumes(
	tlsEnabled bool,
	submitter fabricopsv1alpha1.OrgStatus,
	tlsSecretName string,
	peerCount int,
) []corev1.Volume {
	adminName := sanitizeName(submitter.Name + "-admin")
	mspItems := []corev1.KeyToPath{
		{Key: mspConfigKey, Path: mspConfigKey},
		{Key: mspCACertKey, Path: "cacerts/ca.pem"},
		{Key: mspSignCertKey, Path: "signcerts/cert.pem"},
		{Key: mspKeyStoreKey, Path: "keystore/key.pem"},
	}
	if tlsEnabled {
		mspItems = append(mspItems, corev1.KeyToPath{Key: mspTLSCACertKey, Path: "tlscacerts/tlsca.pem"})
	}
	volumes := []corev1.Volume{
		{
			Name: "admin-msp",
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName: adminName + "-msp",
				Items:      mspItems,
			}},
		},
	}
	if tlsEnabled {
		volumes = append(volumes,
			corev1.Volume{
				Name: "admin-tls",
				VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
					SecretName: adminName + "-tls",
					Items: []corev1.KeyToPath{
						{Key: tlsClientCertKey, Path: tlsClientCertKey},
						{Key: tlsClientKeyKey, Path: tlsClientKeyKey},
						{Key: tlsCACertKey, Path: tlsCACertKey},
					},
				}},
			},
			corev1.Volume{
				Name: "tls-roots",
				VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
					SecretName: tlsSecretName,
					Items:      tlsRootItems(peerCount),
				}},
			},
		)
	}
	return volumes
}

func tlsRootItems(peerCount int) []corev1.KeyToPath {
	items := []corev1.KeyToPath{{Key: "orderer-ca.crt", Path: "orderer-ca.crt"}}
	for i := range peerCount {
		items = append(items, corev1.KeyToPath{
			Key:  fmt.Sprintf("peer-%d-ca.crt", i),
			Path: fmt.Sprintf("peer-%d-ca.crt", i),
		})
	}
	return items
}

func waitForOperationJob(
	ctx context.Context,
	client ctrlclient.Client,
	namespace string,
	name string,
	timeout time.Duration,
) (bool, error) {
	deadline := time.Now().Add(timeout)
	for {
		var job batchv1.Job
		if err := client.Get(ctx, ctrlclient.ObjectKey{Namespace: namespace, Name: name}, &job); err != nil {
			return false, err
		}
		if job.Status.Succeeded > 0 {
			return true, nil
		}
		if job.Status.Failed > 0 {
			return false, nil
		}
		if time.Now().After(deadline) {
			return false, fmt.Errorf("timed out waiting for operation job %s/%s", namespace, name)
		}
		time.Sleep(2 * time.Second)
	}
}

func operationLogs(
	ctx context.Context,
	kubeClient kubernetes.Interface,
	namespace string,
	jobName string,
) ([]byte, error) {
	pods, err := kubeClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "job-name=" + jobName,
	})
	if err != nil {
		return nil, err
	}
	if len(pods.Items) == 0 {
		return nil, fmt.Errorf("no pods found for operation job %s/%s", namespace, jobName)
	}
	logs, err := kubeClient.CoreV1().Pods(namespace).GetLogs(pods.Items[0].Name, &corev1.PodLogOptions{}).Do(ctx).Raw()
	if err != nil {
		return nil, err
	}
	return logs, nil
}

func buildOperationResult(
	operation string,
	network *fabricopsv1alpha1.FabricNetwork,
	options chaincodeOperationOptions,
	job *batchv1.Job,
	submitter fabricopsv1alpha1.OrgStatus,
	targets []operationPeerTarget,
	succeeded bool,
	logs []byte,
) operationResult {
	return operationResult{
		Operation: operation,
		Network:   network.Name,
		Namespace: network.Namespace,
		Channel:   options.channel,
		Chaincode: options.chaincode,
		Function:  options.function,
		Org:       submitter.Name,
		Job: operationResultJob{
			Namespace: job.Namespace,
			Name:      job.Name,
		},
		Peers:       operationResultPeers(targets),
		Succeeded:   succeeded,
		JobRetained: options.keepJob || !succeeded,
		Logs:        string(logs),
	}
}

func operationResultPeers(targets []operationPeerTarget) []operationResultPeer {
	peers := make([]operationResultPeer, 0, len(targets))
	for _, target := range targets {
		peers = append(peers, operationResultPeer{
			Org:     target.orgName,
			Name:    target.endpoint.Name,
			Address: target.endpoint.Address,
		})
	}
	return peers
}

func writeOperationResult(out io.Writer, output string, result operationResult) error {
	switch output {
	case operationOutputJSON:
		return writeJSON(out, result)
	case "", operationOutputText:
		_, err := out.Write([]byte(result.Logs))
		if err != nil {
			return err
		}
		if result.Logs != "" && !strings.HasSuffix(result.Logs, "\n") {
			printLine(out)
		}
		return nil
	default:
		return fmt.Errorf("unsupported output format %q", output)
	}
}

func cleanupOperationObjects(
	ctx context.Context,
	client ctrlclient.Client,
	job *batchv1.Job,
	tlsEnabled bool,
	tlsSecretName string,
) {
	propagation := metav1.DeletePropagationBackground
	_ = client.Delete(ctx, job, &ctrlclient.DeleteOptions{PropagationPolicy: &propagation})
	if !tlsEnabled {
		return
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: tlsSecretName, Namespace: job.Namespace}}
	err := client.Delete(ctx, secret)
	if err != nil && !apierrors.IsNotFound(err) {
		printLine(os.Stderr, "Warning: failed to delete temporary Secret:", err)
	}
}

func submitterPeerAddress(submitterOrg string, targets []operationPeerTarget) string {
	for _, target := range targets {
		if strings.EqualFold(target.orgName, submitterOrg) {
			return target.endpoint.Address
		}
	}
	return targets[0].endpoint.Address
}

func submitterPeerTLSRoot(submitterOrg string, targets []operationPeerTarget) string {
	for i, target := range targets {
		if strings.EqualFold(target.orgName, submitterOrg) {
			return fmt.Sprintf("%s/peer-%d-ca.crt", operationTLSRootPath, i)
		}
	}
	return operationTLSRootPath + "/peer-0-ca.crt"
}

func mspIDForOrg(network *fabricopsv1alpha1.FabricNetwork, orgName string) (string, error) {
	for _, org := range network.Spec.Orgs {
		if strings.EqualFold(org.Organization.Name, orgName) {
			return org.Organization.MSPName, nil
		}
	}
	return "", fmt.Errorf("org %q was not found in FabricNetwork spec", orgName)
}

func mspIDForParticipantOrg(participant *fabricopsv1alpha1.FabricParticipant, orgName string) (string, error) {
	org := participant.Spec.Org.Organization
	if !strings.EqualFold(org.Name, orgName) {
		return "", fmt.Errorf("org %q was not found in FabricParticipant spec", orgName)
	}
	return org.MSPName, nil
}

func participantOperationOrgStatuses(
	participant *fabricopsv1alpha1.FabricParticipant,
) []fabricopsv1alpha1.OrgStatus {
	statuses := []fabricopsv1alpha1.OrgStatus{participant.Status.LocalOrgStatus}
	remoteByOrg := map[string][]fabricopsv1alpha1.PeerEndpointStatus{}
	for _, peer := range participant.Spec.Network.Peers {
		remoteByOrg[peer.Org] = append(remoteByOrg[peer.Org], fabricopsv1alpha1.PeerEndpointStatus{
			Name:                peer.Name,
			Address:             peer.Address,
			TLSHostnameOverride: peer.TLSHostnameOverride,
		})
	}
	orgNames := make([]string, 0, len(remoteByOrg))
	for orgName := range remoteByOrg {
		orgNames = append(orgNames, orgName)
	}
	slices.Sort(orgNames)
	for _, orgName := range orgNames {
		statuses = append(statuses, fabricopsv1alpha1.OrgStatus{
			Name:          orgName,
			PeerEndpoints: remoteByOrg[orgName],
		})
	}
	return statuses
}

func decorateParticipantOperationTargets(
	participant *fabricopsv1alpha1.FabricParticipant,
	targets []operationPeerTarget,
) []operationPeerTarget {
	for i := range targets {
		for _, peer := range participant.Spec.Network.Peers {
			if strings.EqualFold(targets[i].orgName, peer.Org) && targets[i].endpoint.Name == peer.Name {
				targets[i].tlsRootCARef = peer.TLSRootCARef
				break
			}
		}
	}
	return targets
}

func selectParticipantOperationOrderer(
	participant *fabricopsv1alpha1.FabricParticipant,
) (fabricopsv1alpha1.ParticipantOrdererEndpoint, error) {
	for _, orderer := range participant.Spec.Network.Orderers {
		if strings.TrimSpace(orderer.ClientAddress) != "" {
			return orderer, nil
		}
	}
	return fabricopsv1alpha1.ParticipantOrdererEndpoint{}, fmt.Errorf(
		"no orderer endpoint found in FabricParticipant spec",
	)
}

func participantOperationOrdererStatus(
	orderer fabricopsv1alpha1.ParticipantOrdererEndpoint,
) fabricopsv1alpha1.OrdererEndpointStatus {
	return fabricopsv1alpha1.OrdererEndpointStatus{
		Name:                orderer.Name,
		ClientAddress:       orderer.ClientAddress,
		TLSHostnameOverride: orderer.TLSHostnameOverride,
		AdminAddress:        orderer.AdminAddress,
	}
}

func participantOperationNetwork(participant *fabricopsv1alpha1.FabricParticipant) *fabricopsv1alpha1.FabricNetwork {
	return &fabricopsv1alpha1.FabricNetwork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      participant.Name,
			Namespace: participant.Namespace,
		},
		Spec: fabricopsv1alpha1.FabricNetworkSpec{
			Global: participant.Spec.Global,
			Orgs:   []fabricopsv1alpha1.Org{participant.Spec.Org},
		},
	}
}

func endpointNamespace(address string) (string, error) {
	hostPort := strings.Split(address, ":")[0]
	parts := strings.Split(hostPort, ".")
	if len(parts) < 2 {
		return "", fmt.Errorf("could not infer namespace from endpoint %q", address)
	}
	return parts[1], nil
}

func operationJobName(networkName string, operation string) string {
	return sanitizeName(fmt.Sprintf("%s-%s-%d", networkName, operation, time.Now().UnixNano()))
}

func fabricToolsImage(version string) string {
	version = strings.TrimSpace(version)
	if version == "" {
		version = "2.5.12"
	}
	if strings.HasPrefix(strings.TrimPrefix(version, "v"), "3.") {
		version = "2.5.14"
	}

	return fmt.Sprintf("hyperledger/fabric-tools:%s", version)
}

func sanitizeName(name string) string {
	var b strings.Builder
	lastDash := false
	for _, r := range strings.ToLower(strings.TrimSpace(name)) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}

	value := strings.Trim(b.String(), "-")
	if value == "" {
		value = "resource"
	}
	if len(value) <= 63 {
		return value
	}

	hash := fnv.New32a()
	_, _ = hash.Write([]byte(value))
	suffix := fmt.Sprintf("%08x", hash.Sum32())
	prefix := strings.TrimRight(value[:63-len(suffix)-1], "-")
	if prefix == "" {
		prefix = value[:63-len(suffix)-1]
	}
	return prefix + "-" + suffix
}

type stringListFlag []string

func (f *stringListFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *stringListFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

var _ flag.Value = (*stringListFlag)(nil)
