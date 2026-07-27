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
	"encoding/json"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	fabricopsv1alpha1 "github.com/dpereowei/fabricops/api/v1alpha1"
)

func TestSelectOperationTargetsSupportsMultiOrgEndorsement(t *testing.T) {
	statuses := operationTestOrgStatuses()

	targets, submitter, err := selectOperationTargets(
		statuses,
		"BankA",
		[]string{"BankA/peer0", "BankB/peer0"},
	)
	if err != nil {
		t.Fatalf("selectOperationTargets() error = %v", err)
	}
	if submitter.Name != "BankA" {
		t.Fatalf("submitter.Name = %q, want BankA", submitter.Name)
	}
	if len(targets) != 2 {
		t.Fatalf("len(targets) = %d, want 2", len(targets))
	}
	if targets[0].endpoint.Address != "peer0.fo-test-banka.svc.cluster.local:7051" {
		t.Fatalf("targets[0].endpoint.Address = %q", targets[0].endpoint.Address)
	}
	if targets[1].endpoint.Address != "peer0.fo-test-bankb.svc.cluster.local:7051" {
		t.Fatalf("targets[1].endpoint.Address = %q", targets[1].endpoint.Address)
	}
}

func TestValidateSelectedOperationTargetsRejectsCrossOrgQuery(t *testing.T) {
	statuses := operationTestOrgStatuses()
	targets, submitter, err := selectOperationTargets(statuses, "BankA", []string{"BankB/peer0", "BankA/peer0"})
	if err != nil {
		t.Fatalf("selectOperationTargets() error = %v", err)
	}

	if err := validateSelectedOperationTargets(chaincodeOperationQuery, submitter, targets); err == nil {
		t.Fatal("validateSelectedOperationTargets() error = nil, want multi-peer query rejection")
	}
}

func TestBuildOperationJobUsesAdminSecretsAndPeerFlags(t *testing.T) {
	network := operationTestNetwork()
	statuses := operationTestOrgStatuses()
	targets, submitter, err := selectOperationTargets(
		statuses,
		"BankA",
		[]string{"BankA/peer0", "BankB/peer0"},
	)
	if err != nil {
		t.Fatalf("selectOperationTargets() error = %v", err)
	}
	mspID, err := mspIDForOrg(network, submitter.Name)
	if err != nil {
		t.Fatalf("mspIDForOrg() error = %v", err)
	}

	job := buildOperationJob(
		network,
		chaincodeOperationInvoke,
		chaincodeOperationOptions{
			channel:      "settlement",
			chaincode:    "settlement",
			function:     "createSettlement",
			waitForEvent: true,
		},
		`{"Args":["createSettlement","id1"]}`,
		"sample-invoke",
		"sample-invoke-tls-roots",
		mspID,
		submitter,
		statuses[0].OrdererEndpoints[0],
		targets,
	)

	if job.Namespace != "fo-test-banka" {
		t.Fatalf("job.Namespace = %q, want fo-test-banka", job.Namespace)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if container.Image != "hyperledger/fabric-tools:2.5.14" {
		t.Fatalf("container.Image = %q, want Fabric v3 tools fallback", container.Image)
	}

	env := operationEnvMap(container.Env)
	if env["FABRICOPS_MSP_ID"] != "BankAMSP" {
		t.Fatalf("FABRICOPS_MSP_ID = %q, want BankAMSP", env["FABRICOPS_MSP_ID"])
	}
	if env["FABRICOPS_CORE_PEER_ADDRESS"] != "peer0.fo-test-banka.svc.cluster.local:7051" {
		t.Fatalf("FABRICOPS_CORE_PEER_ADDRESS = %q", env["FABRICOPS_CORE_PEER_ADDRESS"])
	}
	if env["FABRICOPS_CORE_PEER_TLS_ROOT"] != operationTLSRootPath+"/peer-0-ca.crt" {
		t.Fatalf("FABRICOPS_CORE_PEER_TLS_ROOT = %q", env["FABRICOPS_CORE_PEER_TLS_ROOT"])
	}
	if env["FABRICOPS_ORDERER_TLS_HOSTNAME_OVERRIDE"] != "localhost" {
		t.Fatalf("FABRICOPS_ORDERER_TLS_HOSTNAME_OVERRIDE = %q", env["FABRICOPS_ORDERER_TLS_HOSTNAME_OVERRIDE"])
	}
	if env["FABRICOPS_PEER_ADDRESS_1"] != "peer0.fo-test-bankb.svc.cluster.local:7051" {
		t.Fatalf("FABRICOPS_PEER_ADDRESS_1 = %q", env["FABRICOPS_PEER_ADDRESS_1"])
	}

	volumes := operationVolumeSecrets(job)
	if volumes["admin-msp"] != "banka-admin-msp" {
		t.Fatalf("admin-msp SecretName = %q, want banka-admin-msp", volumes["admin-msp"])
	}
	if volumes["admin-tls"] != "banka-admin-tls" {
		t.Fatalf("admin-tls SecretName = %q, want banka-admin-tls", volumes["admin-tls"])
	}
	if volumes["tls-roots"] != "sample-invoke-tls-roots" {
		t.Fatalf("tls-roots SecretName = %q, want sample-invoke-tls-roots", volumes["tls-roots"])
	}

	script := container.Command[2]
	for _, want := range []string{
		`set -- "$@" --peerAddresses "$FABRICOPS_PEER_ADDRESS_0"`,
		`set -- "$@" --peerAddresses "$FABRICOPS_PEER_ADDRESS_1"`,
		`set -- "$@" --tlsRootCertFiles "` + operationTLSRootPath + `/peer-0-ca.crt"`,
		`set -- "$@" --tlsRootCertFiles "` + operationTLSRootPath + `/peer-1-ca.crt"`,
		`set -- "$@" --ordererTLSHostnameOverride "$FABRICOPS_ORDERER_TLS_HOSTNAME_OVERRIDE"`,
		`set -- "$@" --waitForEvent`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("operation script does not contain %q\nscript:\n%s", want, script)
		}
	}
}

func TestBuildParticipantOperationJobUsesImportedOrderer(t *testing.T) {
	participant := operationTestParticipant()
	targets, submitter, err := selectOperationTargets(
		[]fabricopsv1alpha1.OrgStatus{participant.Status.LocalOrgStatus},
		"BankB",
		[]string{"peer0"},
	)
	if err != nil {
		t.Fatalf("selectOperationTargets() error = %v", err)
	}
	mspID, err := mspIDForParticipantOrg(participant, submitter.Name)
	if err != nil {
		t.Fatalf("mspIDForParticipantOrg() error = %v", err)
	}
	ordererSpec, err := selectParticipantOperationOrderer(participant)
	if err != nil {
		t.Fatalf("selectParticipantOperationOrderer() error = %v", err)
	}

	job := buildOperationJob(
		participantOperationNetwork(participant),
		chaincodeOperationQuery,
		chaincodeOperationOptions{
			channel:   "settlement",
			chaincode: "settlement",
			function:  "readSettlement",
		},
		`{"Args":["readSettlement","id1"]}`,
		"bankb-query",
		"bankb-query-tls-roots",
		mspID,
		submitter,
		participantOperationOrdererStatus(ordererSpec),
		targets,
	)

	if job.Namespace != "fo-fp-bankb" {
		t.Fatalf("job.Namespace = %q, want fo-fp-bankb", job.Namespace)
	}
	container := job.Spec.Template.Spec.Containers[0]
	env := operationEnvMap(container.Env)
	if env["FABRICOPS_MSP_ID"] != "BankBMSP" {
		t.Fatalf("FABRICOPS_MSP_ID = %q, want BankBMSP", env["FABRICOPS_MSP_ID"])
	}
	if env["FABRICOPS_ORDERER_ADDRESS"] != "host.docker.internal:8050" {
		t.Fatalf("FABRICOPS_ORDERER_ADDRESS = %q", env["FABRICOPS_ORDERER_ADDRESS"])
	}
	if env["FABRICOPS_ORDERER_TLS_HOSTNAME_OVERRIDE"] != "localhost" {
		t.Fatalf("FABRICOPS_ORDERER_TLS_HOSTNAME_OVERRIDE = %q", env["FABRICOPS_ORDERER_TLS_HOSTNAME_OVERRIDE"])
	}
	if env["FABRICOPS_CORE_PEER_ADDRESS"] != "peer0.fo-fp-bankb.svc.cluster.local:7051" {
		t.Fatalf("FABRICOPS_CORE_PEER_ADDRESS = %q", env["FABRICOPS_CORE_PEER_ADDRESS"])
	}

	volumes := operationVolumeSecrets(job)
	if volumes["admin-msp"] != "bankb-admin-msp" {
		t.Fatalf("admin-msp SecretName = %q, want bankb-admin-msp", volumes["admin-msp"])
	}
	if volumes["admin-tls"] != "bankb-admin-tls" {
		t.Fatalf("admin-tls SecretName = %q, want bankb-admin-tls", volumes["admin-tls"])
	}
	if volumes["tls-roots"] != "bankb-query-tls-roots" {
		t.Fatalf("tls-roots SecretName = %q, want bankb-query-tls-roots", volumes["tls-roots"])
	}
}

func TestParticipantOperationTargetsIncludeImportedRemotePeers(t *testing.T) {
	participant := operationTestParticipant()
	statuses := participantOperationOrgStatuses(participant)
	targets, submitter, err := selectOperationTargets(
		statuses,
		"BankB",
		[]string{"BankB/peer0", "BankA/peer0"},
	)
	if err != nil {
		t.Fatalf("selectOperationTargets() error = %v", err)
	}
	targets = decorateParticipantOperationTargets(participant, targets)

	if submitter.Name != "BankB" {
		t.Fatalf("submitter.Name = %q, want BankB", submitter.Name)
	}
	if len(targets) != 2 {
		t.Fatalf("len(targets) = %d, want 2", len(targets))
	}
	if targets[0].endpoint.Address != "peer0.fo-fp-bankb.svc.cluster.local:7051" {
		t.Fatalf("targets[0].endpoint.Address = %q", targets[0].endpoint.Address)
	}
	if targets[0].tlsRootCARef != nil {
		t.Fatal("targets[0].tlsRootCARef is set for local peer")
	}
	if targets[1].orgName != "BankA" {
		t.Fatalf("targets[1].orgName = %q, want BankA", targets[1].orgName)
	}
	if targets[1].endpoint.Address != "192.168.117.3:30052" {
		t.Fatalf("targets[1].endpoint.Address = %q", targets[1].endpoint.Address)
	}
	if targets[1].tlsRootCARef == nil || targets[1].tlsRootCARef.ConfigMapKeyRef == nil {
		t.Fatal("targets[1].tlsRootCARef is nil, want imported ConfigMap ref")
	}
	if targets[1].tlsRootCARef.ConfigMapKeyRef.Name != "banka-peer0-artifacts" {
		t.Fatalf(
			"targets[1].tlsRootCARef.ConfigMapKeyRef.Name = %q",
			targets[1].tlsRootCARef.ConfigMapKeyRef.Name,
		)
	}
}

func TestEnsureParticipantOperationTLSSecretUsesImportedOrdererRoot(t *testing.T) {
	ctx := context.Background()
	participant := operationTestParticipant()
	targets, submitter, err := selectOperationTargets(
		[]fabricopsv1alpha1.OrgStatus{participant.Status.LocalOrgStatus},
		"BankB",
		[]string{"peer0"},
	)
	if err != nil {
		t.Fatalf("selectOperationTargets() error = %v", err)
	}
	ordererSpec, err := selectParticipantOperationOrderer(participant)
	if err != nil {
		t.Fatalf("selectParticipantOperationOrderer() error = %v", err)
	}
	client := fake.NewClientBuilder().
		WithScheme(cliScheme).
		WithObjects(
			&corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "orderer0-artifacts", Namespace: "default"},
				Data:       map[string]string{"tls-ca.pem": "orderer-root"},
			},
			&corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "peer0-tls", Namespace: "fo-fp-bankb"},
				Data:       map[string][]byte{tlsCACertKey: []byte("peer-root")},
			},
		).
		Build()

	if err := ensureParticipantOperationTLSSecret(
		ctx,
		client,
		"bankb-query-tls-roots",
		participant,
		submitter,
		ordererSpec,
		targets,
	); err != nil {
		t.Fatalf("ensureParticipantOperationTLSSecret() error = %v", err)
	}

	var secret corev1.Secret
	if err := client.Get(ctx, ctrlclient.ObjectKey{
		Namespace: "fo-fp-bankb",
		Name:      "bankb-query-tls-roots",
	}, &secret); err != nil {
		t.Fatalf("client.Get() error = %v", err)
	}
	if string(secret.Data["orderer-ca.crt"]) != "orderer-root" {
		t.Fatalf("orderer-ca.crt = %q", string(secret.Data["orderer-ca.crt"]))
	}
	if string(secret.Data["peer-0-ca.crt"]) != "peer-root" {
		t.Fatalf("peer-0-ca.crt = %q", string(secret.Data["peer-0-ca.crt"]))
	}
}

func TestEnsureParticipantOperationTLSSecretUsesImportedPeerRoot(t *testing.T) {
	ctx := context.Background()
	participant := operationTestParticipant()
	targets, submitter, err := selectOperationTargets(
		participantOperationOrgStatuses(participant),
		"BankB",
		[]string{"BankB/peer0", "BankA/peer0"},
	)
	if err != nil {
		t.Fatalf("selectOperationTargets() error = %v", err)
	}
	targets = decorateParticipantOperationTargets(participant, targets)
	ordererSpec, err := selectParticipantOperationOrderer(participant)
	if err != nil {
		t.Fatalf("selectParticipantOperationOrderer() error = %v", err)
	}
	client := fake.NewClientBuilder().
		WithScheme(cliScheme).
		WithObjects(
			&corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "orderer0-artifacts", Namespace: "default"},
				Data:       map[string]string{"tls-ca.pem": "orderer-root"},
			},
			&corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "banka-peer0-artifacts", Namespace: "default"},
				Data:       map[string]string{"tls-ca.pem": "founder-peer-root"},
			},
			&corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "peer0-tls", Namespace: "fo-fp-bankb"},
				Data:       map[string][]byte{tlsCACertKey: []byte("peer-root")},
			},
		).
		Build()

	if err := ensureParticipantOperationTLSSecret(
		ctx,
		client,
		"bankb-invoke-tls-roots",
		participant,
		submitter,
		ordererSpec,
		targets,
	); err != nil {
		t.Fatalf("ensureParticipantOperationTLSSecret() error = %v", err)
	}

	var secret corev1.Secret
	if err := client.Get(ctx, ctrlclient.ObjectKey{
		Namespace: "fo-fp-bankb",
		Name:      "bankb-invoke-tls-roots",
	}, &secret); err != nil {
		t.Fatalf("client.Get() error = %v", err)
	}
	if string(secret.Data["orderer-ca.crt"]) != "orderer-root" {
		t.Fatalf("orderer-ca.crt = %q", string(secret.Data["orderer-ca.crt"]))
	}
	if string(secret.Data["peer-0-ca.crt"]) != "peer-root" {
		t.Fatalf("peer-0-ca.crt = %q", string(secret.Data["peer-0-ca.crt"]))
	}
	if string(secret.Data["peer-1-ca.crt"]) != "founder-peer-root" {
		t.Fatalf("peer-1-ca.crt = %q", string(secret.Data["peer-1-ca.crt"]))
	}
}

func TestOperationScriptPassesTransientForQuery(t *testing.T) {
	script := operationScript(true, false, 1)
	for _, want := range []string{
		`if [ "$FABRICOPS_OPERATION" = "query" ]; then`,
		`set -- peer chaincode query`,
		`set -- "$@" --transient "$FABRICOPS_TRANSIENT"`,
		`"$@"`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("operation script does not contain %q\nscript:\n%s", want, script)
		}
	}
}

func TestParseChaincodeArgsRequiresStringArray(t *testing.T) {
	if _, err := parseChaincodeArgs(`["id1","alice"]`); err != nil {
		t.Fatalf("parseChaincodeArgs() error = %v", err)
	}
	if _, err := parseChaincodeArgs(`["id1",100]`); err == nil {
		t.Fatal("parseChaincodeArgs() error = nil, want non-string rejection")
	}
}

func TestResolveChaincodePayloadSupportsRawFabricPayload(t *testing.T) {
	payload, function, err := resolveChaincodePayload(chaincodeOperationOptions{
		payload: `{"Args":["CreateAsset","asset1","blue"]}`,
	})
	if err != nil {
		t.Fatalf("resolveChaincodePayload() error = %v", err)
	}
	if function != "CreateAsset" {
		t.Fatalf("function = %q, want CreateAsset", function)
	}
	if payload != `{"Args":["CreateAsset","asset1","blue"]}` {
		t.Fatalf("payload = %q", payload)
	}
}

func TestResolveChaincodePayloadRejectsEmptyRawPayloadArgs(t *testing.T) {
	if _, _, err := resolveChaincodePayload(chaincodeOperationOptions{payload: `{"Args":[]}`}); err == nil {
		t.Fatal("resolveChaincodePayload() error = nil, want empty Args rejection")
	}
}

func TestValidateOperationOptionsRejectsPayloadMixedWithFunctionArgs(t *testing.T) {
	err := validateOperationOptions(chaincodeOperationOptions{
		channel:   "settlement",
		chaincode: "settlement",
		function:  "CreateAsset",
		payload:   `{"Args":["CreateAsset"]}`,
	})
	if err == nil {
		t.Fatal("validateOperationOptions() error = nil, want payload/function rejection")
	}

	err = validateOperationOptions(chaincodeOperationOptions{
		channel:   "settlement",
		chaincode: "settlement",
		argsJSON:  `["asset1"]`,
		payload:   `{"Args":["CreateAsset"]}`,
	})
	if err == nil {
		t.Fatal("validateOperationOptions() error = nil, want payload/args rejection")
	}
}

func TestWriteOperationResultSupportsJSONOutput(t *testing.T) {
	var out bytes.Buffer
	result := operationResult{
		Operation: chaincodeOperationQuery,
		Network:   "sample",
		Namespace: "default",
		Channel:   "settlement",
		Chaincode: "settlement",
		Function:  "readSettlement",
		Org:       "BankA",
		Job: operationResultJob{
			Namespace: "fo-test-banka",
			Name:      "sample-query",
		},
		Peers: []operationResultPeer{
			{Org: "BankA", Name: "peer0", Address: "peer0.fo-test-banka.svc.cluster.local:7051"},
		},
		Succeeded:   true,
		JobRetained: false,
		Logs:        "query response\n",
	}

	if err := writeOperationResult(&out, operationOutputJSON, result); err != nil {
		t.Fatalf("writeOperationResult() error = %v", err)
	}

	var decoded operationResult
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v; output = %s", err, out.String())
	}
	if decoded.Operation != chaincodeOperationQuery {
		t.Fatalf("decoded.Operation = %q, want %q", decoded.Operation, chaincodeOperationQuery)
	}
	if decoded.Job.Name != "sample-query" {
		t.Fatalf("decoded.Job.Name = %q, want sample-query", decoded.Job.Name)
	}
	if decoded.Logs != "query response\n" {
		t.Fatalf("decoded.Logs = %q, want query response newline", decoded.Logs)
	}
	if decoded.JobRetained {
		t.Fatal("decoded.JobRetained = true, want false")
	}
}

func TestBuildOperationResultMarksFailedJobRetained(t *testing.T) {
	network := operationTestNetwork()
	statuses := operationTestOrgStatuses()
	targets, submitter, err := selectOperationTargets(statuses, "BankA", []string{"peer0"})
	if err != nil {
		t.Fatalf("selectOperationTargets() error = %v", err)
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "sample-query", Namespace: "fo-test-banka"}}

	result := buildOperationResult(
		chaincodeOperationQuery,
		network,
		chaincodeOperationOptions{
			channel:   "settlement",
			chaincode: "settlement",
			function:  "readSettlement",
		},
		job,
		submitter,
		targets,
		false,
		[]byte("failed\n"),
	)

	if result.Succeeded {
		t.Fatal("result.Succeeded = true, want false")
	}
	if !result.JobRetained {
		t.Fatal("result.JobRetained = false, want true for failed operation")
	}
	if result.Peers[0].Name != "peer0" {
		t.Fatalf("result.Peers[0].Name = %q, want peer0", result.Peers[0].Name)
	}
}

func TestValidateOperationOutputRejectsUnsupportedFormat(t *testing.T) {
	if err := validateOperationOutput(operationOutputJSON); err != nil {
		t.Fatalf("validateOperationOutput(json) error = %v", err)
	}
	if err := validateOperationOutput("yaml"); err == nil {
		t.Fatal("validateOperationOutput(yaml) error = nil, want rejection")
	}
}

func operationTestNetwork() *fabricopsv1alpha1.FabricNetwork {
	return &fabricopsv1alpha1.FabricNetwork{
		ObjectMeta: metav1.ObjectMeta{Name: "sample", Namespace: "default"},
		Spec: fabricopsv1alpha1.FabricNetworkSpec{
			Global: fabricopsv1alpha1.GlobalConfig{
				FabricVersion: "3.0.0",
				TLS:           true,
			},
			Orgs: []fabricopsv1alpha1.Org{
				{Organization: fabricopsv1alpha1.OrgMeta{Name: "OrdererOrg", MSPName: "OrdererMSP"}},
				{Organization: fabricopsv1alpha1.OrgMeta{Name: "BankA", MSPName: "BankAMSP"}},
				{Organization: fabricopsv1alpha1.OrgMeta{Name: "BankB", MSPName: "BankBMSP"}},
			},
		},
	}
}

func operationTestOrgStatuses() []fabricopsv1alpha1.OrgStatus {
	return []fabricopsv1alpha1.OrgStatus{
		{
			Name:      "OrdererOrg",
			Namespace: "fo-test-orderer",
			OrdererEndpoints: []fabricopsv1alpha1.OrdererEndpointStatus{
				{
					Name:                "orderer0",
					Namespace:           "fo-test-orderer",
					ClientAddress:       "host.docker.internal:8050",
					TLSHostnameOverride: "localhost",
				},
			},
		},
		{
			Name:      "BankA",
			Namespace: "fo-test-banka",
			PeerEndpoints: []fabricopsv1alpha1.PeerEndpointStatus{
				{Name: "peer0", Address: "peer0.fo-test-banka.svc.cluster.local:7051"},
				{Name: "peer1", Address: "peer1.fo-test-banka.svc.cluster.local:7051"},
			},
		},
		{
			Name:      "BankB",
			Namespace: "fo-test-bankb",
			PeerEndpoints: []fabricopsv1alpha1.PeerEndpointStatus{
				{Name: "peer0", Address: "peer0.fo-test-bankb.svc.cluster.local:7051"},
			},
		},
	}
}

func operationTestParticipant() *fabricopsv1alpha1.FabricParticipant {
	return &fabricopsv1alpha1.FabricParticipant{
		ObjectMeta: metav1.ObjectMeta{Name: "bankb-participant", Namespace: "default"},
		Spec: fabricopsv1alpha1.FabricParticipantSpec{
			Global: fabricopsv1alpha1.GlobalConfig{
				FabricVersion: "3.0.0",
				TLS:           true,
			},
			Org: fabricopsv1alpha1.Org{
				Organization: fabricopsv1alpha1.OrgMeta{Name: "BankB", MSPName: "BankBMSP"},
			},
			Network: fabricopsv1alpha1.ParticipantNetwork{
				Name: "federated-founder",
				Orderers: []fabricopsv1alpha1.ParticipantOrdererEndpoint{
					{
						Org:                 "Orderer",
						Name:                "orderer0",
						ClientAddress:       "host.docker.internal:8050",
						TLSHostnameOverride: "localhost",
						TLSRootCARef: &fabricopsv1alpha1.ParticipantArtifactKeyRef{
							ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: "orderer0-artifacts"},
								Key:                  "tls-ca.pem",
							},
						},
					},
				},
				Peers: []fabricopsv1alpha1.ParticipantPeerEndpoint{
					{
						Org:     "BankA",
						Name:    "peer0",
						Address: "192.168.117.3:30052",
						TLSRootCARef: &fabricopsv1alpha1.ParticipantArtifactKeyRef{
							ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: "banka-peer0-artifacts"},
								Key:                  "tls-ca.pem",
							},
						},
					},
				},
			},
		},
		Status: fabricopsv1alpha1.FabricParticipantStatus{
			LocalOrgStatus: fabricopsv1alpha1.OrgStatus{
				Name:      "BankB",
				Namespace: "fo-fp-bankb",
				PeerEndpoints: []fabricopsv1alpha1.PeerEndpointStatus{
					{Name: "peer0", Address: "peer0.fo-fp-bankb.svc.cluster.local:7051"},
				},
			},
		},
	}
}

func operationEnvMap(env []corev1.EnvVar) map[string]string {
	values := map[string]string{}
	for _, item := range env {
		values[item.Name] = item.Value
	}
	return values
}

func operationVolumeSecrets(job *batchv1.Job) map[string]string {
	values := map[string]string{}
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.Secret != nil {
			values[volume.Name] = volume.Secret.SecretName
		}
	}
	return values
}
