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

package channelconfig

import (
	"bytes"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

type RaftConsenter struct {
	Host          string `json:"host"`
	Port          uint32 `json:"port"`
	ClientTLSCert []byte `json:"client_tls_cert"`
	ServerTLSCert []byte `json:"server_tls_cert"`
}

type RaftUpdatePlan struct {
	APIVersion     string          `json:"apiVersion"`
	Kind           string          `json:"kind"`
	Channel        string          `json:"channel"`
	ConfigSequence string          `json:"configSequence"`
	Action         string          `json:"action"`
	Target         *RaftConsenter  `json:"target,omitempty"`
	CurrentQuorum  int             `json:"currentQuorum"`
	DesiredQuorum  int             `json:"desiredQuorum"`
	Warnings       []string        `json:"warnings"`
	OriginalConfig json.RawMessage `json:"originalConfig"`
	UpdatedConfig  json.RawMessage `json:"updatedConfig"`
}

func PlanRaftUpdate(block []byte, channel string, desired []RaftConsenter) (*RaftUpdatePlan, error) {
	var decoded struct {
		Data struct {
			Data []struct {
				Payload struct {
					Header struct {
						ChannelHeader struct {
							ChannelID string `json:"channel_id"`
							Type      int    `json:"type"`
						} `json:"channel_header"`
					} `json:"header"`
					Data struct {
						Config json.RawMessage `json:"config"`
					} `json:"data"`
				} `json:"payload"`
			} `json:"data"`
		} `json:"data"`
	}
	if err := json.Unmarshal(block, &decoded); err != nil {
		return nil, fmt.Errorf("decode config block: %w", err)
	}
	if channel == "" || len(decoded.Data.Data) != 1 {
		return nil, fmt.Errorf("expected a config block containing exactly one CONFIG transaction")
	}
	payload := decoded.Data.Data[0].Payload
	if payload.Header.ChannelHeader.Type != 1 || payload.Header.ChannelHeader.ChannelID != channel {
		return nil, fmt.Errorf("expected a CONFIG transaction for channel %q", channel)
	}
	return planRaftConfig(payload.Data.Config, channel, desired)
}

func planRaftConfig(original json.RawMessage, channel string, desired []RaftConsenter) (*RaftUpdatePlan, error) {
	var config struct {
		Sequence string `json:"sequence"`
	}
	if err := json.Unmarshal(original, &config); err != nil {
		return nil, fmt.Errorf("decode channel config: %w", err)
	}
	if _, err := strconv.ParseUint(config.Sequence, 10, 64); err != nil {
		return nil, fmt.Errorf("config sequence must be an unsigned 64-bit decimal string")
	}
	consensusPath := []string{"channel_group", "groups", "Orderer", "values", "ConsensusType", "value"}
	consensusRaw, err := raftJSONValue(original, consensusPath)
	if err != nil {
		return nil, err
	}
	var consensus struct {
		Type  string `json:"type"`
		State string `json:"state"`
	}
	if err := json.Unmarshal(consensusRaw, &consensus); err != nil {
		return nil, fmt.Errorf("decode consensus type: %w", err)
	}
	if consensus.Type != "etcdraft" || consensus.State != "STATE_NORMAL" {
		return nil, fmt.Errorf("only etcdraft in STATE_NORMAL is supported; migration/maintenance updates are unsupported")
	}
	consentersPath := append(consensusPath, "metadata", "consenters")
	currentRaw, err := raftJSONValue(original, consentersPath)
	if err != nil {
		return nil, err
	}
	var current []RaftConsenter
	if err := json.Unmarshal(currentRaw, &current); err != nil {
		return nil, fmt.Errorf("decode current consenters: %w", err)
	}
	currentKeys, err := validateRaftConsenters(current)
	if err != nil {
		return nil, fmt.Errorf("current consenters: %w", err)
	}
	desiredKeys, err := validateRaftConsenters(desired)
	if err != nil {
		return nil, fmt.Errorf("desired consenters: %w", err)
	}
	added, removed := -1, -1
	changes := 0
	for key, index := range desiredKeys {
		if _, ok := currentKeys[key]; !ok {
			added = index
			changes++
		}
	}
	for key, index := range currentKeys {
		if _, ok := desiredKeys[key]; !ok {
			removed = index
			changes++
		}
	}
	if changes > 1 {
		return nil, fmt.Errorf("change exactly one Raft consenter at a time; " +
			"batch changes, replacements, endpoint moves, and certificate rotation are not supported")
	}
	plan := &RaftUpdatePlan{
		APIVersion: "fabricops.io/v1alpha1", Kind: "FabricChannelRaftUpdatePlan", Channel: channel,
		ConfigSequence: config.Sequence, Action: "AlreadyApplied",
		CurrentQuorum: len(current)/2 + 1, DesiredQuorum: len(desired)/2 + 1,
		OriginalConfig: original, UpdatedConfig: original,
		Warnings: []string{
			"Offline plan only: no update is signed or submitted and no workloads are changed",
			"Node health, catch-up, quorum availability, TLS trust/validity, and policy signatures are not verified",
			"Orderer MSPs, endpoint advertisements, policies, and Raft tuning are unchanged",
		},
	}
	if changes == 0 {
		return plan, nil
	}
	// Preserve existing order and unknown protobuf fields instead of rebuilding
	// the config from a partial Go representation.
	var entries []json.RawMessage
	if err := json.Unmarshal(currentRaw, &entries); err != nil {
		return nil, err
	}
	if added >= 0 {
		plan.Action, plan.Target = "Add", &desired[added]
		entry, marshalErr := json.Marshal(desired[added])
		if marshalErr != nil {
			return nil, marshalErr
		}
		entries = append(entries, entry)
	} else {
		plan.Action, plan.Target = "Remove", &current[removed]
		entries = append(entries[:removed], entries[removed+1:]...)
	}
	updatedEntries, err := json.Marshal(entries)
	if err != nil {
		return nil, err
	}
	plan.UpdatedConfig, err = replaceRaftJSONValue(original, consentersPath, updatedEntries)
	return plan, err
}

func validateRaftConsenters(consenters []RaftConsenter) (map[string]int, error) {
	if len(consenters) == 0 {
		return nil, fmt.Errorf("at least one consenter must remain")
	}
	keys, endpoints, certificates := map[string]int{}, map[string]bool{}, map[[32]byte]int{}
	for index, consenter := range consenters {
		host := strings.ToLower(strings.TrimSuffix(consenter.Host, "."))
		if net.ParseIP(host) == nil && len(validation.IsDNS1123Subdomain(host)) != 0 {
			return nil, fmt.Errorf("consenter %d has an invalid host", index)
		}
		if consenter.Port == 0 || consenter.Port > 65535 {
			return nil, fmt.Errorf("consenter %d has an invalid port", index)
		}
		endpoint := net.JoinHostPort(host, strconv.Itoa(int(consenter.Port)))
		if endpoints[endpoint] {
			return nil, fmt.Errorf("duplicate consenter endpoint %s", endpoint)
		}
		endpoints[endpoint] = true
		var key strings.Builder
		key.WriteString(endpoint)
		for _, cert := range [][]byte{consenter.ClientTLSCert, consenter.ServerTLSCert} {
			block, rest := pem.Decode(cert)
			if block == nil || block.Type != "CERTIFICATE" || len(bytes.TrimSpace(rest)) != 0 {
				return nil, fmt.Errorf("consenter %d requires a single public PEM certificate for each TLS role", index)
			}
			parsed, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return nil, fmt.Errorf("consenter %d has an invalid TLS certificate: %w", index, err)
			}
			if parsed.IsCA {
				return nil, fmt.Errorf("consenter %d requires TLS leaf certificates, not CA certificates", index)
			}
			hash := sha256.Sum256(parsed.Raw)
			if owner, ok := certificates[hash]; ok && owner != index {
				return nil, fmt.Errorf("consenters %d and %d reuse a TLS certificate", owner, index)
			}
			certificates[hash] = index
			fmt.Fprintf(&key, "/%x", hash)
		}
		keys[key.String()] = index
	}
	return keys, nil
}

// ValidateRaftPlan recomputes the supplied delta; live freshness is checked by the staging Job.
func ValidateRaftPlan(raw []byte, channel string) (*RaftUpdatePlan, error) {
	var supplied RaftUpdatePlan
	if err := json.Unmarshal(raw, &supplied); err != nil {
		return nil, fmt.Errorf("decode Raft plan: %w", err)
	}
	if supplied.APIVersion != "fabricops.io/v1alpha1" || supplied.Kind != "FabricChannelRaftUpdatePlan" ||
		channel == "" || supplied.Channel != channel {
		return nil, fmt.Errorf("expected a FabricChannelRaftUpdatePlan for channel %q", channel)
	}
	desiredRaw, err := raftJSONValue(supplied.UpdatedConfig,
		[]string{"channel_group", "groups", "Orderer", "values", "ConsensusType", "value", "metadata", "consenters"})
	if err != nil {
		return nil, err
	}
	var desired []RaftConsenter
	if err := json.Unmarshal(desiredRaw, &desired); err != nil {
		return nil, err
	}
	computed, err := planRaftConfig(supplied.OriginalConfig, channel, desired)
	if err != nil {
		return nil, err
	}
	if supplied.ConfigSequence != computed.ConfigSequence || supplied.Action != computed.Action {
		return nil, fmt.Errorf("reviewed Raft plan action or source sequence does not match its config pair")
	}
	decode := func(raw json.RawMessage) (any, error) {
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var value any
		err := decoder.Decode(&value)
		return value, err
	}
	actual, err := decode(supplied.UpdatedConfig)
	if err != nil {
		return nil, err
	}
	want, err := decode(computed.UpdatedConfig)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(actual, want) {
		return nil, fmt.Errorf("reviewed Raft plan changes fields outside the permitted single-consenter delta")
	}
	return computed, nil
}

// RaftConsentersFromConfig reads the public consenter set from a validated config.
func RaftConsentersFromConfig(config json.RawMessage) ([]RaftConsenter, error) {
	raw, err := raftJSONValue(config,
		[]string{"channel_group", "groups", "Orderer", "values", "ConsensusType", "value", "metadata", "consenters"})
	if err != nil {
		return nil, err
	}
	var consenters []RaftConsenter
	err = json.Unmarshal(raw, &consenters)
	return consenters, err
}

func raftJSONValue(raw json.RawMessage, path []string) (json.RawMessage, error) {
	for _, field := range path {
		var object map[string]json.RawMessage
		if err := json.Unmarshal(raw, &object); err != nil {
			return nil, fmt.Errorf("decode config field %s: %w", field, err)
		}
		raw = object[field]
		if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
			return nil, fmt.Errorf("config is missing %s", field)
		}
	}
	return raw, nil
}

func replaceRaftJSONValue(raw json.RawMessage, path []string, value json.RawMessage) (json.RawMessage, error) {
	if len(path) == 0 {
		return value, nil
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil, err
	}
	updated, err := replaceRaftJSONValue(object[path[0]], path[1:], value)
	if err != nil {
		return nil, err
	}
	object[path[0]] = updated
	return json.Marshal(object)
}
