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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/LF-Decentralized-Trust-labs/FabricOps/internal/channelconfig"
)

func TestPlanRaftUpdateChangesOnlyConsenters(t *testing.T) {
	a, b, c := raftTestConsenter(t, 0), raftTestConsenter(t, 1), raftTestConsenter(t, 2)
	for _, tc := range []struct {
		name    string
		current []channelconfig.RaftConsenter
		desired []channelconfig.RaftConsenter
		action  string
	}{
		{"add", []channelconfig.RaftConsenter{a, b}, []channelconfig.RaftConsenter{c, b, a}, "Add"},
		{"remove", []channelconfig.RaftConsenter{a, b, c}, []channelconfig.RaftConsenter{c, a}, "Remove"},
		{"reordered no-op", []channelconfig.RaftConsenter{a, b}, []channelconfig.RaftConsenter{b, a}, "AlreadyApplied"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			block := raftTestBlock(t, tc.current, "etcdraft", "STATE_NORMAL")
			input := append([]byte(nil), block...)
			plan, err := channelconfig.PlanRaftUpdate(block, "settlement", tc.desired)
			if err != nil {
				t.Fatal(err)
			}
			if plan.Action != tc.action || plan.Channel != "settlement" || plan.ConfigSequence != "9007199254740993" {
				t.Fatalf("unexpected plan: %#v", plan)
			}
			if !bytes.Equal(input, block) {
				t.Fatal("planner mutated input block")
			}
			var original, updated map[string]any
			if err := json.Unmarshal(plan.OriginalConfig, &original); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(plan.UpdatedConfig, &updated); err != nil {
				t.Fatal(err)
			}
			currentMetadata := raftTestMetadata(original)
			desiredMetadata := raftTestMetadata(updated)
			var actual []channelconfig.RaftConsenter
			raw, _ := json.Marshal(desiredMetadata["consenters"])
			if err := json.Unmarshal(raw, &actual); err != nil {
				t.Fatal(err)
			}
			var want []channelconfig.RaftConsenter
			switch tc.action {
			case "Add":
				want = []channelconfig.RaftConsenter{a, b, c}
			case "Remove":
				want = []channelconfig.RaftConsenter{a, c}
			default:
				want = tc.current
			}
			if !reflect.DeepEqual(actual, want) {
				t.Fatalf("consenters = %#v, want %#v", actual, want)
			}
			delete(currentMetadata, "consenters")
			delete(desiredMetadata, "consenters")
			if !reflect.DeepEqual(original, updated) {
				t.Fatal("planner changed fields outside the consenter list")
			}
			if len(plan.Warnings) == 0 || plan.Kind != "FabricChannelRaftUpdatePlan" {
				t.Fatal("missing plan identity or offline safety warnings")
			}
		})
	}
}

func TestPlanRaftUpdateRejectsUnsafeInputs(t *testing.T) {
	a, b, c, d := raftTestConsenter(t, 0), raftTestConsenter(t, 1), raftTestConsenter(t, 2), raftTestConsenter(t, 3)
	badCert := c
	badCert.ServerTLSCert = []byte("not a certificate")
	badPort := c
	badPort.Port = 65536
	badHost := c
	badHost.Host = "bad host"
	reusedCert := c
	reusedCert.ClientTLSCert = a.ClientTLSCert
	rotation := c
	rotation.Host = a.Host
	caCert := c
	caCert.ServerTLSCert = raftTestCertificate(t, 100, true)
	privateKey := c
	privateKey.ServerTLSCert = append(append([]byte(nil), c.ServerTLSCert...),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("private")})...)
	for _, tc := range []struct {
		name    string
		current []channelconfig.RaftConsenter
		desired []channelconfig.RaftConsenter
		typeID  string
		state   string
		channel string
	}{
		{"batch add", []channelconfig.RaftConsenter{a, b}, []channelconfig.RaftConsenter{a, b, c, d},
			"etcdraft", "STATE_NORMAL", "settlement"},
		{"batch remove", []channelconfig.RaftConsenter{a, b, c}, []channelconfig.RaftConsenter{a},
			"etcdraft", "STATE_NORMAL", "settlement"},
		{"replace", []channelconfig.RaftConsenter{a, b}, []channelconfig.RaftConsenter{a, c},
			"etcdraft", "STATE_NORMAL", "settlement"},
		{"rotation", []channelconfig.RaftConsenter{a, b}, []channelconfig.RaftConsenter{rotation, b},
			"etcdraft", "STATE_NORMAL", "settlement"},
		{"last node", []channelconfig.RaftConsenter{a}, nil, "etcdraft", "STATE_NORMAL", "settlement"},
		{"duplicate endpoint", []channelconfig.RaftConsenter{a, b}, []channelconfig.RaftConsenter{a, b, a},
			"etcdraft", "STATE_NORMAL", "settlement"},
		{"reused cert", []channelconfig.RaftConsenter{a, b}, []channelconfig.RaftConsenter{a, b, reusedCert},
			"etcdraft", "STATE_NORMAL", "settlement"},
		{"invalid cert", []channelconfig.RaftConsenter{a, b}, []channelconfig.RaftConsenter{a, b, badCert},
			"etcdraft", "STATE_NORMAL", "settlement"},
		{"CA cert", []channelconfig.RaftConsenter{a, b}, []channelconfig.RaftConsenter{a, b, caCert},
			"etcdraft", "STATE_NORMAL", "settlement"},
		{"private key", []channelconfig.RaftConsenter{a, b}, []channelconfig.RaftConsenter{a, b, privateKey},
			"etcdraft", "STATE_NORMAL", "settlement"},
		{"invalid port", []channelconfig.RaftConsenter{a, b}, []channelconfig.RaftConsenter{a, b, badPort},
			"etcdraft", "STATE_NORMAL", "settlement"},
		{"invalid host", []channelconfig.RaftConsenter{a, b}, []channelconfig.RaftConsenter{a, b, badHost},
			"etcdraft", "STATE_NORMAL", "settlement"},
		{"invalid current", []channelconfig.RaftConsenter{a, a}, []channelconfig.RaftConsenter{a, b},
			"etcdraft", "STATE_NORMAL", "settlement"},
		{"BFT", []channelconfig.RaftConsenter{a, b}, []channelconfig.RaftConsenter{a, b, c},
			"BFT", "STATE_NORMAL", "settlement"},
		{"maintenance", []channelconfig.RaftConsenter{a, b}, []channelconfig.RaftConsenter{a, b, c},
			"etcdraft", "STATE_MAINTENANCE", "settlement"},
		{"channel mismatch", []channelconfig.RaftConsenter{a, b}, []channelconfig.RaftConsenter{a, b, c},
			"etcdraft", "STATE_NORMAL", "audit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := channelconfig.PlanRaftUpdate(raftTestBlock(t, tc.current, tc.typeID, tc.state), tc.channel, tc.desired)
			if err == nil {
				t.Fatal("unsafe plan was accepted")
			}
		})
	}
	for _, block := range []string{"{}", "null", "{", `{"data":{"data":[]}}`} {
		if _, err := channelconfig.PlanRaftUpdate([]byte(block), "settlement", []channelconfig.RaftConsenter{a}); err == nil {
			t.Fatalf("accepted malformed config block %s", block)
		}
	}
}

func TestRaftUpdateCommandOffline(t *testing.T) {
	t.Setenv("KUBECONFIG", filepath.Join(t.TempDir(), "does-not-exist"))
	a, b := raftTestConsenter(t, 0), raftTestConsenter(t, 1)
	dir := t.TempDir()
	blockPath := filepath.Join(dir, "block.json")
	desiredPath, outPath := filepath.Join(dir, "desired.json"), filepath.Join(dir, "plan.json")
	block := raftTestBlock(t, []channelconfig.RaftConsenter{a}, "etcdraft", "STATE_NORMAL")
	if err := os.WriteFile(blockPath, block, 0600); err != nil {
		t.Fatal(err)
	}
	desired, _ := json.Marshal([]channelconfig.RaftConsenter{a, b})
	if err := os.WriteFile(desiredPath, desired, 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"channel", "plan-raft-update", "--channel", "settlement",
		"--block", blockPath, "--consenters", desiredPath}
	var stdout, stderr bytes.Buffer
	if err := run(args, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), `"action": "Add"`) {
		t.Fatalf("missing plan: %s", stdout.String())
	}
	if err := run(append(args, "--out", outPath), &bytes.Buffer{}, &stderr); err != nil {
		t.Fatal(err)
	}
	written, err := os.ReadFile(outPath)
	if err != nil || !bytes.Equal(written, stdout.Bytes()) {
		t.Fatal("file and stdout plans differ", err)
	}
	for _, invalid := range [][]string{{"channel"}, {"channel", "bad"}, args[:4], append(args, "extra")} {
		if err := run(invalid, &bytes.Buffer{}, &stderr); err == nil {
			t.Fatalf("accepted invalid arguments %v", invalid)
		}
	}
	for _, invalid := range []string{"null", "[] {}", `[{"unknown":true}]`, "{"} {
		if err := os.WriteFile(desiredPath, []byte(invalid), 0600); err != nil {
			t.Fatal(err)
		}
		if err := run(append(args, "--out", outPath), &bytes.Buffer{}, &stderr); err == nil {
			t.Fatalf("accepted invalid desired JSON %s", invalid)
		}
		unchanged, err := os.ReadFile(outPath)
		if err != nil || !bytes.Equal(unchanged, written) {
			t.Fatal("failed planning replaced an existing output file", err)
		}
	}
}

func TestValidateRaftPlanRejectsTampering(t *testing.T) {
	a, b := raftTestConsenter(t, 0), raftTestConsenter(t, 1)
	plan, err := channelconfig.PlanRaftUpdate(
		raftTestBlock(t, []channelconfig.RaftConsenter{a}, "etcdraft", "STATE_NORMAL"),
		"settlement", []channelconfig.RaftConsenter{a, b})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(plan)
	if _, err := channelconfig.ValidateRaftPlan(raw, "settlement"); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*channelconfig.RaftUpdatePlan){
		func(plan *channelconfig.RaftUpdatePlan) { plan.Channel = "audit" },
		func(plan *channelconfig.RaftUpdatePlan) { plan.ConfigSequence = "0" },
		func(plan *channelconfig.RaftUpdatePlan) { plan.Action = "Remove" },
		func(plan *channelconfig.RaftUpdatePlan) { plan.Kind = "untrusted" },
		func(plan *channelconfig.RaftUpdatePlan) {
			plan.UpdatedConfig = bytes.Replace(plan.UpdatedConfig, []byte(`"Admins":"unchanged"`),
				[]byte(`"Admins":"weakened"`), 1)
		},
	} {
		copy := *plan
		mutate(&copy)
		tampered, _ := json.Marshal(copy)
		if _, err := channelconfig.ValidateRaftPlan(tampered, "settlement"); err == nil {
			t.Fatalf("accepted tampered Raft plan: %s", tampered)
		}
	}
}

func raftTestConsenter(t *testing.T, index int64) channelconfig.RaftConsenter {
	t.Helper()
	public := raftTestCertificate(t, index+1, false)
	return channelconfig.RaftConsenter{Host: "orderer" + big.NewInt(index).String() + ".example.com",
		Port: 7050, ClientTLSCert: public, ServerTLSCert: public}
}

func raftTestCertificate(t *testing.T, serial int64, isCA bool) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "orderer"},
		NotBefore: time.Unix(0, 0), NotAfter: time.Unix(4102444800, 0), KeyUsage: x509.KeyUsageDigitalSignature,
		IsCA: isCA, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func raftTestBlock(t *testing.T, consenters []channelconfig.RaftConsenter, consensus, state string) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"data": map[string]any{"data": []any{map[string]any{
		"payload": map[string]any{
			"header": map[string]any{"channel_header": map[string]any{"channel_id": "settlement", "type": 1}},
			"data": map[string]any{"config": map[string]any{
				"sequence": "9007199254740993",
				"channel_group": map[string]any{"version": "4", "policies": map[string]any{"Admins": "unchanged"},
					"groups": map[string]any{"Application": map[string]any{"version": "2"},
						"Orderer": map[string]any{"values": map[string]any{"Endpoints": "unchanged",
							"ConsensusType": map[string]any{"version": "3", "mod_policy": "Admins", "value": map[string]any{
								"type": consensus, "state": state, "metadata": map[string]any{
									"consenters": consenters, "options": map[string]any{"tick_interval": "500ms"}, "future_field": "preserved",
								},
							}},
						}},
					}},
			}},
		},
	}}}})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func raftTestMetadata(config map[string]any) map[string]any {
	object := config
	for _, key := range []string{"channel_group", "groups", "Orderer", "values", "ConsensusType", "value", "metadata"} {
		object = object[key].(map[string]any)
	}
	return object
}
