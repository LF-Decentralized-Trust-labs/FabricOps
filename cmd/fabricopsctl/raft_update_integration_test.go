//go:build integration

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
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/LF-Decentralized-Trust-labs/FabricOps/internal/channelconfig"
)

// Run explicitly with -tags=integration; requires Docker and the Fabric tools image.
// This checks real protobuf compatibility, not consensus or policy authorization.
func TestRaftUpdateConfigtxlator(t *testing.T) {
	a, b, c := raftTestConsenter(t, 0), raftTestConsenter(t, 1), raftTestConsenter(t, 2)
	for _, tc := range []struct {
		name    string
		desired []channelconfig.RaftConsenter
	}{
		{"add", []channelconfig.RaftConsenter{a, b, c}},
		{"remove", []channelconfig.RaftConsenter{a}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			block := raftTestBlock(t, []channelconfig.RaftConsenter{a, b}, "etcdraft", "STATE_NORMAL")
			// Unit fixtures include deliberately unknown fields. The Fabric codec
			// requires its current schema, so remove those fixture-only sentinels.
			var decoded map[string]any
			if err := json.Unmarshal(block, &decoded); err != nil {
				t.Fatal(err)
			}
			transaction := decoded["data"].(map[string]any)["data"].([]any)[0].(map[string]any)
			payload := transaction["payload"].(map[string]any)["data"].(map[string]any)
			config := payload["config"].(map[string]any)
			delete(raftTestMetadata(config), "future_field")
			group := config["channel_group"].(map[string]any)
			delete(group, "policies")
			values := group["groups"].(map[string]any)["Orderer"].(map[string]any)["values"].(map[string]any)
			delete(values, "Endpoints")
			writeRaftIntegrationJSON(t, dir, "block.json", decoded)
			runRaftFabricTools(t, dir, `
configtxlator proto_encode --input block.json --type common.Block --output block.pb
configtxlator proto_decode --input block.pb --type common.Block --output fetched.json`)
			writeRaftIntegrationJSON(t, dir, "desired.json", tc.desired)
			var stdout, stderr bytes.Buffer
			if err := run([]string{"channel", "plan-raft-update", "--channel", "settlement",
				"--block", filepath.Join(dir, "fetched.json"), "--consenters", filepath.Join(dir, "desired.json")},
				&stdout, &stderr); err != nil {
				t.Fatalf("plan failed: %v: %s", err, stderr.String())
			}
			var plan channelconfig.RaftUpdatePlan
			if err := json.Unmarshal(stdout.Bytes(), &plan); err != nil {
				t.Fatal(err)
			}
			writeRaftIntegrationJSON(t, dir, "original.json", plan.OriginalConfig)
			writeRaftIntegrationJSON(t, dir, "updated.json", plan.UpdatedConfig)
			runRaftFabricTools(t, dir, `
configtxlator proto_encode --input original.json --type common.Config --output original.pb
configtxlator proto_encode --input updated.json --type common.Config --output updated.pb
configtxlator compute_update --channel_id settlement --original original.pb --updated updated.pb --output update.pb
configtxlator proto_decode --input update.pb --type common.ConfigUpdate --output update.json`)
			raw, err := os.ReadFile(filepath.Join(dir, "update.json"))
			if err != nil {
				t.Fatal(err)
			}
			var update map[string]any
			if err := json.Unmarshal(raw, &update); err != nil {
				t.Fatal(err)
			}
			if update["channel_id"] != "settlement" {
				t.Fatal("configtxlator produced the wrong channel", string(raw))
			}
			writeSet := update["write_set"].(map[string]any)
			groups := writeSet["groups"].(map[string]any)
			if _, changed := groups["Application"]; changed {
				t.Fatal("Raft update unexpectedly includes Application group")
			}
			consensus := groups["Orderer"].(map[string]any)["values"].(map[string]any)["ConsensusType"].(map[string]any)
			if consensus["version"] != "4" {
				t.Fatal("expected only ConsensusType value version to increment", string(raw))
			}
		})
	}
}

func writeRaftIntegrationJSON(t *testing.T, dir, name string, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func runRaftFabricTools(t *testing.T, dir, script string) {
	t.Helper()
	image := os.Getenv("FABRIC_TOOLS_TEST_IMAGE")
	if image == "" {
		image = "hyperledger/fabric-tools:2.5.14"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "docker", "run", "--rm", "--network", "none",
		"-v", dir+":/work", "-w", "/work", image, "sh", "-ec", script)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("Fabric tools failed: %v\n%s", err, output)
	}
}
