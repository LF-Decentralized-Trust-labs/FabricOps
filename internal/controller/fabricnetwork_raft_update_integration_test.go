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

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LF-Decentralized-Trust-labs/FabricOps/internal/channelconfig"
)

// Real Fabric protobuf tools are used; peer transport is stubbed to supply a
// config block. This does not prove live authorization or consensus safety.
func TestRaftStagingFabricTools(t *testing.T) {
	for _, tc := range []struct {
		name    string
		stale   bool
		admin   string
		noOp    bool
		wantErr bool
	}{
		{name: "stage", admin: "OrdererMSP"},
		{name: "stale plan", admin: "OrdererMSP", stale: true, wantErr: true},
		{name: "application admin", admin: "BankAMSP", wantErr: true},
		{name: "already applied snapshot", admin: "OrdererMSP", noOp: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, raw := raftStagingFixture(t)
			var source channelconfig.RaftUpdatePlan
			if err := json.Unmarshal(raw, &source); err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			write := func(name string, contents []byte, mode os.FileMode) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(dir, name), contents, mode); err != nil {
					t.Fatal(err)
				}
			}
			block := `{"data":{"data":[{"payload":{"header":{"channel_header":{"type":1,"channel_id":"settlement"}},` +
				`"data":{"config":` + string(source.OriginalConfig) + `}}}]}}`
			write("block.json", []byte(block), 0600)
			output, err := runRaftStagingTools(dir, `
configtxlator proto_encode --input block.json --type common.Block --output block.pb
configtxlator proto_decode --input block.pb --type common.Block --output canonical-block.json`)
			if err != nil {
				t.Fatalf("canonicalize fixture: %v\n%s", err, output)
			}
			canonical, err := os.ReadFile(filepath.Join(dir, "canonical-block.json"))
			if err != nil {
				t.Fatal(err)
			}
			if !json.Valid(canonical) {
				t.Fatalf("invalid canonical block JSON (%d bytes): %q", len(canonical), canonical)
			}
			var updated struct {
				ChannelGroup struct {
					Groups map[string]struct {
						Values map[string]struct {
							Value struct {
								Metadata struct {
									Consenters []channelconfig.RaftConsenter `json:"consenters"`
								} `json:"metadata"`
							} `json:"value"`
						} `json:"values"`
					} `json:"groups"`
				} `json:"channel_group"`
			}
			if err := json.Unmarshal(source.UpdatedConfig, &updated); err != nil {
				t.Fatal(err)
			}
			desired := updated.ChannelGroup.Groups["Orderer"].Values["ConsensusType"].Value.Metadata.Consenters
			if tc.noOp {
				desired = desired[:1]
			}
			plan, err := channelconfig.PlanRaftUpdate(canonical, "settlement", desired)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"raft-input", "bin"} {
				if err := os.Mkdir(filepath.Join(dir, name), 0700); err != nil {
					t.Fatal(err)
				}
			}
			planRaw, _ := json.Marshal(plan)
			write("raft-input/plan.json", planRaw, 0600)
			request := raftUpdateReceipt{Channel: "settlement", OperationID: "test", PlanHash: "hash",
				ConfigSequence: plan.ConfigSequence, Action: plan.Action}
			requestRaw, _ := json.Marshal(request)
			write("raft-input/request.json", requestRaw, 0600)
			write("bin/peer", []byte(`#!/bin/sh
set -eu
[ "$1 $2 $3" = "channel fetch config" ] || { echo "Unexpected peer mutation" >&2; exit 1; }
cp /fabricops/channel/block.pb "$4"
`), 0700)
			if tc.stale {
				write("stale-block.json", []byte(strings.ReplaceAll(string(canonical), `"sequence": "3"`, `"sequence": "4"`)), 0600)
				if output, err := runRaftStagingTools(dir,
					"configtxlator proto_encode --input stale-block.json --type common.Block --output block.pb"); err != nil {
					t.Fatalf("prepare stale block: %v\n%s", err, output)
				}
			}
			write("stage.sh", []byte(raftStagingScript("settlement", tc.admin, "orderer0:7050")), 0600)
			output, err = runRaftStagingTools(dir,
				"export PATH=/fabricops/channel/bin:$PATH; sh /fabricops/channel/stage.sh")
			if tc.wantErr {
				if err == nil {
					t.Fatal("unsafe staging was accepted")
				}
				if _, err := os.Stat(filepath.Join(dir, "output", "config-update.pb")); !os.IsNotExist(err) {
					t.Fatal("rejected staging published an envelope")
				}
				return
			}
			if err != nil {
				t.Fatalf("stage: %v\n%s", err, output)
			}
			receiptRaw, err := os.ReadFile(filepath.Join(dir, "output", "raft-update.json"))
			if err != nil {
				t.Fatal(err)
			}
			var receipt raftUpdateReceipt
			if err := json.Unmarshal(receiptRaw, &receipt); err != nil {
				t.Fatal(err)
			}
			outcome := receipt.Outcome
			receipt.Outcome = ""
			if receipt != request || (tc.noOp && outcome != "alreadyApplied") || (!tc.noOp && outcome != "staged") {
				t.Fatalf("incorrect staging evidence: %+v, outcome %s", receipt, outcome)
			}
			for _, name := range []string{"raft-update.json", "channel-block.pb"} {
				info, err := os.Stat(filepath.Join(dir, "output", name))
				if err != nil || info.Mode().Perm() != 0644 {
					t.Fatal("public evidence is not readable by the non-root publisher", name, err)
				}
			}
			if tc.noOp {
				if _, err := os.Stat(filepath.Join(dir, "output", "config-update.pb")); !os.IsNotExist(err) {
					t.Fatal("no-op created an envelope")
				}
				return
			}
			output, err = runRaftStagingTools(dir, `
configtxlator proto_decode --input output/config-update.pb --type common.Envelope --output envelope.json
jq -e '.payload.header.channel_header.channel_id == "settlement" and
  .payload.header.channel_header.type == 2 and (.payload.data.signatures | length) == 0' envelope.json`)
			if err != nil {
				t.Fatalf("invalid unsigned envelope: %v\n%s", err, output)
			}
		})
	}
}

func TestRaftStagingToolsHostIdentity(t *testing.T) {
	dir := t.TempDir()
	output, err := runRaftStagingTools(dir, fmt.Sprintf(`
test "$(id -u)" = %d
test "$(id -g)" = %d
umask 077
printf owned-by-caller > owner-only.txt`, os.Getuid(), os.Getgid()))
	if err != nil {
		t.Fatalf("Fabric tools must use the test runner's identity: %v\n%s", err, output)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "owner-only.txt"))
	if err != nil || string(raw) != "owned-by-caller" {
		t.Fatalf("container output must remain readable by the test runner: %q, %v", raw, err)
	}
}

func runRaftStagingTools(dir, script string) ([]byte, error) {
	image := os.Getenv("FABRIC_TOOLS_TEST_IMAGE")
	if image == "" {
		image = "hyperledger/fabric-tools:2.5.14"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// Keep owner-only codec outputs accessible to the non-root Linux runner.
	return exec.CommandContext(ctx, "docker", "run", "--rm", "--network", "none",
		"--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()), "-v", dir+":/fabricops/channel",
		"-w", "/fabricops/channel", image, "sh", "-ec", script).CombinedOutput()
}
