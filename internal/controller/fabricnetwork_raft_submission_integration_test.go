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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/LF-Decentralized-Trust-labs/FabricOps/internal/channelconfig"
)

// Fabric codecs are real; peer, osnadmin, and health transports are simulated.
// Fabric signature authorization and distributed consensus require the kind proof.
func TestRaftSubmissionFabricTools(t *testing.T) {
	for _, scenario := range []string{"success", "remove", "remove-unhealthy", "lagging", "unhealthy", "wrong-role", "stale",
		"changed-delta", "wrong-channel", "unsigned", "no-commit", "already-applied"} {
		t.Run(scenario, func(t *testing.T) {
			dir := prepareRaftSubmission(t, scenario == "remove" || scenario == "remove-unhealthy")
			write := func(name string, raw []byte, mode os.FileMode) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(dir, name), raw, mode); err != nil {
					t.Fatal(err)
				}
			}
			write("scenario", []byte(scenario), 0600)
			if scenario == "already-applied" {
				write("applied", nil, 0600)
			}
			if scenario == "stale" {
				output, err := runRaftStagingTools(dir, `
jq '.data.data[0].payload.data.config.sequence="4"' canonical.json > stale.json
configtxlator proto_encode --input stale.json --type common.Block --output block.pb`)
				if err != nil {
					t.Fatalf("prepare stale config: %v\n%s", err, output)
				}
			}
			filter := "."
			switch scenario {
			case "changed-delta":
				filter = `.payload.data.config_update.write_set.version="99"`
			case "wrong-channel":
				filter = `.payload.header.channel_header.channel_id="audit"`
			case "unsigned":
				filter = `del(.payload.data.signatures)`
			}
			write("filter.jq", []byte(filter), 0600)
			output, err := runRaftStagingTools(dir, `
jq -f filter.jq signed.json > adjusted.json
configtxlator proto_encode --input adjusted.json --type common.Envelope --output raft-input/signed.pb`)
			if err != nil {
				t.Fatalf("prepare signed handoff: %v\n%s", err, output)
			}
			write("submit.sh", []byte(raftSubmissionScript("settlement", "OrdererMSP", "orderer0:7050")), 0600)
			output, err = runRaftStagingTools(dir, "export PATH=/fabricops/channel/bin:$PATH; sh submit.sh")
			_, updateErr := os.Stat(filepath.Join(dir, "update-called"))
			if (scenario == "success" || scenario == "remove" || scenario == "no-commit") && updateErr != nil {
				t.Fatalf("expected submission did not reach the orderer: %v\n%s", updateErr, output)
			}
			if scenario != "success" && scenario != "remove" && scenario != "already-applied" {
				if err == nil {
					t.Fatal("unsafe submission succeeded")
				}
				if scenario != "no-commit" && !os.IsNotExist(updateErr) {
					t.Fatalf("rejected handoff reached peer channel update: %s", output)
				}
				if _, receiptErr := os.Stat(filepath.Join(dir, "output", "raft-update.json")); !os.IsNotExist(receiptErr) {
					t.Fatal("failed submission published success evidence")
				}
				return
			}
			if err != nil {
				t.Fatalf("signed submission failed: %v\n%s", err, output)
			}
			if scenario == "already-applied" && !os.IsNotExist(updateErr) {
				t.Fatal("already applied operation was submitted twice")
			}
			receiptRaw, err := os.ReadFile(filepath.Join(dir, "output", "raft-update.json"))
			if err != nil {
				t.Fatal(err)
			}
			var receipt raftUpdateReceipt
			if err := json.Unmarshal(receiptRaw, &receipt); err != nil {
				t.Fatal(err)
			}
			wantOutcome := "submitted"
			if scenario == "already-applied" {
				wantOutcome = "alreadyApplied"
			}
			wantAction := "Add"
			if scenario == "remove" {
				wantAction = "Remove"
			}
			if receipt.Outcome != wantOutcome || receipt.Action != wantAction ||
				receipt.SignedHash != "signed-hash" || receipt.PlanHash != "plan-hash" {
				t.Fatalf("incorrect durable receipt: %+v", receipt)
			}
		})
	}
}

func prepareRaftSubmission(t *testing.T, remove bool) string {
	t.Helper()
	dir := t.TempDir()
	_, _, raw := raftStagingFixture(t)
	var source channelconfig.RaftUpdatePlan
	if err := json.Unmarshal(raw, &source); err != nil {
		t.Fatal(err)
	}
	if remove {
		source.OriginalConfig = source.UpdatedConfig
	}
	write := func(name string, raw []byte, mode os.FileMode) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), raw, mode); err != nil {
			t.Fatal(err)
		}
	}
	block := `{"header":{"number":"0"},"data":{"data":[{"payload":{"header":{"channel_header":{"type":1,"channel_id":"settlement"}},` +
		`"data":{"config":` + string(source.OriginalConfig) + `}}}]}}`
	write("block.json", []byte(block), 0600)
	output, err := runRaftStagingTools(dir, `
configtxlator proto_encode --input block.json --type common.Block --output block.pb
configtxlator proto_decode --input block.pb --type common.Block --output canonical.json`)
	if err != nil {
		t.Fatalf("canonicalize fixture: %v\n%s", err, output)
	}
	canonical, err := os.ReadFile(filepath.Join(dir, "canonical.json"))
	if err != nil {
		t.Fatal(err)
	}
	desired, err := channelconfig.RaftConsentersFromConfig(source.UpdatedConfig)
	if err != nil {
		t.Fatal(err)
	}
	if remove {
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
	request, _ := json.Marshal(raftUpdateReceipt{Channel: "settlement", OperationID: "test", PlanHash: "plan-hash",
		SignedHash: "signed-hash", ConfigSequence: plan.ConfigSequence, Action: plan.Action})
	write("raft-input/request.json", request, 0600)
	nodes, _ := json.Marshal([]raftSubmissionNode{
		{Name: "orderer0", AdminAddress: "orderer0:9443", Operations: "http://orderer0:8443", CAFile: "/ca", Current: true, Desired: true},
		{Name: "orderer1", AdminAddress: "orderer1:9443", Operations: "http://orderer1:8443", CAFile: "/ca", Desired: true},
	})
	if remove {
		nodes, _ = json.Marshal([]raftSubmissionNode{
			{Name: "orderer0", AdminAddress: "orderer0:9443", Operations: "http://orderer0:8443", CAFile: "/ca", Current: true, Desired: true},
			{Name: "orderer1", AdminAddress: "orderer1:9443", Operations: "http://orderer1:8443", CAFile: "/ca", Current: true},
		})
	}
	write("raft-input/nodes.json", nodes, 0600)
	write("scenario", []byte("success"), 0600)
	write("bin/peer", []byte(`#!/bin/sh
set -eu
root=/fabricops/channel
case "$1 $2 $3" in
  'channel fetch config')
    if [ -f "$root/applied" ]; then cp "$root/committed.pb" "$4"; else cp "$root/block.pb" "$4"; fi ;;
  'channel fetch newest') cp "$root/block.pb" "$4" ;;
  'channel update -c')
    touch "$root/update-called"
    [ "$(cat "$root/scenario")" = no-commit ] || touch "$root/applied" ;;
  *) echo "Unexpected peer command" >&2; exit 1 ;;
esac
`), 0700)
	write("bin/curl", []byte(`#!/bin/sh
if echo "$(cat /fabricops/channel/scenario)" | grep -q unhealthy; then
  echo '{"status":"ERROR"}'
else echo '{"status":"OK"}'; fi
`), 0700)
	write("bin/osnadmin", []byte(`#!/bin/sh
set -eu
root=/fabricops/channel
relation=consenter
height=1
if [ -f "$root/applied" ]; then height=2
elif echo "$*" | grep -q orderer1 && [ "$(jq -r .action "$root/raft-input/plan.json")" != Remove ]; then
  relation=follower
  case "$(cat "$root/scenario")" in
    lagging) height=0 ;;
    wrong-role) relation=consenter ;;
  esac
fi
echo "{\"name\":\"settlement\",\"status\":\"active\",\"consensusRelation\":\"$relation\",\"height\":$height}"
`), 0700)
	write("bin/sleep", []byte("#!/bin/sh\nexit 0\n"), 0700)
	write("stage.sh", []byte(raftStagingScript("settlement", "OrdererMSP", "orderer0:7050")), 0600)
	output, err = runRaftStagingTools(dir, `
export PATH=/fabricops/channel/bin:$PATH
sh stage.sh
cp output/config-update.pb raft-input/staged.pb
configtxlator proto_decode --input raft-input/staged.pb --type common.Envelope --output envelope.json
jq '.payload.data.signatures=[{signature_header:{nonce:"bm9uY2U=",creator:{mspid:"OrdererMSP",id_bytes:"Y2VydA=="}},signature:"c2ln"}]' \
  envelope.json > signed.json
jq '.updatedConfig' raft-input/plan.json > updated-config.json
jq --slurpfile config updated-config.json '.header.number="1" |
  .data.data[0].payload.data.config=$config[0] |
  .data.data[0].payload.data.config.sequence="4" |
  .data.data[0].payload.data.config.channel_group.groups.Orderer.values.ConsensusType.version="2"' \
  canonical.json > committed.json
configtxlator proto_encode --input committed.json --type common.Block --output committed.pb`)
	if err != nil {
		t.Fatalf("prepare staged update: %v\n%s", err, output)
	}
	if err := os.Rename(filepath.Join(dir, "output"), filepath.Join(dir, "staging-output")); err != nil {
		t.Fatal(err)
	}
	return dir
}
