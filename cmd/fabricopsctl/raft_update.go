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
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/LF-Decentralized-Trust-labs/FabricOps/internal/channelconfig"
)

func runChannel(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] != "plan-raft-update" {
		printLine(stderr, "Usage: fabricopsctl channel plan-raft-update "+
			"--channel <channel> --block <config-block.json> --consenters <consenters.json> [--out <plan.json>]")
		return errUsage
	}
	var channel, blockPath, consentersPath, outputPath string
	flags := flag.NewFlagSet("channel plan-raft-update", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&channel, "channel", "", "Expected channel ID in the fetched config block")
	flags.StringVar(&blockPath, "block", "", "Fetched config block decoded as common.Block JSON")
	flags.StringVar(&consentersPath, "consenters", "", "Desired Raft consenters JSON array (public TLS certificates only)")
	flags.StringVar(&outputPath, "out", "", "Write the offline plan to this file instead of stdout")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if channel == "" || blockPath == "" || consentersPath == "" || flags.NArg() != 0 {
		return fmt.Errorf("%w: --channel, --block, and --consenters are required; no positional arguments", errUsage)
	}
	block, err := os.ReadFile(blockPath)
	if err != nil {
		return fmt.Errorf("read config block: %w", err)
	}
	raw, err := os.ReadFile(consentersPath)
	if err != nil {
		return fmt.Errorf("read desired consenters: %w", err)
	}
	var desired []channelconfig.RaftConsenter
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&desired); err != nil {
		return fmt.Errorf("decode desired consenters: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("desired consenters must contain exactly one JSON array")
	}
	plan, err := channelconfig.PlanRaftUpdate(block, channel, desired)
	if err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	return writeProfileOutput(stdout, outputPath, string(encoded)+"\n")
}
