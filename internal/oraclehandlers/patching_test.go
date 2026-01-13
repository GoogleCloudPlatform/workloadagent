/*
Copyright 2025 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package oraclehandlers

import (
	"context"
	"strings"
	"testing"

	gpb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/guestactions"
)

func TestDisableAutostart_NotImplemented(t *testing.T) {
	command := &gpb.Command{
		CommandType: &gpb.Command_AgentCommand{
			AgentCommand: &gpb.AgentCommand{
				Command: "oracle_disable_autostart",
			},
		},
	}
	result := DisableAutostart(context.Background(), command, nil)
	if result.GetExitCode() != 1 {
		t.Errorf("DisableAutostart() returned exit code %d, want 1", result.GetExitCode())
	}
	if !strings.Contains(result.GetStdout(), "not implemented") {
		t.Errorf("DisableAutostart() returned stdout %q, want 'not implemented'", result.GetStdout())
	}
}

func TestRunDatapatch_NotImplemented(t *testing.T) {
	command := &gpb.Command{
		CommandType: &gpb.Command_AgentCommand{
			AgentCommand: &gpb.AgentCommand{
				Command: "oracle_run_datapatch",
			},
		},
	}
	result := RunDatapatch(context.Background(), command, nil)
	if result.GetExitCode() != 1 {
		t.Errorf("RunDatapatch() returned exit code %d, want 1", result.GetExitCode())
	}
	if !strings.Contains(result.GetStdout(), "not implemented") {
		t.Errorf("RunDatapatch() returned stdout %q, want 'not implemented'", result.GetStdout())
	}
}

func TestDisableRestrictedMode_NotImplemented(t *testing.T) {
	command := &gpb.Command{
		CommandType: &gpb.Command_AgentCommand{
			AgentCommand: &gpb.AgentCommand{
				Command: "oracle_disable_restricted_mode",
			},
		},
	}
	result := DisableRestrictedMode(context.Background(), command, nil)
	if result.GetExitCode() != 1 {
		t.Errorf("DisableRestrictedMode() returned exit code %d, want 1", result.GetExitCode())
	}
	if !strings.Contains(result.GetStdout(), "not implemented") {
		t.Errorf("DisableRestrictedMode() returned stdout %q, want 'not implemented'", result.GetStdout())
	}
}

func TestStartListener_NotImplemented(t *testing.T) {
	command := &gpb.Command{
		CommandType: &gpb.Command_AgentCommand{
			AgentCommand: &gpb.AgentCommand{
				Command: "oracle_start_listener",
			},
		},
	}
	result := StartListener(context.Background(), command, nil)
	if result.GetExitCode() != 1 {
		t.Errorf("StartListener() returned exit code %d, want 1", result.GetExitCode())
	}
	if !strings.Contains(result.GetStdout(), "not implemented") {
		t.Errorf("StartListener() returned stdout %q, want 'not implemented'", result.GetStdout())
	}
}

func TestEnableAutostart_NotImplemented(t *testing.T) {
	command := &gpb.Command{
		CommandType: &gpb.Command_AgentCommand{
			AgentCommand: &gpb.AgentCommand{
				Command: "oracle_enable_autostart",
			},
		},
	}
	result := EnableAutostart(context.Background(), command, nil)
	if result.GetExitCode() != 1 {
		t.Errorf("EnableAutostart() returned exit code %d, want 1", result.GetExitCode())
	}
	if !strings.Contains(result.GetStdout(), "not implemented") {
		t.Errorf("EnableAutostart() returned stdout %q, want 'not implemented'", result.GetStdout())
	}
}
