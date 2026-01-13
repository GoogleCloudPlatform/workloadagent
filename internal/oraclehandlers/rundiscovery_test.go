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

func TestRunDiscovery_NotImplemented(t *testing.T) {
	command := &gpb.Command{
		CommandType: &gpb.Command_AgentCommand{
			AgentCommand: &gpb.AgentCommand{
				Command: "oracle_run_discovery",
			},
		},
	}
	result := RunDiscovery(context.Background(), command, nil)
	if result.GetExitCode() != 1 {
		t.Errorf("RunDiscovery() returned exit code %d, want 1", result.GetExitCode())
	}
	if !strings.Contains(result.GetStdout(), "not implemented") {
		t.Errorf("RunDiscovery() returned stdout %q, want 'not implemented'", result.GetStdout())
	}
}
