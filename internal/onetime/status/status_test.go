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

// Package status provides information on the agent, configuration, IAM and functional statuses.

package status

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/commandlineexecutor"

	spb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/status"
)

func TestAgentStatus(t *testing.T) {
	tests := []struct {
		name string
		exec commandlineexecutor.Execute
		want *spb.AgentStatus
	}{
		{
			name: "Success",
			exec: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				if strings.Contains(params.ArgsToSplit, "is-enabled") {
					return commandlineexecutor.Result{StdOut: "enabled", ExitCode: 0}
				}
				if strings.Contains(params.ArgsToSplit, "is-active") {
					return commandlineexecutor.Result{StdOut: "active", ExitCode: 0}
				}
				return commandlineexecutor.Result{}
			},
			want: &spb.AgentStatus{
				SystemdServiceEnabled: spb.State_SUCCESS_STATE,
				SystemdServiceRunning: spb.State_SUCCESS_STATE,
			},
		},
		{
			name: "Failure",
			exec: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				if strings.Contains(params.ArgsToSplit, "is-enabled") {
					return commandlineexecutor.Result{StdOut: "disabled", ExitCode: 1}
				}
				if strings.Contains(params.ArgsToSplit, "is-active") {
					return commandlineexecutor.Result{StdOut: "inactive", ExitCode: 3}
				}
				return commandlineexecutor.Result{}
			},
			want: &spb.AgentStatus{
				SystemdServiceEnabled: spb.State_FAILURE_STATE,
				SystemdServiceRunning: spb.State_FAILURE_STATE,
			},
		},
		{
			name: "EnabledButNotRunning",
			exec: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				if strings.Contains(params.ArgsToSplit, "is-enabled") {
					return commandlineexecutor.Result{StdOut: "enabled", ExitCode: 0}
				}
				if strings.Contains(params.ArgsToSplit, "is-active") {
					return commandlineexecutor.Result{StdOut: "inactive", ExitCode: 3}
				}
				return commandlineexecutor.Result{}
			},
			want: &spb.AgentStatus{
				SystemdServiceEnabled: spb.State_SUCCESS_STATE,
				SystemdServiceRunning: spb.State_FAILURE_STATE,
			},
		},
		{
			name: "RunningButNotEnabled",
			exec: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				if strings.Contains(params.ArgsToSplit, "is-enabled") {
					return commandlineexecutor.Result{StdOut: "disabled", ExitCode: 1}
				}
				if strings.Contains(params.ArgsToSplit, "is-active") {
					return commandlineexecutor.Result{StdOut: "active", ExitCode: 0}
				}
				return commandlineexecutor.Result{}
			},
			want: &spb.AgentStatus{
				SystemdServiceEnabled: spb.State_FAILURE_STATE,
				SystemdServiceRunning: spb.State_SUCCESS_STATE,
			},
		},
		{
			name: "Error",
			exec: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				return commandlineexecutor.Result{
					StdErr: "command failed",
					Error:  errors.New("command failed"),
				}
			},
			want: &spb.AgentStatus{
				SystemdServiceEnabled: spb.State_ERROR_STATE,
				SystemdServiceRunning: spb.State_ERROR_STATE,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &Status{
				exec: tc.exec,
			}
			ctx := context.Background()
			got := s.agentStatus(ctx)
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("agentStatus() returned unexpected diff (-want +got):\n%s", diff)
			}
		})
	}
}
