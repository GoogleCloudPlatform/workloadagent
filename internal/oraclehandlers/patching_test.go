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
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/commandlineexecutor"

	anypb "google.golang.org/protobuf/types/known/anypb"
	codepb "google.golang.org/genproto/googleapis/rpc/code"
	spb "google.golang.org/genproto/googleapis/rpc/status"
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

func TestDisableRestrictedSession(t *testing.T) {
	tests := []struct {
		name          string
		params        map[string]string
		sqlQueries    map[string]*commandlineexecutor.Result
		wantErrorCode codepb.Code
	}{
		{
			name:          "DisableRestrictedSession_Validation_Fail",
			params:        map[string]string{},
			wantErrorCode: codepb.Code_INVALID_ARGUMENT,
		},
		{
			name: "DisableRestrictedSession_Success",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"ALTER SYSTEM DISABLE RESTRICTED SESSION;": &commandlineexecutor.Result{StdOut: "System altered."},
			},
			wantErrorCode: codepb.Code_OK,
		},
		{
			name: "DisableRestrictedSession_Fail",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"ALTER SYSTEM DISABLE RESTRICTED SESSION;": &commandlineexecutor.Result{ExitCode: 1, Error: fmt.Errorf("alter failed")},
			},
			wantErrorCode: codepb.Code_FAILED_PRECONDITION,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			origRunSQL := runSQL
			defer func() { runSQL = origRunSQL }()
			runSQL = createMockRunSQL(tc.sqlQueries)

			command := &gpb.Command{
				CommandType: &gpb.Command_AgentCommand{
					AgentCommand: &gpb.AgentCommand{
						Command:    "oracle_disable_restricted_session",
						Parameters: tc.params,
					},
				},
			}
			result := DisableRestrictedSession(context.Background(), command, nil)

			s := &spb.Status{}
			if err := anypb.UnmarshalTo(result.Payload, s, proto.UnmarshalOptions{}); err != nil {
				t.Fatalf("Failed to unmarshal payload: %v", err)
			}
			if s.Code != int32(tc.wantErrorCode) {
				t.Errorf("DisableRestrictedSession() with params %v returned error code %d, want %d", tc.params, s.Code, tc.wantErrorCode)
			}
		})
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
