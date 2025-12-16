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
	"testing"

	anypb "google.golang.org/protobuf/types/known/anypb"
	codepb "google.golang.org/genproto/googleapis/rpc/code"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/commandlineexecutor"
	gpb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/guestactions"
)

func TestCommandResult(t *testing.T) {
	tests := []struct {
		name    string
		command *gpb.Command
		result  *commandlineexecutor.Result
		code    codepb.Code
		message string
	}{
		{
			name: "success",
			command: &gpb.Command{
				CommandType: &gpb.Command_AgentCommand{
					AgentCommand: &gpb.AgentCommand{
						Command: "test_command",
						Parameters: map[string]string{
							"oracle_sid": "orcl",
						},
					},
				},
			},
			result: &commandlineexecutor.Result{
				ExitCode: 0,
				StdOut:   "stdout",
				StdErr:   "stderr",
			},
			code:    codepb.Code_OK,
			message: "success",
		},
		{
			name: "failure",
			command: &gpb.Command{
				CommandType: &gpb.Command_AgentCommand{
					AgentCommand: &gpb.AgentCommand{
						Command: "test_command",
						Parameters: map[string]string{
							"oracle_sid": "orcl",
						},
					},
				},
			},
			result: &commandlineexecutor.Result{
				ExitCode: 1,
				StdOut:   "stdout",
				StdErr:   "stderr",
			},
			code:    codepb.Code_INTERNAL,
			message: "failure",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			if tc.code != codepb.Code_OK {
				err = fmt.Errorf("%s", tc.message)
			}
			got := commandResult(context.Background(), tc.command, tc.result.StdOut, tc.result.StdErr, tc.code, tc.message, err)
			if got.GetCommand() != tc.command {
				t.Errorf("commandResult() command = %v, want %v", got.GetCommand(), tc.command)
			}
			if got.GetExitCode() != int32(tc.result.ExitCode) {
				t.Errorf("commandResult() exit code = %d, want %d", got.GetExitCode(), tc.result.ExitCode)
			}
			if got.GetStdout() != tc.result.StdOut {
				t.Errorf("commandResult() stdout = %s, want %s", got.GetStdout(), tc.result.StdOut)
			}
			if got.GetStderr() != tc.result.StdErr {
				t.Errorf("commandResult() stderr = %s, want %s", got.GetStderr(), tc.result.StdErr)
			}
			s := &spb.Status{}
			if err := anypb.UnmarshalTo(got.Payload, s, proto.UnmarshalOptions{}); err != nil {
				t.Fatalf("Failed to unmarshal payload: %v", err)
			}
			if s.Code != int32(tc.code) {
				t.Errorf("commandResult() status code = %d, want %d", s.Code, tc.code)
			}
			if s.Message != tc.message {
				t.Errorf("commandResult() status message = %s, want %s", s.Message, tc.message)
			}
		})
	}
}
