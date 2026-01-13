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

	"google.golang.org/protobuf/proto"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/commandlineexecutor"

	anypb "google.golang.org/protobuf/types/known/anypb"
	codepb "google.golang.org/genproto/googleapis/rpc/code"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	gpb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/guestactions"
)

func TestHealthCheck(t *testing.T) {
	tests := []struct {
		name          string
		params        map[string]string
		sqlQueries    map[string]*commandlineexecutor.Result
		wantErrorCode codepb.Code
	}{
		{
			name:          "input parameters validation failure",
			params:        map[string]string{},
			wantErrorCode: codepb.Code_INVALID_ARGUMENT,
		},
		{
			name: "health check success",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"SELECT 1 FROM dual;": &commandlineexecutor.Result{StdOut: "1"},
			},
			wantErrorCode: codepb.Code_OK,
		},
		{
			name: "health check fail with ORA- error",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"SELECT 1 FROM dual;": &commandlineexecutor.Result{StdOut: "ORA-01034: ORACLE not available"},
			},
			wantErrorCode: codepb.Code_FAILED_PRECONDITION,
		},
		{
			name: "health check fail with execution error",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"SELECT 1 FROM dual;": &commandlineexecutor.Result{ExitCode: 1, Error: fmt.Errorf("sqlplus execution failed")},
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
						Command:    "oracle_health_check",
						Parameters: tc.params,
					},
				},
			}
			result := HealthCheck(context.Background(), command, nil)

			s := &spb.Status{}
			if err := anypb.UnmarshalTo(result.Payload, s, proto.UnmarshalOptions{}); err != nil {
				t.Fatalf("Failed to unmarshal payload: %v", err)
			}
			if s.Code != int32(tc.wantErrorCode) {
				t.Errorf("HealthCheck() with params %v returned error code %d, want %d", tc.params, s.Code, tc.wantErrorCode)
			}
		})
	}
}
