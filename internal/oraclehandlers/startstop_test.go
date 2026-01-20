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

func createMockRunSQL(queries map[string]*commandlineexecutor.Result) func(context.Context, map[string]string, string, int, bool) (string, string, error) {
	return func(ctx context.Context, params map[string]string, query string, timeout int, failOnSQLError bool) (string, string, error) {
		result, ok := queries[query]
		if !ok {
			return "", "", fmt.Errorf("unexpected query: %s", query)
		}
		if result.Error != nil {
			return result.StdOut, result.StdErr, result.Error
		}
		return result.StdOut, result.StdErr, nil
	}
}

func TestStopDatabase(t *testing.T) {
	tests := []struct {
		name          string
		params        map[string]string
		sqlQueries    map[string]*commandlineexecutor.Result
		wantErrorCode codepb.Code
	}{
		{
			name:          "InputParametersValidationFailure",
			params:        map[string]string{},
			wantErrorCode: codepb.Code_INVALID_ARGUMENT,
		},
		{
			name: "ShutdownImmediateSuccess",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"SELECT database_role FROM v$database;": &commandlineexecutor.Result{StdOut: "PRIMARY"},
				"SHUTDOWN IMMEDIATE":                    &commandlineexecutor.Result{StdOut: shutdownSuccess},
			},
			wantErrorCode: codepb.Code_OK,
		},
		{
			name: "ShutdownImmediateStandby",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"SELECT database_role FROM v$database;":                                       &commandlineexecutor.Result{StdOut: "PHYSICAL STANDBY"},
				"ALTER DATABASE RECOVER MANAGED STANDBY DATABASE CANCEL;\nSHUTDOWN IMMEDIATE": &commandlineexecutor.Result{StdOut: shutdownSuccess},
			},
			wantErrorCode: codepb.Code_OK,
		},
		{
			name: "ShutdownImmediateRoleCheckFail",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"SELECT database_role FROM v$database;": &commandlineexecutor.Result{ExitCode: 1, Error: fmt.Errorf("role check failed")},
				"SHUTDOWN IMMEDIATE":                    &commandlineexecutor.Result{StdOut: shutdownSuccess},
			},
			wantErrorCode: codepb.Code_OK,
		},
		{
			name: "ShutdownImmediateFail",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"SELECT database_role FROM v$database;": &commandlineexecutor.Result{StdOut: "PRIMARY"},
				"SHUTDOWN IMMEDIATE":                    &commandlineexecutor.Result{ExitCode: 1, Error: fmt.Errorf("shutdown failed")},
			},
			wantErrorCode: codepb.Code_FAILED_PRECONDITION,
		},
		{
			name: "ShutdownImmediateAlreadyDown",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"SELECT database_role FROM v$database;": &commandlineexecutor.Result{StdOut: "PRIMARY"},
				"SHUTDOWN IMMEDIATE":                    &commandlineexecutor.Result{StdOut: alreadyDown},
			},
			wantErrorCode: codepb.Code_OK,
		},
		{
			name: "ShutdownImmediateUnexpectedOutput",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"SELECT database_role FROM v$database;": &commandlineexecutor.Result{StdOut: "PRIMARY"},
				"SHUTDOWN IMMEDIATE":                    &commandlineexecutor.Result{StdOut: "Some unexpected output"},
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
						Command:    "oracle_stop_database",
						Parameters: tc.params,
					},
				},
			}
			result := StopDatabase(context.Background(), command, nil)

			s := &spb.Status{}
			if err := anypb.UnmarshalTo(result.Payload, s, proto.UnmarshalOptions{}); err != nil {
				t.Fatalf("Failed to unmarshal payload: %v", err)
			}
			if s.Code != int32(tc.wantErrorCode) {
				t.Errorf("StopDatabase() with params %v returned error code %d, want %d", tc.params, s.Code, tc.wantErrorCode)
			}
		})
	}
}

func TestStartDatabase(t *testing.T) {
	tests := []struct {
		name          string
		params        map[string]string
		sqlQueries    map[string]*commandlineexecutor.Result
		wantErrorCode codepb.Code
	}{
		{
			name:          "InputParametersValidationFailure",
			params:        map[string]string{},
			wantErrorCode: codepb.Code_INVALID_ARGUMENT,
		},
		{
			name: "DBAlreadyOpen",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP":                        &commandlineexecutor.Result{StdOut: alreadyRunning},
				"SELECT status FROM v$instance;": &commandlineexecutor.Result{StdOut: "OPEN"},
			},
			wantErrorCode: codepb.Code_OK,
		},
		{
			name: "DBAlreadyMountedOpenSucceeds",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP":                        &commandlineexecutor.Result{StdOut: alreadyRunning},
				"SELECT status FROM v$instance;": &commandlineexecutor.Result{StdOut: "MOUNTED"},
				"ALTER DATABASE OPEN;":           &commandlineexecutor.Result{StdOut: "something"},
			},
			wantErrorCode: codepb.Code_OK,
		},
		{
			name: "DBAlreadyMountedOpenFails",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP":                        &commandlineexecutor.Result{StdOut: alreadyRunning},
				"SELECT status FROM v$instance;": &commandlineexecutor.Result{StdOut: "MOUNTED"},
				"ALTER DATABASE OPEN;":           &commandlineexecutor.Result{ExitCode: 1, Error: fmt.Errorf("open failed")},
			},
			wantErrorCode: codepb.Code_FAILED_PRECONDITION,
		},
		{
			name: "StartupSuccess",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP": &commandlineexecutor.Result{StdOut: startupSuccess},
			},
			wantErrorCode: codepb.Code_OK,
		},
		{
			name: "RestrictedStartupSuccess",
			params: map[string]string{
				"oracle_sid":   "orcl",
				"oracle_home":  "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user":  "oracle",
				"startup_mode": "restricted",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP RESTRICT": &commandlineexecutor.Result{StdOut: startupSuccess},
			},
			wantErrorCode: codepb.Code_OK,
		},
		{
			name: "StartupFail",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP": &commandlineexecutor.Result{ExitCode: 1, Error: fmt.Errorf("startup failed")},
			},
			wantErrorCode: codepb.Code_FAILED_PRECONDITION,
		},
		{
			name: "StartupUnexpectedOutput",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP": &commandlineexecutor.Result{StdOut: "Some unexpected output"},
			},
			wantErrorCode: codepb.Code_FAILED_PRECONDITION,
		},
		{
			name: "StartupAlreadyRunningStatusCheckFail",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP":                        &commandlineexecutor.Result{StdOut: alreadyRunning},
				"SELECT status FROM v$instance;": &commandlineexecutor.Result{ExitCode: 1, Error: fmt.Errorf("status check failed")},
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
						Command:    "oracle_start_database",
						Parameters: tc.params,
					},
				},
			}
			result := StartDatabase(context.Background(), command, nil)

			s := &spb.Status{}
			if err := anypb.UnmarshalTo(result.Payload, s, proto.UnmarshalOptions{}); err != nil {
				t.Fatalf("Failed to unmarshal payload: %v", err)
			}
			if s.Code != int32(tc.wantErrorCode) {
				t.Errorf("StartDatabase() with params %v returned error code %d, want %d", tc.params, s.Code, tc.wantErrorCode)
			}
		})
	}
}
