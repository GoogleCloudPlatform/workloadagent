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

	anypb "google.golang.org/protobuf/types/known/anypb"
	codepb "google.golang.org/genproto/googleapis/rpc/code"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/commandlineexecutor"
	gpb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/guestactions"
)

func createMockRunSQL(queries map[string]*commandlineexecutor.Result) func(context.Context, map[string]string, string) (string, string, error) {
	return func(ctx context.Context, params map[string]string, query string) (string, string, error) {
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
			name:          "input parameters validation failure",
			params:        map[string]string{},
			wantErrorCode: codepb.Code_INVALID_ARGUMENT,
		},
		{
			name: "shutdown immediate success",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"SHUTDOWN IMMEDIATE": &commandlineexecutor.Result{StdOut: shutdownSuccess},
			},
			wantErrorCode: codepb.Code_OK,
		},
		{
			name: "shutdown immediate fail",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"SHUTDOWN IMMEDIATE": &commandlineexecutor.Result{ExitCode: 1, Error: fmt.Errorf("shutdown failed")},
			},
			wantErrorCode: codepb.Code_FAILED_PRECONDITION,
		},
		{
			name: "shutdown immediate already down",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"SHUTDOWN IMMEDIATE": &commandlineexecutor.Result{StdOut: alreadyDown},
			},
			wantErrorCode: codepb.Code_OK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			origRunSQL := runSQL
			h := New()
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
			result := h.StopDatabase(context.Background(), command, nil)

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

func TestStopDatabaseLocked(t *testing.T) {
	h := New()
	sid := "locked_sid"
	params := map[string]string{
		"oracle_sid":  sid,
		"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
		"oracle_user": "oracle",
	}
	command := &gpb.Command{
		CommandType: &gpb.Command_AgentCommand{
			AgentCommand: &gpb.AgentCommand{
				Command:    "oracle_stop_database",
				Parameters: params,
			},
		},
	}

	origRunSQL := runSQL
	defer func() { runSQL = origRunSQL }()

	runSQLBlocked := make(chan bool)
	unblockRunSQL := make(chan bool)

	runSQL = func(ctx context.Context, params map[string]string, query string) (string, string, error) {
		runSQLBlocked <- true
		<-unblockRunSQL
		return shutdownSuccess, "", nil
	}

	// Start the first operation in a goroutine.
	go h.StopDatabase(context.Background(), command, nil)

	// Wait until the first operation has locked the DB and is blocked in runSQL.
	<-runSQLBlocked

	// Attempt to start a second operation on the same SID.
	result := h.StopDatabase(context.Background(), command, nil)

	// Unblock the first operation.
	unblockRunSQL <- true

	s := &spb.Status{}
	if err := anypb.UnmarshalTo(result.Payload, s, proto.UnmarshalOptions{}); err != nil {
		t.Fatalf("Failed to unmarshal payload: %v", err)
	}
	if s.Code != int32(codepb.Code_ABORTED) {
		t.Errorf("StopDatabase() returned error code %d, want %d", s.Code, codepb.Code_ABORTED)
	}
	if !strings.Contains(s.Message, `"oracle_stop_database"`) {
		t.Errorf("StopDatabase() returned message %q, want it to contain %q", s.Message, `"oracle_stop_database"`)
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
			name:          "input parameters validation failure",
			params:        map[string]string{},
			wantErrorCode: codepb.Code_INVALID_ARGUMENT,
		},
		{
			name: "db already open",
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
			name: "db already mounted, open succeeds",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP":                        &commandlineexecutor.Result{StdOut: alreadyRunning},
				"SELECT status FROM v$instance;": &commandlineexecutor.Result{StdOut: "MOUNTED"},
				"WHENEVER SQLERROR EXIT FAILURE\nALTER DATABASE OPEN": &commandlineexecutor.Result{StdOut: "something"},
			},
			wantErrorCode: codepb.Code_OK,
		},
		{
			name: "db already mounted, open fails",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP":                        &commandlineexecutor.Result{StdOut: alreadyRunning},
				"SELECT status FROM v$instance;": &commandlineexecutor.Result{StdOut: "MOUNTED"},
				"WHENEVER SQLERROR EXIT FAILURE\nALTER DATABASE OPEN": &commandlineexecutor.Result{ExitCode: 1, Error: fmt.Errorf("open failed")},
			},
			wantErrorCode: codepb.Code_FAILED_PRECONDITION,
		},
		{
			name: "startup success",
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
			name: "restricted startup success",
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
			name: "startup fail",
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			origRunSQL := runSQL
			h := New()
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
			result := h.StartDatabase(context.Background(), command, nil)

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

func TestStartDatabaseLocked(t *testing.T) {
	h := New()
	sid := "locked_sid"
	params := map[string]string{
		"oracle_sid":  sid,
		"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
		"oracle_user": "oracle",
	}
	command := &gpb.Command{
		CommandType: &gpb.Command_AgentCommand{
			AgentCommand: &gpb.AgentCommand{
				Command:    "oracle_start_database",
				Parameters: params,
			},
		},
	}

	origRunSQL := runSQL
	defer func() { runSQL = origRunSQL }()

	runSQLBlocked := make(chan bool)
	unblockRunSQL := make(chan bool)

	runSQL = func(ctx context.Context, params map[string]string, query string) (string, string, error) {
		runSQLBlocked <- true // Signal that runSQL has been reached
		<-unblockRunSQL       // Pause here until signaled to continue
		return startupSuccess, "", nil
	}

	// Start the first operation in a goroutine.
	go h.StartDatabase(context.Background(), command, nil)

	// Wait until the first operation has locked the DB and is blocked in runSQL.
	<-runSQLBlocked

	// Attempt to start a second operation on the same SID.
	result := h.StartDatabase(context.Background(), command, nil)

	// Unblock the first operation.
	unblockRunSQL <- true

	s := &spb.Status{}
	if err := anypb.UnmarshalTo(result.Payload, s, proto.UnmarshalOptions{}); err != nil {
		t.Fatalf("Failed to unmarshal payload: %v", err)
	}
	if s.Code != int32(codepb.Code_ABORTED) {
		t.Errorf("StartDatabase() returned error code %d, want %d", s.Code, codepb.Code_ABORTED)
	}
	if !strings.Contains(s.Message, `"oracle_start_database"`) {
		t.Errorf("StartDatabase() returned message %q, want it to contain %q", s.Message, `"oracle_start_database"`)
	}
}
