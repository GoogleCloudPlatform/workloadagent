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
	"time"

	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/commandlineexecutor"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce"

	spb "google.golang.org/genproto/googleapis/rpc/status"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	gpb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/guestactions"
)

func getStatusMessage(res *gpb.CommandResult) string {
	if res.Payload == nil {
		return ""
	}
	status := &spb.Status{}
	if err := res.Payload.UnmarshalTo(status); err != nil {
		return ""
	}
	return status.Message
}

func createMockRunDGMGRL(commands map[string]*commandlineexecutor.Result) func(context.Context, map[string]string, string, string, string, string, time.Duration) (string, string, error) {
	return func(ctx context.Context, params map[string]string, dbUser, password, connectString, command string, timeout time.Duration) (string, string, error) {
		result, ok := commands[command]
		if !ok {
			return "", "", fmt.Errorf("unexpected command: %s", command)
		}
		if result.Error != nil {
			return result.StdOut, result.StdErr, result.Error
		}
		return result.StdOut, result.StdErr, nil
	}
}

func TestDataGuardSwitchover(t *testing.T) {
	tests := []struct {
		name                 string
		params               map[string]string
		dgmgrlCommands       map[string]*commandlineexecutor.Result
		wantExitCode         int32
		wantMessage          string
		config               *cpb.Configuration
		mockRetrievePassword func(context.Context, *gce.GCE, *cpb.ConnectionParameters) (string, error)
		mockNewGCEClient     func(context.Context) (*gce.GCE, error)
	}{
		{
			name: "Success",
			params: map[string]string{
				"oracle_sid":             "TESTSID",
				"oracle_home":            "/u01/app/oracle/product/19.0.0/dbhome_1",
				"oracle_user":            "oracle",
				"standby_db_unique_name": "TESTDB_S",
				"db_connect_string":      "TESTDB",
			},
			config: &cpb.Configuration{
				OracleConfiguration: &cpb.OracleConfiguration{
					OracleHandlers: &cpb.OracleConfiguration_OracleHandlers{
						ConnectionParameters: &cpb.ConnectionParameters{
							ServiceName: "TESTSID",
							Host:        "localhost",
							Port:        1521,
							Secret: &cpb.SecretRef{
								ProjectId:  "test_project",
								SecretName: "test_secret",
							},
						},
					},
				},
			},
			dgmgrlCommands: map[string]*commandlineexecutor.Result{
				"VALIDATE DATABASE 'TESTDB_S';": {StdOut: "Ready for Switchover: Yes"},
				"SWITCHOVER TO 'TESTDB_S';": {
					StdOut: `
Performing switchover NOW, please wait...
Switchover succeeded, new primary is "testdb_s"
`,
				},
			},
			mockRetrievePassword: func(ctx context.Context, client *gce.GCE, params *cpb.ConnectionParameters) (string, error) {
				return "password", nil
			},
			wantExitCode: 0,
			wantMessage:  "Switchover completed successfully",
		},
		{
			name: "Success_WithExtraSpaces",
			params: map[string]string{
				"oracle_sid":             "TESTSID",
				"oracle_home":            "/u01/app/oracle/product/19.0.0/dbhome_1",
				"oracle_user":            "oracle",
				"standby_db_unique_name": "TESTDB_S",
				"db_connect_string":      "TESTDB",
			},
			config: &cpb.Configuration{
				OracleConfiguration: &cpb.OracleConfiguration{
					OracleHandlers: &cpb.OracleConfiguration_OracleHandlers{
						ConnectionParameters: &cpb.ConnectionParameters{
							ServiceName: "TESTSID",
							Host:        "localhost",
							Port:        1521,
							Secret: &cpb.SecretRef{
								ProjectId:  "test_project",
								SecretName: "test_secret",
							},
						},
					},
				},
			},
			dgmgrlCommands: map[string]*commandlineexecutor.Result{
				"VALIDATE DATABASE 'TESTDB_S';": {StdOut: `
  Database Role:     Physical standby database
  Primary Database:  testdb

  Ready for Switchover:  Yes
  Ready for Failover:    Yes (Primary Running)
`},
				"SWITCHOVER TO 'TESTDB_S';": {
					StdOut: `
Performing switchover NOW, please wait...
Switchover succeeded, new primary is "testdb_s"
`,
				},
			},
			mockRetrievePassword: func(ctx context.Context, client *gce.GCE, params *cpb.ConnectionParameters) (string, error) {
				return "password", nil
			},
			wantExitCode: 0,
			wantMessage:  "Switchover completed successfully",
		},
		{
			name: "Success_AlreadyPrimary",
			params: map[string]string{
				"oracle_sid":             "TESTSID",
				"oracle_home":            "/u01/app/oracle/product/19.0.0/dbhome_1",
				"oracle_user":            "oracle",
				"standby_db_unique_name": "TESTDB_S",
				"db_connect_string":      "TESTDB",
			},
			config: &cpb.Configuration{
				OracleConfiguration: &cpb.OracleConfiguration{
					OracleHandlers: &cpb.OracleConfiguration_OracleHandlers{
						ConnectionParameters: &cpb.ConnectionParameters{
							ServiceName: "TESTSID",
							Host:        "localhost",
							Port:        1521,
							Secret: &cpb.SecretRef{
								ProjectId:  "test_project",
								SecretName: "test_secret",
							},
						},
					},
				},
			},
			dgmgrlCommands: map[string]*commandlineexecutor.Result{
				"VALIDATE DATABASE 'TESTDB_S';": {StdOut: `
  Database Role:     Primary database

  Ready for Switchover:  Yes
`},
				"SWITCHOVER TO 'TESTDB_S';": {
					StdOut: `
Performing switchover NOW, please wait...
Error: ORA-16558: database specified for switchover is not a standby database

Failed.
Unable to switchover, primary database is still "testdb_s"
`,
					ExitCode: 1,
					Error:    fmt.Errorf("dgmgrl exited with code 1"),
				},
			},
			mockRetrievePassword: func(ctx context.Context, client *gce.GCE, params *cpb.ConnectionParameters) (string, error) {
				return "password", nil
			},
			wantExitCode: 0,
			wantMessage:  "Switchover completed successfully",
		},

		{
			name: "Failure_ValidationFailed",
			params: map[string]string{
				"oracle_sid":             "TESTSID",
				"oracle_home":            "/u01/app/oracle/product/19.0.0/dbhome_1",
				"oracle_user":            "oracle",
				"standby_db_unique_name": "TESTDB_S",
				"db_connect_string":      "TESTDB",
			},
			config: &cpb.Configuration{
				OracleConfiguration: &cpb.OracleConfiguration{
					OracleHandlers: &cpb.OracleConfiguration_OracleHandlers{
						ConnectionParameters: &cpb.ConnectionParameters{
							ServiceName: "TESTSID",
							Host:        "localhost",
							Port:        1521,
							Secret: &cpb.SecretRef{
								ProjectId:  "test_project",
								SecretName: "test_secret",
							},
						},
					},
				},
			},
			dgmgrlCommands: map[string]*commandlineexecutor.Result{
				"VALIDATE DATABASE 'TESTDB_S';": {StdOut: "Ready for Switchover: No"},
			},
			mockRetrievePassword: func(ctx context.Context, client *gce.GCE, params *cpb.ConnectionParameters) (string, error) {
				return "password", nil
			},
			wantExitCode: 1,
			wantMessage:  "validation failed for standby database",
		},
		{
			name: "Failure_SwitchoverCommandFailed",
			params: map[string]string{
				"oracle_sid":             "TESTSID",
				"oracle_home":            "/u01/app/oracle/product/19.0.0/dbhome_1",
				"oracle_user":            "oracle",
				"standby_db_unique_name": "TESTDB_S",
				"db_connect_string":      "TESTDB",
			},
			config: &cpb.Configuration{
				OracleConfiguration: &cpb.OracleConfiguration{
					OracleHandlers: &cpb.OracleConfiguration_OracleHandlers{
						ConnectionParameters: &cpb.ConnectionParameters{
							ServiceName: "TESTSID",
							Host:        "localhost",
							Port:        1521,
							Secret: &cpb.SecretRef{
								ProjectId:  "test_project",
								SecretName: "test_secret",
							},
						},
					},
				},
			},
			dgmgrlCommands: map[string]*commandlineexecutor.Result{
				"VALIDATE DATABASE 'TESTDB_S';": {StdOut: "Ready for Switchover: Yes"},
				"SWITCHOVER TO 'TESTDB_S';": {
					StdOut: `
Performing switchover NOW, please wait...
ORA-01034: ORACLE not available
`,
				},
			},
			mockRetrievePassword: func(ctx context.Context, client *gce.GCE, params *cpb.ConnectionParameters) (string, error) {
				return "password", nil
			},
			wantExitCode: 1,
			wantMessage:  "switchover failed or status unknown",
		},
		{
			name: "MissingParams",
			params: map[string]string{
				"oracle_sid": "TESTSID",
			},
			wantExitCode: 1,
			wantMessage:  "Parameter oracle_home is missing",
		},
		{
			name: "Failure_NoConnectionParams",
			params: map[string]string{
				"oracle_sid":             "TESTSID",
				"oracle_home":            "/u01/app/oracle/product/19.0.0/dbhome_1",
				"oracle_user":            "oracle",
				"standby_db_unique_name": "TESTDB_S",
			},
			config: &cpb.Configuration{
				OracleConfiguration: &cpb.OracleConfiguration{},
			},
			wantExitCode: 1,
			wantMessage:  "oracle connection parameters are missing",
		},
		{
			name: "Failure_GCEClientCreation",
			params: map[string]string{
				"oracle_sid":             "TESTSID",
				"oracle_home":            "/u01/app/oracle/product/19.0.0/dbhome_1",
				"oracle_user":            "oracle",
				"standby_db_unique_name": "TESTDB_S",
			},
			config: &cpb.Configuration{
				OracleConfiguration: &cpb.OracleConfiguration{
					OracleHandlers: &cpb.OracleConfiguration_OracleHandlers{
						ConnectionParameters: &cpb.ConnectionParameters{
							ServiceName: "TESTSID",
							Host:        "localhost",
							Port:        1521,
							Secret: &cpb.SecretRef{
								ProjectId:  "test_project",
								SecretName: "test_secret",
							},
						},
					},
				},
			},
			mockNewGCEClient: func(ctx context.Context) (*gce.GCE, error) {
				return nil, fmt.Errorf("GCE client error")
			},
			wantExitCode: 1,
			wantMessage:  "Failed to create GCE client",
		},
		{
			name: "Failure_ValidationExecutionError",
			params: map[string]string{
				"oracle_sid":             "TESTSID",
				"oracle_home":            "/u01/app/oracle/product/19.0.0/dbhome_1",
				"oracle_user":            "oracle",
				"standby_db_unique_name": "TESTDB_S",
				"db_connect_string":      "TESTDB",
			},
			config: &cpb.Configuration{
				OracleConfiguration: &cpb.OracleConfiguration{
					OracleHandlers: &cpb.OracleConfiguration_OracleHandlers{
						ConnectionParameters: &cpb.ConnectionParameters{
							ServiceName: "TESTSID",
							Host:        "localhost",
							Port:        1521,
							Secret: &cpb.SecretRef{
								ProjectId:  "test_project",
								SecretName: "test_secret",
							},
						},
					},
				},
			},
			dgmgrlCommands: map[string]*commandlineexecutor.Result{
				"VALIDATE DATABASE 'TESTDB_S';": {Error: fmt.Errorf("execution error")},
			},
			mockRetrievePassword: func(ctx context.Context, client *gce.GCE, params *cpb.ConnectionParameters) (string, error) {
				return "password", nil
			},
			wantExitCode: 1,
			wantMessage:  "DGMGRL validation execution failed",
		},
		{
			name: "Failure_SwitchoverExecutionError",
			params: map[string]string{
				"oracle_sid":             "TESTSID",
				"oracle_home":            "/u01/app/oracle/product/19.0.0/dbhome_1",
				"oracle_user":            "oracle",
				"standby_db_unique_name": "TESTDB_S",
				"db_connect_string":      "TESTDB",
			},
			config: &cpb.Configuration{
				OracleConfiguration: &cpb.OracleConfiguration{
					OracleHandlers: &cpb.OracleConfiguration_OracleHandlers{
						ConnectionParameters: &cpb.ConnectionParameters{
							ServiceName: "TESTSID",
							Host:        "localhost",
							Port:        1521,
							Secret: &cpb.SecretRef{
								ProjectId:  "test_project",
								SecretName: "test_secret",
							},
						},
					},
				},
			},
			dgmgrlCommands: map[string]*commandlineexecutor.Result{
				"VALIDATE DATABASE 'TESTDB_S';": {StdOut: "Ready for Switchover: Yes"},
				"SWITCHOVER TO 'TESTDB_S';":     {Error: fmt.Errorf("execution error")},
			},
			mockRetrievePassword: func(ctx context.Context, client *gce.GCE, params *cpb.ConnectionParameters) (string, error) {
				return "password", nil
			},
			wantExitCode: 1,
			wantMessage:  "DGMGRL switchover execution failed",
		},
	}

	originalRunDGMGRL := runDGMGRL
	originalNewGCEClient := newGCEClient
	originalRetrievePassword := retrievePassword

	defer func() {
		runDGMGRL = originalRunDGMGRL
		newGCEClient = originalNewGCEClient
		retrievePassword = originalRetrievePassword
	}()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runDGMGRL = originalRunDGMGRL
			newGCEClient = originalNewGCEClient
			retrievePassword = originalRetrievePassword

			if tc.dgmgrlCommands != nil {
				runDGMGRL = createMockRunDGMGRL(tc.dgmgrlCommands)
			}
			newGCEClient = func(ctx context.Context) (*gce.GCE, error) {
				return nil, nil
			}
			if tc.mockNewGCEClient != nil {
				newGCEClient = tc.mockNewGCEClient
			}
			if tc.mockRetrievePassword != nil {
				retrievePassword = tc.mockRetrievePassword
			}

			command := &gpb.Command{
				CommandType: &gpb.Command_AgentCommand{
					AgentCommand: &gpb.AgentCommand{
						Command:    "oracle_data_guard_switchover",
						Parameters: tc.params,
					},
				},
			}
			handler := DataGuardSwitchover(tc.config)
			result := handler(t.Context(), command, nil)
			if result.GetExitCode() != tc.wantExitCode {
				t.Errorf("DataGuardSwitchover() exit code = %d, want %d", result.GetExitCode(), tc.wantExitCode)
			}
			gotMessage := getStatusMessage(result)
			if !strings.Contains(gotMessage, tc.wantMessage) {
				t.Errorf("DataGuardSwitchover() message = %q, want substring %q", gotMessage, tc.wantMessage)
			}
		})
	}
}

func TestEasyConnectString(t *testing.T) {
	tests := []struct {
		name   string
		params *cpb.ConnectionParameters
		want   string
	}{
		{
			name: "AllFieldsPresent",
			params: &cpb.ConnectionParameters{
				Host:        "localhost",
				Port:        1521,
				ServiceName: "orcl",
			},
			want: "//localhost:1521/orcl",
		},
		{
			name: "MissingHost",
			params: &cpb.ConnectionParameters{
				Port:        1521,
				ServiceName: "orcl",
			},
			want: "//:1521/orcl",
		},
		{
			name: "MissingPort",
			params: &cpb.ConnectionParameters{
				Host:        "localhost",
				ServiceName: "orcl",
			},
			want: "//localhost:0/orcl",
		},
		{
			name: "MissingServiceName",
			params: &cpb.ConnectionParameters{
				Host: "localhost",
				Port: 1521,
			},
			want: "//localhost:1521/",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := easyConnectString(tc.params)
			if got != tc.want {
				t.Errorf("easyConnectString() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRunDGMGRL(t *testing.T) {
	originalExecuteCommand := executeCommand
	defer func() { executeCommand = originalExecuteCommand }()

	tests := []struct {
		name          string
		commandResult commandlineexecutor.Result
		wantStdout    string
		wantStderr    string
		wantErr       bool
	}{
		{
			name: "Success",
			commandResult: commandlineexecutor.Result{
				StdOut:   "output",
				StdErr:   "",
				ExitCode: 0,
			},
			wantStdout: "output",
			wantStderr: "",
			wantErr:    false,
		},
		{
			name: "ExitCodeNonZero",
			commandResult: commandlineexecutor.Result{
				StdOut:   "output",
				StdErr:   "error",
				ExitCode: 1,
				Error:    fmt.Errorf("exit code 1"),
			},
			wantStdout: "output",
			wantStderr: "error",
			wantErr:    true,
		},
		{
			name: "ExecutionError",
			commandResult: commandlineexecutor.Result{
				StdOut: "",
				StdErr: "",
				Error:  fmt.Errorf("context deadline exceeded"),
			},
			wantStdout: "",
			wantStderr: "",
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			executeCommand = func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				if params.Executable != "/u01/app/oracle/product/19.0.0/dbhome_1/bin/dgmgrl" {
					t.Errorf("executeCommand executable = %q, want %q", params.Executable, "/u01/app/oracle/product/19.0.0/dbhome_1/bin/dgmgrl")
				}
				if params.User != "oracle" {
					t.Errorf("executeCommand user = %q, want %q", params.User, "oracle")
				}
				if !strings.Contains(params.Stdin, "CONNECT sys/\"password\"@\"//localhost:1521/orcl\"") {
					t.Errorf("executeCommand stdin = %q, want to contain connection string", params.Stdin)
				}
				return tc.commandResult
			}

			params := map[string]string{
				"oracle_home": "/u01/app/oracle/product/19.0.0/dbhome_1",
				"oracle_user": "oracle",
			}
			stdout, stderr, err := runDGMGRL(t.Context(), params, "sys", "password", "//localhost:1521/orcl", "VALIDATE DATABASE;", 10*time.Second)

			if (err != nil) != tc.wantErr {
				t.Errorf("runDGMGRL() error = %v, wantErr %v", err, tc.wantErr)
			}
			if stdout != tc.wantStdout {
				t.Errorf("runDGMGRL() stdout = %q, want %q", stdout, tc.wantStdout)
			}
			if stderr != tc.wantStderr {
				t.Errorf("runDGMGRL() stderr = %q, want %q", stderr, tc.wantStderr)
			}
		})
	}
}
