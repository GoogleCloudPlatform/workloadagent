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
			name:          "Stop_Validation_Fail",
			params:        map[string]string{},
			wantErrorCode: codepb.Code_INVALID_ARGUMENT,
		},
		{
			name: "Stop_Primary_ShutdownImmediate_Success",
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
			name: "Stop_Standby_ShutdownImmediate_Success",
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
			name: "Stop_RoleCheckFail_Proceeds_Success",
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
			name: "Stop_ShutdownImmediate_Fail",
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
			name: "Stop_AlreadyDown_Success",
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
			name: "Stop_UnexpectedOutput_Fail",
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
			name:          "Start_Validation_Fail",
			params:        map[string]string{},
			wantErrorCode: codepb.Code_INVALID_ARGUMENT,
		},
		{
			name: "Start_Primary_AlreadyOpen_Success",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP MOUNT":                         &commandlineexecutor.Result{StdOut: alreadyRunning},
				"SELECT database_role FROM v$database;": &commandlineexecutor.Result{StdOut: "PRIMARY"},
				"SELECT open_mode FROM v$database;":     &commandlineexecutor.Result{StdOut: "READ WRITE"},
			},
			wantErrorCode: codepb.Code_OK,
		},
		{
			name: "Start_Primary_AlreadyMounted_Open_Success",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP MOUNT":                         &commandlineexecutor.Result{StdOut: alreadyRunning},
				"SELECT database_role FROM v$database;": &commandlineexecutor.Result{StdOut: "PRIMARY"},
				"SELECT open_mode FROM v$database;":     &commandlineexecutor.Result{StdOut: "MOUNTED"},
				"ALTER DATABASE OPEN;":                  &commandlineexecutor.Result{StdOut: "Database opened."},
			},
			wantErrorCode: codepb.Code_OK,
		},
		{
			name: "Start_Primary_AlreadyMounted_Open_Fail",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP MOUNT":                         &commandlineexecutor.Result{StdOut: alreadyRunning},
				"SELECT database_role FROM v$database;": &commandlineexecutor.Result{StdOut: "PRIMARY"},
				"SELECT open_mode FROM v$database;":     &commandlineexecutor.Result{StdOut: "MOUNTED"},
				"ALTER DATABASE OPEN;":                  &commandlineexecutor.Result{ExitCode: 1, Error: fmt.Errorf("open failed")},
			},
			wantErrorCode: codepb.Code_FAILED_PRECONDITION,
		},
		{
			name: "Start_Primary_MountAndOpen_Success",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP MOUNT":                         &commandlineexecutor.Result{StdOut: startupSuccess},
				"SELECT database_role FROM v$database;": &commandlineexecutor.Result{StdOut: "PRIMARY"},
				"SELECT open_mode FROM v$database;":     &commandlineexecutor.Result{StdOut: "MOUNTED"},
				"ALTER DATABASE OPEN;":                  &commandlineexecutor.Result{StdOut: "Database opened."},
			},
			wantErrorCode: codepb.Code_OK,
		},
		{
			name: "Start_Primary_MountAndOpenRestricted_Success",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
				"restrict":    "true",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP MOUNT RESTRICT":                &commandlineexecutor.Result{StdOut: startupSuccess},
				"SELECT database_role FROM v$database;": &commandlineexecutor.Result{StdOut: "PRIMARY"},
				"SELECT open_mode FROM v$database;":     &commandlineexecutor.Result{StdOut: "MOUNTED"},
				"ALTER DATABASE OPEN;":                  &commandlineexecutor.Result{StdOut: "Database opened."},
			},
			wantErrorCode: codepb.Code_OK,
		},
		{
			name: "Start_Command_Failure",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP MOUNT": &commandlineexecutor.Result{ExitCode: 1, Error: fmt.Errorf("startup failed")},
			},
			wantErrorCode: codepb.Code_FAILED_PRECONDITION,
		},
		{
			name: "Start_OracleError_Failure",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP MOUNT": &commandlineexecutor.Result{StdOut: "ORA-01234: some failure"},
			},
			wantErrorCode: codepb.Code_FAILED_PRECONDITION,
		},
		{
			name: "Start_AlreadyRunning_StatusCheck_Fail",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP MOUNT":                         &commandlineexecutor.Result{StdOut: alreadyRunning},
				"SELECT database_role FROM v$database;": &commandlineexecutor.Result{ExitCode: 1, Error: fmt.Errorf("status check failed")},
			},
			wantErrorCode: codepb.Code_FAILED_PRECONDITION,
		},
		{
			name: "Start_Standby_Standard_Mount_Success",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP MOUNT":                         &commandlineexecutor.Result{StdOut: startupSuccess},
				"SELECT database_role FROM v$database;": &commandlineexecutor.Result{StdOut: "PHYSICAL STANDBY"},
				"SELECT open_mode FROM v$database;":     &commandlineexecutor.Result{StdOut: "MOUNTED"},
				"ALTER DATABASE RECOVER MANAGED STANDBY DATABASE DISCONNECT FROM SESSION USING CURRENT LOGFILE;": &commandlineexecutor.Result{StdOut: ""},
			},
			wantErrorCode: codepb.Code_OK,
		},
		{
			name: "Start_Standby_ADG_MountAndOpenReadOnly_Success",
			params: map[string]string{
				"oracle_sid":           "orcl",
				"oracle_home":          "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user":          "oracle",
				"open_standby_for_adg": "true",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP MOUNT":                         &commandlineexecutor.Result{StdOut: startupSuccess},
				"SELECT database_role FROM v$database;": &commandlineexecutor.Result{StdOut: "PHYSICAL STANDBY"},
				"SELECT open_mode FROM v$database;":     &commandlineexecutor.Result{StdOut: "MOUNTED"},
				"ALTER DATABASE OPEN READ ONLY;":        &commandlineexecutor.Result{StdOut: "Database opened"},
				"ALTER DATABASE RECOVER MANAGED STANDBY DATABASE DISCONNECT FROM SESSION USING CURRENT LOGFILE;": &commandlineexecutor.Result{StdOut: ""},
			},
			wantErrorCode: codepb.Code_OK,
		},
		{
			name: "Start_Standby_ADG_AlreadyReadOnly_Success",
			params: map[string]string{
				"oracle_sid":           "orcl",
				"oracle_home":          "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user":          "oracle",
				"open_standby_for_adg": "true",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP MOUNT":                         &commandlineexecutor.Result{StdOut: startupSuccess},
				"SELECT database_role FROM v$database;": &commandlineexecutor.Result{StdOut: "PHYSICAL STANDBY"},
				"SELECT open_mode FROM v$database;":     &commandlineexecutor.Result{StdOut: "READ ONLY"},
				"ALTER DATABASE RECOVER MANAGED STANDBY DATABASE DISCONNECT FROM SESSION USING CURRENT LOGFILE;": &commandlineexecutor.Result{StdOut: ""},
			},
			wantErrorCode: codepb.Code_OK,
		},
		{
			name: "Start_Standby_Standard_AlreadyReadOnly_Success",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP MOUNT":                         &commandlineexecutor.Result{StdOut: startupSuccess},
				"SELECT database_role FROM v$database;": &commandlineexecutor.Result{StdOut: "PHYSICAL STANDBY"},
				"SELECT open_mode FROM v$database;":     &commandlineexecutor.Result{StdOut: "READ ONLY"},
				"ALTER DATABASE RECOVER MANAGED STANDBY DATABASE DISCONNECT FROM SESSION USING CURRENT LOGFILE;": &commandlineexecutor.Result{StdOut: ""},
			},
			wantErrorCode: codepb.Code_OK,
		},
		{
			name: "Start_Standby_Standard_AlreadyReadOnlyWithApply_RecoveryRunning_Success",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP MOUNT":                         &commandlineexecutor.Result{StdOut: startupSuccess},
				"SELECT database_role FROM v$database;": &commandlineexecutor.Result{StdOut: "PHYSICAL STANDBY"},
				"SELECT open_mode FROM v$database;":     &commandlineexecutor.Result{StdOut: "READ ONLY WITH APPLY"},
				"ALTER DATABASE RECOVER MANAGED STANDBY DATABASE DISCONNECT FROM SESSION USING CURRENT LOGFILE;": &commandlineexecutor.Result{StdOut: ""},
			},
			wantErrorCode: codepb.Code_OK,
		},
		{
			name: "Start_Standby_ADG_True_ReadOnlyWithApply_Success",
			params: map[string]string{
				"oracle_sid":           "orcl",
				"oracle_home":          "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user":          "oracle",
				"open_standby_for_adg": "true",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP MOUNT":                         &commandlineexecutor.Result{StdOut: startupSuccess},
				"SELECT database_role FROM v$database;": &commandlineexecutor.Result{StdOut: "PHYSICAL STANDBY"},
				"SELECT open_mode FROM v$database;":     &commandlineexecutor.Result{StdOut: "READ ONLY WITH APPLY"},
			},
			wantErrorCode: codepb.Code_OK,
		},
		{
			name: "Start_Standby_ADG_True_UnknownOpenMode_Fail",
			params: map[string]string{
				"oracle_sid":           "orcl",
				"oracle_home":          "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user":          "oracle",
				"open_standby_for_adg": "true",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP MOUNT":                         &commandlineexecutor.Result{StdOut: startupSuccess},
				"SELECT database_role FROM v$database;": &commandlineexecutor.Result{StdOut: "PHYSICAL STANDBY"},
				"SELECT open_mode FROM v$database;":     &commandlineexecutor.Result{StdOut: "UNKNOWN MODE"},
			},
			wantErrorCode: codepb.Code_FAILED_PRECONDITION,
		},
		{
			name: "Start_Standby_ADG_InvalidFlag_DefaultsToStandard_Success",
			params: map[string]string{
				"oracle_sid":           "orcl",
				"oracle_home":          "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user":          "oracle",
				"open_standby_for_adg": "INVALID",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP MOUNT":                         &commandlineexecutor.Result{StdOut: startupSuccess},
				"SELECT database_role FROM v$database;": &commandlineexecutor.Result{StdOut: "PHYSICAL STANDBY"},
				"SELECT open_mode FROM v$database;":     &commandlineexecutor.Result{StdOut: "MOUNTED"},
			},
			wantErrorCode: codepb.Code_FAILED_PRECONDITION,
		},
		{
			name: "Start_Standby_RecoveryAlreadyActive_Success",
			params: map[string]string{
				"oracle_sid":  "orcl",
				"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
				"oracle_user": "oracle",
			},
			sqlQueries: map[string]*commandlineexecutor.Result{
				"STARTUP MOUNT":                         &commandlineexecutor.Result{StdOut: startupSuccess},
				"SELECT database_role FROM v$database;": &commandlineexecutor.Result{StdOut: "PHYSICAL STANDBY"},
				"SELECT open_mode FROM v$database;":     &commandlineexecutor.Result{StdOut: "MOUNTED"},
				"ALTER DATABASE RECOVER MANAGED STANDBY DATABASE DISCONNECT FROM SESSION USING CURRENT LOGFILE;": &commandlineexecutor.Result{
					StdOut: "ORA-01153: an incompatible media recovery is active",
					Error:  fmt.Errorf("ORA-01153: an incompatible media recovery is active"),
				},
			},
			wantErrorCode: codepb.Code_OK,
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
