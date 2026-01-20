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
	"errors"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/commandlineexecutor"
)

func TestRunSQL(t *testing.T) {
	defaultParams := map[string]string{
		"oracle_sid":  "orcl",
		"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
		"oracle_user": "oracle",
	}

	tests := []struct {
		name           string
		params         map[string]string
		query          string
		timeout        int
		failOnSQLError bool
		mockResult     commandlineexecutor.Result
		wantStdout     string
		wantStderr     string
		wantErr        bool
		wantParams     *commandlineexecutor.Params
	}{
		{
			name:   "Success",
			params: defaultParams,
			query:  "SELECT 1 FROM DUAL;",
			mockResult: commandlineexecutor.Result{
				StdOut:   "1\n",
				ExitCode: 0,
			},
			wantStdout: "1",
			wantParams: &commandlineexecutor.Params{
				Executable: "/u01/app/oracle/product/19.3.0/dbhome_1/bin/sqlplus",
				Args:       []string{"-LOGON", "-SILENT", "-NOLOGINTIME", "/", "as", "sysdba"},
				Env:        []string{"ORACLE_SID=orcl", "ORACLE_HOME=/u01/app/oracle/product/19.3.0/dbhome_1", "LD_LIBRARY_PATH=/u01/app/oracle/product/19.3.0/dbhome_1/lib"},
				User:       "oracle",
				Stdin:      fmt.Sprintf("%s\n%s", sqlSettings, "SELECT 1 FROM DUAL;"),
				Timeout:    0,
			},
		},
		{
			name:           "SuccessWithFailOnSQLError",
			params:         defaultParams,
			query:          "SELECT 1 FROM DUAL;",
			failOnSQLError: true,
			mockResult: commandlineexecutor.Result{
				StdOut:   "1\n",
				ExitCode: 0,
			},
			wantStdout: "1",
			wantParams: &commandlineexecutor.Params{
				Executable: "/u01/app/oracle/product/19.3.0/dbhome_1/bin/sqlplus",
				Args:       []string{"-LOGON", "-SILENT", "-NOLOGINTIME", "/", "as", "sysdba"},
				Env:        []string{"ORACLE_SID=orcl", "ORACLE_HOME=/u01/app/oracle/product/19.3.0/dbhome_1", "LD_LIBRARY_PATH=/u01/app/oracle/product/19.3.0/dbhome_1/lib"},
				User:       "oracle",
				Stdin:      fmt.Sprintf("WHENEVER SQLERROR EXIT FAILURE\n%s\n%s", sqlSettings, "SELECT 1 FROM DUAL;"),
				Timeout:    0,
			},
		},
		{
			name:   "ExecutionError",
			params: defaultParams,
			query:  "SELECT 1 FROM DUAL;",
			mockResult: commandlineexecutor.Result{
				Error: errors.New("execution failed"),
			},
			wantErr: true,
		},
		{
			name:   "ExitCodeError",
			params: defaultParams,
			query:  "SELECT 1 FROM DUAL;",
			mockResult: commandlineexecutor.Result{
				ExitCode: 1,
				Error:    errors.New("exit code 1"),
				StdErr:   "ORA-12345",
			},
			wantStdout: "",
			wantStderr: "ORA-12345",
			wantErr:    true,
		},
		{
			name:    "TimeoutSet",
			params:  defaultParams,
			query:   "SELECT 1 FROM DUAL;",
			timeout: 120,
			mockResult: commandlineexecutor.Result{
				StdOut: "1",
			},
			wantStdout: "1",
			wantParams: &commandlineexecutor.Params{
				Executable: "/u01/app/oracle/product/19.3.0/dbhome_1/bin/sqlplus",
				Args:       []string{"-LOGON", "-SILENT", "-NOLOGINTIME", "/", "as", "sysdba"},
				Env:        []string{"ORACLE_SID=orcl", "ORACLE_HOME=/u01/app/oracle/product/19.3.0/dbhome_1", "LD_LIBRARY_PATH=/u01/app/oracle/product/19.3.0/dbhome_1/lib"},
				User:       "oracle",
				Stdin:      fmt.Sprintf("%s\n%s", sqlSettings, "SELECT 1 FROM DUAL;"),
				Timeout:    120,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			origExecuteCommand := executeCommand
			defer func() { executeCommand = origExecuteCommand }()

			var capturedParams commandlineexecutor.Params
			executeCommand = func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				capturedParams = params
				return tc.mockResult
			}

			gotStdout, gotStderr, err := runSQL(context.Background(), tc.params, tc.query, tc.timeout, tc.failOnSQLError)

			if (err != nil) != tc.wantErr {
				t.Errorf("runSQL() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if gotStdout != tc.wantStdout {
				t.Errorf("runSQL() gotStdout = %v, want %v", gotStdout, tc.wantStdout)
			}
			if gotStderr != tc.wantStderr {
				t.Errorf("runSQL() gotStderr = %v, want %v", gotStderr, tc.wantStderr)
			}
			if tc.wantParams != nil {
				if diff := cmp.Diff(*tc.wantParams, capturedParams); diff != "" {
					t.Errorf("runSQL() params mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}
