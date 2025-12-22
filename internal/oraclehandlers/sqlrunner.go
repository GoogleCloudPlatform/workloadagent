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
	"path/filepath"
	"strings"

	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/commandlineexecutor"
)

var (
	// -LOGON: Attempts to log on just once, instead of re-prompting on error.
	// -SILENT: Suppresses the display of the SQLPlus banner, prompts, and echoing of commands.
	// -NOLOGINTIME: Don't display last successful login time.
	sqlPlusArgs = []string{"-LOGON", "-SILENT", "-NOLOGINTIME", "/", "as", "sysdba"}
	// sqlSettings is used to make parsing the command output easier.
	// -PAGESIZE: Sets the page size to 0 to get continuous output without page headers.
	// -LINESIZE: Prevents SQLPlus from wrapping long lines.
	// -LONG: When printing LONG/CLOB data, allow up to 50,000 characters to be shown.
	// -FEEDBACK: Suppresses the message like: 14 rows selected.
	// -ECHO: Disables echoing of commands.
	// -TERMOUT: Controls whether output is shown on the screen when executing scripts.
	// -HEADING: Hides column headers in query results.
	// -VERIFY: Disables the display of old/new values when substituting variables.
	sqlSettings = "SET PAGESIZE 0 LINESIZE 32000 LONG 50000 FEEDBACK OFF ECHO OFF TERMOUT ON HEADING OFF VERIFY OFF"
)

// runSQL executes the given SQL query and returns the result.
var runSQL = func(ctx context.Context, params map[string]string, query string, timeout int) (string, string, error) {
	oracleSID := params["oracle_sid"]
	oracleHome := params["oracle_home"]
	oracleUser := params["oracle_user"]
	// oracleUser is the OS user that the oracle software is installed as.
	// We execute sqlplus as this user to allow for OS authentication ("/ as sysdba").
	result := commandlineexecutor.ExecuteCommand(ctx, commandlineexecutor.Params{
		Executable: filepath.Join(oracleHome, "bin", "sqlplus"),
		Args:       sqlPlusArgs,
		// LD_LIBRARY_PATH is needed for older Oracle versions that do not set rpath.
		Env:     []string{"ORACLE_SID=" + oracleSID, "ORACLE_HOME=" + oracleHome, "LD_LIBRARY_PATH=" + filepath.Join(oracleHome, "lib")},
		User:    oracleUser,
		Stdin:   fmt.Sprintf("%s\n%s", sqlSettings, query),
		Timeout: timeout,
	})

	stdout := strings.TrimSpace(result.StdOut)
	stderr := result.StdErr

	if result.Error != nil {
		if result.ExitCode != 0 {
			return stdout, stderr, fmt.Errorf("sqlplus exited with code %d: %s", result.ExitCode, stderr)
		}
		return stdout, stderr, result.Error
	}
	return stdout, stderr, nil
}
