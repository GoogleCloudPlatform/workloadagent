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
	"os"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/commandlineexecutor"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"

	anypb "google.golang.org/protobuf/types/known/anypb"
	codepb "google.golang.org/genproto/googleapis/rpc/code"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	gpb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/guestactions"
)

func TestSetAutostartInOratab(t *testing.T) {
	tests := []struct {
		name         string
		initialFile  string
		targetSID    string
		enable       bool
		wantFile     string
		mockReadErr  error
		mockStatErr  error
		mockWriteErr error
		wantErr      bool
	}{
		{
			name:        "Enable",
			initialFile: "ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:N\n",
			targetSID:   "ORCL",
			enable:      true,
			wantFile:    "ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:Y\n",
			wantErr:     false,
		},
		{
			name:        "Disable",
			initialFile: "ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:Y\n",
			targetSID:   "ORCL",
			enable:      false,
			wantFile:    "ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:N\n",
			wantErr:     false,
		},
		{
			name:        "NoChange",
			initialFile: "ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:Y\n",
			targetSID:   "ORCL",
			enable:      true,
			wantFile:    "ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:Y\n",
			wantErr:     false,
		},
		{
			name:        "SIDNotFound",
			initialFile: "OTHER:/u01/app/oracle/product/19.0.0/dbhome_1:N\n",
			targetSID:   "ORCL",
			enable:      true,
			wantFile:    "OTHER:/u01/app/oracle/product/19.0.0/dbhome_1:N\n",
			wantErr:     false,
		},
		{
			name: "CommentsPreserved",
			initialFile: "# Header\n" +
				"ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:N\n",
			targetSID: "ORCL",
			enable:    true,
			wantFile: "# Header\n" +
				"ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:Y\n",
			wantErr: false,
		},
		{
			name:        "CommentedSIDIgnored",
			initialFile: "#ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:N\n",
			targetSID:   "ORCL",
			enable:      true,
			wantFile:    "#ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:N\n",
			wantErr:     false,
		},
		{
			name:        "LeadingWhitespace",
			initialFile: "   ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:N\n",
			targetSID:   "ORCL",
			enable:      true,
			wantFile:    "ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:Y\n",
			wantErr:     false,
		},
		{
			name:        "CommentedSIDWithWhitespace",
			initialFile: "   #ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:Y\n",
			targetSID:   "ORCL",
			enable:      false,
			wantFile:    "   #ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:Y\n",
			wantErr:     false,
		},
		{
			name:        "TargetSIDStartsWithHash",
			initialFile: "#ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:N\n",
			targetSID:   "#ORCL",
			enable:      true,
			wantFile:    "#ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:N\n",
			wantErr:     false,
		},
		{
			name:        "EmptyLinesPreserved",
			initialFile: "\nORCL:/u01/app/oracle/product/19.0.0/dbhome_1:N\n\n",
			targetSID:   "ORCL",
			enable:      true,
			wantFile:    "\nORCL:/u01/app/oracle/product/19.0.0/dbhome_1:Y\n\n",
			wantErr:     false,
		},
		{
			name:        "LineWithMissingFlag_Preserved",
			initialFile: "ORCL:/u01/app/oracle/product/19.0.0/dbhome_1\n",
			targetSID:   "ORCL",
			enable:      true,
			wantFile:    "ORCL:/u01/app/oracle/product/19.0.0/dbhome_1\n",
			wantErr:     false,
		},
		{
			name:        "ReadError",
			initialFile: "",
			mockReadErr: errors.New("read error"),
			wantErr:     true,
		},
		{
			name:        "StatError",
			initialFile: "ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:N\n",
			mockStatErr: errors.New("stat error"),
			wantErr:     true,
		},
		{
			name:         "WriteError",
			initialFile:  "ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:N\n",
			mockWriteErr: errors.New("write error"),
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mock osReadFile
			oldOsReadFile := osReadFile
			defer func() { osReadFile = oldOsReadFile }()
			osReadFile = func(name string) ([]byte, error) {
				if tt.mockReadErr != nil {
					return nil, tt.mockReadErr
				}
				return []byte(tt.initialFile), nil
			}

			// Mock osStat
			oldOsStat := osStat
			defer func() { osStat = oldOsStat }()
			osStat = func(name string) (os.FileInfo, error) {
				if tt.mockStatErr != nil {
					return nil, tt.mockStatErr
				}
				// Create a temporary file to get a valid FileInfo
				tmpFile, err := os.CreateTemp("", "mock_oratab")
				if err != nil {
					t.Fatalf("Failed to create temp file for mock stat: %v", err)
				}
				defer os.Remove(tmpFile.Name())
				return tmpFile.Stat()
			}

			// Mock osWriteFile
			oldOsWriteFile := osWriteFile
			defer func() { osWriteFile = oldOsWriteFile }()
			var capturedWrite []byte
			osWriteFile = func(name string, data []byte, perm os.FileMode) error {
				if tt.mockWriteErr != nil {
					return tt.mockWriteErr
				}
				capturedWrite = data
				return nil
			}

			err := setAutostartInOratab("/etc/oratab", tt.targetSID, tt.enable)
			if (err != nil) != tt.wantErr {
				t.Errorf("setAutostartInOratab() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && string(capturedWrite) != tt.wantFile {
				t.Errorf("setAutostartInOratab() wrote %q, want %q", string(capturedWrite), tt.wantFile)
			}
		})
	}
}

func TestIsAutostartEnabledInOratab(t *testing.T) {
	tests := []struct {
		name        string
		fileContent string
		targetSID   string
		readFileErr error
		want        bool
		wantErr     bool
	}{
		{
			name:        "Enabled",
			fileContent: "ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:Y\n",
			targetSID:   "ORCL",
			want:        true,
			wantErr:     false,
		},
		{
			name:        "Disabled",
			fileContent: "ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:N\n",
			targetSID:   "ORCL",
			want:        false,
			wantErr:     false,
		},
		{
			name:        "NotFound",
			fileContent: "OTHER:/u01/app/oracle/product/19.0.0/dbhome_1:Y\n",
			targetSID:   "ORCL",
			want:        false,
			wantErr:     false,
		},
		{
			name: "CommentsIgnored",
			fileContent: "# This is a comment\n" +
				"ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:Y\n",
			targetSID: "ORCL",
			want:      true,
			wantErr:   false,
		},
		{
			name:        "CommentedSIDIgnored",
			fileContent: "#ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:Y\n",
			targetSID:   "ORCL",
			want:        false,
			wantErr:     false,
		},
		{
			name:        "LeadingWhitespace",
			fileContent: "   ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:Y\n",
			targetSID:   "ORCL",
			want:        true,
			wantErr:     false,
		},
		{
			name:        "CommentedSIDWithWhitespace",
			fileContent: "   #ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:Y\n",
			targetSID:   "ORCL",
			want:        false,
			wantErr:     false,
		},
		{
			name:        "TargetSIDStartsWithHash",
			fileContent: "#ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:Y\n",
			targetSID:   "#ORCL",
			want:        false,
			wantErr:     false,
		},
		{
			name:        "EmptyLinesIgnored",
			fileContent: "\n\nORCL:/u01/app/oracle/product/19.0.0/dbhome_1:Y\n",
			targetSID:   "ORCL",
			want:        true,
			wantErr:     false,
		},
		{
			name:        "ReadError",
			fileContent: "",
			targetSID:   "ORCL",
			readFileErr: errors.New("read error"),
			want:        false,
			wantErr:     true,
		},
		{
			name:        "MalformedLine",
			fileContent: "ORCL:/u01/app/oracle/product/19.0.0/dbhome_1\n", // Missing flag
			targetSID:   "ORCL",
			want:        false,
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldOsReadFile := osReadFile
			defer func() { osReadFile = oldOsReadFile }()
			osReadFile = func(name string) ([]byte, error) {
				if tt.readFileErr != nil {
					return nil, tt.readFileErr
				}
				return []byte(tt.fileContent), nil
			}

			got, err := isAutostartEnabledInOratab("/etc/oratab", tt.targetSID)
			if (err != nil) != tt.wantErr {
				t.Errorf("isAutostartEnabledInOratab() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("isAutostartEnabledInOratab() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFindOracleFreeSystemdServiceName(t *testing.T) {
	tests := []struct {
		name       string
		mockOutput string
		mockExit   int
		mockError  string
		want       string
		wantErr    bool
	}{
		{
			name:       "ServiceFound",
			mockOutput: "oracle-free-23c.service loaded active running Oracle Database Free 23c",
			mockExit:   0,
			want:       "oracle-free-23c",
			wantErr:    false,
		},
		{
			name:       "NoServiceFound",
			mockOutput: "",
			mockExit:   0,
			want:       "",
			wantErr:    true,
		},
		{
			name:       "CommandFailed",
			mockOutput: "",
			mockExit:   1,
			mockError:  "command failed",
			want:       "",
			wantErr:    true,
		},
		{
			name:       "MultipleServicesFirstTaken",
			mockOutput: "oracle-free-23c.service loaded active running\noracle-free-21c.service loaded active running",
			mockExit:   0,
			want:       "oracle-free-23c",
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldExecuteCommand := executeCommand
			defer func() { executeCommand = oldExecuteCommand }()
			executeCommand = func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				return commandlineexecutor.Result{
					StdOut:   tt.mockOutput,
					StdErr:   tt.mockError,
					ExitCode: tt.mockExit,
				}
			}

			got, err := findOracleFreeSystemdServiceName(context.Background())
			if (err != nil) != tt.wantErr {
				t.Errorf("findOracleFreeSystemdServiceName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("findOracleFreeSystemdServiceName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEnableAutostart(t *testing.T) {
	defaultParams := map[string]string{
		"oracle_sid":     "ORCL",
		"oracle_home":    "/u01/app/oracle/product/19.0.0/dbhome_1",
		"oracle_user":    "oracle",
		"db_unique_name": "ORCL_SITE1",
	}

	tests := []struct {
		name             string
		mockCmds         map[string]commandlineexecutor.Result
		initialFile      string
		wantWriteContent string
		wantErr          bool
	}{
		{
			name: "OracleRestart_Success",
			mockCmds: map[string]commandlineexecutor.Result{
				"srvctl_config": {ExitCode: 0},
				"srvctl":        {ExitCode: 0},
			},
			wantErr: false,
		},
		{
			name: "OracleRestart_Failure",
			mockCmds: map[string]commandlineexecutor.Result{
				"srvctl_config": {ExitCode: 0},
				"srvctl":        {ExitCode: 1, StdErr: "srvctl failed"},
			},
			wantErr: true,
		},
		{
			name: "Oratab_Success",
			mockCmds: map[string]commandlineexecutor.Result{
				"srvctl_config":    {ExitCode: 1},
				"oracle-free-list": {ExitCode: 0, StdOut: ""},
			},
			initialFile:      "ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:N\n",
			wantWriteContent: "ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:Y\n",
			wantErr:          false,
		},
		{
			name: "SystemdFree_Success",
			mockCmds: map[string]commandlineexecutor.Result{
				"srvctl_config":    {ExitCode: 1},
				"oracle-free-list": {ExitCode: 0, StdOut: "oracle-free-23c.service"},
				"enable-service":   {ExitCode: 0},
			},
			wantErr: false,
		},
		{
			name: "SystemdFree_Failure",
			mockCmds: map[string]commandlineexecutor.Result{
				"srvctl_config":    {ExitCode: 1},
				"oracle-free-list": {ExitCode: 0, StdOut: "oracle-free-23c.service"},
				"enable-service":   {ExitCode: 1, StdErr: "systemctl failed"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mock executeCommand
			oldExecuteCommand := executeCommand
			defer func() { executeCommand = oldExecuteCommand }()
			executeCommand = func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				cmd := params.Executable
				args := strings.Join(params.Args, " ")

				if strings.HasSuffix(cmd, "srvctl") {
					if strings.Contains(args, "config database") {
						return tt.mockCmds["srvctl_config"]
					}
					return tt.mockCmds["srvctl"]
				}
				if cmd == "systemctl" {
					if strings.Contains(args, "list-units") && strings.Contains(args, "oracle-free") {
						return tt.mockCmds["oracle-free-list"]
					}
					if strings.Contains(args, "enable") {
						return tt.mockCmds["enable-service"]
					}
				}

				return commandlineexecutor.Result{ExitCode: 1, StdErr: "mock command not found: " + cmd + " " + args}
			}

			// Mock file system for oratab
			oldOsReadFile := osReadFile
			defer func() { osReadFile = oldOsReadFile }()
			osReadFile = func(name string) ([]byte, error) {
				return []byte(tt.initialFile), nil
			}

			oldOsStat := osStat
			defer func() { osStat = oldOsStat }()
			osStat = func(name string) (os.FileInfo, error) {
				tmpFile, _ := os.CreateTemp("", "mock_oratab")
				defer os.Remove(tmpFile.Name())
				return tmpFile.Stat()
			}

			oldOsWriteFile := osWriteFile
			defer func() { osWriteFile = oldOsWriteFile }()
			var capturedWrite []byte
			osWriteFile = func(name string, data []byte, perm os.FileMode) error {
				capturedWrite = data
				return nil
			}

			err := enableAutostart(context.Background(), log.CtxLogger(context.Background()), defaultParams)
			if (err != nil) != tt.wantErr {
				t.Errorf("enableAutostart() error = %v, wantErr %v", err, tt.wantErr)
			}

			if tt.wantWriteContent != "" {
				if string(capturedWrite) != tt.wantWriteContent {
					t.Errorf("enableAutostart() wrote %q, want %q", string(capturedWrite), tt.wantWriteContent)
				}
			}
		})
	}
}

func TestDisableAutostart(t *testing.T) {
	defaultParams := map[string]string{
		"oracle_sid":     "ORCL",
		"oracle_home":    "/u01/app/oracle/product/19.0.0/dbhome_1",
		"oracle_user":    "oracle",
		"db_unique_name": "ORCL_SITE1",
	}

	tests := []struct {
		name             string
		mockCmds         map[string]commandlineexecutor.Result
		initialFile      string
		wantWriteContent string
		wantErr          bool
	}{
		{
			name: "OracleRestart_Success",
			mockCmds: map[string]commandlineexecutor.Result{
				"srvctl_config": {ExitCode: 0}, // Oracle Restart detected
				"srvctl":        {ExitCode: 0}, // Disable success
			},
			wantErr: false,
		},
		{
			name: "OracleRestart_Failure",
			mockCmds: map[string]commandlineexecutor.Result{
				"srvctl_config": {ExitCode: 0},
				"srvctl":        {ExitCode: 1, StdErr: "srvctl failed"},
			},
			wantErr: true,
		},
		{
			name: "Oratab_Success",
			mockCmds: map[string]commandlineexecutor.Result{
				"srvctl_config":    {ExitCode: 1}, // No Oracle Restart
				"oracle-free-list": {ExitCode: 0, StdOut: ""},
			},
			initialFile:      "ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:Y\n",
			wantWriteContent: "ORCL:/u01/app/oracle/product/19.0.0/dbhome_1:N\n",
			wantErr:          false,
		},
		{
			name: "SystemdFree_Success",
			mockCmds: map[string]commandlineexecutor.Result{
				"srvctl_config":    {ExitCode: 1},
				"oracle-free-list": {ExitCode: 0, StdOut: "oracle-free-23c.service"},
				"disable-service":  {ExitCode: 0},
			},
			wantErr: false,
		},
		{
			name: "SystemdFree_Failure",
			mockCmds: map[string]commandlineexecutor.Result{
				"srvctl_config":    {ExitCode: 1},
				"oracle-free-list": {ExitCode: 0, StdOut: "oracle-free-23c.service"},
				"disable-service":  {ExitCode: 1, StdErr: "systemctl failed"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mock executeCommand
			oldExecuteCommand := executeCommand
			defer func() { executeCommand = oldExecuteCommand }()
			executeCommand = func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				cmd := params.Executable
				args := strings.Join(params.Args, " ")

				if strings.HasSuffix(cmd, "srvctl") {
					if strings.Contains(args, "config database") {
						return tt.mockCmds["srvctl_config"]
					}
					return tt.mockCmds["srvctl"]
				}
				if cmd == "systemctl" {
					if strings.Contains(args, "list-units") && strings.Contains(args, "oracle-free") {
						return tt.mockCmds["oracle-free-list"]
					}
					if strings.Contains(args, "disable") {
						return tt.mockCmds["disable-service"]
					}
				}

				return commandlineexecutor.Result{ExitCode: 1, StdErr: "mock command not found: " + cmd + " " + args}
			}

			// Mock file system for oratab
			oldOsReadFile := osReadFile
			defer func() { osReadFile = oldOsReadFile }()
			osReadFile = func(name string) ([]byte, error) {
				return []byte(tt.initialFile), nil
			}

			oldOsStat := osStat
			defer func() { osStat = oldOsStat }()
			osStat = func(name string) (os.FileInfo, error) {
				tmpFile, _ := os.CreateTemp("", "mock_oratab")
				defer os.Remove(tmpFile.Name())
				return tmpFile.Stat()
			}

			oldOsWriteFile := osWriteFile
			defer func() { osWriteFile = oldOsWriteFile }()
			var capturedWrite []byte
			osWriteFile = func(name string, data []byte, perm os.FileMode) error {
				capturedWrite = data
				return nil
			}

			err := disableAutostart(context.Background(), log.CtxLogger(context.Background()), defaultParams)
			if (err != nil) != tt.wantErr {
				t.Errorf("disableAutostart() error = %v, wantErr %v", err, tt.wantErr)
			}

			if tt.wantWriteContent != "" {
				if string(capturedWrite) != tt.wantWriteContent {
					t.Errorf("disableAutostart() wrote %q, want %q", string(capturedWrite), tt.wantWriteContent)
				}
			}
		})
	}
}

func TestDetectStartupMechanism(t *testing.T) {
	tests := []struct {
		name     string
		mockCmds map[string]commandlineexecutor.Result
		want     startupMechanism
		wantErr  bool
	}{
		{
			name: "OracleRestart_Srvctl",
			mockCmds: map[string]commandlineexecutor.Result{
				"srvctl_config": {ExitCode: 0},
			},
			want:    startupOracleRestart,
			wantErr: false,
		},
		{
			name: "Oratab_Default",
			mockCmds: map[string]commandlineexecutor.Result{
				"srvctl_config": {ExitCode: 1},
				"oracle-free":   {ExitCode: 0, StdOut: ""},
			},
			want:    startupOratab,
			wantErr: false,
		},
		{
			name: "SystemdFree",
			mockCmds: map[string]commandlineexecutor.Result{
				"srvctl_config": {ExitCode: 1},
				"oracle-free":   {ExitCode: 0, StdOut: "oracle-free-23c.service"},
			},
			want:    startupSystemdFree,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldExecuteCommand := executeCommand
			defer func() { executeCommand = oldExecuteCommand }()
			executeCommand = func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				cmd := params.Executable
				args := strings.Join(params.Args, " ")

				if strings.HasSuffix(cmd, "srvctl") {
					if strings.Contains(args, "config database") {
						return tt.mockCmds["srvctl_config"]
					}
				}
				if cmd == "systemctl" {
					if strings.Contains(args, "list-units") && strings.Contains(args, "oracle-free") {
						return tt.mockCmds["oracle-free"]
					}
				}
				return commandlineexecutor.Result{ExitCode: 1, StdErr: "mock command not found"}
			}

			got, err := detectStartupMechanism(context.Background(), "home", "user", "db")
			if (err != nil) != tt.wantErr {
				t.Errorf("detectStartupMechanism() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("detectStartupMechanism() = %v, want %v", got, tt.want)
			}
		})
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

func TestDisableAutostart_Exported(t *testing.T) {
	defaultParams := map[string]string{
		"oracle_sid":     "ORCL",
		"oracle_home":    "/u01/app/oracle/product/19.0.0/dbhome_1",
		"oracle_user":    "oracle",
		"db_unique_name": "ORCL_SITE1",
	}

	tests := []struct {
		name          string
		params        map[string]string
		commands      map[string]commandlineexecutor.Result
		wantErrorCode codepb.Code
	}{
		{
			name:          "Validation_Fail_MissingParams",
			params:        map[string]string{},
			wantErrorCode: codepb.Code_INVALID_ARGUMENT,
		},
		{
			name:   "Success_OracleRestart",
			params: defaultParams,
			commands: map[string]commandlineexecutor.Result{
				"srvctl_config": {ExitCode: 0},
				"srvctl":        {ExitCode: 0},
			},
			wantErrorCode: codepb.Code_OK,
		},
		{
			name:   "Failure_OracleRestart_SrvctlFail",
			params: defaultParams,
			commands: map[string]commandlineexecutor.Result{
				"srvctl_config": {ExitCode: 0},
				"srvctl":        {ExitCode: 1, StdErr: "srvctl failed"},
			},
			wantErrorCode: codepb.Code_INTERNAL,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oldExecuteCommand := executeCommand
			defer func() { executeCommand = oldExecuteCommand }()
			executeCommand = func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				cmd := params.Executable
				args := strings.Join(params.Args, " ")
				if strings.HasSuffix(cmd, "srvctl") {
					if strings.Contains(args, "config database") {
						return tc.commands["srvctl_config"]
					}
					return tc.commands["srvctl"]
				}
				return commandlineexecutor.Result{ExitCode: 1, StdErr: "mock command not found: " + cmd + " " + args}
			}

			command := &gpb.Command{
				CommandType: &gpb.Command_AgentCommand{
					AgentCommand: &gpb.AgentCommand{
						Command:    "oracle_disable_autostart",
						Parameters: tc.params,
					},
				},
			}
			result := DisableAutostart(context.Background(), command, nil)

			s := &spb.Status{}
			if err := anypb.UnmarshalTo(result.Payload, s, proto.UnmarshalOptions{}); err != nil {
				t.Fatalf("Failed to unmarshal payload: %v", err)
			}
			if s.Code != int32(tc.wantErrorCode) {
				t.Errorf("DisableAutostart() with params %v returned error code %d, want %d", tc.params, s.Code, tc.wantErrorCode)
			}
		})
	}
}

func TestEnableAutostart_Exported(t *testing.T) {
	defaultParams := map[string]string{
		"oracle_sid":     "ORCL",
		"oracle_home":    "/u01/app/oracle/product/19.0.0/dbhome_1",
		"oracle_user":    "oracle",
		"db_unique_name": "ORCL_SITE1",
	}

	tests := []struct {
		name          string
		params        map[string]string
		commands      map[string]commandlineexecutor.Result
		wantErrorCode codepb.Code
	}{
		{
			name:          "Validation_Fail_MissingParams",
			params:        map[string]string{},
			wantErrorCode: codepb.Code_INVALID_ARGUMENT,
		},
		{
			name:   "Success_OracleRestart",
			params: defaultParams,
			commands: map[string]commandlineexecutor.Result{
				"srvctl_config": {ExitCode: 0},
				"srvctl":        {ExitCode: 0},
			},
			wantErrorCode: codepb.Code_OK,
		},
		{
			name:   "Failure_OracleRestart_SrvctlFail",
			params: defaultParams,
			commands: map[string]commandlineexecutor.Result{
				"srvctl_config": {ExitCode: 0},
				"srvctl":        {ExitCode: 1, StdErr: "srvctl failed"},
			},
			wantErrorCode: codepb.Code_INTERNAL,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oldExecuteCommand := executeCommand
			defer func() { executeCommand = oldExecuteCommand }()
			executeCommand = func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				cmd := params.Executable
				args := strings.Join(params.Args, " ")
				if strings.HasSuffix(cmd, "srvctl") {
					if strings.Contains(args, "config database") {
						return tc.commands["srvctl_config"]
					}
					return tc.commands["srvctl"]
				}
				return commandlineexecutor.Result{ExitCode: 1, StdErr: "mock command not found: " + cmd + " " + args}
			}

			command := &gpb.Command{
				CommandType: &gpb.Command_AgentCommand{
					AgentCommand: &gpb.AgentCommand{
						Command:    "oracle_enable_autostart",
						Parameters: tc.params,
					},
				},
			}
			result := EnableAutostart(context.Background(), command, nil)

			s := &spb.Status{}
			if err := anypb.UnmarshalTo(result.Payload, s, proto.UnmarshalOptions{}); err != nil {
				t.Fatalf("Failed to unmarshal payload: %v", err)
			}
			if s.Code != int32(tc.wantErrorCode) {
				t.Errorf("EnableAutostart() with params %v returned error code %d, want %d", tc.params, s.Code, tc.wantErrorCode)
			}
		})
	}
}
