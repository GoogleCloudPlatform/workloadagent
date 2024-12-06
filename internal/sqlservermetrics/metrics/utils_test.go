/*
Copyright 2024 Google LLC

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

package metrics

import (
	"context"
	"errors"
	"path"
	"strings"
	"testing"

	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/commandlineexecutor"
)

func TestConvertHexStringToBoolean(t *testing.T) {
	testcases := []struct {
		name     string
		input    string
		expected bool
		hasError bool
	}{
		{
			name:     "Return true for value 0x1",
			input:    "0x1",
			expected: true,
			hasError: false,
		},
		{
			name:     "Return false for other values",
			input:    "0x0",
			expected: false,
			hasError: false,
		},
		{
			name:     "invalid input returns false and error",
			input:    "invalid string",
			expected: false,
			hasError: true,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := convertHexStringToBoolean(tc.input)
			if tc.expected != actual || (err != nil) != tc.hasError {
				t.Errorf("convertHexStringToBoolean(%v) = %v, want: %v", tc.input, actual, tc.expected)
			}
		})
	}
}

func TestHandleNilFloat64(t *testing.T) {
	testcases := []struct {
		name     string
		input    any
		expected string
	}{
		{
			name:     "return float for no nil input",
			input:    1.0,
			expected: "1.000000",
		},
		{
			name:     "return 0 for 0 input",
			input:    float64(0),
			expected: "0.000000",
		},
		{
			name:     "return 0 for nil input",
			input:    nil,
			expected: "unknown",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			actual := handleNilFloat64(tc.input)
			if tc.expected != actual {
				t.Errorf("handleNilFloat64(%v) = %v, want: %v", tc.input, actual, tc.expected)
			}
		})
	}
}

func TestHandleNilInt(t *testing.T) {
	testcases := []struct {
		name     string
		input    any
		expected string
	}{
		{
			name:     "return int64 for no nil input",
			input:    int64(1),
			expected: "1",
		},
		{
			name:     "return 0 for 0 input",
			input:    int64(0),
			expected: "0",
		},
		{
			name:     "return unknown for nil input",
			input:    nil,
			expected: "unknown",
		},
		{
			name:     "return unknown for non-int input",
			input:    "test",
			expected: "unknown",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			actual := handleNilInt(tc.input)
			if tc.expected != actual {
				t.Errorf("handleNilInt64(%v) = %v, want: %v", tc.input, actual, tc.expected)
			}
		})
	}
}

func TestHandleNilString(t *testing.T) {
	testcases := []struct {
		name     string
		input    any
		expected string
	}{
		{
			name:     "return string for no nil input",
			input:    "test",
			expected: "test",
		},
		{
			name:     "return an emptry string for an emptry string input",
			input:    "",
			expected: "",
		},
		{
			name:     "return unknown for nil input",
			input:    nil,
			expected: "unknown",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			actual := handleNilString(tc.input)
			if tc.expected != actual {
				t.Errorf("handleNil(%v) = %v, want: %v", tc.input, actual, tc.expected)
			}
		})
	}
}

func TestHandleNilBoolean(t *testing.T) {
	testcases := []struct {
		name     string
		input    any
		expected string
	}{
		{
			name:     "return true for no nil input 'true'",
			input:    true,
			expected: "true",
		},
		{
			name:     "return false for no nil input 'false'",
			input:    false,
			expected: "false",
		},
		{
			name:     "return false for nil input",
			input:    nil,
			expected: "unknown",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			actual := handleNilBool(tc.input)
			if tc.expected != actual {
				t.Errorf("handleNil(%v) = %v, want: %v", tc.input, actual, tc.expected)
			}
		})
	}
}

func TestSaveToFile(t *testing.T) {
	tempFilePath := path.Join(t.TempDir(), "test.json")
	content := []byte("test")
	if err := SaveToFile(tempFilePath, content); err != nil {
		t.Errorf("SaveToFile() returned unexpected error %v", err)
	}
}

func TestPrettyStruct(t *testing.T) {

	type testStruct struct {
		TestField string
	}

	tests := []struct {
		name    string
		data    any
		want    string
		wantErr bool
	}{
		{
			name: "success",
			data: &testStruct{
				TestField: "test",
			},
			want: `{
    "TestField": "test"
}`,
		},
		{
			name:    "error",
			data:    make(chan int),
			wantErr: true,
		},
	}

	for _, tc := range tests {
		got, err := PrettyStruct(tc.data)
		if got != tc.want {
			t.Errorf("PrettyStruct() = %v, want: %v", got, tc.want)
		}

		if gotErr := err != nil; gotErr != tc.wantErr {
			t.Errorf("PrettyStruct() = %v, want error presence = %v", err, tc.wantErr)
			continue
		}

	}
}

func TestCommandLineExecutorWrapper(t *testing.T) {
	tests := []struct {
		executable  string
		argsToSplit string
		exec        commandlineexecutor.Execute
		hasError    bool
		want        string
	}{
		{
			executable:  "testFail",
			argsToSplit: " blah blah",
			exec:        commandlineexecutor.ExecuteCommand,
			hasError:    true,
			want:        "",
		},
		{
			executable: "testSuccess",
			exec: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
				return commandlineexecutor.Result{StdOut: "success"}
			},
			want: "success",
		},
		{
			executable:  "ls",
			argsToSplit: " 'blah ' blah",
			exec:        commandlineexecutor.ExecuteCommand,
			hasError:    true,
			want:        "",
		},
	}

	ctx := context.Background()

	for _, tc := range tests {
		got, err := CommandLineExecutorWrapper(ctx, tc.executable, tc.argsToSplit, tc.exec)
		if err != nil && !tc.hasError {
			t.Errorf("CommandLineExecutorWrapper(%v, %v, %v) returned an unexpected error: %v", tc.executable, tc.argsToSplit, tc.exec, err)
			continue
		}

		if got != tc.want {
			t.Errorf("CommandLineExecutorWrapper(%v, %v, %v) = %v, want: %v", tc.executable, tc.argsToSplit, tc.exec, got, tc.want)
		}
	}
}

func TestGetPhysicalDriveFromPath(t *testing.T) {
	tests := []struct {
		path     string
		windows  bool
		exec     commandlineexecutor.Execute
		hasError bool
		want     string
	}{
		// can't unit test success linux commands as the vm that the unit test spins up might not have the command available
		{
			path:    "experimental",
			windows: false,
			exec:    commandlineexecutor.ExecuteCommand,
			want:    "unknown",
		},
		{
			path:    "C:\\testPath\\testing",
			windows: true,
			exec:    commandlineexecutor.ExecuteCommand,
			want:    "C",
		},
		{
			path:    "",
			windows: false,
			exec:    commandlineexecutor.ExecuteCommand,
			want:    "unknown",
		},
		{
			path:    "testing",
			windows: true,
			exec:    commandlineexecutor.ExecuteCommand,
			want:    "unknown",
		},
		{
			path:    "test happy path linux",
			windows: false,
			exec: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				if strings.Contains(params.ArgsToSplit, "df") {
					return commandlineexecutor.Result{StdOut: "/"}
				} else if strings.Contains(params.ArgsToSplit, "mount") {
					return commandlineexecutor.Result{StdOut: "/dev/sda1 on / type"}
				}
				return commandlineexecutor.Result{StdOut: "success"}
			},
			want: "sda1",
		},
		{
			path:    "find file path failed",
			windows: false,
			exec: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				if strings.Contains(params.ArgsToSplit, "df") {
					return commandlineexecutor.Result{Error: errors.New("")}
				} else if strings.Contains(params.ArgsToSplit, "mount") {
					return commandlineexecutor.Result{StdOut: "/dev/sda1 on / type"}
				}
				return commandlineexecutor.Result{StdOut: "success"}
			},
			want: "unknown",
		},
		{
			path:    "find file path failed",
			windows: false,
			exec: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				if strings.Contains(params.ArgsToSplit, "df") {
					return commandlineexecutor.Result{StdOut: "/"}
				} else if strings.Contains(params.ArgsToSplit, "mount") {
					return commandlineexecutor.Result{Error: errors.New("")}
				}
				return commandlineexecutor.Result{StdOut: "success"}
			},
			want: "unknown",
		},
		{
			path:    "find file path failed",
			windows: false,
			exec: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				if strings.Contains(params.ArgsToSplit, "df") {
					return commandlineexecutor.Result{StdOut: "/"}
				} else if strings.Contains(params.ArgsToSplit, "mount") {
					return commandlineexecutor.Result{StdOut: "/dev/sda1 on / type"}
				}
				return commandlineexecutor.Result{StdOut: "success", Error: errors.New("")}
			},
			want: "unknown",
		},
	}

	ctx := context.Background()

	for _, tc := range tests {
		got := GetPhysicalDriveFromPath(ctx, tc.path, tc.windows, tc.exec)
		if got != tc.want {
			t.Errorf("GetPhysicalDriveFromPath(%v, %v) = %v, want: %v", tc.path, tc.windows, got, tc.want)
		}
	}
}

func TestIntegerToString(t *testing.T) {
	tests := []struct {
		num     any
		want    string
		wantErr bool
	}{
		{num: int(1), want: "1"},
		{num: int8(1), want: "1"},
		{num: int16(1), want: "1"},
		{num: int32(1), want: "1"},
		{num: int64(1), want: "1"},
		{num: uint(1), want: "1"},
		{num: uint8(1), want: "1"},
		{num: uint16(1), want: "1"},
		{num: uint32(1), want: "1"},
		{num: uint64(1), want: "1"},
		{num: []uint8{49}, want: "1"},
		{num: "test", wantErr: true},
	}
	for _, tc := range tests {
		got, err := integerToString(tc.num)
		if gotErr := err != nil; gotErr != tc.wantErr {
			t.Errorf("integerToString(%v) returned an unexpected error: %v, want error: %v", tc.num, err, tc.wantErr)
			continue
		}
		if got != tc.want {
			t.Errorf("integerToString(%v) = %v, want: %v", tc.num, got, tc.want)
		}
	}
}
