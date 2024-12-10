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

package sqlcollector

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestFields(t *testing.T) {
	testcases := []struct {
		name    string
		windows bool
		input   [][]any
		want    []map[string]string
	}{
		{
			name: "DB_LOG_DISK_SEPARATION",
			input: [][]any{
				{
					int64(0),
					"test_db_name",
					"C:\\test_physical_name",
					int64(0),
					int64(0),
					int64(0),
					true,
				},
			},
			want: []map[string]string{
				{
					"db_name":           "test_db_name",
					"filetype":          "0",
					"physical_name":     "C:\\test_physical_name",
					"physical_drive":    "unknown",
					"state":             "0",
					"size":              "0",
					"growth":            "0",
					"is_percent_growth": "true",
				},
			},
		},
		{
			name: "DB_MAX_PARALLELISM",
			input: [][]any{
				{
					int64(0),
				},
			},
			want: []map[string]string{
				{
					"maxDegreeOfParallelism": "0",
				},
			},
		},
		{
			name: "DB_TRANSACTION_LOG_HANDLING",
			input: [][]any{
				{
					"test_db_name",
					int64(0),
					int64(0),
					int64(0),
					int64(0),
				},
			},
			want: []map[string]string{
				{
					"db_name":                "test_db_name",
					"backup_age_in_hours":    "0",
					"backup_size":            "0",
					"compressed_backup_size": "0",
					"auto_growth":            "0",
				},
			},
		},
		{
			name: "DB_VIRTUAL_LOG_FILE_COUNT",
			input: [][]any{
				{
					"test_db_name",
					int64(0),
					float64(1.0),
					int64(0),
					float64(1.0),
				},
			},
			want: []map[string]string{
				{
					"db_name":               "test_db_name",
					"vlf_count":             "0",
					"vlf_size_in_mb":        "1.000000",
					"active_vlf_count":      "0",
					"active_vlf_size_in_mb": "1.000000",
				},
			},
		},
		{
			name: "DB_BUFFER_POOL_EXTENSION",
			input: [][]any{
				{
					"test_path",
					int64(0),
					int64(1),
				},
			},
			want: []map[string]string{
				{
					"path":       "test_path",
					"state":      "0",
					"size_in_kb": "1",
				},
			},
		},
		{
			name: "DB_MAX_SERVER_MEMORY",
			input: [][]any{
				{
					"test_name",
					int64(0),
					int64(0),
				},
			},
			want: []map[string]string{
				{
					"name":         "test_name",
					"value":        "0",
					"value_in_use": "0",
				},
			},
		},
		{
			name: "DB_INDEX_FRAGMENTATION",
			input: [][]any{
				{
					int64(0),
				},
			},
			want: []map[string]string{
				{
					"found_index_fragmentation": "0",
				},
			},
		},
		{
			name: "DB_TABLE_INDEX_COMPRESSION",
			input: [][]any{
				{
					int64(0),
				},
			},
			want: []map[string]string{
				{
					"numOfPartitionsWithCompressionEnabled": "0",
				},
			},
		},
		{
			name: "INSTANCE_METRICS",
			input: [][]any{
				{
					"test_product_version",
					"test_product_level",
					"test_edition",
					int64(0),
					int64(0),
					int64(0),
					int64(0),
					int64(0),
					int64(0),
					int64(0),
					"windows",
				},
			},
			want: []map[string]string{
				{
					"os":                 "windows",
					"product_version":    "test_product_version",
					"product_level":      "test_product_level",
					"edition":            "test_edition",
					"cpu_count":          "0",
					"hyperthread_ratio":  "0",
					"physical_memory_kb": "0",
					"virtual_memory_kb":  "0",
					"socket_count":       "0",
					"cores_per_socket":   "0",
					"numa_node_count":    "0",
				},
			},
		},
		{
			name: "DB_BACKUP_POLICY",
			input: [][]any{
				{
					int64(0),
				},
			},
			want: []map[string]string{
				{
					"max_backup_age": "0",
				},
			},
		},
	}
	for idx, tc := range testcases {
		got := SQLMetrics[idx].Fields(tc.input)
		if diff := cmp.Diff(got, tc.want); diff != "" {
			t.Errorf("Fields() for rule %s returned wrong result (-got +want):\n%s", SQLMetrics[idx].Name, diff)
		}
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
