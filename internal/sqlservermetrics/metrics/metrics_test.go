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
		got := MasterRules[idx].Fields(tc.input)
		if diff := cmp.Diff(got, tc.want); diff != "" {
			t.Errorf("Fields() for rule %s returned wrong result (-got +want):\n%s", MasterRules[idx].Name, diff)
		}
	}
}
