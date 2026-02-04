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
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/DATA-DOG/go-sqlmock"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

type fakeGCEClient struct {
	getSecret func(ctx context.Context, projectID, secretName string) (string, error)
}

func (f *fakeGCEClient) GetSecret(ctx context.Context, projectID, secretName string) (string, error) {
	if f.getSecret != nil {
		return f.getSecret(ctx, projectID, secretName)
	}
	return "", errors.New("GetSecret not implemented")
}

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

func TestPingSQLServer(t *testing.T) {
	oldNewCollectorFunc := newCollectorFunc
	t.Cleanup(func() { newCollectorFunc = oldNewCollectorFunc })

	tests := []struct {
		name       string
		cfg        *cpb.SQLServerConfiguration
		gce        *fakeGCEClient
		cloudProps *cpb.CloudProperties
		wantErr    bool
		wantErrMsg string
		wantConn   string
	}{
		{
			name: "Success",
			cfg: &cpb.SQLServerConfiguration{
				CredentialConfigurations: []*cpb.SQLServerConfiguration_CredentialConfiguration{
					{
						ConnectionParameters: []*cpb.ConnectionParameters{
							{
								Host: "localhost",
								Port: 1433,
								Secret: &cpb.SecretRef{
									SecretName: "test-secret",
								},
							},
						},
					},
				},
			},
			gce: &fakeGCEClient{
				getSecret: func(ctx context.Context, projectID, secretName string) (string, error) {
					return "password", nil
				},
			},
			cloudProps: &cpb.CloudProperties{
				ProjectId: "test-project",
			},
			wantConn: "sqlserver://:password@localhost:1433?database=&encrypt=false",
			wantErr:  false,
		},
		{
			name: "SuccessWithNewlinePassword",
			cfg: &cpb.SQLServerConfiguration{
				CredentialConfigurations: []*cpb.SQLServerConfiguration_CredentialConfiguration{
					{
						ConnectionParameters: []*cpb.ConnectionParameters{
							{
								Host: "localhost",
								Port: 1433,
								Secret: &cpb.SecretRef{
									SecretName: "test-secret",
								},
							},
						},
					},
				},
			},
			gce: &fakeGCEClient{
				getSecret: func(ctx context.Context, projectID, secretName string) (string, error) {
					return "password\n", nil
				},
			},
			cloudProps: &cpb.CloudProperties{
				ProjectId: "test-project",
			},
			wantConn: "sqlserver://:password@localhost:1433?database=&encrypt=false",
			wantErr:  false,
		},
		{
			name: "ConnectionFailure",
			cfg: &cpb.SQLServerConfiguration{
				CredentialConfigurations: []*cpb.SQLServerConfiguration_CredentialConfiguration{
					{
						ConnectionParameters: []*cpb.ConnectionParameters{
							{
								Host: "localhost",
								Port: 1433,
								Secret: &cpb.SecretRef{
									SecretName: "test-secret",
								},
							},
						},
					},
				},
			},
			gce: &fakeGCEClient{
				getSecret: func(ctx context.Context, projectID, secretName string) (string, error) {
					return "password", nil
				},
			},
			cloudProps: &cpb.CloudProperties{
				ProjectId: "test-project",
			},
			wantErr:    true,
			wantErrMsg: "failed to ping SQL Server",
		},
		{
			name: "getSecretError",
			cfg: &cpb.SQLServerConfiguration{
				CredentialConfigurations: []*cpb.SQLServerConfiguration_CredentialConfiguration{
					{
						ConnectionParameters: []*cpb.ConnectionParameters{
							{
								Host: "localhost",
								Port: 1433,
								Secret: &cpb.SecretRef{
									SecretName: "test-secret",
								},
							},
						},
					},
				},
			},
			gce: &fakeGCEClient{
				getSecret: func(ctx context.Context, projectID, secretName string) (string, error) {
					return "", errors.New("getSecret error")
				},
			},
			cloudProps: &cpb.CloudProperties{
				ProjectId: "test-project",
			},
			wantErr:    true,
			wantErrMsg: "failed to get SQL Server password secret",
		},
		{
			name:       "noCredentialConfigurations",
			cfg:        &cpb.SQLServerConfiguration{},
			gce:        &fakeGCEClient{},
			cloudProps: &cpb.CloudProperties{},
			wantErr:    true,
			wantErrMsg: "no credential configurations",
		},
		{
			name: "noConnectionParameters",
			cfg: &cpb.SQLServerConfiguration{
				CredentialConfigurations: []*cpb.SQLServerConfiguration_CredentialConfiguration{
					{},
				},
			},
			gce:        &fakeGCEClient{},
			cloudProps: &cpb.CloudProperties{},
			wantErr:    true,
			wantErrMsg: "no connections configured",
		},
		{
			name: "fallbackProjectID",
			cfg: &cpb.SQLServerConfiguration{
				CredentialConfigurations: []*cpb.SQLServerConfiguration_CredentialConfiguration{
					{
						ConnectionParameters: []*cpb.ConnectionParameters{
							{
								Host: "localhost",
								Port: 1433,
								Secret: &cpb.SecretRef{
									SecretName: "test-secret",
									// ProjectId is empty
								},
							},
						},
					},
				},
			},
			gce: &fakeGCEClient{
				getSecret: func(ctx context.Context, projectID, secretName string) (string, error) {
					if projectID != "fallback-project" {
						return "", fmt.Errorf("unexpected projectID: %s", projectID)
					}
					return "password", nil
				},
			},
			cloudProps: &cpb.CloudProperties{
				ProjectId: "fallback-project",
			},
			wantErr:    true,
			wantErrMsg: "failed to ping SQL Server",
		},
		{
			name: "ExplicitProjectID",
			cfg: &cpb.SQLServerConfiguration{
				CredentialConfigurations: []*cpb.SQLServerConfiguration_CredentialConfiguration{
					{
						ConnectionParameters: []*cpb.ConnectionParameters{
							{
								Host: "localhost",
								Port: 1433,
								Secret: &cpb.SecretRef{
									SecretName: "test-secret",
									ProjectId:  "secret-project",
								},
							},
						},
					},
				},
			},
			gce: &fakeGCEClient{
				getSecret: func(ctx context.Context, projectID, secretName string) (string, error) {
					if projectID != "secret-project" {
						return "", fmt.Errorf("unexpected projectID: %s", projectID)
					}
					return "password", nil
				},
			},
			cloudProps: &cpb.CloudProperties{
				ProjectId: "cloud-project",
			},
			wantErr:    true,
			wantErrMsg: "failed to ping SQL Server",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "Success" || tc.name == "SuccessWithNewlinePassword" {
				db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
				if err != nil {
					t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
				}
				defer db.Close()
				mock.ExpectPing()

				newCollectorFunc = func(driver string, connString string, encrypt bool) (*V1, error) {
					if tc.wantConn != "" && connString != tc.wantConn {
						t.Errorf("newCollectorFunc got connString %q, want %q", connString, tc.wantConn)
					}
					return &V1{dbConn: db}, nil
				}
			} else {
				newCollectorFunc = oldNewCollectorFunc
			}

			ctx := context.Background()
			err := PingSQLServer(ctx, tc.cfg, tc.gce, tc.cloudProps)
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Errorf("PingSQLServer() returned error: %v, want error: %v", err, tc.wantErr)
			}
			if tc.wantErr && tc.wantErrMsg != "" && err != nil {
				if !strings.Contains(err.Error(), tc.wantErrMsg) {
					t.Errorf("PingSQLServer() error = %v, want error containing %q", err, tc.wantErrMsg)
				}
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
