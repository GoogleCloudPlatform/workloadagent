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

package mysqlmetrics

import (
	"context"
	"errors"
	"testing"

	"github.com/GoogleCloudPlatform/sapagent/shared/commandlineexecutor"

	gcefake "github.com/GoogleCloudPlatform/sapagent/shared/gce/fake"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

func TestInitPassword(t *testing.T) {
	tests := []struct {
		name    string
		m       MySQLMetrics
		want    string
		wantErr bool
	}{
		{
			name:    "Default",
			m:       MySQLMetrics{},
			want:    "",
			wantErr: false,
		},
		{
			name: "GCEErr",
			m: MySQLMetrics{
				GCEService: &gcefake.TestGCE{
					GetSecretResp: []string{""},
					GetSecretErr:  []error{errors.New("fake-error")},
				},
				Config: &configpb.Configuration{
					MysqlConfiguration: &configpb.MySQLConfiguration{
						Secret: &configpb.SecretRef{ProjectId: "fake-project-id", SecretName: "fake-secret-name"},
					},
				},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "GCESecret",
			m: MySQLMetrics{
				GCEService: &gcefake.TestGCE{
					GetSecretResp: []string{"fake-password"},
					GetSecretErr:  []error{nil},
				},
				Config: &configpb.Configuration{
					MysqlConfiguration: &configpb.MySQLConfiguration{
						Secret: &configpb.SecretRef{ProjectId: "fake-project-id", SecretName: "fake-secret-name"},
					},
				},
			},
			want:    "fake-password",
			wantErr: false,
		},
		{
			name: "MissingProjectId",
			m: MySQLMetrics{
				GCEService: &gcefake.TestGCE{
					GetSecretResp: []string{"fake-password"},
					GetSecretErr:  []error{nil},
				},
				Config: &configpb.Configuration{
					MysqlConfiguration: &configpb.MySQLConfiguration{
						Secret: &configpb.SecretRef{SecretName: "fake-secret-name"},
					},
				},
			},
			want:    "",
			wantErr: false,
		},
		{
			name: "MissingSecretName",
			m: MySQLMetrics{
				GCEService: &gcefake.TestGCE{
					GetSecretResp: []string{"fake-password"},
					GetSecretErr:  []error{nil},
				},
				Config: &configpb.Configuration{
					MysqlConfiguration: &configpb.MySQLConfiguration{
						Secret: &configpb.SecretRef{ProjectId: "fake-project-id"},
					},
				},
			},
			want:    "",
			wantErr: false,
		},
		{
			name: "UsingPassword",
			m: MySQLMetrics{
				Config: &configpb.Configuration{
					MysqlConfiguration: &configpb.MySQLConfiguration{
						Password: "fake-password",
					},
				},
			},
			want:    "fake-password",
			wantErr: false,
		},
	}
	for _, tc := range tests {
		err := tc.m.initPassword(context.Background())
		if (err == nil && tc.wantErr) || (err != nil && !tc.wantErr) {
			t.Errorf("initPassword() = %v, wantErr %v", err, tc.wantErr)
		}
		got := tc.m.password.SecretValue()
		if got != tc.want {
			t.Errorf("initPassword() = %v, want %v", got, tc.want)
		}
	}
}

func TestBufferPoolSize(t *testing.T) {
	tests := []struct {
		name    string
		m       MySQLMetrics
		want    int
		wantErr bool
	}{
		{
			name: "HappyPath",
			m: MySQLMetrics{
				Execute: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{
						StdOut: "@@innodb_buffer_pool_size\n134217728\n",
					}
				},
			},
			want:    134217728,
			wantErr: false,
		},
		{
			name: "TooManyLines",
			m: MySQLMetrics{
				Execute: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{
						StdOut: "@@innodb_buffer_pool_size\n134217728\ntesttext\ntesttext\n",
					}
				},
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "TooFewLines",
			m: MySQLMetrics{
				Execute: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{
						StdOut: "testtext",
					}
				},
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "TooManyFields",
			m: MySQLMetrics{
				Execute: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{
						StdOut: "@@innodb_buffer_pool_size\n134217728 testtext testtext\n",
					}
				},
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "NonInt",
			m: MySQLMetrics{
				Execute: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{
						StdOut: "@@innodb_buffer_pool_size\ntesttext\n",
					}
				},
			},
			want:    0,
			wantErr: true,
		},
	}
	for _, tc := range tests {
		got, err := tc.m.bufferPoolSize(context.Background())
		if (err == nil && tc.wantErr) || (err != nil && !tc.wantErr) {
			t.Errorf("bufferPoolSize() = %v, wantErr %v", err, tc.wantErr)
		}
		if got != tc.want {
			t.Errorf("bufferPoolSize() = %v, want %v", got, tc.want)
		}
	}
}

func TestTotalRAM(t *testing.T) {
	tests := []struct {
		name    string
		m       MySQLMetrics
		want    int
		wantErr bool
	}{
		{
			name: "HappyPath",
			m: MySQLMetrics{
				Execute: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{
						StdOut: "MemTotal:        4025040 kB\n",
					}
				},
			},
			want:    4025040 * 1024,
			wantErr: false,
		},
		{
			name: "TooManyLines",
			m: MySQLMetrics{
				Execute: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{
						StdOut: "MemTotal:        4025040 kB\ntesttext\ntesttext\n",
					}
				},
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "TooManyFields",
			m: MySQLMetrics{
				Execute: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{
						StdOut: "MemTotal:        4025040 kB testtext testtext\n",
					}
				},
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "NonInt",
			m: MySQLMetrics{
				Execute: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{
						StdOut: "MemTotal:        testtext kB\n",
					}
				},
			},
			want:    0,
			wantErr: true,
		},
	}
	for _, tc := range tests {
		got, err := tc.m.totalRAM(context.Background())
		if (err == nil && tc.wantErr) || (err != nil && !tc.wantErr) {
			t.Errorf("totalRAM() = %v, wantErr %v", err, tc.wantErr)
		}
		if got != tc.want {
			t.Errorf("totalRAM() = %v, want %v", got, tc.want)
		}
	}
}
