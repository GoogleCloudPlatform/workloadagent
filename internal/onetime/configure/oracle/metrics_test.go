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

package oracle

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/configuration"
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime/configure/cliconfig"

	dpb "google.golang.org/protobuf/types/known/durationpb"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

func TestMetricsCommand(t *testing.T) {
	defaultFrequency := time.Duration(configuration.DefaultOracleMetricsFrequency)
	defaultMaxThreads := int64(configuration.DefaultOracleMetricsMaxThreads)
	defaultQueryTimeout := time.Duration(configuration.DefaultOracleMetricsQueryTimeout)
	tests := []struct {
		name    string
		args    string
		got     *cliconfig.Configure
		wantErr string
		want    *cliconfig.Configure
	}{
		{
			name: "UpdateAllFlags",
			args: "--frequency=10m --max-threads=10 --query-timeout=10s",
			got: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					OracleConfiguration: &cpb.OracleConfiguration{
						OracleMetrics: &cpb.OracleMetrics{
							Enabled:             proto.Bool(true),
							CollectionFrequency: dpb.New(defaultFrequency),
							MaxExecutionThreads: defaultMaxThreads,
							QueryTimeout:        dpb.New(defaultQueryTimeout),
						},
					},
				},
			},
			want: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					OracleConfiguration: &cpb.OracleConfiguration{
						OracleMetrics: &cpb.OracleMetrics{
							Enabled:             proto.Bool(true),
							CollectionFrequency: dpb.New(10 * time.Minute),
							MaxExecutionThreads: 10,
							QueryTimeout:        dpb.New(10 * time.Second),
						},
					},
				},
				OracleConfigModified: true,
			},
		},
		{
			name: "EnableMetricsEmptyConnectionParams",
			args: "--enabled",
			got: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					OracleConfiguration: &cpb.OracleConfiguration{
						OracleMetrics: &cpb.OracleMetrics{
							Enabled: proto.Bool(false),
						},
					},
				},
				OracleConfigModified: false,
			},
			want: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					OracleConfiguration: &cpb.OracleConfiguration{
						OracleMetrics: &cpb.OracleMetrics{
							Enabled: proto.Bool(false),
						},
					},
				},
				OracleConfigModified: true,
			},
		},
		{
			name: "EnableMetricsNonEmptyConnectionParams",
			args: "--enabled",
			got: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					OracleConfiguration: &cpb.OracleConfiguration{
						OracleMetrics: &cpb.OracleMetrics{
							Enabled: proto.Bool(false),
							ConnectionParameters: []*cpb.ConnectionParameters{
								&cpb.ConnectionParameters{},
							},
						},
					},
				},
				OracleConfigModified: false,
			},
			want: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					OracleConfiguration: &cpb.OracleConfiguration{
						OracleMetrics: &cpb.OracleMetrics{
							Enabled: proto.Bool(true),
							ConnectionParameters: []*cpb.ConnectionParameters{
								&cpb.ConnectionParameters{},
							},
						},
					},
				},
				OracleConfigModified: true,
			},
		},
		{
			name: "AddNewConnectionParams",
			args: "connection-add --username=test-user --host=127.0.0.1 --port=1521 --service-name=test-service --project-id=test-project --secret-name=test-secret",
			got: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					OracleConfiguration: &cpb.OracleConfiguration{
						OracleMetrics: &cpb.OracleMetrics{
							Enabled: proto.Bool(true),
						},
					},
				},
				OracleConfigModified: false,
			},
			want: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					OracleConfiguration: &cpb.OracleConfiguration{
						OracleMetrics: &cpb.OracleMetrics{
							Enabled: proto.Bool(true),
							ConnectionParameters: []*cpb.ConnectionParameters{
								&cpb.ConnectionParameters{
									Username:    "test-user",
									Host:        "127.0.0.1",
									Port:        1521,
									ServiceName: "test-service",
									Secret: &cpb.SecretRef{
										ProjectId:  "test-project",
										SecretName: "test-secret",
									},
								},
							},
						},
					},
				},
				OracleConfigModified: true,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// 'got' is the configuration that will be modified by the command.
			cmd := MetricsCommand(tc.got)
			// Set the args for the command.
			cmd.SetArgs(strings.Split(tc.args, " "))
			// Capture stdout to avoid printing during tests.
			cmd.SetOut(bytes.NewBufferString(""))
			// Execute the command.
			err := cmd.Execute()
			if err != nil && err.Error() != tc.wantErr {
				t.Errorf("MetricsCommand().Execute() = %v, want: %v", err, tc.wantErr)
			}

			// Compare the configurations.
			if diff := cmp.Diff(tc.want, tc.got, protocmp.Transform(), cmpopts.IgnoreUnexported(cliconfig.Configure{})); diff != "" {
				t.Errorf("MetricsCommand().Execute() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
