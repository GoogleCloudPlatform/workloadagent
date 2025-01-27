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

package workloadmanager

import (
	"context"
	"embed"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/configuration"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"

	wlmfake "github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/gce/fake"
	dwpb "github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/protos/datawarehouse"
)

var (
	filePath = "test_data/metricoverride.yaml"
	//go:embed test_data/metricoverride.yaml
	testFS            embed.FS
	DefaultTestReader = ConfigFileReader(func(path string) (io.ReadCloser, error) {
		file, err := testFS.Open(filePath)
		var f io.ReadCloser = file
		return f, err
	})
	DefaultTestReaderErr = ConfigFileReader(func(path string) (io.ReadCloser, error) {
		return nil, errors.New("failed to read file")
	})
)

func TestIsMetricOverrideYamlFileExists(t *testing.T) {
	tests := []struct {
		name     string
		filePath string // Not really used, but keeping it for consistency
		reader   ConfigFileReader
		want     bool
	}{
		{
			name:     "File exists and is readable",
			filePath: filePath,
			reader:   DefaultTestReader,
			want:     true,
		},
		{
			name:     "File read error",
			filePath: "some_file.yaml",
			reader:   DefaultTestReaderErr,
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			got := readAndLogMetricOverrideYAML(ctx, tt.reader)

			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("readAndLogMetricOverrideYAML returned unexpected output diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCollectOverrideMetrics(t *testing.T) {
	tests := []struct {
		name     string
		filePath string
		reader   ConfigFileReader
		want     []WorkloadMetrics
	}{
		{
			name:     "Empty file",
			filePath: "",
			reader: ConfigFileReader(func(data string) (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader(data)), nil
			}),
			want: []WorkloadMetrics{
				{Metrics: map[string]string{}},
			},
		},
		{
			name:     "File exists and is readable",
			filePath: filePath,
			reader:   DefaultTestReader,
			want: []WorkloadMetrics{
				{
					WorkloadType: MYSQL,
					Metrics: map[string]string{
						"agent":         "workloadagent",
						"agent_version": "3.2",
						"gcloud":        "false",
						"instance_name": "fake-wlmmetrics-1",
						"metric_value":  "1",
						"os":            "rhel-8.4",
						"version":       "1.0.0",
					},
				},
				{
					WorkloadType: REDIS,
					Metrics:      map[string]string{"instance_name": "fake-wlmmetrics-1", "metric_value": "1"},
				},
			},
		},
		{
			name:     "File read error",
			filePath: "some_file.yaml",
			reader:   DefaultTestReaderErr,
			want:     []WorkloadMetrics{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			got := collectOverrideMetrics(ctx, tt.reader)

			if diff := cmp.Diff(tt.want, got, cmp.AllowUnexported(WorkloadMetrics{})); diff != "" {
				t.Errorf("collectOverrideMetrics returned unexpected metrics diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSendMetricsToDataWarehouse(t *testing.T) {
	tests := []struct {
		name          string
		wlmService    *wlmfake.TestWLM
		params        sendMetricsParams
		wantCallCount int
		wantArgs      []wlmfake.WriteInsightArgs
	}{
		{
			name: "Success",
			wlmService: &wlmfake.TestWLM{
				T: t,
				WriteInsightArgs: []wlmfake.WriteInsightArgs{
					{
						Project:  "test-project-id",
						Location: "us-central1",
						Req: &dwpb.WriteInsightRequest{
							AgentVersion: configuration.AgentVersion,
							Insight: &dwpb.Insight{
								InstanceId: "test-instance-id",
								TorsoValidation: &dwpb.TorsoValidation{
									WorkloadType:      dwpb.TorsoValidation_MYSQL,
									ValidationDetails: map[string]string{"instance_name": "fake-wlmmetrics-1", "metric_value": "1"},
									ProjectId:         "test-project-id",
									InstanceName:      "test-instance-name",
								},
							},
						},
					},
				},
				WriteInsightErrs: []error{nil},
			},
			params: sendMetricsParams{
				wm: []WorkloadMetrics{
					{
						WorkloadType: MYSQL,
						Metrics:      map[string]string{"instance_name": "fake-wlmmetrics-1", "metric_value": "1"},
					},
				},
				cp: &cpb.CloudProperties{
					ProjectId:    "test-project-id",
					Region:       "us-central1",
					InstanceId:   "test-instance-id",   // Add instance ID for the test
					InstanceName: "test-instance-name", // Add instance ID for the test
				},
			},
			wantCallCount: 1,
			wantArgs: []wlmfake.WriteInsightArgs{
				{
					Project:  "test-project-id",
					Location: "us-central1", // Expected formatted location
					Req: &dwpb.WriteInsightRequest{
						AgentVersion: configuration.AgentVersion,
						Insight: &dwpb.Insight{
							InstanceId: "test-instance-id",
							TorsoValidation: &dwpb.TorsoValidation{
								WorkloadType:      dwpb.TorsoValidation_MYSQL,
								ValidationDetails: map[string]string{"instance_name": "fake-wlmmetrics-1", "metric_value": "1"},
								ProjectId:         "test-project-id",
								InstanceName:      "test-instance-name",
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			tt.params.wlmService = tt.wlmService
			sendMetricsToDataWarehouse(ctx, tt.params)

			// Assert that WriteInsight was called the expected number of times
			if gotCallCount := tt.params.wlmService.(*wlmfake.TestWLM).WriteInsightCallCount; gotCallCount != tt.wantCallCount {
				t.Errorf("WriteInsight call count mismatch: got %d, want %d", gotCallCount, tt.wantCallCount)
			}

			// TODO: b/391766041 - Remove panic during argument checks
			// Assert that the arguments passed to WriteInsight are as expected
			// if diff := cmp.Diff(tt.wantArgs, tt.params.wlmService.(*wlmfake.TestWLM).WriteInsightArgs, cmpopts.IgnoreUnexported(wlmfake.WriteInsightArgs{}), cmpopts.IgnoreFields(wlmfake.WriteInsightArgs{}, "Req")); diff != "" {
			// 	t.Errorf("WriteInsight args mismatch (-want +got):\n%s", diff)
			// }
		})
	}
}

func TestCreateWriteInsightRequest(t *testing.T) {
	tests := []struct {
		name string
		wm   WorkloadMetrics
		cp   *cpb.CloudProperties
		want *dwpb.WriteInsightRequest
	}{
		{
			name: "MySQL workload",
			wm: WorkloadMetrics{
				WorkloadType: MYSQL,
				Metrics: map[string]string{
					"instance_name": "mysql-instance",
					"metric1":       "value1",
				},
			},
			cp: &cpb.CloudProperties{
				ProjectId:    "test-project",
				InstanceId:   "test-instance-id",
				InstanceName: "test-instance-name",
			},
			want: &dwpb.WriteInsightRequest{
				Insight: &dwpb.Insight{
					InstanceId: "test-instance-id",
					TorsoValidation: &dwpb.TorsoValidation{
						WorkloadType:      dwpb.TorsoValidation_MYSQL,
						ValidationDetails: map[string]string{"instance_name": "mysql-instance", "metric1": "value1"},
						ProjectId:         "test-project",
						InstanceName:      "test-instance-name",
					},
				},
				AgentVersion: configuration.AgentVersion,
			},
		},
		{
			name: "Unknown workload",
			wm: WorkloadMetrics{
				WorkloadType: UNKNOWN,
				Metrics: map[string]string{
					"metric1": "value1",
				},
			},
			cp: &cpb.CloudProperties{
				ProjectId:    "test-project",
				InstanceId:   "test-instance-id",
				InstanceName: "test-instance-name",
			},
			want: &dwpb.WriteInsightRequest{
				Insight: &dwpb.Insight{
					InstanceId: "test-instance-id",
					TorsoValidation: &dwpb.TorsoValidation{
						WorkloadType:      dwpb.TorsoValidation_WORKLOAD_TYPE_UNSPECIFIED,
						ValidationDetails: map[string]string{"metric1": "value1"},
						ProjectId:         "test-project",
						InstanceName:      "test-instance-name",
					},
				},
				AgentVersion: configuration.AgentVersion,
			},
		},
	}
	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := createWriteInsightRequest(ctx, tt.wm, tt.cp)
			if !proto.Equal(got, tt.want) {
				t.Errorf("createWriteInsightRequest() mismatch, got: %v, want: %v", got, tt.want)
			}
		})
	}
}
