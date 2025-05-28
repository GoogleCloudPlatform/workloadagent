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
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/api/googleapi"
	"google.golang.org/protobuf/proto"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/configuration"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"

	wlmfake "github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce/fake"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce/wlm"
	dwpb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/datawarehouse"
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
	DefaultCloudProperties = &cpb.CloudProperties{
		ProjectId:    "test-project",
		Region:       "us-central1",
		InstanceId:   "test-instance-id",
		InstanceName: "test-instance-name",
	}
	DefaultWriteInsightRequest = &dwpb.WriteInsightRequest{
		Insight: &dwpb.Insight{
			InstanceId: "test-instance-id",
			TorsoValidation: &dwpb.TorsoValidation{
				WorkloadType:      dwpb.TorsoValidation_WORKLOAD_TYPE_UNSPECIFIED,
				ValidationDetails: map[string]string{"metric1": "value1"},
				ProjectId:         "test-project",
				InstanceName:      "test-instance-name",
				AgentVersion:      configuration.AgentVersion,
			},
		},
	}
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
				WriteInsightResponses: []*wlm.WriteInsightResponse{
					&wlm.WriteInsightResponse{ServerResponse: googleapi.ServerResponse{HTTPStatusCode: 201}},
				},
				WriteInsightArgs: []wlmfake.WriteInsightArgs{
					{
						Project:  "test-project-id",
						Location: "us-central1",
						Req: &dwpb.WriteInsightRequest{
							Insight: &dwpb.Insight{
								InstanceId: "test-instance-id",
								TorsoValidation: &dwpb.TorsoValidation{
									WorkloadType:      dwpb.TorsoValidation_MYSQL,
									ValidationDetails: map[string]string{"instance_name": "fake-wlmmetrics-1", "metric_value": "1"},
									ProjectId:         "test-project-id",
									InstanceName:      "test-instance-name",
									AgentVersion:      configuration.AgentVersion,
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
						Insight: &dwpb.Insight{
							InstanceId: "test-instance-id",
							TorsoValidation: &dwpb.TorsoValidation{
								WorkloadType:      dwpb.TorsoValidation_MYSQL,
								ValidationDetails: map[string]string{"instance_name": "fake-wlmmetrics-1", "metric_value": "1"},
								ProjectId:         "test-project-id",
								InstanceName:      "test-instance-name",
								AgentVersion:      configuration.AgentVersion,
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
			cp: DefaultCloudProperties,
			want: &dwpb.WriteInsightRequest{
				Insight: &dwpb.Insight{
					InstanceId: "test-instance-id",
					TorsoValidation: &dwpb.TorsoValidation{
						WorkloadType:      dwpb.TorsoValidation_MYSQL,
						ValidationDetails: map[string]string{"instance_name": "mysql-instance", "metric1": "value1"},
						ProjectId:         "test-project",
						InstanceName:      "test-instance-name",
						AgentVersion:      configuration.AgentVersion,
					},
				},
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
			cp:   DefaultCloudProperties,
			want: DefaultWriteInsightRequest,
		},
		{
			name: "Random workload",
			wm: WorkloadMetrics{
				WorkloadType: "RANDOM WORKLOAD",
				Metrics: map[string]string{
					"metric1": "value1",
				},
			},
			cp:   DefaultCloudProperties,
			want: DefaultWriteInsightRequest,
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

func TestSendDataInsight(t *testing.T) {
	tests := []struct {
		name         string
		wlmService   *wlmfake.TestWLM
		params       SendDataInsightParams
		wantRespBody *wlm.WriteInsightResponse
		wantErr      error
	}{
		{
			name: "Success",
			wlmService: &wlmfake.TestWLM{
				T: t,
				WriteInsightResponses: []*wlm.WriteInsightResponse{
					&wlm.WriteInsightResponse{ServerResponse: googleapi.ServerResponse{HTTPStatusCode: 201}},
				},
				WriteInsightErrs: []error{nil},
			},
			params: SendDataInsightParams{
				WLMetrics: WorkloadMetrics{
					WorkloadType: MYSQL,
					Metrics: map[string]string{
						"instance_name": "mysql-instance",
						"metric1":       "value1",
					},
				},
				CloudProps: DefaultCloudProperties,
			},
			wantRespBody: &wlm.WriteInsightResponse{ServerResponse: googleapi.ServerResponse{HTTPStatusCode: 201}},
			wantErr:      nil,
		},
		{
			name: "Error",
			wlmService: &wlmfake.TestWLM{
				T: t,
				WriteInsightResponses: []*wlm.WriteInsightResponse{
					&wlm.WriteInsightResponse{ServerResponse: googleapi.ServerResponse{HTTPStatusCode: 400}},
				},
				WriteInsightErrs: []error{fmt.Errorf("error")},
			},
			params: SendDataInsightParams{
				WLMetrics: WorkloadMetrics{
					WorkloadType: MYSQL,
					Metrics: map[string]string{
						"instance_name": "mysql-instance",
						"metric1":       "value1",
					},
				},
				CloudProps: DefaultCloudProperties,
			},
			wantRespBody: nil,
			wantErr:      cmpopts.AnyError,
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params.WLMService = tt.wlmService
			got, err := SendDataInsight(ctx, tt.params)
			if !cmp.Equal(err, tt.wantErr, cmpopts.EquateErrors()) {
				t.Errorf("Error mismatch: got %v, want %v.", err, tt.wantErr)
			}

			if diff := cmp.Diff(tt.wantRespBody, got); diff != "" {
				t.Errorf("SendDataInsight(%v) returned an unexpected diff (-want +got): \n%s", tt.params, diff)
			}
		})
	}
}

func TestQuietSendDataInsight(t *testing.T) {
	tests := []struct {
		name         string
		wlmService   *wlmfake.TestWLM
		params       SendDataInsightParams
		wantRespBody *wlm.WriteInsightResponse
		wantErr      bool
	}{
		{
			name: "Success",
			wlmService: &wlmfake.TestWLM{
				T: t,
				WriteInsightResponses: []*wlm.WriteInsightResponse{
					&wlm.WriteInsightResponse{ServerResponse: googleapi.ServerResponse{HTTPStatusCode: 201}},
				},
				WriteInsightErrs: []error{nil},
			},
			params: SendDataInsightParams{
				WLMetrics: WorkloadMetrics{
					WorkloadType: MYSQL,
					Metrics: map[string]string{
						"instance_name": "mysql-instance",
						"metric1":       "value1",
					},
				},
				CloudProps: DefaultCloudProperties,
			},
			wantRespBody: &wlm.WriteInsightResponse{ServerResponse: googleapi.ServerResponse{HTTPStatusCode: 201}},
			wantErr:      false,
		},
		{
			name: "Error",
			wlmService: &wlmfake.TestWLM{
				T: t,
				WriteInsightResponses: []*wlm.WriteInsightResponse{
					&wlm.WriteInsightResponse{ServerResponse: googleapi.ServerResponse{HTTPStatusCode: 400}},
				},
				WriteInsightErrs: []error{fmt.Errorf("error")},
			},
			params: SendDataInsightParams{
				WLMetrics: WorkloadMetrics{
					WorkloadType: MYSQL,
					Metrics: map[string]string{
						"instance_name": "mysql-instance",
						"metric1":       "value1",
					},
				},
				CloudProps: DefaultCloudProperties,
			},
			wantRespBody: &wlm.WriteInsightResponse{ServerResponse: googleapi.ServerResponse{HTTPStatusCode: 400}},
			wantErr:      true,
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params.WLMService = tt.wlmService
			got, err := QuietSendDataInsight(ctx, tt.params)
			if (err != nil) != tt.wantErr {
				t.Errorf("Error mismatch: got %v, want %v.", err, tt.wantErr)
			} else if diff := cmp.Diff(tt.wantRespBody, got); diff != "" {
				t.Errorf("QuietSendDataInsight(%v) returned an unexpected diff (-want +got): \n%s", tt.params, diff)
			}
		})
	}
}
