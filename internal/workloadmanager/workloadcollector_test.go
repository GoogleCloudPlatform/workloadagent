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
				{metrics: map[string]string{}},
			},
		},
		{
			name:     "File exists and is readable",
			filePath: filePath,
			reader:   DefaultTestReader,
			want: []WorkloadMetrics{
				{
					workloadType: "MYSQL",
					metrics: map[string]string{
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
					workloadType: "REDIS",
					metrics:      map[string]string{"instance_name": "fake-wlmmetrics-1", "metric_value": "1"},
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
