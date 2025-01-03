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
	"testing"

	"github.com/google/go-cmp/cmp"
)

var (
	filePath = "test_data/metricoverride.yaml"
	//go:embed test_data/metricoverride.yaml
	testFS embed.FS
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
			reader: ConfigFileReader(func(path string) (io.ReadCloser, error) {
				file, err := testFS.Open(filePath)
				var f io.ReadCloser = file
				return f, err
			}),
			want: true,
		},
		{
			name:     "File read error",
			filePath: "some_file.yaml",
			reader: ConfigFileReader(func(path string) (io.ReadCloser, error) {
				return nil, errors.New("failed to read file")
			}),
			want: false,
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
