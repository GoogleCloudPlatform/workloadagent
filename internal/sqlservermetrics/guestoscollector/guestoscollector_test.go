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

package guestoscollector

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/GoogleCloudPlatform/workloadagent/internal/sqlservermetrics/sqlserverutils"
)

func TestCollectionOSFields(t *testing.T) {
	want := []string{
		PowerProfileSetting,
		UseLocalSSD,
		DataDiskAllocationUnits,
		GCBDRAgentRunning,
	}
	got := CollectionOSFields()
	if diff := cmp.Diff(got, want); diff != "" {
		t.Errorf("CollectionOSFields() returned wrong result (-got +want):\n%s", diff)
	}
}

func TestUnknownOsFields(t *testing.T) {
	testcases := []struct {
		name     string
		input    *[]sqlserverutils.MetricDetails
		want     *[]sqlserverutils.MetricDetails
		hasError bool
	}{
		{
			name: "success",
			input: &[]sqlserverutils.MetricDetails{
				sqlserverutils.MetricDetails{
					Name: "OS",
					Fields: []map[string]string{
						map[string]string{
							"power_profile_setting":      "Balanced",
							"local_ssd":                  `{"C:":"OTHER"}`,
							"data_disk_allocation_units": `[{"BlockSize":4096,"Caption":"C:\\"},{"BlockSize":1024,"Caption":"D:\\"}]`,
							"gcbdr_agent_running":        "true",
						},
					},
				},
			},
			want: &[]sqlserverutils.MetricDetails{
				sqlserverutils.MetricDetails{
					Name: "OS",
					Fields: []map[string]string{
						map[string]string{
							"power_profile_setting":      "Balanced",
							"local_ssd":                  `{"C:":"OTHER"}`,
							"data_disk_allocation_units": `[{"BlockSize":4096,"Caption":"C:\\"},{"BlockSize":1024,"Caption":"D:\\"}]`,
							"gcbdr_agent_running":        "true",
						},
					},
				},
			},
			hasError: false,
		},
		{
			name: "success_with_nil_fields",
			input: &[]sqlserverutils.MetricDetails{
				sqlserverutils.MetricDetails{
					Name:   "OS",
					Fields: []map[string]string{},
				},
			},
			want: &[]sqlserverutils.MetricDetails{
				sqlserverutils.MetricDetails{
					Name: "OS",
					Fields: []map[string]string{
						map[string]string{
							"power_profile_setting":      "unknown",
							"local_ssd":                  "unknown",
							"data_disk_allocation_units": "unknown",
							"gcbdr_agent_running":        "unknown",
						},
					},
				},
			},
			hasError: false,
		},
		{
			name: "success_with_missing_fields",
			input: &[]sqlserverutils.MetricDetails{
				sqlserverutils.MetricDetails{
					Name: "OS",
					Fields: []map[string]string{
						map[string]string{},
					},
				},
			},
			want: &[]sqlserverutils.MetricDetails{
				sqlserverutils.MetricDetails{
					Name: "OS",
					Fields: []map[string]string{
						map[string]string{
							"power_profile_setting":      "unknown",
							"local_ssd":                  "unknown",
							"data_disk_allocation_units": "unknown",
							"gcbdr_agent_running":        "unknown",
						},
					},
				},
			},
			hasError: false,
		},
		{
			name: "error_with_multiple_details",
			input: &[]sqlserverutils.MetricDetails{
				sqlserverutils.MetricDetails{
					Name:   "OS",
					Fields: []map[string]string{},
				},
				sqlserverutils.MetricDetails{
					Name:   "OS",
					Fields: []map[string]string{},
				},
			},
			hasError: true,
		},
		{
			name: "error_with_multiple_fields",
			input: &[]sqlserverutils.MetricDetails{
				sqlserverutils.MetricDetails{
					Name: "OS",
					Fields: []map[string]string{
						map[string]string{
							"power_profile_setting":      "Balanced",
							"local_ssd":                  `{"C:":"OTHER"}`,
							"data_disk_allocation_units": `[{"BlockSize":4096,"Caption":"C:\\"},{"BlockSize":1024,"Caption":"D:\\"}]`,
							"gcbdr_agent_running":        "true",
						},
						map[string]string{
							"power_profile_setting":      "Balanced",
							"local_ssd":                  `{"C:":"OTHER"}`,
							"data_disk_allocation_units": `[{"BlockSize":4096,"Caption":"C:\\"},{"BlockSize":1024,"Caption":"D:\\"}]`,
							"gcbdr_agent_running":        "true",
						},
					},
				},
			},
			hasError: true,
		},
		{
			name: "error_with_invalid_name",
			input: &[]sqlserverutils.MetricDetails{
				sqlserverutils.MetricDetails{
					Name: "invalid_name",
					Fields: []map[string]string{
						map[string]string{
							"power_profile_setting":      "Balanced",
							"local_ssd":                  `{"C:":"OTHER"}`,
							"data_disk_allocation_units": `[{"BlockSize":4096,"Caption":"C:\\"},{"BlockSize":1024,"Caption":"D:\\"}]`,
							"gcbdr_agent_running":        "true",
						},
					},
				},
			},
			hasError: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			got := UnknownOsFields(tc.input)
			if got != nil && !tc.hasError {
				t.Errorf("UnknownOsFields() returned error: %v, want nil", got)
			}
			if got == nil && tc.hasError {
				t.Errorf("UnknownOsFields() returned nil, want error")
			}
			if tc.hasError {
				return
			}
			if diff := cmp.Diff(tc.input, tc.want); diff != "" {
				t.Errorf("UnknownOsFields() returned wrong result (-got +want):\n%s", diff)
			}
		})
	}
}
