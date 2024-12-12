//go:build windows
// +build windows

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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/GoogleCloudPlatform/workloadagent/internal/sqlservermetrics/sqlserverutils"
)

func TestCollectGuestRules(t *testing.T) {
	testcases := []struct {
		name                string
		mockRuleMap         bool
		mockWMIErr          bool
		guestRuleWMIMapMock map[string]wmiExecutor
		want                sqlserverutils.MetricDetails
	}{
		{
			name: "success",
			want: sqlserverutils.MetricDetails{
				Name: "OS",
				Fields: []map[string]string{
					map[string]string{
						"power_profile_setting":      "Balanced",
						"local_ssd":                  `{"C:":"OTHER"}`,
						"data_disk_allocation_units": `[{"BlockSize":4096,"Caption":"C:\\"},{"BlockSize":1024,"Caption":"D:\\"}]`,
						"gcbdr_agent_running":        "false",
					},
				},
			},
		},
		{
			name:        "success with mocked data",
			mockRuleMap: true,
			guestRuleWMIMapMock: map[string]wmiExecutor{
				"testname": wmiExecutor{
					isRule: true,
					runWMIQuery: func(connArgs wmiConnectionArgs) (string, error) {
						return "testvalue", nil
					},
				},
			},
			want: sqlserverutils.MetricDetails{
				Name: "OS",
				Fields: []map[string]string{
					map[string]string{
						"testname":  "testvalue",
						"local_ssd": "unknown",
					},
				},
			},
		},
		{
			name:        "do not save to result if isRule is false",
			mockRuleMap: true,
			guestRuleWMIMapMock: map[string]wmiExecutor{
				"testname": wmiExecutor{
					runWMIQuery: func(connArgs wmiConnectionArgs) (string, error) {
						return "testvalue", nil
					},
				},
			},
			want: sqlserverutils.MetricDetails{
				Name:   "OS",
				Fields: []map[string]string{map[string]string{"local_ssd": "unknown"}},
			},
		},
		{
			name:        "empty detail when runWmi returns error",
			mockRuleMap: true,
			guestRuleWMIMapMock: map[string]wmiExecutor{
				PowerProfileSetting: wmiExecutor{
					runWMIQuery: func(connArgs wmiConnectionArgs) (string, error) {
						return "", fmt.Errorf("error")
					},
				},
			},
			want: sqlserverutils.MetricDetails{
				Name:   "OS",
				Fields: []map[string]string{map[string]string{"local_ssd": "unknown"}},
			},
		},
		{
			name:       "invalid wmi query return unknown result",
			mockWMIErr: true,
			want: sqlserverutils.MetricDetails{
				Name: "OS",
				Fields: []map[string]string{
					map[string]string{
						"data_disk_allocation_units": "unknown",
						"local_ssd":                  "unknown",
						"power_profile_setting":      "unknown",
						"gcbdr_agent_running":        "unknown",
					},
				},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			collector := NewWindowsCollector(nil, nil, nil)
			// apply mock rule map
			if tc.mockRuleMap {
				collector.guestRuleWMIMap = tc.guestRuleWMIMapMock
			} else if tc.mockWMIErr {
				for r, m := range collector.guestRuleWMIMap {
					m.query = "any query"
					collector.guestRuleWMIMap[r] = m
				}
			}
			got := collector.CollectGuestRules(context.Background(), time.Minute)
			if diff := cmp.Diff(got, tc.want); diff != "" {
				t.Errorf("CollectGuestRules() returned wrong result (-got +want):\n%s", diff)
			}
		})
	}
}

func TestLogicalDiskMediaType(t *testing.T) {
	testcases := []struct {
		name                      string
		logicalToDiskMapMock      map[string]string
		physicalDiskToTypeMapMock map[string]string
		inputDetails              *sqlserverutils.MetricDetails
		want                      *sqlserverutils.MetricDetails
	}{
		{
			name: "success",
			logicalToDiskMapMock: map[string]string{
				"C:": "0",
			},
			physicalDiskToTypeMapMock: map[string]string{"0": "LOCAL-SSD"},
			inputDetails: &sqlserverutils.MetricDetails{
				Name: "testname",
				Fields: []map[string]string{
					map[string]string{"testfield": "testvalue"},
				},
			},
			want: &sqlserverutils.MetricDetails{
				Name: "testname",
				Fields: []map[string]string{
					map[string]string{
						"testfield": "testvalue",
						"local_ssd": `{"C:":"LOCAL-SSD"}`,
					},
				},
			},
		},
		{
			name: "disk id not matching",
			logicalToDiskMapMock: map[string]string{
				"C": "0",
			},
			physicalDiskToTypeMapMock: map[string]string{"somedisk": "Persistent-SSD"},
			inputDetails: &sqlserverutils.MetricDetails{
				Name: "testname",
				Fields: []map[string]string{
					map[string]string{"testfield": "testvalue"},
				},
			},
			want: &sqlserverutils.MetricDetails{
				Name: "testname",
				Fields: []map[string]string{
					map[string]string{
						"testfield": "testvalue",
						"local_ssd": "unknown",
					},
				},
			},
		},
	}
	collector := NewWindowsCollector(nil, nil, nil)
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			collector.logicalToPhysicalDiskMap = tc.logicalToDiskMapMock
			collector.physicalDiskToTypeMap = tc.physicalDiskToTypeMapMock
			collector.logicalDiskMediaType(tc.inputDetails)
			got := tc.inputDetails
			if diff := cmp.Diff(got, tc.want); diff != "" {
				t.Errorf("LogicalDiskMediaType() returned wrong result (-got +want):\n%s", diff)
			}
		})
	}
}

func TestFriendlyNameToDiskType(t *testing.T) {
	tests := []struct {
		friendlyName string
		size         int64
		mediaType    int16
		want         string
	}{
		{
			friendlyName: "nvme_card",
			size:         402653184000,
			mediaType:    4,
			want:         "LOCAL-SSD",
		},
		{
			friendlyName: "Google EphemeralDisk",
			size:         402653184000,
			mediaType:    4,
			want:         "LOCAL-SSD",
		},
		{
			friendlyName: "Google PersistentDisk",
			size:         10,
			mediaType:    4,
			want:         "PERSISTENT-SSD",
		},
		{
			friendlyName: "Google PersistentDisk",
			size:         10,
			mediaType:    2,
			want:         "OTHER",
		},
		{
			friendlyName: "Other friendly name",
			size:         402653184000,
			mediaType:    2,
			want:         "OTHER",
		},
		{
			friendlyName: "nvme_card",
			size:         10,
			mediaType:    4,
			want:         "OTHER",
		},
	}

	for _, tc := range tests {
		got := friendlyNameToDiskType(tc.friendlyName, tc.size, tc.mediaType)
		if got != tc.want {
			t.Errorf("FriendlyNameToDiskType(%v, %v, %v) = %v, want: %v", tc.friendlyName, tc.size, tc.mediaType, got, tc.want)
		}
	}
}
