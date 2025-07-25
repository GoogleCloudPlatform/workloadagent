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

package sqlserver

import (
	"testing"
	"time"

	durationpb "google.golang.org/protobuf/types/known/durationpb"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

func TestDBCenterMetricCollectionFrequency(t *testing.T) {
	tests := []struct {
		name     string
		args     runDBCenterMetricCollectionArgs
		wantFreq time.Duration
	}{
		{
			name:     "nil_service",
			args:     runDBCenterMetricCollectionArgs{},
			wantFreq: dbCenterMetricCollectionFrequencyDefault,
		},
		{
			name: "nil_config",
			args: runDBCenterMetricCollectionArgs{
				s: &Service{},
			},
			wantFreq: dbCenterMetricCollectionFrequencyDefault,
		},
		{
			name: "config_with_nil_sqlserver_config",
			args: runDBCenterMetricCollectionArgs{
				s: &Service{
					Config: &configpb.Configuration{},
				},
			},
			wantFreq: dbCenterMetricCollectionFrequencyDefault,
		},
		{
			name: "config_with_nil_collection_frequency",
			args: runDBCenterMetricCollectionArgs{
				s: &Service{
					Config: &configpb.Configuration{
						SqlserverConfiguration: &configpb.SQLServerConfiguration{},
					},
				},
			},
			wantFreq: dbCenterMetricCollectionFrequencyDefault,
		},
		{
			name: "config_with_valid_collection_frequency",
			args: runDBCenterMetricCollectionArgs{
				s: &Service{
					Config: &configpb.Configuration{
						SqlserverConfiguration: &configpb.SQLServerConfiguration{
							CollectionConfiguration: &configpb.SQLServerConfiguration_CollectionConfiguration{
								DbcenterMetricsCollectionFrequency: durationpb.New(30 * time.Minute),
							},
						},
					},
				},
			},
			wantFreq: 30 * time.Minute,
		},
		{
			name: "config_with_very_small_collection_frequency",
			args: runDBCenterMetricCollectionArgs{
				s: &Service{
					Config: &configpb.Configuration{
						SqlserverConfiguration: &configpb.SQLServerConfiguration{
							CollectionConfiguration: &configpb.SQLServerConfiguration_CollectionConfiguration{
								DbcenterMetricsCollectionFrequency: durationpb.New(1 * time.Second),
							},
						},
					},
				},
			},
			wantFreq: dbCenterMetricCollectionFrequencyMin,
		},
		{
			name: "config_with_very_large_collection_frequency",
			args: runDBCenterMetricCollectionArgs{
				s: &Service{
					Config: &configpb.Configuration{
						SqlserverConfiguration: &configpb.SQLServerConfiguration{
							CollectionConfiguration: &configpb.SQLServerConfiguration_CollectionConfiguration{
								DbcenterMetricsCollectionFrequency: durationpb.New(6*time.Hour + 1*time.Second),
							},
						},
					},
				},
			},
			wantFreq: dbCenterMetricCollectionFrequencyMax,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotFreq := dbcenterMetricCollectionFrequency(tc.args)
			if gotFreq != tc.wantFreq {
				t.Errorf("dbcenterMetricCollectionFrequency(%v) = %v, want = %v", tc.args, gotFreq, tc.wantFreq)
			}
		})
	}
}
