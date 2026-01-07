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
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce/metadataserver"
	gpb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/guestactions"
)

func TestConvertCloudProperties(t *testing.T) {
	tests := []struct {
		name string
		cp   *cpb.CloudProperties
		want *metadataserver.CloudProperties
	}{
		{
			name: "nil cloud properties",
			cp:   nil,
			want: nil,
		},
		{
			name: "non-nil cloud properties",
			cp: &cpb.CloudProperties{
				ProjectId:           "test-project",
				NumericProjectId:    "12345",
				InstanceId:          "test-instance",
				Zone:                "us-central1-a",
				InstanceName:        "test-instance-name",
				Image:               "test-image",
				MachineType:         "n1-standard-1",
				Region:              "us-central1",
				ServiceAccountEmail: "test-sa@google.com",
				Scopes:              []string{"scope1", "scope2"},
			},
			want: &metadataserver.CloudProperties{
				ProjectID:           "test-project",
				NumericProjectID:    "12345",
				InstanceID:          "test-instance",
				Zone:                "us-central1-a",
				InstanceName:        "test-instance-name",
				Image:               "test-image",
				MachineType:         "n1-standard-1",
				Region:              "us-central1",
				ServiceAccountEmail: "test-sa@google.com",
				Scopes:              []string{"scope1", "scope2"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := convertCloudProperties(tc.cp)
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("convertCloudProperties(%v) returned an unexpected diff (-want +got): %v", tc.cp, diff)
			}
		})
	}
}

func TestOracleCommandKey(t *testing.T) {
	tests := []struct {
		name        string
		cmd         *gpb.Command
		wantKey     string
		wantTimeout time.Duration
		wantLock    bool
	}{
		{
			name:        "nil agent command",
			cmd:         &gpb.Command{},
			wantKey:     "",
			wantTimeout: 0,
			wantLock:    false,
		},
		{
			name: "nil parameters",
			cmd: &gpb.Command{
				CommandType: &gpb.Command_AgentCommand{
					AgentCommand: &gpb.AgentCommand{
						Command: "oracle_start_database",
					},
				},
			},
			wantKey:     "",
			wantTimeout: 0,
			wantLock:    false,
		},
		{
			name: "empty parameters",
			cmd: &gpb.Command{
				CommandType: &gpb.Command_AgentCommand{
					AgentCommand: &gpb.AgentCommand{
						Command:    "oracle_start_database",
						Parameters: map[string]string{},
					},
				},
			},
			wantKey:     "",
			wantTimeout: 0,
			wantLock:    false,
		},
		{
			name: "missing oracle_sid",
			cmd: &gpb.Command{
				CommandType: &gpb.Command_AgentCommand{
					AgentCommand: &gpb.AgentCommand{
						Command: "oracle_start_database",
						Parameters: map[string]string{
							"param1": "value1",
						},
					},
				},
			},
			wantKey:     "",
			wantTimeout: 0,
			wantLock:    false,
		},
		{
			name: "empty oracle_sid",
			cmd: &gpb.Command{
				CommandType: &gpb.Command_AgentCommand{
					AgentCommand: &gpb.AgentCommand{
						Command: "oracle_start_database",
						Parameters: map[string]string{
							"oracle_sid": "",
						},
					},
				},
			},
			wantKey:     "",
			wantTimeout: 0,
			wantLock:    false,
		},
		{
			name: "oracle_start_database",
			cmd: &gpb.Command{
				CommandType: &gpb.Command_AgentCommand{
					AgentCommand: &gpb.AgentCommand{
						Command: "oracle_start_database",
						Parameters: map[string]string{
							"oracle_sid": "ORCL",
						},
					},
				},
			},
			wantKey:     "orcl",
			wantTimeout: 3 * time.Minute,
			wantLock:    true,
		},
		{
			name: "oracle_stop_database",
			cmd: &gpb.Command{
				CommandType: &gpb.Command_AgentCommand{
					AgentCommand: &gpb.AgentCommand{
						Command: "oracle_stop_database",
						Parameters: map[string]string{
							"oracle_sid": "ORCL",
						},
					},
				},
			},
			wantKey:     "orcl",
			wantTimeout: 3 * time.Minute,
			wantLock:    true,
		},
		{
			name: "oracle_data_guard_switchover",
			cmd: &gpb.Command{
				CommandType: &gpb.Command_AgentCommand{
					AgentCommand: &gpb.AgentCommand{
						Command: "oracle_data_guard_switchover",
						Parameters: map[string]string{
							"oracle_sid": "ORCL",
						},
					},
				},
			},
			wantKey:     "orcl",
			wantTimeout: 3 * time.Minute,
			wantLock:    true,
		},
		{
			name: "oracle_run_discovery",
			cmd: &gpb.Command{
				CommandType: &gpb.Command_AgentCommand{
					AgentCommand: &gpb.AgentCommand{
						Command: "oracle_run_discovery",
						Parameters: map[string]string{
							"oracle_sid": "ORCL",
						},
					},
				},
			},
			wantKey:     "orcl",
			wantTimeout: 3 * time.Minute,
			wantLock:    true,
		},
		{
			name: "oracle_start_listener",
			cmd: &gpb.Command{
				CommandType: &gpb.Command_AgentCommand{
					AgentCommand: &gpb.AgentCommand{
						Command: "oracle_start_listener",
						Parameters: map[string]string{
							"oracle_sid": "ORCL",
						},
					},
				},
			},
			wantKey:     "orcl",
			wantTimeout: 3 * time.Minute,
			wantLock:    true,
		},
		{
			name: "oracle_run_datapatch",
			cmd: &gpb.Command{
				CommandType: &gpb.Command_AgentCommand{
					AgentCommand: &gpb.AgentCommand{
						Command: "oracle_run_datapatch",
						Parameters: map[string]string{
							"oracle_sid": "ORCL",
						},
					},
				},
			},
			wantKey:     "orcl",
			wantTimeout: 30 * time.Minute,
			wantLock:    true,
		},
		{
			name: "oracle_disable_autostart",
			cmd: &gpb.Command{
				CommandType: &gpb.Command_AgentCommand{
					AgentCommand: &gpb.AgentCommand{
						Command: "oracle_disable_autostart",
						Parameters: map[string]string{
							"oracle_sid": "ORCL",
						},
					},
				},
			},
			wantKey:     "orcl",
			wantTimeout: 1 * time.Minute,
			wantLock:    true,
		},
		{
			name: "oracle_disable_restricted_mode",
			cmd: &gpb.Command{
				CommandType: &gpb.Command_AgentCommand{
					AgentCommand: &gpb.AgentCommand{
						Command: "oracle_disable_restricted_mode",
						Parameters: map[string]string{
							"oracle_sid": "ORCL",
						},
					},
				},
			},
			wantKey:     "orcl",
			wantTimeout: 1 * time.Minute,
			wantLock:    true,
		},
		{
			name: "oracle_enable_autostart",
			cmd: &gpb.Command{
				CommandType: &gpb.Command_AgentCommand{
					AgentCommand: &gpb.AgentCommand{
						Command: "oracle_enable_autostart",
						Parameters: map[string]string{
							"oracle_sid": "ORCL",
						},
					},
				},
			},
			wantKey:     "orcl",
			wantTimeout: 1 * time.Minute,
			wantLock:    true,
		},
		{
			name: "oracle_health_check",
			cmd: &gpb.Command{
				CommandType: &gpb.Command_AgentCommand{
					AgentCommand: &gpb.AgentCommand{
						Command: "oracle_health_check",
						Parameters: map[string]string{
							"oracle_sid": "ORCL",
						},
					},
				},
			},
			wantKey:     "orcl",
			wantTimeout: 1 * time.Minute,
			wantLock:    true,
		},
		{
			name: "unknown_command",
			cmd: &gpb.Command{
				CommandType: &gpb.Command_AgentCommand{
					AgentCommand: &gpb.AgentCommand{
						Command: "unknown_command",
						Parameters: map[string]string{
							"oracle_sid": "ORCL",
						},
					},
				},
			},
			wantKey:     "orcl",
			wantTimeout: 10 * time.Minute,
			wantLock:    true,
		},
		{
			name: "case insensitive command",
			cmd: &gpb.Command{
				CommandType: &gpb.Command_AgentCommand{
					AgentCommand: &gpb.AgentCommand{
						Command: "ORACLE_HEALTH_CHECK",
						Parameters: map[string]string{
							"oracle_sid": "ORCL",
						},
					},
				},
			},
			wantKey:     "orcl",
			wantTimeout: 1 * time.Minute,
			wantLock:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotKey, gotTimeout, gotLock := oracleCommandKey(tc.cmd)
			if gotKey != tc.wantKey || gotTimeout != tc.wantTimeout || gotLock != tc.wantLock {
				t.Errorf("oracleCommandKey(%v) = (%q, %v, %v), want (%q, %v, %v)", tc.cmd, gotKey, gotTimeout, gotLock, tc.wantKey, tc.wantTimeout, tc.wantLock)
			}
		})
	}
}
