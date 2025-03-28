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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/configuration"

	dpb "google.golang.org/protobuf/types/known/durationpb"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

func TestDiscoveryCommand_Flags(t *testing.T) {
	defaultFrequency := time.Duration(configuration.DefaultOracleDiscoveryFrequency)
	tests := []struct {
		name    string
		args    string
		got     *Config
		wantErr string
		want    *Config
	}{
		{
			name: "Enable discovery",
			args: "--enabled=true",
			got: &Config{
				OracleConfiguration: &cpb.OracleConfiguration{
					OracleDiscovery: &cpb.OracleDiscovery{
						UpdateFrequency: dpb.New(defaultFrequency),
					},
				},
			},
			want: &Config{
				ConfigModified: true,
				OracleConfiguration: &cpb.OracleConfiguration{
					OracleDiscovery: &cpb.OracleDiscovery{
						Enabled:         proto.Bool(true),
						UpdateFrequency: dpb.New(defaultFrequency),
					},
				},
			},
		},
		{
			name: "Disable discovery",
			args: "--enabled=false",
			got: &Config{
				OracleConfiguration: &cpb.OracleConfiguration{
					OracleDiscovery: &cpb.OracleDiscovery{
						Enabled:         proto.Bool(true),
						UpdateFrequency: dpb.New(defaultFrequency),
					},
				},
			},
			want: &Config{
				ConfigModified: true,
				OracleConfiguration: &cpb.OracleConfiguration{
					OracleDiscovery: &cpb.OracleDiscovery{
						Enabled:         proto.Bool(false),
						UpdateFrequency: dpb.New(defaultFrequency),
					},
				},
			},
		},
		{
			name: "Change frequency",
			args: "--frequency=5m",
			got: &Config{
				OracleConfiguration: &cpb.OracleConfiguration{
					OracleDiscovery: &cpb.OracleDiscovery{
						UpdateFrequency: dpb.New(defaultFrequency),
					},
				},
			},
			want: &Config{
				ConfigModified: true,
				OracleConfiguration: &cpb.OracleConfiguration{
					OracleDiscovery: &cpb.OracleDiscovery{
						UpdateFrequency: dpb.New(5 * time.Minute),
					},
				},
			},
		},
		{
			name: "Enable discovery and change frequency",
			args: "--enabled=true --frequency=10m",
			got: &Config{
				OracleConfiguration: &cpb.OracleConfiguration{
					OracleDiscovery: &cpb.OracleDiscovery{},
				},
			},
			want: &Config{
				ConfigModified: true,
				OracleConfiguration: &cpb.OracleConfiguration{
					OracleDiscovery: &cpb.OracleDiscovery{
						Enabled:         proto.Bool(true),
						UpdateFrequency: dpb.New(10 * time.Minute),
					},
				},
			},
		},
		{
			name: "No flags provided",
			args: "",
			got: &Config{
				ConfigModified: false,
				OracleConfiguration: &cpb.OracleConfiguration{
					OracleDiscovery: &cpb.OracleDiscovery{
						Enabled:         proto.Bool(true),
						UpdateFrequency: dpb.New(defaultFrequency),
					},
				},
			},
			want: &Config{
				ConfigModified: false,
				OracleConfiguration: &cpb.OracleConfiguration{
					OracleDiscovery: &cpb.OracleDiscovery{
						Enabled:         proto.Bool(true),
						UpdateFrequency: dpb.New(defaultFrequency),
					},
				},
			},
		},
		{
			name: "Wrong flags provided",
			args: "--wrong_flag=true",
			got: &Config{
				ConfigModified: false,
				OracleConfiguration: &cpb.OracleConfiguration{
					OracleDiscovery: &cpb.OracleDiscovery{
						Enabled:         proto.Bool(true),
						UpdateFrequency: dpb.New(defaultFrequency),
					},
				},
			},
			wantErr: "unknown flag: --wrong_flag",
			want: &Config{
				ConfigModified: false,
				OracleConfiguration: &cpb.OracleConfiguration{
					OracleDiscovery: &cpb.OracleDiscovery{
						Enabled:         proto.Bool(true),
						UpdateFrequency: dpb.New(defaultFrequency),
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 'got' is the configuration that will be modified by the command.
			cmd := DiscoveryCommand(test.got)
			// Set the args for the command
			cmd.SetArgs(strings.Split(test.args, " "))
			// Capture stdout to avoid printing during tests
			cmd.SetOut(bytes.NewBufferString(""))
			// Execute the command
			err := cmd.Execute()
			if err != nil && err.Error() != test.wantErr {
				t.Errorf("Error mismatch: %v, want error presence = %v", err, test.wantErr)
			}

			// Compare the configurations
			if diff := cmp.Diff(test.want, test.got, protocmp.Transform()); diff != "" {
				t.Errorf("DiscoveryCommand() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
