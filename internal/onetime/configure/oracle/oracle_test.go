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

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime/configure/cliconfig"

	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

func TestNewCommand(t *testing.T) {
	tests := []struct {
		name    string
		args    string
		got     *cliconfig.Configure
		wantErr string
		want    *cliconfig.Configure
	}{
		{
			name: "EnableOracle",
			args: "--enabled",
			got: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					OracleConfiguration: &cpb.OracleConfiguration{},
				},
			},
			want: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					OracleConfiguration: &cpb.OracleConfiguration{
						Enabled: proto.Bool(true),
					},
				},
				OracleConfigModified: true,
			},
		},
		{
			name: "DisableOracle",
			args: "--enabled=false",
			got: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					OracleConfiguration: &cpb.OracleConfiguration{
						Enabled: proto.Bool(true),
					},
				},
			},
			want: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					OracleConfiguration: &cpb.OracleConfiguration{
						Enabled: proto.Bool(false),
					},
				},
				OracleConfigModified: true,
			},
		},
		{
			name: "NoFlags",
			args: "",
			got: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					OracleConfiguration: &cpb.OracleConfiguration{
						Enabled: proto.Bool(true),
					},
				},
				OracleConfigModified: false,
			},
			want: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					OracleConfiguration: &cpb.OracleConfiguration{
						Enabled: proto.Bool(true),
					},
				},
				OracleConfigModified: false,
			},
		},
		{
			name: "WrongFlag",
			args: "--wrong_flag=true",
			got: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					OracleConfiguration: &cpb.OracleConfiguration{
						Enabled: proto.Bool(true),
					},
				},
				OracleConfigModified: false,
			},
			wantErr: "unknown flag: --wrong_flag",
			want: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					OracleConfiguration: &cpb.OracleConfiguration{
						Enabled: proto.Bool(true),
					},
				},
				OracleConfigModified: false,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// 'got' is the configuration that will be modified by the command.
			cmd := NewCommand(tc.got)
			// Set the args for the command.
			cmd.SetArgs(strings.Split(tc.args, " "))
			// Capture stdout to avoid printing during tests.
			cmd.SetOut(bytes.NewBufferString(""))
			// Execute the command.
			err := cmd.Execute()
			if err != nil && err.Error() != tc.wantErr {
				t.Errorf("NewCommand().Execute() = %v, want: %v", err, tc.wantErr)
			}

			// Compare the configurations.
			if diff := cmp.Diff(tc.want, tc.got, protocmp.Transform(), cmpopts.IgnoreUnexported(cliconfig.Configure{})); diff != "" {
				t.Errorf("NewCommand().Execute() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
