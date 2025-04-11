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

package mysql

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
		name           string
		args           string
		configToModify *cliconfig.Configure
		wantErr        string
		want           *cliconfig.Configure
	}{
		{
			name: "EnableMySQL",
			args: "--enabled",
			configToModify: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					MysqlConfiguration: &cpb.MySQLConfiguration{
						Enabled: proto.Bool(false),
					},
				},
			},
			want: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					MysqlConfiguration: &cpb.MySQLConfiguration{
						Enabled: proto.Bool(true),
					},
				},
				MySQLConfigModified: true,
			},
		},
		{
			name: "WrongFlag",
			args: "--wrong_flag=true",
			configToModify: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					MysqlConfiguration: &cpb.MySQLConfiguration{
						Enabled: proto.Bool(true),
					},
				},
				MySQLConfigModified: false,
			},
			wantErr: "unknown flag: --wrong_flag",
			want: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					MysqlConfiguration: &cpb.MySQLConfiguration{
						Enabled: proto.Bool(true),
					},
				},
				MySQLConfigModified: false,
			},
		},
		{
			name: "MissingMysqlConfiguration",
			args: "connection-params --username=test-user --password=test-password",
			configToModify: &cliconfig.Configure{
				Configuration: &cpb.Configuration{},
			},
			want: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					MysqlConfiguration: &cpb.MySQLConfiguration{
						ConnectionParameters: &cpb.ConnectionParameters{
							Username: "test-user",
							Password: "test-password",
						},
					},
				},
				MySQLConfigModified: true,
			},
		},
		{
			name: "AddNewConnectionParams",
			args: "connection-params --username=test-user --password=test-password --project-id=test-project --secret-name=test-secret",
			configToModify: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					MysqlConfiguration: &cpb.MySQLConfiguration{
						Enabled: proto.Bool(true),
					},
				},
				MySQLConfigModified: false,
			},
			want: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					MysqlConfiguration: &cpb.MySQLConfiguration{
						Enabled: proto.Bool(true),
						ConnectionParameters: &cpb.ConnectionParameters{
							Username: "test-user",
							Password: "test-password",
							Secret: &cpb.SecretRef{
								ProjectId:  "test-project",
								SecretName: "test-secret",
							},
						},
					},
				},
				MySQLConfigModified: true,
			},
		},
		{
			name: "UpdateConnectionParams",
			args: "connection-params --username=new-user --secret-name=new-secret",
			configToModify: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					MysqlConfiguration: &cpb.MySQLConfiguration{
						Enabled: proto.Bool(true),
						ConnectionParameters: &cpb.ConnectionParameters{
							Username: "old-user",
							Password: "old-password",
							Secret: &cpb.SecretRef{
								ProjectId:  "old-project",
								SecretName: "old-secret",
							},
						},
					},
				},
				MySQLConfigModified: false,
			},
			want: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					MysqlConfiguration: &cpb.MySQLConfiguration{
						Enabled: proto.Bool(true),
						ConnectionParameters: &cpb.ConnectionParameters{
							Username: "new-user",
							Password: "old-password",
							Secret: &cpb.SecretRef{
								ProjectId:  "old-project",
								SecretName: "new-secret",
							},
						},
					},
				},
				MySQLConfigModified: true,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// 'configToModify' is the configuration that will be modified by the command.
			cmd := NewCommand(tc.configToModify)
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
			if diff := cmp.Diff(tc.want, tc.configToModify, protocmp.Transform(), cmpopts.IgnoreUnexported(cliconfig.Configure{})); diff != "" {
				t.Errorf("NewCommand().Execute() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
