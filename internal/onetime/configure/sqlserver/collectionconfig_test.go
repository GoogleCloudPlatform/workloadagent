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
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/testing/protocmp"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/configuration"
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime/configure/cliconfig"

	dpb "google.golang.org/protobuf/types/known/durationpb"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

func TestCollectionConfigCommand(t *testing.T) {
	defaultFrequency := time.Duration(configuration.DefaultSQLServerCollectionFrequency)
	tests := []struct {
		name string
		args string
		got  *cliconfig.Configure
		want *cliconfig.Configure
	}{
		{
			name: "UpdateAllCollectionConfigFlags",
			args: "--collect-guest-os-metrics=false --collect-sql-metrics --collection-frequency=10m",
			got: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					SqlserverConfiguration: &cpb.SQLServerConfiguration{
						CollectionConfiguration: &cpb.SQLServerConfiguration_CollectionConfiguration{
							CollectGuestOsMetrics: true,
							CollectSqlMetrics:     false,
							CollectionFrequency:   dpb.New(defaultFrequency),
						},
					},
				},
			},
			want: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					SqlserverConfiguration: &cpb.SQLServerConfiguration{
						CollectionConfiguration: &cpb.SQLServerConfiguration_CollectionConfiguration{
							CollectGuestOsMetrics: false,
							CollectSqlMetrics:     true,
							CollectionFrequency:   dpb.New(10 * time.Minute),
						},
					},
				},
				SQLServerConfigModified: true,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// 'got' is the configuration that will be modified by the command.
			cmd := CollectionConfigCommand(tc.got)
			// Set the args for the command.
			cmd.SetArgs(strings.Split(tc.args, " "))
			// Capture stdout to avoid printing during tests.
			cmd.SetOut(bytes.NewBufferString(""))
			// Execute the command.
			cmd.Execute()

			// Compare the configurations.
			if diff := cmp.Diff(tc.want, tc.got, protocmp.Transform(), cmpopts.IgnoreUnexported(cliconfig.Configure{})); diff != "" {
				t.Errorf("CollectionConfigCommand() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
