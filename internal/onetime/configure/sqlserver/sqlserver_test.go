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

// Package sqlserver implements the sqlserver subcommand.
package sqlserver

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/configuration"
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime/configure/cliconfig"

	dpb "google.golang.org/protobuf/types/known/durationpb"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

func TestNewCommand(t *testing.T) {
	defaultSQLServerCollectionTimeout := time.Duration(configuration.DefaultSQLServerCollectionTimeout)
	defaultSQLServerMaxRetries := int32(configuration.DefaultSQLServerMaxRetries)
	defaultSQLServerRetryFrequency := time.Duration(configuration.DefaultSQLServerRetryFrequency)
	tests := []struct {
		name string
		args string
		got  *cliconfig.Configure
		want *cliconfig.Configure
	}{
		{
			name: "UpdateAllSQLServerFlags",
			args: "--enabled --collection-timeout=30s --max-retries=5 --retry-frequency=10m --remote-collection",
			got: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					SqlserverConfiguration: &cpb.SQLServerConfiguration{
						Enabled:           proto.Bool(false),
						CollectionTimeout: dpb.New(defaultSQLServerCollectionTimeout),
						MaxRetries:        defaultSQLServerMaxRetries,
						RetryFrequency:    dpb.New(defaultSQLServerRetryFrequency),
						RemoteCollection:  false,
					},
				},
			},
			want: &cliconfig.Configure{
				Configuration: &cpb.Configuration{
					SqlserverConfiguration: &cpb.SQLServerConfiguration{
						Enabled:           proto.Bool(true),
						CollectionTimeout: dpb.New(30 * time.Second),
						MaxRetries:        5,
						RetryFrequency:    dpb.New(600 * time.Second),
						RemoteCollection:  true,
					},
				},
				SQLServerConfigModified: true,
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
			cmd.Execute()

			// Compare the configurations.
			if diff := cmp.Diff(tc.want, tc.got, protocmp.Transform(), cmpopts.IgnoreUnexported(cliconfig.Configure{})); diff != "" {
				t.Errorf("DiscoveryCommand().Execute() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
