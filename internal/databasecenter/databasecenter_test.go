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

package databasecenter

import (
	"context"
	"fmt"
	"testing"
	"time"

	anypb "google.golang.org/protobuf/types/known/anypb"
	"github.com/GoogleCloudPlatform/agentcommunication_client"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	dcpb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/databasecenter"
)

var (
	defaultConfig = &configpb.Configuration{
		CloudProperties: &configpb.CloudProperties{
			ProjectId:        "test-project",
			NumericProjectId: "12345",
			InstanceId:       "test-instance-id",
			InstanceName:     "test-instance-name",
			Region:           "us-central1",
			Zone:             "us-central1-a",
		},
	}
	defaultResourceName = "//compute.googleapis.com/projects/test-project/zones/us-central1-a/instances/test-instance-name"
	defaultResourceID   = &dcpb.DatabaseResourceId{
		Provider:     dcpb.DatabaseResourceId_GCP,
		UniqueId:     "test-instance-id",
		ResourceType: "compute.googleapis.com/Instance",
	}
)

type sendAgentMessageCall struct {
	AgentType   string
	MessageType string
	Msg         *anypb.Any
}

type MockCommunication struct {
	Calls                        []sendAgentMessageCall
	establishACSConnectionCalled bool
	sendAgentMessageError        error
	establishACSConnectionError  error
}

func (m *MockCommunication) EstablishACSConnection(ctx context.Context, endpoint string, channel string) (*client.Connection, error) {
	m.establishACSConnectionCalled = true
	return &client.Connection{}, m.establishACSConnectionError
}

func (m *MockCommunication) SendAgentMessage(ctx context.Context, agentType string, messageType string, msg *anypb.Any, conn *client.Connection) error {
	m.Calls = append(m.Calls, sendAgentMessageCall{AgentType: agentType, MessageType: messageType, Msg: msg})
	return m.sendAgentMessageError
}

func TestGetSignalType(t *testing.T) {
	tests := []struct {
		key  string
		want dcpb.SignalType
	}{
		{
			key:  NoRootPasswordKey,
			want: dcpb.SignalType_SIGNAL_TYPE_NO_ROOT_PASSWORD,
		},
		{
			key:  ExposedToPublicAccessKey,
			want: dcpb.SignalType_SIGNAL_TYPE_EXPOSED_TO_PUBLIC_ACCESS,
		},
		{
			key:  UnencryptedConnectionsKey,
			want: dcpb.SignalType_SIGNAL_TYPE_UNENCRYPTED_CONNECTIONS,
		},
		{
			key:  DatabaseAuditingDisabledKey,
			want: dcpb.SignalType_SIGNAL_TYPE_DATABASE_AUDITING_DISABLED,
		},
		{
			key:  "unknown_key",
			want: dcpb.SignalType_SIGNAL_TYPE_UNSPECIFIED,
		},
	}

	client := &realClient{}
	for _, tc := range tests {
		got := client.getSignalType(tc.key)
		if got != tc.want {
			t.Errorf("getSignalType(%q) = %v, want %v", tc.key, got, tc.want)
		}
	}
}

func TestGetSignalValue(t *testing.T) {
	tests := []struct {
		value string
		want  bool
	}{
		{value: "true", want: true},
		{value: "false", want: false},
		{value: "TRUE", want: false},
		{value: "False", want: false},
		{value: "1", want: false},
		{value: "", want: false},
	}

	client := &realClient{}
	for _, tc := range tests {
		got := client.getSignalValue(tc.value)
		if got != tc.want {
			t.Errorf("getSignalValue(%q) = %v, want %v", tc.value, got, tc.want)
		}
	}
}

func TestBuildConfigBasedSignalMessage(t *testing.T) {
	tests := []struct {
		name    string
		config  *configpb.Configuration
		key     string
		value   string
		want    *dcpb.DatabaseResourceFeed
		wantErr bool
	}{
		{
			name:   "no root password true",
			config: defaultConfig,
			key:    NoRootPasswordKey,
			value:  "true",
			want: &dcpb.DatabaseResourceFeed{
				FeedType: dcpb.DatabaseResourceFeed_CONFIG_BASED_SIGNAL_DATA,
				Content: &dcpb.DatabaseResourceFeed_ConfigBasedSignalData{
					ConfigBasedSignalData: &dcpb.ConfigBasedSignalData{
						ResourceId:       defaultResourceID,
						FullResourceName: defaultResourceName,
						SignalType:       dcpb.SignalType_SIGNAL_TYPE_NO_ROOT_PASSWORD,
						SignalMetadata:   &dcpb.ConfigBasedSignalData_SignalBoolValue{SignalBoolValue: true},
					},
				},
			},
		},
		{
			name:   "exposed to public false",
			config: defaultConfig,
			key:    ExposedToPublicAccessKey,
			value:  "false",
			want: &dcpb.DatabaseResourceFeed{
				FeedType: dcpb.DatabaseResourceFeed_CONFIG_BASED_SIGNAL_DATA,
				Content: &dcpb.DatabaseResourceFeed_ConfigBasedSignalData{
					ConfigBasedSignalData: &dcpb.ConfigBasedSignalData{
						ResourceId:       defaultResourceID,
						FullResourceName: defaultResourceName,
						SignalType:       dcpb.SignalType_SIGNAL_TYPE_EXPOSED_TO_PUBLIC_ACCESS,
						SignalMetadata:   &dcpb.ConfigBasedSignalData_SignalBoolValue{SignalBoolValue: false},
					},
				},
			},
		},
		{
			name:   "empty config",
			config: &configpb.Configuration{},
			key:    UnencryptedConnectionsKey,
			value:  "true",
			want: &dcpb.DatabaseResourceFeed{
				FeedType: dcpb.DatabaseResourceFeed_CONFIG_BASED_SIGNAL_DATA,
				Content: &dcpb.DatabaseResourceFeed_ConfigBasedSignalData{
					ConfigBasedSignalData: &dcpb.ConfigBasedSignalData{
						ResourceId: &dcpb.DatabaseResourceId{
							Provider:     dcpb.DatabaseResourceId_GCP,
							UniqueId:     "",
							ResourceType: "compute.googleapis.com/Instance",
						},
						FullResourceName: "//compute.googleapis.com/projects//zones//instances/",
						SignalType:       dcpb.SignalType_SIGNAL_TYPE_UNENCRYPTED_CONNECTIONS,
						SignalMetadata:   &dcpb.ConfigBasedSignalData_SignalBoolValue{SignalBoolValue: true},
					},
				},
			},
		},
		{
			name:   "unknown key",
			config: defaultConfig,
			key:    "some_unknown_signal",
			value:  "true",
			want: &dcpb.DatabaseResourceFeed{
				FeedType: dcpb.DatabaseResourceFeed_CONFIG_BASED_SIGNAL_DATA,
				Content: &dcpb.DatabaseResourceFeed_ConfigBasedSignalData{
					ConfigBasedSignalData: &dcpb.ConfigBasedSignalData{
						ResourceId:       defaultResourceID,
						FullResourceName: defaultResourceName,
						SignalType:       dcpb.SignalType_SIGNAL_TYPE_UNSPECIFIED,
						SignalMetadata:   &dcpb.ConfigBasedSignalData_SignalBoolValue{SignalBoolValue: true},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			client := &realClient{Config: tc.config}

			gotAny, err := client.buildConfigBasedSignalMessage(ctx, tc.key, tc.value)
			if (err != nil) != tc.wantErr {
				t.Fatalf("buildConfigBasedSignalMessage(%q, %q) error = %v, wantErr %v", tc.key, tc.value, err, tc.wantErr)
			}
			if err != nil {
				return
			}

			got := &dcpb.DatabaseResourceFeed{}
			if err := gotAny.UnmarshalTo(got); err != nil {
				t.Fatalf("Failed to unmarshal anyMsg: %v", err)
			}

			if diff := cmp.Diff(tc.want, got, protocmp.Transform(),
				protocmp.IgnoreFields(&dcpb.DatabaseResourceFeed{}, "feed_timestamp"),
				protocmp.IgnoreFields(&dcpb.ConfigBasedSignalData{}, "last_refresh_time")); diff != "" {
				t.Errorf("buildConfigBasedSignalMessage(%q, %q) returned unexpected diff (-want +got):\n%s", tc.key, tc.value, diff)
			}
		})
	}
}

func TestBuildDatabaseResourceMetadataMessage(t *testing.T) {
	testTime := time.Now()
	testTimestamp := timestamppb.New(testTime)

	tests := []struct {
		name    string
		config  *configpb.Configuration
		metrics DBCenterMetrics
		want    *dcpb.DatabaseResourceFeed
		wantErr bool
	}{
		{
			name: "valid config mysql",
			config: &configpb.Configuration{
				CloudProperties: &configpb.CloudProperties{
					ProjectId:        "test-project",
					NumericProjectId: "12345",
					InstanceId:       "test-instance",
					InstanceName:     "test-instance-name",
					Region:           "us-central1",
					Zone:             "us-central1-a",
					VcpuCount:        2,
					MemorySizeMb:     8192,
				},
			},
			metrics: DBCenterMetrics{
				EngineType: MYSQL,
				Metrics: map[string]string{
					"major_version": "8.0",
					"minor_version": "8.0.26",
				},
			},
			want: &dcpb.DatabaseResourceFeed{
				FeedTimestamp: testTimestamp,
				FeedType:      dcpb.DatabaseResourceFeed_RESOURCE_METADATA,
				Content: &dcpb.DatabaseResourceFeed_ResourceMetadata{
					ResourceMetadata: &dcpb.DatabaseResourceMetadata{
						Id: &dcpb.DatabaseResourceId{
							Provider:     dcpb.DatabaseResourceId_GCP,
							UniqueId:     "test-instance",
							ResourceType: "compute.googleapis.com/Instance",
						},
						ResourceName:      "//compute.googleapis.com/projects/test-project/zones/us-central1-a/instances/test-instance-name",
						ResourceContainer: "projects/12345",
						Location:          "us-central1",
						CreationTime:      testTimestamp,
						UpdationTime:      testTimestamp,
						ExpectedState:     dcpb.DatabaseResourceMetadata_HEALTHY,
						CurrentState:      dcpb.DatabaseResourceMetadata_HEALTHY,
						InstanceType:      dcpb.InstanceType_SUB_RESOURCE_TYPE_PRIMARY,
						Product: &dcpb.Product{
							Type:         dcpb.ProductType_PRODUCT_TYPE_COMPUTE_ENGINE,
							Engine:       dcpb.Engine_ENGINE_MYSQL,
							Version:      "8.0",
							MinorVersion: "8.0.26",
						},
						MachineConfiguration: &dcpb.MachineConfiguration{
							VcpuCount:         2,
							MemorySizeInBytes: 8388608, // 8 GiB
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid config postgres",
			config: &configpb.Configuration{
				CloudProperties: &configpb.CloudProperties{
					ProjectId:        "test-project",
					NumericProjectId: "12345",
					InstanceId:       "test-instance",
					InstanceName:     "test-instance-name",
					Region:           "us-central1",
					Zone:             "us-central1-a",
					VcpuCount:        2,
					MemorySizeMb:     8192,
				},
			},
			metrics: DBCenterMetrics{
				EngineType: POSTGRES,
				Metrics: map[string]string{
					"major_version": "17",
					"minor_version": "17.4",
				},
			},
			want: &dcpb.DatabaseResourceFeed{
				FeedTimestamp: testTimestamp,
				FeedType:      dcpb.DatabaseResourceFeed_RESOURCE_METADATA,
				Content: &dcpb.DatabaseResourceFeed_ResourceMetadata{
					ResourceMetadata: &dcpb.DatabaseResourceMetadata{
						Id: &dcpb.DatabaseResourceId{
							Provider:     dcpb.DatabaseResourceId_GCP,
							UniqueId:     "test-instance",
							ResourceType: "compute.googleapis.com/Instance",
						},
						ResourceName:      "//compute.googleapis.com/projects/test-project/zones/us-central1-a/instances/test-instance-name",
						ResourceContainer: "projects/12345",
						Location:          "us-central1",
						CreationTime:      testTimestamp,
						UpdationTime:      testTimestamp,
						ExpectedState:     dcpb.DatabaseResourceMetadata_HEALTHY,
						CurrentState:      dcpb.DatabaseResourceMetadata_HEALTHY,
						InstanceType:      dcpb.InstanceType_SUB_RESOURCE_TYPE_PRIMARY,
						Product: &dcpb.Product{
							Type:         dcpb.ProductType_PRODUCT_TYPE_COMPUTE_ENGINE,
							Engine:       dcpb.Engine_ENGINE_POSTGRES,
							Version:      "17",
							MinorVersion: "17.4",
						},
						MachineConfiguration: &dcpb.MachineConfiguration{
							VcpuCount:         2,
							MemorySizeInBytes: 8388608, // 8 GiB
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid config sqlserver",
			config: &configpb.Configuration{
				CloudProperties: &configpb.CloudProperties{
					ProjectId:        "test-project",
					NumericProjectId: "12345",
					InstanceId:       "test-instance",
					InstanceName:     "test-instance-name",
					Region:           "us-central1",
					Zone:             "us-central1-a",
					VcpuCount:        2,
					MemorySizeMb:     8192,
				},
			},
			metrics: DBCenterMetrics{
				EngineType: SQLSERVER,
				Metrics: map[string]string{
					"major_version": "SQL Server 2022 Express",
					"minor_version": "CU13",
				},
			},
			want: &dcpb.DatabaseResourceFeed{
				FeedTimestamp: testTimestamp,
				FeedType:      dcpb.DatabaseResourceFeed_RESOURCE_METADATA,
				Content: &dcpb.DatabaseResourceFeed_ResourceMetadata{
					ResourceMetadata: &dcpb.DatabaseResourceMetadata{
						Id: &dcpb.DatabaseResourceId{
							Provider:     dcpb.DatabaseResourceId_GCP,
							UniqueId:     "test-instance",
							ResourceType: "compute.googleapis.com/Instance",
						},
						ResourceName:      "//compute.googleapis.com/projects/test-project/zones/us-central1-a/instances/test-instance-name",
						ResourceContainer: "projects/12345",
						Location:          "us-central1",
						CreationTime:      testTimestamp,
						UpdationTime:      testTimestamp,
						ExpectedState:     dcpb.DatabaseResourceMetadata_HEALTHY,
						CurrentState:      dcpb.DatabaseResourceMetadata_HEALTHY,
						InstanceType:      dcpb.InstanceType_SUB_RESOURCE_TYPE_PRIMARY,
						Product: &dcpb.Product{
							Type:         dcpb.ProductType_PRODUCT_TYPE_COMPUTE_ENGINE,
							Engine:       dcpb.Engine_ENGINE_SQL_SERVER,
							Version:      "SQL Server 2022 Express",
							MinorVersion: "CU13",
						},
						MachineConfiguration: &dcpb.MachineConfiguration{
							VcpuCount:         2,
							MemorySizeInBytes: 8388608, // 8 GiB
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name:   "empty config",
			config: &configpb.Configuration{},
			metrics: DBCenterMetrics{
				EngineType: POSTGRES,
				Metrics: map[string]string{
					"major_version": "17",
					"minor_version": "17.4",
				},
			},
			wantErr: false, // The function will still build a message with empty values.
			want: &dcpb.DatabaseResourceFeed{
				FeedTimestamp: testTimestamp,
				FeedType:      dcpb.DatabaseResourceFeed_RESOURCE_METADATA,
				Content: &dcpb.DatabaseResourceFeed_ResourceMetadata{
					ResourceMetadata: &dcpb.DatabaseResourceMetadata{
						Id: &dcpb.DatabaseResourceId{
							Provider:     dcpb.DatabaseResourceId_GCP,
							ResourceType: "compute.googleapis.com/Instance",
						},
						ResourceName:      "//compute.googleapis.com/projects//zones//instances/",
						ResourceContainer: "projects/",
						CreationTime:      testTimestamp,
						UpdationTime:      testTimestamp,
						ExpectedState:     dcpb.DatabaseResourceMetadata_HEALTHY,
						CurrentState:      dcpb.DatabaseResourceMetadata_HEALTHY,
						InstanceType:      dcpb.InstanceType_SUB_RESOURCE_TYPE_PRIMARY,
						Product: &dcpb.Product{
							Type:         dcpb.ProductType_PRODUCT_TYPE_COMPUTE_ENGINE,
							Engine:       dcpb.Engine_ENGINE_POSTGRES,
							Version:      "17",
							MinorVersion: "17.4",
						},
						MachineConfiguration: &dcpb.MachineConfiguration{
							VcpuCount:         0,
							MemorySizeInBytes: 0,
						},
					},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := realClient{Config: tc.config, CommClient: &MockCommunication{}}

			gotAny, err := client.buildDatabaseResourceMetadataMessage(context.Background(), tc.metrics)
			if (err != nil) != tc.wantErr {
				t.Fatalf("buildDatabaseResourceMetadataMessage() error = %v, wantErr %v", err, tc.wantErr)
			}
			if err != nil {
				return
			}
			got := &dcpb.DatabaseResourceFeed{}
			if err := gotAny.UnmarshalTo(got); err != nil {
				t.Fatalf("Failed to unmarshal any to DatabaseResourceFeed: %v", err)
			}
			if diff := cmp.Diff(tc.want, got, protocmp.Transform(), protocmp.IgnoreFields(&dcpb.DatabaseResourceFeed{}, "feed_timestamp"), protocmp.IgnoreFields(&dcpb.DatabaseResourceMetadata{}, "creation_time", "updation_time")); diff != "" {
				t.Errorf("buildDatabaseResourceMetadataMessage() returned unexpected diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSendMetadataToDatabaseCenter(t *testing.T) {
	defaultConfig := &configpb.Configuration{
		CloudProperties: &configpb.CloudProperties{
			ProjectId:        "test-project",
			NumericProjectId: "12345",
			InstanceId:       "test-instance",
			InstanceName:     "test-instance-name",
			Region:           "us-central1",
			Zone:             "us-central1-a",
			VcpuCount:        1,
			MemorySizeMb:     1024,
		},
	}

	tests := []struct {
		name                        string
		config                      *configpb.Configuration
		metrics                     DBCenterMetrics
		establishACSConnectionError error
		sendAgentMessageError       error
		wantErr                     error
		wantSendCallCount           int
		wantSignals                 map[dcpb.SignalType]bool
	}{
		{
			name:   "success no signals",
			config: defaultConfig,
			metrics: DBCenterMetrics{
				EngineType: SQLSERVER,
				Metrics: map[string]string{
					"major_version": "SQL Server 2022 Express",
					"minor_version": "CU13",
				},
			},
			wantSendCallCount: 1,
			wantSignals:       map[dcpb.SignalType]bool{},
		},
		{
			name:   "success with signals",
			config: defaultConfig,
			metrics: DBCenterMetrics{
				EngineType: MYSQL,
				Metrics: map[string]string{
					"major_version":             "8.0",
					"minor_version":             "8.0.26",
					NoRootPasswordKey:           "true",
					ExposedToPublicAccessKey:    "false",
					DatabaseAuditingDisabledKey: "true",
					UnencryptedConnectionsKey:   "TRUE", // Should be false
				},
			},
			wantSendCallCount: 5, // 1 metadata + 4 signals
			wantSignals: map[dcpb.SignalType]bool{
				dcpb.SignalType_SIGNAL_TYPE_NO_ROOT_PASSWORD:           true,
				dcpb.SignalType_SIGNAL_TYPE_EXPOSED_TO_PUBLIC_ACCESS:   false,
				dcpb.SignalType_SIGNAL_TYPE_DATABASE_AUDITING_DISABLED: true,
				dcpb.SignalType_SIGNAL_TYPE_UNENCRYPTED_CONNECTIONS:    false,
			},
		},
		{
			name:   "failed to send metadata message",
			config: defaultConfig,
			metrics: DBCenterMetrics{
				EngineType: MYSQL,
				Metrics: map[string]string{
					"major_version": "8.0",
				},
			},
			sendAgentMessageError: fmt.Errorf("send error"),
			wantErr:               fmt.Errorf("failed to send metadata message to database center: send error"),
			wantSendCallCount:     1,
			wantSignals:           map[dcpb.SignalType]bool{},
		},
		{
			name:   "failed to send signal message",
			config: defaultConfig,
			metrics: DBCenterMetrics{
				EngineType: MYSQL,
				Metrics: map[string]string{
					"major_version":   "8.0",
					NoRootPasswordKey: "true",
				},
			},
			sendAgentMessageError: fmt.Errorf("send error"),
			wantErr:               fmt.Errorf("failed to send metadata message to database center: send error"), // Error is from the first send
			wantSendCallCount:     1,
			wantSignals:           map[dcpb.SignalType]bool{},
		},
		{
			name:                        "failed to establish connection",
			config:                      &configpb.Configuration{},
			metrics:                     DBCenterMetrics{EngineType: MYSQL},
			establishACSConnectionError: fmt.Errorf("connection error"),
			wantErr:                     fmt.Errorf("failed to establish ACS connection: connection error"),
			wantSendCallCount:           0,
			wantSignals:                 map[dcpb.SignalType]bool{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockComm := &MockCommunication{
				establishACSConnectionError: tc.establishACSConnectionError,
				sendAgentMessageError:       tc.sendAgentMessageError,
			}
			client := NewClient(tc.config, mockComm)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			err := client.SendMetadataToDatabaseCenter(ctx, tc.metrics)
			if err != nil && err.Error() != tc.wantErr.Error() {
				t.Errorf("SendMetadataToDatabaseCenter() error = %v, wantErr %v", err, tc.wantErr)
			}
			if len(mockComm.Calls) != tc.wantSendCallCount {
				t.Errorf("SendMetadataToDatabaseCenter(%v) unexpected number of SendAgentMessage calls: got %d, want %d", tc.metrics, len(mockComm.Calls), tc.wantSendCallCount)
			}
		})
	}
}
