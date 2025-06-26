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

type MockCommunication struct {
	sendAgentMessageCalled       bool
	establishACSConnectionCalled bool
	sendAgentMessageError        error
	establishACSConnectionError  error
}

func (m *MockCommunication) EstablishACSConnection(ctx context.Context, endpoint string, channel string) (*client.Connection, error) {
	m.establishACSConnectionCalled = true
	return &client.Connection{}, m.establishACSConnectionError
}

func (m *MockCommunication) SendAgentMessage(ctx context.Context, agentType string, messageType string, msg *anypb.Any, conn *client.Connection) error {
	m.sendAgentMessageCalled = true
	return m.sendAgentMessageError
}

func TestBuildCondorMessage(t *testing.T) {
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
					},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := realClient{Config: tc.config, CommClient: &MockCommunication{}}

			gotAny, err := client.buildCondorMessage(context.Background(), tc.metrics)
			if (err != nil) != tc.wantErr {
				t.Fatalf("buildCondorMessage() error = %v, wantErr %v", err, tc.wantErr)
			}
			if err != nil {
				return
			}
			got := &dcpb.DatabaseResourceFeed{}
			if err := gotAny.UnmarshalTo(got); err != nil {
				t.Fatalf("Failed to unmarshal any to DatabaseResourceFeed: %v", err)
			}
			if diff := cmp.Diff(tc.want, got, protocmp.Transform(), protocmp.IgnoreFields(&dcpb.DatabaseResourceFeed{}, "feed_timestamp"), protocmp.IgnoreFields(&dcpb.DatabaseResourceMetadata{}, "creation_time", "updation_time")); diff != "" {
				t.Errorf("buildCondorMessage() returned unexpected diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSendMetadataToDatabaseCenter(t *testing.T) {
	tests := []struct {
		name                        string
		config                      *configpb.Configuration
		metrics                     DBCenterMetrics
		establishACSConnectionError error
		sendAgentMessageError       error
		wantErr                     error
	}{
		{
			name: "success",
			config: &configpb.Configuration{
				CloudProperties: &configpb.CloudProperties{
					ProjectId:        "test-project",
					NumericProjectId: "12345",
					InstanceId:       "test-instance",
					InstanceName:     "test-instance-name",
					Region:           "us-central1",
					Zone:             "us-central1-a",
				},
			},
			metrics: DBCenterMetrics{
				EngineType: SQLSERVER,
				Metrics: map[string]string{
					"version":       "SQL Server 2022 Express",
					"minor_version": "CU13",
				},
			},
			establishACSConnectionError: nil,
			sendAgentMessageError:       nil,
			wantErr:                     nil,
		},
		{
			name: "failed to send message",
			config: &configpb.Configuration{
				CloudProperties: &configpb.CloudProperties{
					ProjectId:        "test-project",
					NumericProjectId: "12345",
					InstanceId:       "test-instance",
					InstanceName:     "test-instance-name",
					Region:           "us-central1",
					Zone:             "us-central1-a",
				},
			},
			metrics: DBCenterMetrics{
				EngineType: MYSQL,
				Metrics: map[string]string{
					"version":       "8.0",
					"minor_version": "8.0.26",
				},
			},
			establishACSConnectionError: nil,
			sendAgentMessageError:       fmt.Errorf(""),
			wantErr:                     fmt.Errorf("failed to send message to database center: "),
		},
		{
			name:                        "failed to establish connection",
			config:                      &configpb.Configuration{},
			establishACSConnectionError: fmt.Errorf(""),
			sendAgentMessageError:       nil,
			wantErr:                     fmt.Errorf("failed to establish ACS connection: "),
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
		})
	}
}
