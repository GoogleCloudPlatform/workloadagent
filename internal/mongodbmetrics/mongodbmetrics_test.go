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

package mongodbmetrics

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/api/googleapi"
	"github.com/GoogleCloudPlatform/workloadagent/internal/databasecenter"
	"github.com/GoogleCloudPlatform/workloadagent/internal/workloadmanager"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce/wlm"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/secret"
	dwpb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/datawarehouse"
)

// mockGCE is a mock implementation of the gceInterface.
type mockGCE struct {
	getSecret func(ctx context.Context, projectID, secretName string) (string, error)
}

func (m *mockGCE) GetSecret(ctx context.Context, projectID, secretName string) (string, error) {
	if m.getSecret != nil {
		return m.getSecret(ctx, projectID, secretName)
	}
	return "", errors.New("GetSecret not implemented")
}

// mockWLMClient is a mock implementation of the workloadmanager.WLMWriter.
type mockWLMClient struct {
	writeInsight func(ctx context.Context, req *dwpb.WriteInsightRequest) (*wlm.WriteInsightResponse, error)
}

func (m *mockWLMClient) WriteInsightAndGetResponse(project, location string, req *dwpb.WriteInsightRequest) (*wlm.WriteInsightResponse, error) {
	if m.writeInsight != nil {
		return m.writeInsight(context.Background(), req)
	}
	return nil, errors.New("WriteInsight not implemented")
}

func TestPassword(t *testing.T) {
	tests := []struct {
		name    string
		config  *configpb.Configuration
		gce     *mockGCE
		want    secret.String
		wantErr bool
	}{
		{
			name: "PasswordFromConfig",
			config: &configpb.Configuration{
				MongoDbConfiguration: &configpb.MongoDBConfiguration{
					ConnectionParameters: &configpb.ConnectionParameters{
						Password: "configpassword",
					},
				},
			},
			want: secret.String("configpassword"),
		},
		{
			name: "PasswordFromSecretManager",
			config: &configpb.Configuration{
				MongoDbConfiguration: &configpb.MongoDBConfiguration{
					ConnectionParameters: &configpb.ConnectionParameters{
						Secret: &configpb.SecretRef{
							ProjectId:  "test-project",
							SecretName: "test-secret",
						},
					},
				},
			},
			gce: &mockGCE{
				getSecret: func(ctx context.Context, projectID, secretName string) (string, error) {
					if projectID == "test-project" && secretName == "test-secret" {
						return "secretpassword", nil
					}
					return "", errors.New("secret not found")
				},
			},
			want: secret.String("secretpassword"),
		},
		{
			name: "SecretManagerError",
			config: &configpb.Configuration{
				MongoDbConfiguration: &configpb.MongoDBConfiguration{
					ConnectionParameters: &configpb.ConnectionParameters{
						Secret: &configpb.SecretRef{
							ProjectId:  "test-project",
							SecretName: "test-secret",
						},
					},
				},
			},
			gce: &mockGCE{
				getSecret: func(ctx context.Context, projectID, secretName string) (string, error) {
					return "", errors.New("gce error")
				},
			},
			wantErr: true,
		},
		{
			name:   "NoPasswordConfigured",
			config: &configpb.Configuration{},
			want:   secret.String(""),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := &MongoDBMetrics{Config: tc.config}
			got, err := m.password(context.Background(), tc.gce)

			if (err != nil) != tc.wantErr {
				t.Errorf("password() got error: %v, wantErr: %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("password() got: %v, want: %v", got, tc.want)
			}
		})
	}
}

type mockMongoDatabase struct {
	RunCommandFunc func(ctx context.Context, runCommand any, opts ...*options.RunCmdOptions) *mongo.SingleResult
}

func (m *mockMongoDatabase) RunCommand(ctx context.Context, runCommand any, opts ...*options.RunCmdOptions) *mongo.SingleResult {
	if m.RunCommandFunc != nil {
		return m.RunCommandFunc(ctx, runCommand)
	}
	// Create a SingleResult with an error
	return mongo.NewSingleResultFromDocument(nil, errors.New("RunCommand not implemented"), nil)
}

// mockSingleResult is a mock implementation of mongo.SingleResult.
type mockSingleResult struct {
	DecodeFunc func(v any) error
	ErrFunc    func() error
}

func (m *mockSingleResult) Decode(v any) error {
	if m.DecodeFunc != nil {
		return m.DecodeFunc(v)
	}
	return errors.New("Decode not implemented")
}

func (m *mockSingleResult) Err() error {
	if m.ErrFunc != nil {
		return m.ErrFunc()
	}
	return nil // Default to no error
}

func TestGetVersion(t *testing.T) {
	tests := []struct {
		name       string
		runCommand func(ctx context.Context, client *mongo.Client, dbName string, cmd bson.D, receiver any) (any, error)
		want       string
		wantErr    bool
	}{
		{
			name: "Successful",
			runCommand: func(ctx context.Context, client *mongo.Client, dbName string, cmd bson.D, receiver any) (any, error) {
				return bson.D{
					{
						Key:   "version",
						Value: "1.0.0",
					},
				}, nil
			},
			want:    "1.0.0",
			wantErr: false,
		},
		{
			name: "CommandError",
			runCommand: func(ctx context.Context, client *mongo.Client, dbName string, cmd bson.D, receiver any) (any, error) {
				return nil, errors.New("command error")
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := &MongoDBMetrics{
				RunCommand: tc.runCommand,
			}
			got, err := m.version(context.Background())

			if tc.wantErr {
				if err == nil {
					t.Errorf("getWorkMem() expected an error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("getWorkMem() got unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("getWorkMem() got: %v, want: %v", got, tc.want)
			}
		})
	}
}

func TestCollectMetricsOnce(t *testing.T) {
	tests := []struct {
		name     string
		m        *MongoDBMetrics
		dwActive bool
		version  func(ctx context.Context) (string, error)
		sendData func(ctx context.Context, params workloadmanager.SendDataInsightParams) (*wlm.WriteInsightResponse, error)
		want     *workloadmanager.WorkloadMetrics
		wantErr  bool
	}{
		{
			name: "SuccessfulCollection",
			m: &MongoDBMetrics{
				Config: &configpb.Configuration{CloudProperties: &configpb.CloudProperties{ProjectId: "test-proj"}},
				WLMClient: &mockWLMClient{
					writeInsight: func(ctx context.Context, req *dwpb.WriteInsightRequest) (*wlm.WriteInsightResponse, error) {
						return &wlm.WriteInsightResponse{ServerResponse: googleapi.ServerResponse{HTTPStatusCode: 200}}, nil
					},
				},
				RunCommand: func(ctx context.Context, client *mongo.Client, dbName string, cmd bson.D, receiver any) (any, error) {
					return bson.D{
						{
							Key:   "version",
							Value: "1.0.0",
						},
					}, nil
				},
			},
			dwActive: true,
			want: &workloadmanager.WorkloadMetrics{
				WorkloadType: workloadmanager.MONGODB,
				Metrics:      map[string]string{versionKey: "1.0.0"},
			},
		},
		{
			name: "VersionError",
			m: &MongoDBMetrics{
				RunCommand: func(ctx context.Context, client *mongo.Client, dbName string, cmd bson.D, receiver any) (any, error) {
					return nil, errors.New("command error")
				},
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.m.CollectMetricsOnce(context.Background(), false)
			gotErr := err != nil
			if gotErr && !tc.wantErr {
				t.Errorf("CollectMetricsOnce() got error: %v, wantErr: %v", err, tc.wantErr)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("CollectMetricsOnce() returned diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestNew(t *testing.T) {
	config := &configpb.Configuration{}
	var wlmClient mockWLMClient
	var dbcenterClient databasecenter.Client

	m := New(context.Background(), config, &wlmClient, dbcenterClient, DefaultRunCommand)

	if m == nil {
		t.Fatalf("New() returned nil, want non-nil")
	}
	if m.Config != config {
		t.Errorf("New() Config got: %v, want: %v", m.Config, config)
	}
	if m.WLMClient != &wlmClient {
		t.Errorf("New() WLMClient got: %v, want: %v", m.WLMClient, &wlmClient)
	}
	if m.DBcenterClient != dbcenterClient {
		t.Errorf("New() DBcenterClient got: %v, want: %v", m.DBcenterClient, dbcenterClient)
	}
	if m.execute == nil {
		t.Errorf("New() execute function is nil, want non-nil")
	}
}

func TestInitDB(t *testing.T) {
	tests := []struct {
		name    string
		m       *MongoDBMetrics
		gce     *mockGCE
		wantErr bool
		// a substring of the error message that is unlikely to change.
		errStr string
	}{
		{
			name: "PasswordFromConfig",
			m: &MongoDBMetrics{
				Config: &configpb.Configuration{
					MongoDbConfiguration: &configpb.MongoDBConfiguration{
						ConnectionParameters: &configpb.ConnectionParameters{
							Username: "test-user",
							Password: "configpassword",
						},
					},
				},
			},
			gce: &mockGCE{
				getSecret: func(ctx context.Context, projectID, secretName string) (string, error) {
					return "", errors.New("gce error")
				},
			},
			wantErr: true,
			errStr:  "failed to ping",
		},
		{
			name: "PasswordFromSecretManager",
			m: &MongoDBMetrics{
				Config: &configpb.Configuration{
					MongoDbConfiguration: &configpb.MongoDBConfiguration{
						ConnectionParameters: &configpb.ConnectionParameters{
							Username: "test-user",
							Secret: &configpb.SecretRef{
								ProjectId:  "test-project",
								SecretName: "test-secret",
							},
						},
					},
				},
			},
			gce: &mockGCE{
				getSecret: func(ctx context.Context, projectID, secretName string) (string, error) {
					return "fakesecretpassword", nil
				},
			},
			wantErr: true,
			errStr:  "failed to ping",
		},
		{
			name: "NoPasswordConfigured",
			m: &MongoDBMetrics{
				Config: &configpb.Configuration{
					MongoDbConfiguration: &configpb.MongoDBConfiguration{
						ConnectionParameters: &configpb.ConnectionParameters{},
					},
				},
			},
			gce: &mockGCE{
				getSecret: func(ctx context.Context, projectID, secretName string) (string, error) {
					return "", errors.New("gce error")
				},
			},
			wantErr: true,
			errStr:  "error validating uri",
		},
		{
			name: "SecretManagerError",
			m: &MongoDBMetrics{
				Config: &configpb.Configuration{
					MongoDbConfiguration: &configpb.MongoDBConfiguration{
						ConnectionParameters: &configpb.ConnectionParameters{
							Username: "test-user",
							Secret: &configpb.SecretRef{
								ProjectId:  "test-project",
								SecretName: "test-secret",
							},
						},
					},
				},
			},
			gce: &mockGCE{
				getSecret: func(ctx context.Context, projectID, secretName string) (string, error) {
					return "", errors.New("gce error")
				},
			},
			wantErr: true,
			errStr:  "secret manager",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.m.InitDB(context.Background(), tc.gce, 10*time.Millisecond)
			gotErr := err != nil
			if gotErr != tc.wantErr {
				t.Errorf("InitDB() got error: %v, wantErr: %v", err, tc.wantErr)
			}
			if tc.wantErr && gotErr && !strings.Contains(err.Error(), tc.errStr) {
				t.Errorf("InitDB() got error: %v, wantErr: %v", err, tc.errStr)
			}
		})
	}
}
