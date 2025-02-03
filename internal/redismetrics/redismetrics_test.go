/*
Copyright 2024 Google LLC

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

package redismetrics

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/redis/go-redis/v9"
	"google.golang.org/api/googleapi"
	"google.golang.org/protobuf/testing/protocmp"
	"github.com/GoogleCloudPlatform/workloadagent/internal/workloadmanager"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	gcefake "github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/gce/fake"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/gce/wlm"
)

type testGCE struct {
	secret string
	err    error
}

func (t *testGCE) GetSecret(ctx context.Context, projectID, secretName string) (string, error) {
	return t.secret, t.err
}

type testDB struct {
	info         *redis.StringCmd
	saveConfig   *redis.MapStringStringCmd
	appendConfig *redis.MapStringStringCmd
	infoErr      error
	cfgErr       error
	addr         string
	db           int
}

func (t *testDB) Info(ctx context.Context, args ...string) *redis.StringCmd {
	return t.info
}

func (t *testDB) ConfigGet(ctx context.Context, key string) *redis.MapStringStringCmd {
	if key == appendonly {
		return t.appendConfig
	}
	if key == save {
		return t.saveConfig
	}
	return nil
}

func (t *testDB) String() string {
	return fmt.Sprintf("Redis<%s db:%d>", t.addr, t.db)
}

func TestInitPassword(t *testing.T) {
	tests := []struct {
		name    string
		r       RedisMetrics
		gce     *gcefake.TestGCE
		want    string
		wantErr bool
	}{
		{
			name:    "Default",
			r:       RedisMetrics{},
			want:    "",
			wantErr: false,
		},
		{
			name: "GCEErr",
			r: RedisMetrics{
				Config: &configpb.Configuration{
					RedisConfiguration: &configpb.RedisConfiguration{
						ConnectionParameters: &configpb.ConnectionParameters{
							Secret: &configpb.SecretRef{ProjectId: "fake-project-id", SecretName: "fake-secret-name"},
						},
					},
				},
			},
			gce: &gcefake.TestGCE{
				GetSecretResp: []string{""},
				GetSecretErr:  []error{errors.New("fake-error")},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "GCESecret",
			r: RedisMetrics{
				Config: &configpb.Configuration{
					RedisConfiguration: &configpb.RedisConfiguration{
						ConnectionParameters: &configpb.ConnectionParameters{
							Secret: &configpb.SecretRef{ProjectId: "fake-project-id", SecretName: "fake-secret-name"},
						},
					},
				},
			},
			gce: &gcefake.TestGCE{
				GetSecretResp: []string{"fake-password"},
				GetSecretErr:  []error{nil},
			},
			want:    "fake-password",
			wantErr: false,
		},
		{
			name: "MissingProjectId",
			r: RedisMetrics{
				Config: &configpb.Configuration{
					RedisConfiguration: &configpb.RedisConfiguration{
						ConnectionParameters: &configpb.ConnectionParameters{
							Secret: &configpb.SecretRef{SecretName: "fake-secret-name"},
						},
					},
				},
			},
			gce: &gcefake.TestGCE{
				GetSecretResp: []string{"fake-password"},
				GetSecretErr:  []error{nil},
			},
			want:    "",
			wantErr: false,
		},
		{
			name: "MissingSecretName",
			r: RedisMetrics{
				Config: &configpb.Configuration{
					RedisConfiguration: &configpb.RedisConfiguration{
						ConnectionParameters: &configpb.ConnectionParameters{
							Secret: &configpb.SecretRef{ProjectId: "fake-project-id"},
						},
					},
				},
			},
			gce: &gcefake.TestGCE{
				GetSecretResp: []string{"fake-password"},
				GetSecretErr:  []error{nil},
			},
			want:    "",
			wantErr: false,
		},
		{
			name: "UsingPassword",
			r: RedisMetrics{
				Config: &configpb.Configuration{
					RedisConfiguration: &configpb.RedisConfiguration{
						ConnectionParameters: &configpb.ConnectionParameters{
							Password: "fake-password",
						},
					},
				},
			},
			want:    "fake-password",
			wantErr: false,
		},
	}
	for _, tc := range tests {
		got, err := tc.r.password(context.Background(), tc.gce)
		gotErr := err != nil
		if gotErr != tc.wantErr {
			t.Errorf("password() = %v, wantErr %v", err, tc.wantErr)
		}
		if got.SecretValue() != tc.want {
			t.Errorf("password() = %v, want %v", got, tc.want)
		}
	}
}

func TestCollectMetricsOnce(t *testing.T) {
	tests := []struct {
		name             string
		r                RedisMetrics
		stringCmdValue   string
		saveMapContent   map[string]string
		appendMapContent map[string]string
		wlmClient        workloadmanager.WLMWriter
		want             *workloadmanager.WorkloadMetrics
		wantErr          bool
	}{
		{
			name: "HappyPath",
			r: RedisMetrics{
				Config: &configpb.Configuration{
					CloudProperties: &configpb.CloudProperties{
						ProjectId: "fake-project-id",
					},
				},
				WLMClient: &gcefake.TestWLM{
					WriteInsightErrs: []error{nil},
					WriteInsightResponses: []*wlm.WriteInsightResponse{
						&wlm.WriteInsightResponse{ServerResponse: googleapi.ServerResponse{HTTPStatusCode: 201}},
					},
				},
			},
			stringCmdValue: "role:master\nconnected_slaves:1\n",
			saveMapContent: map[string]string{
				"save": "3600 1 300 100 60 10000",
			},
			appendMapContent: map[string]string{
				"appendonly": "no",
			},
			want: &workloadmanager.WorkloadMetrics{
				WorkloadType: workloadmanager.REDIS,
				Metrics: map[string]string{
					replicationKey: "true",
					persistenceKey: "true",
				},
			},
			wantErr: false,
		},
		{
			name: "WLMError",
			r: RedisMetrics{
				Config: &configpb.Configuration{
					CloudProperties: &configpb.CloudProperties{
						ProjectId: "fake-project-id",
					},
				},
				WLMClient: &gcefake.TestWLM{
					WriteInsightErrs: []error{errors.New("fake-error")},
					WriteInsightResponses: []*wlm.WriteInsightResponse{
						&wlm.WriteInsightResponse{ServerResponse: googleapi.ServerResponse{HTTPStatusCode: 400}},
					},
				},
			},
			stringCmdValue: "role:master\nconnected_slaves:1\n",
			saveMapContent: map[string]string{
				"save": "3600 1 300 100 60 10000",
			},
			appendMapContent: map[string]string{
				"appendonly": "no",
			},
			want:    nil,
			wantErr: true,
		},
	}

	ctx := context.Background()

	for _, tc := range tests {
		testDB := &testDB{
			info:         &redis.StringCmd{},
			saveConfig:   &redis.MapStringStringCmd{},
			appendConfig: &redis.MapStringStringCmd{},
		}
		testDB.info.SetVal(tc.stringCmdValue)
		testDB.saveConfig.SetVal(tc.saveMapContent)
		testDB.appendConfig.SetVal(tc.appendMapContent)
		tc.r.db = testDB

		got, err := tc.r.CollectMetricsOnce(ctx)
		if tc.wantErr {
			if err == nil {
				t.Errorf("CollectMetricsOnce(%v) returned no error, want error", tc.name)
			}
			continue
		}
		if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
			t.Errorf("CollectMetricsOnce(%v) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestReplicationModeActive(t *testing.T) {
	tests := []struct {
		name           string
		stringCmdValue string
		want           bool
	}{
		{
			name:           "HappyPathMain",
			stringCmdValue: "role:master\nconnected_slaves:1\n",
			want:           true,
		},
		{
			name:           "HappyPathWorker",
			stringCmdValue: "role:slave\nmaster_link_status:up\n",
			want:           true,
		},
		{
			name:           "NoWorkers",
			stringCmdValue: "role:master\nconnected_slaves:0\n",
			want:           false,
		},
		{
			name:           "WorkerNotUp",
			stringCmdValue: "role:slave\nmaster_link_status:down\n",
			want:           false,
		},
		{
			name:           "EmptySpaceHappyPathMain",
			stringCmdValue: "role:master\n     connected_slaves:1\n",
			want:           true,
		},
		{
			name:           "EmptySpaceHappyPathWorker",
			stringCmdValue: "role:slave\n     master_link_status:up\n",
			want:           true,
		},
		{
			name:           "UnknownRole",
			stringCmdValue: "role:unknown\n",
			want:           false,
		},
	}
	for _, tc := range tests {
		testDB := &testDB{info: &redis.StringCmd{}}
		testDB.info.SetVal(tc.stringCmdValue)
		r := RedisMetrics{
			db: testDB,
		}

		got := r.replicationModeActive(context.Background())
		if got != tc.want {
			t.Errorf("replicationModeActive() test %v = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestPersistenceEnabled(t *testing.T) {
	tests := []struct {
		name             string
		saveMapContent   map[string]string
		appendMapContent map[string]string
		want             bool
	}{
		{
			name: "HappyPathSave",
			saveMapContent: map[string]string{
				"save": "3600 1 300 100 60 10000",
			},
			appendMapContent: map[string]string{
				"appendonly": "no",
			},
			want: true,
		},
		{
			name: "HappyPathAppendonly",
			saveMapContent: map[string]string{
				"save": "",
			},
			appendMapContent: map[string]string{
				"appendonly": "yes",
			},
			want: true,
		},
		{
			name: "Disabled",
			saveMapContent: map[string]string{
				"save": "",
			},
			appendMapContent: map[string]string{
				"appendonly": "no",
			},
			want: false,
		},
		{
			name:             "Empty",
			saveMapContent:   nil,
			appendMapContent: nil,
			want:             false,
		},
	}
	for _, tc := range tests {
		testDB := &testDB{
			saveConfig:   &redis.MapStringStringCmd{},
			appendConfig: &redis.MapStringStringCmd{},
		}
		testDB.saveConfig.SetVal(tc.saveMapContent)
		testDB.appendConfig.SetVal(tc.appendMapContent)
		r := RedisMetrics{
			db: testDB,
		}
		got := r.persistenceEnabled(context.Background())
		if got != tc.want {
			t.Errorf("persistenceEnabled() test %v = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestInitDB(t *testing.T) {
	tests := []struct {
		name       string
		r          RedisMetrics
		gceService gceInterface
		want       string
		wantErr    bool
	}{
		{
			name: "HappyPath",
			r: RedisMetrics{
				Config: &configpb.Configuration{
					RedisConfiguration: &configpb.RedisConfiguration{
						ConnectionParameters: &configpb.ConnectionParameters{
							Secret: &configpb.SecretRef{ProjectId: "fake-project-id", SecretName: "fake-secret-name"},
						},
					},
				},
			},
			gceService: &gcefake.TestGCE{
				GetSecretResp: []string{"fake-password"},
				GetSecretErr:  []error{nil},
			},
			want:    fmt.Sprintf("Redis<%s db:%d>", "localhost:6379", 0),
			wantErr: false,
		},
		{
			name: "ConfigPassword",
			r: RedisMetrics{
				Config: &configpb.Configuration{
					RedisConfiguration: &configpb.RedisConfiguration{
						ConnectionParameters: &configpb.ConnectionParameters{
							Password: "fake-password",
						},
					},
				},
			},
			gceService: &gcefake.TestGCE{},
			want:       fmt.Sprintf("Redis<%s db:%d>", "localhost:6379", 0),
			wantErr:    false,
		},
		{
			name: "ConfigPort",
			r: RedisMetrics{
				Config: &configpb.Configuration{
					RedisConfiguration: &configpb.RedisConfiguration{
						ConnectionParameters: &configpb.ConnectionParameters{
							Port:     1234,
							Password: "fake-password",
						},
					},
				},
			},
			gceService: &gcefake.TestGCE{},
			want:       fmt.Sprintf("Redis<%s db:%d>", "localhost:1234", 0),
			wantErr:    false,
		},
		{
			name: "PasswordError",
			r: RedisMetrics{
				Config: &configpb.Configuration{
					RedisConfiguration: &configpb.RedisConfiguration{
						ConnectionParameters: &configpb.ConnectionParameters{
							Secret: &configpb.SecretRef{ProjectId: "fake-project-id", SecretName: "fake-secret-name"},
						},
					},
				},
			},
			gceService: &gcefake.TestGCE{
				GetSecretResp: []string{""},
				GetSecretErr:  []error{errors.New("fake-error")},
			},
			want:    fmt.Sprintf("Redis<%s db:%d>", "localhost:6379", 0),
			wantErr: true,
		},
	}

	ctx := context.Background()

	for _, tc := range tests {
		err := tc.r.InitDB(ctx, tc.gceService, &gcefake.TestWLM{})
		gotErr := err != nil
		if gotErr != tc.wantErr {
			t.Errorf("InitDB(%v) = %v, wantErr %v", tc.name, err, tc.wantErr)
		}
		if !gotErr && (tc.r.db.String() != tc.want || tc.r.WLMClient == nil) {
			t.Errorf("InitDB(%v) = %v, want %v", tc.name, tc.r.db.String(), tc.want)
		}
	}
}
