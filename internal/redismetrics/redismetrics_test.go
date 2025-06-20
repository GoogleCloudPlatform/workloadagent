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
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/commandlineexecutor"
	gcefake "github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce/fake"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce/wlm"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/osinfo"
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
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.r.password(context.Background(), tc.gce)
			gotErr := err != nil
			if gotErr != tc.wantErr {
				t.Errorf("password() = %v, wantErr %v", err, tc.wantErr)
			}
			if got.SecretValue() != tc.want {
				t.Errorf("password() = %v, want %v", got, tc.want)
			}
		})
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
				OSData: osinfo.Data{OSName: "linux", OSVendor: "rhel", OSVersion: "9.4"},
				execute: func(ctx context.Context, p commandlineexecutor.Params) commandlineexecutor.Result {
					switch p.Args[0] {
					case "is-enabled":
						return commandlineexecutor.Result{StdOut: "enabled"}
					case "show":
						return commandlineexecutor.Result{StdOut: "Restart=always"}
					default:
						return commandlineexecutor.Result{Error: errors.New("unknown command")}
					}
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
					replicationKey:      "true",
					persistenceKey:      "true",
					serviceEnabledKey:   "true",
					serviceRestartKey:   "true",
					replicationZonesKey: "",
					currentRoleKey:      main,
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
				OSData: osinfo.Data{OSName: "linux", OSVendor: "rhel", OSVersion: "9.4"},
				execute: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{}
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
		{
			name: "NilWriteInsightResponse",
			r: RedisMetrics{
				Config: &configpb.Configuration{
					CloudProperties: &configpb.CloudProperties{
						ProjectId: "fake-project-id",
					},
				},
				OSData: osinfo.Data{OSName: "linux", OSVendor: "rhel", OSVersion: "9.4"},
				execute: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{}
				},
				WLMClient: &gcefake.TestWLM{
					WriteInsightErrs:      []error{nil},
					WriteInsightResponses: []*wlm.WriteInsightResponse{nil},
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
					replicationKey:      "true",
					persistenceKey:      "true",
					serviceEnabledKey:   "true",
					serviceRestartKey:   "true",
					replicationZonesKey: "",
					currentRoleKey:      main,
				},
			},
			wantErr: false,
		},
	}

	ctx := context.Background()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testDB := &testDB{
				info:         &redis.StringCmd{},
				saveConfig:   &redis.MapStringStringCmd{},
				appendConfig: &redis.MapStringStringCmd{},
			}
			testDB.info.SetVal(tc.stringCmdValue)
			testDB.saveConfig.SetVal(tc.saveMapContent)
			testDB.appendConfig.SetVal(tc.appendMapContent)
			tc.r.db = testDB

			got, err := tc.r.CollectMetricsOnce(ctx, true)
			if tc.wantErr {
				if err == nil {
					t.Errorf("CollectMetricsOnce(%v) returned no error, want error", tc.name)
				}
				return
			}
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("CollectMetricsOnce(%v) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

func TestReplicationModeActive(t *testing.T) {
	tests := []struct {
		name           string
		stringCmdValue string
		role           string
		want           bool
	}{
		{
			name:           "HappyPathMain",
			stringCmdValue: "role:master\nconnected_slaves:1\n",
			role:           main,
			want:           true,
		},
		{
			name:           "HappyPathWorker",
			stringCmdValue: "role:slave\nmaster_link_status:up\n",
			role:           worker,
			want:           true,
		},
		{
			name:           "NoWorkers",
			stringCmdValue: "role:master\nconnected_slaves:0\n",
			role:           main,
			want:           false,
		},
		{
			name:           "WorkerNotUp",
			stringCmdValue: "role:slave\nmaster_link_status:down\n",
			role:           worker,
			want:           false,
		},
		{
			name:           "EmptySpaceHappyPathMain",
			stringCmdValue: "role:master\n     connected_slaves:1\n",
			role:           main,
			want:           true,
		},
		{
			name:           "EmptySpaceHappyPathWorker",
			stringCmdValue: "role:slave\n     master_link_status:up\n",
			role:           worker,
			want:           true,
		},
		{
			name:           "UnknownRole",
			stringCmdValue: "role:unknown\n",
			role:           "",
			want:           false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testDB := &testDB{info: &redis.StringCmd{}}
			testDB.info.SetVal(tc.stringCmdValue)
			r := RedisMetrics{
				db: testDB,
			}

			got := r.replicationModeActive(context.Background(), tc.role)
			if got != tc.want {
				t.Errorf("replicationModeActive() test %v = %v, want %v", tc.name, got, tc.want)
			}
		})
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
		t.Run(tc.name, func(t *testing.T) {
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
		})
	}
}

func TestServiceEnabled(t *testing.T) {
	tests := []struct {
		name string
		r    RedisMetrics
		want bool
	}{
		{
			name: "Success",
			r: RedisMetrics{
				OSData: osinfo.Data{OSName: "linux", OSVendor: "debian", OSVersion: "12"},
				execute: func(ctx context.Context, p commandlineexecutor.Params) commandlineexecutor.Result {
					if p.Args[0] == "is-enabled" {
						return commandlineexecutor.Result{StdOut: "enabled"}
					}
					return commandlineexecutor.Result{StdOut: "disabled"}
				},
			},
			want: true,
		},
		{
			name: "Error",
			r: RedisMetrics{
				OSData: osinfo.Data{OSName: "linux", OSVendor: "rhel", OSVersion: "9.4"},
				execute: func(ctx context.Context, p commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{Error: errors.New("systemctl is-enabled error")}
				},
			},
			want: false,
		},
		{
			name: "Disabled",
			r: RedisMetrics{
				OSData: osinfo.Data{OSName: "linux", OSVendor: "sles", OSVersion: "15-SP4"},
				execute: func(ctx context.Context, p commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{Error: errors.New("Non-zero exit code"), ExitCode: 1, StdOut: "disabled"}
				},
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.r.serviceEnabled(context.Background())
			if got != tc.want {
				t.Errorf("serviceEnabled() = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestServiceRestart(t *testing.T) {
	tests := []struct {
		name string
		r    RedisMetrics
		want bool
	}{
		{
			name: "Success",
			r: RedisMetrics{
				OSData: osinfo.Data{OSName: "linux", OSVendor: "debian", OSVersion: "12"},
				execute: func(ctx context.Context, p commandlineexecutor.Params) commandlineexecutor.Result {
					if p.Args[0] == "show" {
						return commandlineexecutor.Result{StdOut: "Restart=always\n"}
					}
					return commandlineexecutor.Result{StdOut: "Restart=no"}
				},
			},
			want: true,
		},
		{
			name: "Error",
			r: RedisMetrics{
				OSData: osinfo.Data{OSName: "linux", OSVendor: "rhel", OSVersion: "9.4"},
				execute: func(ctx context.Context, p commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{Error: errors.New("systemctl show error")}
				},
			},
			want: false,
		},
		{
			name: "NoRestart",
			r: RedisMetrics{
				OSData: osinfo.Data{OSName: "linux", OSVendor: "sles", OSVersion: "15-SP4"},
				execute: func(ctx context.Context, p commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{StdOut: "Restart=no\n"}
				},
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.r.serviceRestart(context.Background())
			if got != tc.want {
				t.Errorf("serviceRestart() = %t, want %t", got, tc.want)
			}
		})
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
		t.Run(tc.name, func(t *testing.T) {
			err := tc.r.InitDB(ctx, tc.gceService)
			gotErr := err != nil
			if gotErr != tc.wantErr {
				t.Errorf("InitDB(%v) = %v, wantErr %v", tc.name, err, tc.wantErr)
			}
			if !gotErr && (tc.r.db.String() != tc.want) {
				t.Errorf("InitDB(%v) = %v, want %v", tc.name, tc.r.db.String(), tc.want)
			}
		})
	}
}

func TestReplicationZones(t *testing.T) {
	tests := []struct {
		name           string
		stringCmdValue string
		role           string
		want           []string
	}{
		{
			name:           "HappyPathMain",
			stringCmdValue: "role:master\nslave0:ip=1.2.3.4,port=6379,state=online\n",
			role:           main,
			want:           []string{"test-zone"}, // Assuming 1.2.3.4 resolves to test-zone
		},
		{
			name:           "NoWorkers",
			stringCmdValue: "role:master\n",
			role:           main,
			want:           nil,
		},
		{
			name:           "InvalidIP",
			stringCmdValue: "role:master\nslave0:ip=invalid,port=6379,state=online\n",
			role:           main,
			want:           nil,
		},
		{
			name:           "WorkerRole",
			stringCmdValue: "role:slave\nmaster_link_status:up\n",
			role:           worker,
			want:           nil,
		},
		{
			name:           "EmptyResponse",
			stringCmdValue: "",
			role:           main,
			want:           nil,
		},
		{
			name: "MultipleWorkers",
			stringCmdValue: `role:master
slave0:ip=1.2.3.4,port=6379,state=online
slave1:ip=5.6.7.8,port=6380,state=online`,
			role: main,
			want: []string{"test-zone", "test-zone2"}, // Assuming 5.6.7.8 resolves to test-zone2
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testDB := &testDB{info: &redis.StringCmd{}}
			testDB.info.SetVal(tc.stringCmdValue)
			r := RedisMetrics{
				db: testDB,
			}
			// Mock net.LookupAddr for testing purposes
			netLookupAddr := func(ip string) ([]string, error) {
				switch ip {
				case "1.2.3.4":
					return []string{"hostname.test-zone.c.fake-project.internal."}, nil
				case "5.6.7.8":
					return []string{"hostname.test-zone2.c.fake-project.internal."}, nil
				default:
					return nil, errors.New("invalid IP")
				}
			}

			got := r.replicationZones(context.Background(), tc.role, netLookupAddr)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("replicationZones() test %v returned diff (-want +got):\n%s", tc.name, diff)
			}
		})
	}
}

func TestGetIP(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{
			name:  "ValidIP",
			input: "slave0:ip=1.2.3.4,port=6379,state=online",
			want:  "1.2.3.4",
		},
		{
			name:    "NoIP",
			input:   "slave0:port=6379,state=online",
			wantErr: true,
		},
		{
			name:    "EmptyInput",
			input:   "",
			wantErr: true,
		},
		{
			name:  "IPAtBeginning",
			input: "ip=1.2.3.4,port=6379,state=online",
			want:  "1.2.3.4",
		},
		{
			name:  "IPAtEnd",
			input: "slave0:port=6379,state=online,ip=1.2.3.4",
			want:  "1.2.3.4",
		},
		{
			name:  "ExtraSpaces",
			input: "  slave0:  ip=1.2.3.4 , port=6379, state=online  ",
			want:  "1.2.3.4",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := getIP(tc.input)
			if (err != nil) != tc.wantErr {
				t.Errorf("getIP() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if got != tc.want {
				t.Errorf("getIP() got = %v, want %v", got, tc.want)
			}
		})
	}
}
