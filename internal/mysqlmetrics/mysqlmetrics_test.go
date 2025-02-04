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

package mysqlmetrics

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/api/googleapi"
	"google.golang.org/protobuf/testing/protocmp"
	"github.com/GoogleCloudPlatform/workloadagent/internal/workloadmanager"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/commandlineexecutor"
	gcefake "github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/gce/fake"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/gce/wlm"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/log"
)

type testGCE struct {
	secret string
	err    error
}

func (t *testGCE) GetSecret(ctx context.Context, projectID, secretName string) (string, error) {
	return t.secret, t.err
}

type testDB struct {
	engineRows     rowsInterface
	engineErr      error
	bufferPoolRows rowsInterface
	bufferPoolErr  error
}

func (t *testDB) QueryContext(ctx context.Context, query string, args ...any) (rowsInterface, error) {
	if query == "SHOW ENGINES" {
		return t.engineRows, t.engineErr
	} else if query == "SELECT @@innodb_buffer_pool_size" {
		return t.bufferPoolRows, t.bufferPoolErr
	}
	return nil, nil
}

func (t *testDB) Ping() error {
	return nil
}

var emptyDB = &testDB{}

type bufferPoolRows struct {
	count     int
	size      int
	data      int64
	shouldErr bool
}

func (f *bufferPoolRows) Scan(dest ...any) error {
	log.CtxLogger(context.Background()).Infow("fakeRows.Scan", "dest", dest, "dest[0]", dest[0], "data", f.data)
	if f.shouldErr {
		return errors.New("test-error")
	}
	*dest[0].(*int64) = f.data
	return nil
}

func (f *bufferPoolRows) Next() bool {
	f.count++
	return f.count <= f.size
}

func (f *bufferPoolRows) Close() error {
	return nil
}

type isInnoDBRows struct {
	count     int
	size      int
	data      []sql.NullString
	shouldErr bool
}

func (f *isInnoDBRows) Scan(dest ...any) error {
	log.CtxLogger(context.Background()).Infow("fakeRows.Scan", "dest", dest, "dest[0]", dest[0], "data", f.data)
	if f.shouldErr {
		return errors.New("test-error")
	}
	for i := range f.data {
		*dest[i].(*sql.NullString) = f.data[i]
	}
	return nil
}

func (f *isInnoDBRows) Next() bool {
	f.count++
	return f.count <= f.size
}

func (f *isInnoDBRows) Close() error {
	return nil
}

func TestInitPassword(t *testing.T) {
	tests := []struct {
		name    string
		m       MySQLMetrics
		gce     *gcefake.TestGCE
		want    string
		wantErr bool
	}{
		{
			name:    "Default",
			m:       MySQLMetrics{},
			want:    "",
			wantErr: false,
		},
		{
			name: "GCEErr",
			m: MySQLMetrics{
				Config: &configpb.Configuration{
					MysqlConfiguration: &configpb.MySQLConfiguration{
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
			m: MySQLMetrics{
				Config: &configpb.Configuration{
					MysqlConfiguration: &configpb.MySQLConfiguration{
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
			m: MySQLMetrics{
				Config: &configpb.Configuration{
					MysqlConfiguration: &configpb.MySQLConfiguration{
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
			m: MySQLMetrics{
				Config: &configpb.Configuration{
					MysqlConfiguration: &configpb.MySQLConfiguration{
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
			m: MySQLMetrics{
				Config: &configpb.Configuration{
					MysqlConfiguration: &configpb.MySQLConfiguration{
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
		got, err := tc.m.password(context.Background(), tc.gce)
		gotErr := err != nil
		if gotErr != tc.wantErr {
			t.Errorf("password() = %v, wantErr %v", err, tc.wantErr)
		}
		if got.SecretValue() != tc.want {
			t.Errorf("password() = %v, want %v", got, tc.want)
		}
	}
}

func TestBufferPoolSize(t *testing.T) {
	tests := []struct {
		name    string
		m       MySQLMetrics
		want    int64
		wantErr bool
	}{
		{
			name: "HappyPath",
			m: MySQLMetrics{
				db: &testDB{
					bufferPoolRows: &bufferPoolRows{count: 0, size: 1, data: 134217728, shouldErr: false},
					bufferPoolErr:  nil,
				},
			},
			want:    134217728,
			wantErr: false,
		},
		{
			name: "EmptyResult",
			m: MySQLMetrics{
				db: &testDB{
					bufferPoolRows: &bufferPoolRows{count: 0, size: 0, data: 0, shouldErr: false},
					bufferPoolErr:  nil,
				},
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "QueryError",
			m: MySQLMetrics{
				db: &testDB{
					bufferPoolErr: errors.New("test-error"),
				},
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "ScanError",
			m: MySQLMetrics{
				db: &testDB{
					bufferPoolRows: &bufferPoolRows{count: 0, size: 1, data: 0, shouldErr: true},
					bufferPoolErr:  nil,
				},
			},
			want:    0,
			wantErr: true,
		},
	}
	for _, tc := range tests {
		got, err := tc.m.bufferPoolSize(context.Background())
		gotErr := err != nil
		if gotErr != tc.wantErr {
			t.Errorf("bufferPoolSize() = %v, wantErr %v", err, tc.wantErr)
		}
		if got != tc.want {
			t.Errorf("bufferPoolSize() = %v, want %v", got, tc.want)
		}
	}
}

func TestIsInnoDBStorageEngine(t *testing.T) {
	tests := []struct {
		name    string
		m       MySQLMetrics
		want    bool
		wantErr bool
	}{
		{
			name: "HappyPath",
			m: MySQLMetrics{
				db: &testDB{
					engineRows: &isInnoDBRows{
						count: 0,
						size:  1,
						data: []sql.NullString{
							sql.NullString{String: "InnoDB"},
							sql.NullString{String: "DEFAULT"},
							sql.NullString{String: "teststring3"},
							sql.NullString{String: "teststring4"},
							sql.NullString{String: "teststring5"},
							sql.NullString{String: "teststring6"},
						},
						shouldErr: false,
					},
					engineErr: nil,
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "NotDefault",
			m: MySQLMetrics{
				db: &testDB{
					engineRows: &isInnoDBRows{
						count: 0,
						size:  1,
						data: []sql.NullString{
							sql.NullString{String: "InnoDB"},
							sql.NullString{String: "YES"},
							sql.NullString{String: "teststring3"},
							sql.NullString{String: "teststring4"},
							sql.NullString{String: "teststring5"},
							sql.NullString{String: "teststring6"},
						},
						shouldErr: false,
					},
					engineErr: nil,
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "OtherStorageEngineAsDefault",
			m: MySQLMetrics{
				db: &testDB{
					engineRows: &isInnoDBRows{
						count: 0,
						size:  1,
						data: []sql.NullString{
							sql.NullString{String: "OtherStorageEngine"},
							sql.NullString{String: "DEFAULT"},
							sql.NullString{String: "teststring3"},
							sql.NullString{String: "teststring4"},
							sql.NullString{String: "teststring5"},
							sql.NullString{String: "teststring6"},
						},
						shouldErr: false,
					},
					engineErr: nil,
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "EmptyResult",
			m: MySQLMetrics{
				db: &testDB{
					engineRows: &isInnoDBRows{
						count:     0,
						size:      0,
						data:      []sql.NullString{},
						shouldErr: false,
					},
					engineErr: nil,
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "QueryError",
			m: MySQLMetrics{
				db: &testDB{
					engineErr: errors.New("test-error"),
				},
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "ScanError",
			m: MySQLMetrics{
				db: &testDB{
					engineRows: &isInnoDBRows{
						count:     0,
						size:      1,
						data:      []sql.NullString{},
						shouldErr: true,
					},
					engineErr: nil,
				},
			},
			want:    false,
			wantErr: false,
		},
	}
	for _, tc := range tests {
		got, err := tc.m.isInnoDBStorageEngine(context.Background())
		gotErr := err != nil
		if gotErr != tc.wantErr {
			t.Errorf("isInnoDBStorageEngine() test %v = %v, wantErr %v", tc.name, err, tc.wantErr)
		}
		if got != tc.want {
			t.Errorf("isInnoDBStorageEngine() test %v = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestTotalRAM(t *testing.T) {
	tests := []struct {
		name    string
		m       MySQLMetrics
		want    int
		wantErr bool
	}{
		{
			name: "HappyPath",
			m: MySQLMetrics{
				execute: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{
						StdOut: "MemTotal:        4025040 kB\n",
					}
				},
			},
			want:    4025040 * 1024,
			wantErr: false,
		},
		{
			name: "TooManyLines",
			m: MySQLMetrics{
				execute: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{
						StdOut: "MemTotal:        4025040 kB\ntesttext\ntesttext\n",
					}
				},
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "TooManyFields",
			m: MySQLMetrics{
				execute: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{
						StdOut: "MemTotal:        4025040 kB testtext testtext\n",
					}
				},
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "NonInt",
			m: MySQLMetrics{
				execute: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{
						StdOut: "MemTotal:        testtext kB\n",
					}
				},
			},
			want:    0,
			wantErr: true,
		},
	}
	for _, tc := range tests {
		got, err := tc.m.totalRAM(context.Background())
		gotErr := err != nil
		if gotErr != tc.wantErr {
			t.Errorf("totalRAM() = %v, wantErr %v", err, tc.wantErr)
		}
		if got != tc.want {
			t.Errorf("totalRAM() = %v, want %v", got, tc.want)
		}
	}
}

func TestNew(t *testing.T) {
	tests := []struct {
		name string
		cfg  *configpb.Configuration
		want *configpb.Configuration
	}{
		{
			name: "HappyPath",
			cfg: &configpb.Configuration{
				MysqlConfiguration: &configpb.MySQLConfiguration{
					ConnectionParameters: &configpb.ConnectionParameters{
						Username: "test-user",
						Secret:   &configpb.SecretRef{ProjectId: "test-project-id", SecretName: "test-secret-name"},
					},
				},
			},
			want: &configpb.Configuration{
				MysqlConfiguration: &configpb.MySQLConfiguration{
					ConnectionParameters: &configpb.ConnectionParameters{
						Username: "test-user",
						Secret:   &configpb.SecretRef{ProjectId: "test-project-id", SecretName: "test-secret-name"},
					},
				},
			},
		},
	}
	for _, tc := range tests {
		got := New(context.Background(), tc.cfg, nil)
		if diff := cmp.Diff(tc.want, got.Config, protocmp.Transform()); diff != "" {
			t.Errorf("New() test %v returned diff (-want +got):\n%s", tc.name, diff)
		}
	}
}

func TestDbDSN(t *testing.T) {
	tests := []struct {
		name       string
		m          MySQLMetrics
		gceService gceInterface
		want       string
		wantErr    bool
	}{
		{
			name: "HappyPath",
			m: MySQLMetrics{
				Config: &configpb.Configuration{
					MysqlConfiguration: &configpb.MySQLConfiguration{
						ConnectionParameters: &configpb.ConnectionParameters{
							Username: "test-user",
							Secret:   &configpb.SecretRef{ProjectId: "fake-project-id", SecretName: "fake-secret-name"},
						},
					},
				},
			},
			gceService: &gcefake.TestGCE{
				GetSecretResp: []string{"fake-password"},
				GetSecretErr:  []error{nil},
			},
			want:    "test-user:fake-password@/mysql?allowNativePasswords=false&checkConnLiveness=false&maxAllowedPacket=0",
			wantErr: false,
		},
		{
			name: "ConfigPassword",
			m: MySQLMetrics{
				Config: &configpb.Configuration{
					MysqlConfiguration: &configpb.MySQLConfiguration{
						ConnectionParameters: &configpb.ConnectionParameters{
							Username: "test-user",
							Password: "fake-password",
						},
					},
				},
			},
			gceService: &gcefake.TestGCE{},
			want:       "test-user:fake-password@/mysql?allowNativePasswords=false&checkConnLiveness=false&maxAllowedPacket=0",
			wantErr:    false,
		},
		{
			name: "PasswordError",
			m: MySQLMetrics{
				Config: &configpb.Configuration{
					MysqlConfiguration: &configpb.MySQLConfiguration{
						ConnectionParameters: &configpb.ConnectionParameters{
							Username: "test-user",
							Secret:   &configpb.SecretRef{ProjectId: "fake-project-id", SecretName: "fake-secret-name"},
						},
					},
				},
			},
			gceService: &gcefake.TestGCE{
				GetSecretResp: []string{""},
				GetSecretErr:  []error{errors.New("fake-error")},
			},
			want:    "",
			wantErr: true,
		},
	}

	ctx := context.Background()

	for _, tc := range tests {
		got, err := tc.m.dbDSN(ctx, tc.gceService)
		gotErr := err != nil
		if gotErr != tc.wantErr {
			t.Errorf("dbDSN(%v) = %v, wantErr %v", tc.name, err, tc.wantErr)
		}
		if got != tc.want {
			t.Errorf("dbDSN(%v) = %q, want: %q", tc.name, got, tc.want)
		}
	}
}

func TestInitDB(t *testing.T) {
	tests := []struct {
		name       string
		m          MySQLMetrics
		gceService gceInterface
		wantErr    bool
	}{
		{
			name: "HappyPath",
			m: MySQLMetrics{
				Config: &configpb.Configuration{
					MysqlConfiguration: &configpb.MySQLConfiguration{
						ConnectionParameters: &configpb.ConnectionParameters{
							Username: "test-user",
							Secret:   &configpb.SecretRef{ProjectId: "fake-project-id", SecretName: "fake-secret-name"},
						},
					},
				},
				connect: func(ctx context.Context, dataSource string) (dbInterface, error) { return emptyDB, nil },
			},
			gceService: &gcefake.TestGCE{
				GetSecretResp: []string{"fake-password"},
				GetSecretErr:  []error{nil},
			},
			wantErr: false,
		},
		{
			name: "ConfigPassword",
			m: MySQLMetrics{
				Config: &configpb.Configuration{
					MysqlConfiguration: &configpb.MySQLConfiguration{
						ConnectionParameters: &configpb.ConnectionParameters{
							Username: "test-user",
							Password: "fake-password",
						},
					},
				},
				connect: func(ctx context.Context, dataSource string) (dbInterface, error) { return emptyDB, nil },
			},
			gceService: &gcefake.TestGCE{},
			wantErr:    false,
		},
		{
			name: "PasswordError",
			m: MySQLMetrics{
				Config: &configpb.Configuration{
					MysqlConfiguration: &configpb.MySQLConfiguration{
						ConnectionParameters: &configpb.ConnectionParameters{
							Username: "test-user",
							Secret:   &configpb.SecretRef{ProjectId: "fake-project-id", SecretName: "fake-secret-name"},
						},
					},
				},
				connect: func(ctx context.Context, dataSource string) (dbInterface, error) { return emptyDB, nil },
			},
			gceService: &gcefake.TestGCE{
				GetSecretResp: []string{""},
				GetSecretErr:  []error{errors.New("fake-error")},
			},
			wantErr: true,
		},
		{
			name: "ConnectError",
			m: MySQLMetrics{
				Config: &configpb.Configuration{
					MysqlConfiguration: &configpb.MySQLConfiguration{
						ConnectionParameters: &configpb.ConnectionParameters{
							Username: "test-user",
							Secret:   &configpb.SecretRef{ProjectId: "fake-project-id", SecretName: "fake-secret-name"},
						},
					},
				},
				connect: func(ctx context.Context, dataSource string) (dbInterface, error) {
					return nil, errors.New("fake-error")
				},
			},
			gceService: &gcefake.TestGCE{
				GetSecretResp: []string{"fake-password"},
				GetSecretErr:  []error{nil},
			},
			wantErr: true,
		},
	}

	ctx := context.Background()

	for _, tc := range tests {
		err := tc.m.InitDB(ctx, tc.gceService)
		gotErr := err != nil
		if gotErr != tc.wantErr {
			t.Errorf("InitDB(%v) = %v, wantErr %v", tc.name, err, tc.wantErr)
		}
	}
}

func TestCollectMetricsOnce(t *testing.T) {
	tests := []struct {
		name        string
		m           MySQLMetrics
		wantMetrics *workloadmanager.WorkloadMetrics
		wantErr     bool
	}{
		{
			name: "HappyPath",
			m: MySQLMetrics{
				db: &testDB{
					engineRows: &isInnoDBRows{
						count: 0,
						size:  1,
						data: []sql.NullString{
							sql.NullString{String: "InnoDB"},
							sql.NullString{String: "DEFAULT"},
							sql.NullString{String: "teststring3"},
							sql.NullString{String: "teststring4"},
							sql.NullString{String: "teststring5"},
							sql.NullString{String: "teststring6"},
						},
						shouldErr: false,
					},
					engineErr:      nil,
					bufferPoolRows: &bufferPoolRows{count: 0, size: 1, data: 134217728, shouldErr: false},
					bufferPoolErr:  nil,
				},
				execute: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{
						StdOut: "MemTotal:        4025040 kB\n",
					}
				},
				WLMClient: &gcefake.TestWLM{
					WriteInsightErrs: []error{nil},
					WriteInsightResponses: []*wlm.WriteInsightResponse{
						&wlm.WriteInsightResponse{ServerResponse: googleapi.ServerResponse{HTTPStatusCode: 201}},
					},
				},
			},
			wantMetrics: &workloadmanager.WorkloadMetrics{
				WorkloadType: workloadmanager.MYSQL,
				Metrics: map[string]string{
					bufferPoolKey: "134217728",
					totalRAMKey:   strconv.Itoa(4025040 * 1024),
					innoDBKey:     "true",
				},
			},
			wantErr: false,
		},
		{
			name: "BufferPoolSizeError",
			m: MySQLMetrics{
				db: &testDB{
					engineRows: &isInnoDBRows{
						count: 0,
						size:  1,
						data: []sql.NullString{
							sql.NullString{String: "InnoDB"},
							sql.NullString{String: "DEFAULT"},
							sql.NullString{String: "teststring3"},
							sql.NullString{String: "teststring4"},
							sql.NullString{String: "teststring5"},
							sql.NullString{String: "teststring6"},
						},
						shouldErr: false,
					},
					engineErr:      nil,
					bufferPoolRows: nil,
					bufferPoolErr:  errors.New("test-error"),
				},
				execute: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{
						StdOut: "MemTotal:        4025040 kB\n",
					}
				},
				WLMClient: &gcefake.TestWLM{
					WriteInsightErrs: []error{nil},
					WriteInsightResponses: []*wlm.WriteInsightResponse{
						&wlm.WriteInsightResponse{ServerResponse: googleapi.ServerResponse{HTTPStatusCode: 201}},
					},
				},
			},
			wantMetrics: &workloadmanager.WorkloadMetrics{
				WorkloadType: workloadmanager.MYSQL,
				Metrics: map[string]string{
					bufferPoolKey: "0",
					totalRAMKey:   strconv.Itoa(4025040 * 1024),
					innoDBKey:     "false",
				},
			},
			wantErr: true,
		}, {
			name: "TotalRAMError",
			m: MySQLMetrics{
				db: &testDB{
					engineRows: &isInnoDBRows{
						count: 0,
						size:  1,
						data: []sql.NullString{
							sql.NullString{String: "InnoDB"},
							sql.NullString{String: "DEFAULT"},
							sql.NullString{String: "teststring3"},
							sql.NullString{String: "teststring4"},
							sql.NullString{String: "teststring5"},
							sql.NullString{String: "teststring6"},
						},
						shouldErr: false,
					},
					engineErr:      nil,
					bufferPoolRows: &bufferPoolRows{count: 0, size: 1, data: 134217728, shouldErr: false},
					bufferPoolErr:  nil,
				},
				execute: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{
						Error: errors.New("test-error"),
					}
				},
				WLMClient: &gcefake.TestWLM{
					WriteInsightErrs: []error{nil},
					WriteInsightResponses: []*wlm.WriteInsightResponse{
						&wlm.WriteInsightResponse{ServerResponse: googleapi.ServerResponse{HTTPStatusCode: 201}},
					},
				},
			},
			wantMetrics: &workloadmanager.WorkloadMetrics{
				WorkloadType: workloadmanager.MYSQL,
				Metrics: map[string]string{
					bufferPoolKey: "134217728",
					totalRAMKey:   "0",
					innoDBKey:     "true",
				},
			},
			wantErr: true,
		},
		{
			name: "IsInnoDBDefaultError",
			m: MySQLMetrics{
				db: &testDB{
					engineRows:     nil,
					engineErr:      errors.New("test-error"),
					bufferPoolRows: &bufferPoolRows{count: 0, size: 1, data: 134217728, shouldErr: false},
					bufferPoolErr:  nil,
				},
				execute: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{
						StdOut: "MemTotal:        4025040 kB\n",
					}
				},
				WLMClient: &gcefake.TestWLM{
					WriteInsightErrs: []error{nil},
					WriteInsightResponses: []*wlm.WriteInsightResponse{
						&wlm.WriteInsightResponse{ServerResponse: googleapi.ServerResponse{HTTPStatusCode: 201}},
					},
				},
			},
			wantMetrics: &workloadmanager.WorkloadMetrics{
				WorkloadType: workloadmanager.MYSQL,
				Metrics: map[string]string{
					bufferPoolKey: "134217728",
					totalRAMKey:   strconv.Itoa(4025040 * 1024),
					innoDBKey:     "false",
				},
			},
			wantErr: true,
		},
		{
			name: "WLMClientError",
			m: MySQLMetrics{
				db: &testDB{
					engineRows: &isInnoDBRows{
						count: 0,
						size:  1,
						data: []sql.NullString{
							sql.NullString{String: "InnoDB"},
							sql.NullString{String: "DEFAULT"},
							sql.NullString{String: "teststring3"},
							sql.NullString{String: "teststring4"},
							sql.NullString{String: "teststring5"},
							sql.NullString{String: "teststring6"},
						},
						shouldErr: false,
					},
					engineErr:      nil,
					bufferPoolRows: &bufferPoolRows{count: 0, size: 1, data: 134217728, shouldErr: false},
					bufferPoolErr:  nil,
				},
				execute: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{
						StdOut: "MemTotal:        4025040 kB\n",
					}
				},
				WLMClient: &gcefake.TestWLM{
					WriteInsightErrs: []error{errors.New("test-error")},
					WriteInsightResponses: []*wlm.WriteInsightResponse{
						&wlm.WriteInsightResponse{ServerResponse: googleapi.ServerResponse{HTTPStatusCode: 400}},
					},
				},
			},
			wantMetrics: &workloadmanager.WorkloadMetrics{
				WorkloadType: workloadmanager.MYSQL,
				Metrics: map[string]string{
					bufferPoolKey: "134217728",
					totalRAMKey:   strconv.Itoa(4025040 * 1024),
					innoDBKey:     "true",
				},
			},
			wantErr: true,
		},
		{
			name: "NilWriteInsightResponse",
			m: MySQLMetrics{
				db: &testDB{
					engineRows: &isInnoDBRows{
						count: 0,
						size:  1,
						data: []sql.NullString{
							sql.NullString{String: "InnoDB"},
							sql.NullString{String: "DEFAULT"},
							sql.NullString{String: "teststring3"},
							sql.NullString{String: "teststring4"},
							sql.NullString{String: "teststring5"},
							sql.NullString{String: "teststring6"},
						},
						shouldErr: false,
					},
					engineErr:      nil,
					bufferPoolRows: &bufferPoolRows{count: 0, size: 1, data: 134217728, shouldErr: false},
					bufferPoolErr:  nil,
				},
				execute: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{
						StdOut: "MemTotal:        4025040 kB\n",
					}
				},
				WLMClient: &gcefake.TestWLM{
					WriteInsightErrs:      []error{nil},
					WriteInsightResponses: []*wlm.WriteInsightResponse{nil},
				},
			},
			wantMetrics: &workloadmanager.WorkloadMetrics{
				WorkloadType: workloadmanager.MYSQL,
				Metrics: map[string]string{
					bufferPoolKey: "134217728",
					totalRAMKey:   strconv.Itoa(4025040 * 1024),
					innoDBKey:     "true",
				},
			},
			wantErr: false,
		},
	}

	ctx := context.Background()

	for _, tc := range tests {
		gotMetrics, err := tc.m.CollectMetricsOnce(ctx)
		if tc.wantErr {
			if err == nil {
				t.Errorf("CollectMetricsOnce(%v) returned no error, want error", tc.name)
			}
			continue
		}
		if diff := cmp.Diff(tc.wantMetrics, gotMetrics, protocmp.Transform()); diff != "" {
			t.Errorf("CollectMetricsOnce(%v) returned diff (-want +got):\n%s", tc.name, diff)
		}
	}
}
