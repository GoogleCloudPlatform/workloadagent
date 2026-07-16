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
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/api/googleapi"
	"google.golang.org/protobuf/testing/protocmp"
	"github.com/GoogleCloudPlatform/workloadagent/internal/databasecenter"
	"github.com/GoogleCloudPlatform/workloadagent/internal/workloadmanager"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/commandlineexecutor"
	gcefake "github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce/fake"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce/wlm"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
)

type mockNetInterface struct {
	lookupHostValue map[string][]string
	lookupHostErr   map[string]error
	lookupAddrValue map[string][]string
	lookupAddrErr   map[string]error
}

func (m mockNetInterface) LookupHost(host string) ([]string, error) {
	if err, ok := m.lookupHostErr[host]; ok {
		return nil, err
	}
	return m.lookupHostValue[host], nil
}

func (m mockNetInterface) ParseIP(ip string) net.IP {
	if ip == "" {
		return nil
	}
	if ip == "1.2.3.4" || ip == "5.6.7.8" {
		return net.ParseIP("127.0.0.1")
	}
	return net.ParseIP(ip)
}

func (m mockNetInterface) LookupAddr(ip string) ([]string, error) {
	if err, ok := m.lookupAddrErr[ip]; ok {
		return nil, err
	}
	return m.lookupAddrValue[ip], nil
}

type testGCE struct {
	secret string
	err    error
}

func (t *testGCE) GetSecret(ctx context.Context, projectID, secretName string) (string, error) {
	return t.secret, t.err
}

type testDB struct {
	engineRows                 rowsInterface
	engineErr                  error
	bufferPoolRows             rowsInterface
	bufferPoolErr              error
	replicaRows                rowsInterface
	replicaErr                 error
	slaveRows                  rowsInterface
	slaveErr                   error
	replicationZonesRows       rowsInterface
	replicationZonesErr        error
	versionRows                rowsInterface
	versionErr                 error
	mysqlUserRows              rowsInterface
	mysqlUserErr               error
	exposedToPublicAccessRows  rowsInterface
	exposedToPublicAccessErr   error
	requireSecureTransportRows rowsInterface
	requireSecureTransportErr  error
	auditLogPluginRows         rowsInterface
	auditLogPluginErr          error
	groupReplicationRows       rowsInterface
	groupReplicationErr        error
	wsrepStatusRows            rowsInterface
	wsrepStatusErr             error
	ndbEngineRows              rowsInterface
	ndbEngineErr               error
	logBinVarRows              rowsInterface
	logBinVarErr               error
	dumpThreadsRows            rowsInterface
	dumpThreadsErr             error
	managerUserRows            rowsInterface
	managerUserErr             error
	mebHistoryRows             rowsInterface
	mebHistoryErr              error
	xtrabackupHistoryRows      rowsInterface
	xtrabackupHistoryErr       error
}

func (t *testDB) QueryContext(ctx context.Context, query string, args ...any) (rowsInterface, error) {
	if query == "SHOW ENGINES" {
		return t.engineRows, t.engineErr
	}
	if query == "SELECT @@innodb_buffer_pool_size" {
		return t.bufferPoolRows, t.bufferPoolErr
	}
	if query == "SHOW REPLICA STATUS" {
		if r, ok := t.replicaRows.(*replicaRows); ok && r != nil {
			return &replicaRows{size: r.size, data: r.data, shouldErr: r.shouldErr}, t.replicaErr
		}
		return t.replicaRows, t.replicaErr
	}
	if query == "SHOW SLAVE STATUS" {
		if r, ok := t.slaveRows.(*slaveRows); ok && r != nil {
			return &slaveRows{size: r.size, data: r.data, shouldErr: r.shouldErr}, t.slaveErr
		}
		return t.slaveRows, t.slaveErr
	}
	if query == replicationZonesQuery {
		return t.replicationZonesRows, t.replicationZonesErr
	}
	if query == "SELECT @@version" {
		return t.versionRows, t.versionErr
	}
	if strings.Contains(query, "replication_group_members") {
		return t.groupReplicationRows, t.groupReplicationErr
	}
	if strings.Contains(query, "wsrep_cluster_size") {
		return t.wsrepStatusRows, t.wsrepStatusErr
	}
	if strings.Contains(query, "ndbcluster") {
		return t.ndbEngineRows, t.ndbEngineErr
	}
	if query == "SHOW GLOBAL VARIABLES LIKE 'log_bin'" {
		return t.logBinVarRows, t.logBinVarErr
	}
	if strings.Contains(query, "Binlog Dump") {
		return t.dumpThreadsRows, t.dumpThreadsErr
	}
	if strings.Contains(query, "user LIKE '%orchestrator%'") {
		return t.managerUserRows, t.managerUserErr
	}
	if strings.Contains(query, "mysql.backup_history") {
		if r, ok := t.mebHistoryRows.(*countMockRows); ok && r != nil {
			return &countMockRows{size: r.size, data: r.data, shouldErr: r.shouldErr}, t.mebHistoryErr
		}
		return t.mebHistoryRows, t.mebHistoryErr
	}
	if strings.Contains(query, "PERCONA_SCHEMA.xtrabackup_history") {
		if r, ok := t.xtrabackupHistoryRows.(*countMockRows); ok && r != nil {
			return &countMockRows{size: r.size, data: r.data, shouldErr: r.shouldErr}, t.xtrabackupHistoryErr
		}
		return t.xtrabackupHistoryRows, t.xtrabackupHistoryErr
	}
	if strings.TrimSpace(query) == strings.TrimSpace(`
		SELECT user, host, plugin, authentication_string
		FROM mysql.user
		WHERE user = 'root'
	`) {
		return t.mysqlUserRows, t.mysqlUserErr
	}
	if strings.TrimSpace(query) == strings.TrimSpace(`
		SELECT User, Host
		FROM mysql.user
		WHERE Host = '%'
	`) {
		return t.exposedToPublicAccessRows, t.exposedToPublicAccessErr
	}
	if query == `SHOW GLOBAL VARIABLES LIKE 'require_secure_transport'` {
		return t.requireSecureTransportRows, t.requireSecureTransportErr
	}
	if strings.TrimSpace(query) == strings.TrimSpace(`
		SELECT PLUGIN_STATUS
		FROM INFORMATION_SCHEMA.PLUGINS
		WHERE PLUGIN_NAME = 'audit_log'
	`) {
		return t.auditLogPluginRows, t.auditLogPluginErr
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

type replicaRows struct {
	count     int
	size      int
	data      []sql.NullString
	shouldErr bool
}

func (f *replicaRows) Scan(dest ...any) error {
	log.CtxLogger(context.Background()).Infow("fakeRows.Scan", "dest", dest, "dest[0]", dest[0], "data", f.data)
	if f.shouldErr {
		return errors.New("test-error")
	}
	for i := range f.data {
		*dest[i].(*sql.NullString) = f.data[i]
	}
	return nil
}

func (f *replicaRows) Next() bool {
	f.count++
	return f.count <= f.size
}

func (f *replicaRows) Close() error {
	return nil
}

type slaveRows struct {
	count     int
	size      int
	data      []sql.NullString
	shouldErr bool
}

func (f *slaveRows) Scan(dest ...any) error {
	log.CtxLogger(context.Background()).Infow("fakeRows.Scan", "dest", dest, "dest[0]", dest[0], "data", f.data)
	if f.shouldErr {
		return errors.New("test-error")
	}
	for i := range f.data {
		*dest[i].(*sql.NullString) = f.data[i]
	}
	return nil
}

func (f *slaveRows) Next() bool {
	f.count++
	return f.count <= f.size
}

func (f *slaveRows) Close() error {
	return nil
}

type replicationZonesRows struct {
	count     int
	size      int
	data      []sql.NullString
	shouldErr bool
}

func (f *replicationZonesRows) Scan(dest ...any) error {
	log.CtxLogger(context.Background()).Infow("fakeRows.Scan", "dest", dest, "dest[0]", dest[0], "data", f.data)
	if f.shouldErr {
		return errors.New("test-error")
	}
	for i := range f.data {
		*dest[i].(*sql.NullString) = f.data[i]
	}
	return nil
}

func (f *replicationZonesRows) Next() bool {
	f.count++
	return f.count <= f.size
}

func (f *replicationZonesRows) Close() error {
	return nil
}

type versionRows struct {
	count     int
	size      int
	data      []string
	shouldErr bool
}

func (f *versionRows) Scan(dest ...any) error {
	log.CtxLogger(context.Background()).Infow("fakeRows.Scan", "dest", dest, "dest[0]", dest[0], "data", f.data)
	if f.shouldErr {
		return errors.New("test-error")
	}
	for i := range f.data {
		*dest[i].(*string) = f.data[i]
	}
	return nil
}

func (f *versionRows) Next() bool {
	f.count++
	return f.count <= f.size
}

func (f *versionRows) Close() error {
	return nil
}

type mysqlUserMockRows struct {
	count   int
	size    int
	data    [][]any // Each inner slice represents a row's columns
	scanErr bool    // To trigger an error in Scan
}

func (f *mysqlUserMockRows) Scan(dest ...any) error {
	if f.scanErr {
		return errors.New("test scan error")
	}
	if f.count == 0 || f.count > f.size {
		return errors.New("Scan called on invalid row")
	}
	currentRow := f.data[f.count-1]
	if len(dest) != len(currentRow) {
		return fmt.Errorf("scan destination count mismatch: got %d, want %d", len(dest), len(currentRow))
	}

	for i := 0; i < len(dest); i++ {
		switch d := dest[i].(type) {
		case *string:
			val, ok := currentRow[i].(string)
			if !ok {
				return fmt.Errorf("type assertion failed for column %d to string", i)
			}
			*d = val
		case *sql.NullString:
			val, ok := currentRow[i].(sql.NullString)
			if !ok {
				return fmt.Errorf("type assertion failed for column %d to sql.NullString", i)
			}
			*d = val
		default:
			return fmt.Errorf("unsupported type for Scan: %T at column %d", dest[i], i)
		}
	}
	return nil
}

func (f *mysqlUserMockRows) Next() bool {
	if f.count < f.size {
		f.count++
		return true
	}
	return false
}

func (f *mysqlUserMockRows) Close() error {
	return nil
}

type exposedToPublicAccessMockRows struct {
	count   int
	size    int
	data    [][]string // {user, host}
	scanErr bool
}

func (f *exposedToPublicAccessMockRows) Scan(dest ...any) error {
	if f.scanErr {
		return errors.New("test scan error")
	}
	if f.count == 0 || f.count > f.size {
		return errors.New("Scan called on invalid row")
	}
	currentRow := f.data[f.count-1]
	if len(dest) != len(currentRow) {
		return fmt.Errorf("scan destination count mismatch: got %d, want %d", len(dest), len(currentRow))
	}
	*dest[0].(*string) = currentRow[0]
	*dest[1].(*string) = currentRow[1]
	return nil
}

func (f *exposedToPublicAccessMockRows) Next() bool {
	if f.count < f.size {
		f.count++
		return true
	}
	return false
}

func (f *exposedToPublicAccessMockRows) Close() error {
	return nil
}

type globalVarMockRows struct {
	count   int
	size    int
	data    [][]string // {var_name, value}
	scanErr bool
}

func (f *globalVarMockRows) Scan(dest ...any) error {
	if f.scanErr {
		return errors.New("test scan error")
	}
	if f.count == 0 || f.count > f.size {
		return errors.New("Scan called on invalid row")
	}
	currentRow := f.data[f.count-1]
	if len(dest) != len(currentRow) {
		return fmt.Errorf("scan destination count mismatch: got %d, want %d", len(dest), len(currentRow))
	}
	for i := range dest {
		*dest[i].(*string) = currentRow[i]
	}
	return nil
}

func (f *globalVarMockRows) Next() bool {
	if f.count < f.size {
		f.count++
		return true
	}
	return false
}

func (f *globalVarMockRows) Close() error {
	return nil
}

type pluginStatusMockRows struct {
	count   int
	size    int
	data    [][]string // {plugin_status}
	scanErr bool
}

func (f *pluginStatusMockRows) Scan(dest ...any) error {
	if f.scanErr {
		return errors.New("test scan error")
	}
	if f.count == 0 || f.count > f.size {
		return errors.New("Scan called on invalid row")
	}
	currentRow := f.data[f.count-1]
	*dest[0].(*string) = currentRow[0]
	return nil
}

func (f *pluginStatusMockRows) Next() bool {
	if f.count < f.size {
		f.count++
		return true
	}
	return false
}

func (f *pluginStatusMockRows) Close() error {
	return nil
}

func (f *bufferPoolRows) Columns() ([]string, error) { return []string{"size"}, nil }
func (f *isInnoDBRows) Columns() ([]string, error)   { return []string{"engine", "support"}, nil }
func (f *replicaRows) Columns() ([]string, error) {
	return []string{"Replica_IO_Running", "Replica_SQL_Running"}, nil
}
func (f *slaveRows) Columns() ([]string, error) {
	return []string{"Slave_IO_Running", "Slave_SQL_Running"}, nil
}
func (f *replicationZonesRows) Columns() ([]string, error) { return []string{"host"}, nil }
func (f *versionRows) Columns() ([]string, error)          { return []string{"version"}, nil }
func (f *mysqlUserMockRows) Columns() ([]string, error)    { return []string{"user", "host"}, nil }
func (f *exposedToPublicAccessMockRows) Columns() ([]string, error) {
	return []string{"user", "host"}, nil
}
func (f *globalVarMockRows) Columns() ([]string, error) { return []string{"var_name", "value"}, nil }
func (f *pluginStatusMockRows) Columns() ([]string, error) {
	return []string{"plugin_status"}, nil
}

type countMockRows struct {
	count     int
	size      int
	data      int
	shouldErr bool
}

func (f *countMockRows) Scan(dest ...any) error {
	if f.shouldErr {
		return errors.New("test scan error")
	}
	if f.count == 0 {
		return errors.New("scan called before Next")
	}
	*dest[0].(*int) = f.data
	return nil
}

func (f *countMockRows) Next() bool {
	if f.count < f.size {
		f.count++
		return true
	}
	return false
}

func (f *countMockRows) Close() error { return nil }

func (f *countMockRows) Columns() ([]string, error) { return []string{"count"}, nil }

type MockDatabaseCenterClient struct {
	sendMetadataCalled bool
	sendMetadataErr    error
	gotMetrics         databasecenter.DBCenterMetrics
}

func (m *MockDatabaseCenterClient) SendMetadataToDatabaseCenter(ctx context.Context, metrics databasecenter.DBCenterMetrics) error {
	m.sendMetadataCalled = true
	m.gotMetrics = metrics
	return m.sendMetadataErr
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
		name        string
		m           MySQLMetrics
		isWindowsOS bool
		want        int
		wantErr     bool
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
			name: "HappyPathWindows",
			m: MySQLMetrics{
				execute: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{
						StdOut: "TotalPhysicalMemory\n134876032413 ",
					}
				},
			},
			isWindowsOS: true,
			want:        134876032413,
			wantErr:     false,
		},
		{
			name: "SingleLineWindows",
			m: MySQLMetrics{
				execute: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{
						StdOut: "TotalPhysicalMemory: 134876032413 ",
					}
				},
			},
			isWindowsOS: true,
			want:        0,
			wantErr:     true,
		},
		{
			name: "NonIntWindows",
			m: MySQLMetrics{
				execute: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{
						StdOut: "TotalPhysicalMemory\ntesttext ",
					}
				},
			},
			isWindowsOS: true,
			want:        0,
			wantErr:     true,
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
		got, err := tc.m.totalRAM(context.Background(), tc.isWindowsOS)
		gotErr := err != nil
		if gotErr != tc.wantErr {
			t.Errorf("totalRAM(%s) = %v, wantErr %v", tc.name, err, tc.wantErr)
		}
		if got != tc.want {
			t.Errorf("totalRAM(%s) = %v, want %v", tc.name, got, tc.want)
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
		got := New(context.Background(), tc.cfg, nil, databasecenter.NewClient(tc.cfg, nil))
		if diff := cmp.Diff(tc.want, got.Config, protocmp.Transform()); diff != "" {
			t.Errorf("New() test %v returned diff (-want +got):\n%s", tc.name, diff)
		}
	}
}

func TestDbDSN(t *testing.T) {
	tests := []struct {
		name       string
		m          MySQLMetrics
		gceService GceInterface
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
		gceService GceInterface
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

func TestCollectWlmMetricsOnce(t *testing.T) {
	tests := []struct {
		name        string
		m           MySQLMetrics
		wantMetrics *workloadmanager.WorkloadMetrics
		wantErr     bool
	}{
		{
			// This is the HappyPath test for running on Linux. It will fail if run on Windows.
			// Windows specific functionality is tested in TestTotalRAM.
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
				DBcenterClient: databasecenter.NewClient(&configpb.Configuration{}, nil),
			},
			wantMetrics: &workloadmanager.WorkloadMetrics{
				WorkloadType: workloadmanager.MYSQL,
				Metrics: map[string]string{
					bufferPoolKey:       "134217728",
					currentRoleKey:      sourceRole,
					totalRAMKey:         strconv.Itoa(4025040 * 1024),
					innoDBKey:           "true",
					replicationZonesKey: "",
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
				DBcenterClient: databasecenter.NewClient(&configpb.Configuration{}, nil),
			},
			wantMetrics: &workloadmanager.WorkloadMetrics{
				WorkloadType: workloadmanager.MYSQL,
				Metrics: map[string]string{
					bufferPoolKey:       "0",
					currentRoleKey:      sourceRole,
					totalRAMKey:         strconv.Itoa(4025040 * 1024),
					innoDBKey:           "false",
					replicationZonesKey: "",
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
				DBcenterClient: databasecenter.NewClient(&configpb.Configuration{}, nil),
			},
			wantMetrics: &workloadmanager.WorkloadMetrics{
				WorkloadType: workloadmanager.MYSQL,
				Metrics: map[string]string{
					bufferPoolKey:       "134217728",
					currentRoleKey:      sourceRole,
					totalRAMKey:         "0",
					innoDBKey:           "true",
					replicationZonesKey: "",
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
				DBcenterClient: databasecenter.NewClient(&configpb.Configuration{}, nil),
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
				DBcenterClient: databasecenter.NewClient(&configpb.Configuration{}, nil),
			},
			wantMetrics: &workloadmanager.WorkloadMetrics{
				WorkloadType: workloadmanager.MYSQL,
				Metrics: map[string]string{
					bufferPoolKey:       "134217728",
					currentRoleKey:      sourceRole,
					totalRAMKey:         strconv.Itoa(4025040 * 1024),
					innoDBKey:           "true",
					replicationZonesKey: "",
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
				DBcenterClient: databasecenter.NewClient(&configpb.Configuration{}, nil),
			},
			wantMetrics: &workloadmanager.WorkloadMetrics{
				WorkloadType: workloadmanager.MYSQL,
				Metrics: map[string]string{
					bufferPoolKey:       "134217728",
					currentRoleKey:      sourceRole,
					totalRAMKey:         strconv.Itoa(4025040 * 1024),
					innoDBKey:           "true",
					replicationZonesKey: "",
				},
			},
			wantErr: false,
		},
	}

	ctx := context.Background()

	for _, tc := range tests {
		gotMetrics, err := tc.m.CollectWlmMetricsOnce(ctx, true)
		if tc.wantErr {
			if err == nil {
				t.Errorf("CollectWlmMetricsOnce(%v) returned no error, want error", tc.name)
			}
			continue
		}
		if diff := cmp.Diff(tc.wantMetrics, gotMetrics, protocmp.Transform()); diff != "" {
			t.Errorf("CollectWlmMetricsOnce(%v) returned diff (-want +got):\n%s", tc.name, diff)
		}
	}
}

func TestGetCurrentRole(t *testing.T) {
	tests := []struct {
		name        string
		isReplica   bool
		replicaRows rowsInterface
		replicaErr  error
		slaveRows   rowsInterface
		slaveErr    error
		want        string
	}{
		{
			name:      "IsReplica",
			isReplica: true,
			replicaRows: &replicaRows{
				count: 0,
				size:  1,
				data: []sql.NullString{
					sql.NullString{String: "teststring1"},
				},
				shouldErr: false,
			},
			want: replicaRole,
		},
		{
			name:      "IsReplicaOldVersion",
			isReplica: true,
			slaveRows: &replicaRows{
				count: 0,
				size:  1,
				data: []sql.NullString{
					sql.NullString{String: "teststring1"},
				},
				shouldErr: false,
			},
			want: replicaRole,
		},
		{
			name:       "Error",
			isReplica:  true,
			replicaErr: errors.New("test-error"),
			want:       sourceRole,
		},
		{
			name:      "ErrorOldVersion",
			isReplica: true,
			slaveErr:  errors.New("test-error"),
			want:      sourceRole,
		},
		{
			name:      "IsSource",
			isReplica: false,
			want:      sourceRole,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := MySQLMetrics{
				db: &testDB{
					replicaRows: tc.replicaRows,
					replicaErr:  tc.replicaErr,
					slaveRows:   tc.slaveRows,
					slaveErr:    tc.slaveErr,
				},
			}

			got := m.currentRole(context.Background())
			if got != tc.want {
				t.Errorf("getCurrentRole() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestReplicationZones(t *testing.T) {
	tests := []struct {
		name                 string
		replicationZonesRows rowsInterface
		replicationZonesErr  error
		lookupHostValue      map[string][]string
		lookupHostErr        map[string]error
		lookupAddrValue      map[string][]string
		lookupAddrErr        map[string]error
		role                 string
		want                 []string
	}{
		{
			name: "HappyPathIp",
			replicationZonesRows: &replicationZonesRows{
				count: 0,
				size:  1,
				data: []sql.NullString{
					sql.NullString{String: "1.2.3.4"},
				},
				shouldErr: false,
			},
			lookupAddrValue: map[string][]string{
				"1.2.3.4": []string{"testname.test-zone.c.fake-project.internal."},
			},
			role: sourceRole,
			want: []string{"test-zone"},
		},
		{
			name: "HappyPathIp2",
			replicationZonesRows: &replicationZonesRows{
				count: 0,
				size:  1,
				data: []sql.NullString{
					sql.NullString{String: "5.6.7.8"},
				},
				shouldErr: false,
			},
			lookupAddrValue: map[string][]string{
				"5.6.7.8": []string{"testname.test-zone2.c.fake-project.internal."},
			},
			role: sourceRole,
			want: []string{"test-zone2"},
		},
		{
			name: "NoWorkers",
			role: sourceRole,
			want: nil,
		},
		{
			name: "InvalidIP",
			replicationZonesRows: &replicationZonesRows{
				count: 0,
				size:  1,
				data: []sql.NullString{
					sql.NullString{String: "1.241234.3.4"},
				},
				shouldErr: false,
			},
			lookupHostErr: map[string]error{
				"1.241234.3.4": errors.New("test-error"),
			},
			role: sourceRole,
			want: nil,
		},
		{
			name: "ReplicaRole",
			replicationZonesRows: &replicationZonesRows{
				count: 0,
				size:  1,
				data: []sql.NullString{
					sql.NullString{String: "1.2.3.4"},
				},
				shouldErr: false,
			},
			role: replicaRole,
			want: nil,
		},
		{
			name: "EmptyResult",
			role: sourceRole,
			want: nil,
		},
		{
			name: "HappyPathHostname",
			replicationZonesRows: &replicationZonesRows{
				count: 0,
				size:  1,
				data: []sql.NullString{
					sql.NullString{String: "testname.test-zone.c.fake-project.internal."},
				},
				shouldErr: false,
			},
			lookupHostValue: map[string][]string{
				"testname.test-zone.c.fake-project.internal.": []string{"valid"},
			},
			role: sourceRole,
			want: []string{"test-zone"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := MySQLMetrics{
				db: &testDB{
					replicationZonesRows: tc.replicationZonesRows,
					replicationZonesErr:  tc.replicationZonesErr,
				},
			}
			netMock := mockNetInterface{
				lookupHostValue: tc.lookupHostValue,
				lookupHostErr:   tc.lookupHostErr,
				lookupAddrValue: tc.lookupAddrValue,
				lookupAddrErr:   tc.lookupAddrErr,
			}

			got := m.replicationZones(context.Background(), tc.role, netMock)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("replicationZones() returned diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestVersion(t *testing.T) {
	tests := []struct {
		name         string
		m            MySQLMetrics
		majorVersion string
		minorVersion string
		wantErr      bool
	}{
		{
			name: "HappyPath",
			m: MySQLMetrics{
				db: &testDB{
					versionRows: &versionRows{
						count: 0,
						size:  1,
						data: []string{
							"8.0.26",
						},
						shouldErr: false,
					},
					versionErr: nil,
				},
			},
			majorVersion: "8.0",
			minorVersion: "8.0.26",
			wantErr:      false,
		},
		{
			name: "HappyPath2",
			m: MySQLMetrics{
				db: &testDB{
					versionRows: &versionRows{
						count: 0,
						size:  1,
						data: []string{
							"8.0",
						},
						shouldErr: false,
					},
					versionErr: nil,
				},
			},
			majorVersion: "8.0",
			minorVersion: "8.0",
			wantErr:      false,
		},
		{
			name: "HappyPath3",
			m: MySQLMetrics{
				db: &testDB{
					versionRows: &versionRows{
						count: 0,
						size:  1,
						data: []string{
							"8",
						},
						shouldErr: false,
					},
					versionErr: nil,
				},
			},
			majorVersion: "8",
			minorVersion: "8",
			wantErr:      false,
		},
		{
			name: "EmptyVersion",
			m: MySQLMetrics{
				db: &testDB{
					versionRows: &versionRows{
						count: 0,
						size:  1,
						data: []string{
							"",
						},
						shouldErr: false,
					},
					versionErr: nil,
				},
			},
			majorVersion: "",
			minorVersion: "",
			wantErr:      false,
		},
		{
			name: "Error",
			m: MySQLMetrics{
				db: &testDB{
					versionRows: &versionRows{
						count: 0,
						size:  1,
						data: []string{
							"",
						},
						shouldErr: false,
					},
					versionErr: errors.New("test-error"),
				},
			},
			majorVersion: "",
			minorVersion: "",
			wantErr:      true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			majorVersion, minorVersion, err := tc.m.version(context.Background())
			if err != nil && !tc.wantErr {
				t.Errorf("version() returned an unexpected error: %v", err)
			}
			if err == nil && tc.wantErr {
				t.Errorf("version() did not return an expected error")
			}
			if majorVersion != tc.majorVersion {
				t.Errorf("version() majorVersion = %v, want %v", majorVersion, tc.majorVersion)
			}
			if minorVersion != tc.minorVersion {
				t.Errorf("version() minorVersion = %v, want %v", minorVersion, tc.minorVersion)
			}
		})
	}
}

func TestRootPasswordNotSet(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name         string
		db           *testDB
		expectResult bool
		expectErr    bool
	}{
		{
			name: "root_native_no_password",
			db: &testDB{
				mysqlUserRows: &mysqlUserMockRows{
					size: 1,
					data: [][]any{
						{"root", "%", "mysql_native_password", sql.NullString{String: "", Valid: true}},
					},
				},
			},
			expectResult: true,
			expectErr:    false,
		},
		{
			name: "root_caching_sha2_null_password",
			db: &testDB{
				mysqlUserRows: &mysqlUserMockRows{
					size: 1,
					data: [][]any{
						{"root", "%", "caching_sha2_password", sql.NullString{Valid: false}},
					},
				},
			},
			expectResult: true,
			expectErr:    false,
		},
		{
			name: "root_native_with_password",
			db: &testDB{
				mysqlUserRows: &mysqlUserMockRows{
					size: 1,
					data: [][]any{
						{"root", "%", "mysql_native_password", sql.NullString{String: "somehash", Valid: true}},
					},
				},
			},
			expectResult: false,
			expectErr:    false,
		},
		{
			name: "root_caching_sha2_with_password",
			db: &testDB{
				mysqlUserRows: &mysqlUserMockRows{
					size: 1,
					data: [][]any{
						{"root", "%", "caching_sha2_password", sql.NullString{String: "anotherhash", Valid: true}},
					},
				},
			},
			expectResult: false,
			expectErr:    false,
		},
		{
			name: "root_auth_socket",
			db: &testDB{
				mysqlUserRows: &mysqlUserMockRows{
					size: 1,
					data: [][]any{
						{"root", "localhost", "auth_socket", sql.NullString{String: "", Valid: true}},
					},
				},
			},
			expectResult: false,
			expectErr:    false,
		},
		{
			name: "multiple_root_one_insecure",
			db: &testDB{
				mysqlUserRows: &mysqlUserMockRows{
					size: 2,
					data: [][]any{
						{"root", "localhost", "auth_socket", sql.NullString{String: "", Valid: true}},
						{"root", "%", "mysql_native_password", sql.NullString{String: "", Valid: true}},
					},
				},
			},
			expectResult: true,
			expectErr:    false,
		},
		{
			name: "multiple_root_all_secure",
			db: &testDB{
				mysqlUserRows: &mysqlUserMockRows{
					size: 2,
					data: [][]any{
						{"root", "localhost", "auth_socket", sql.NullString{String: "", Valid: true}},
						{"root", "%", "caching_sha2_password", sql.NullString{String: "somehash", Valid: true}},
					},
				},
			},
			expectResult: false,
			expectErr:    false,
		},
		{
			name: "no_root_users",
			db: &testDB{
				mysqlUserRows: &mysqlUserMockRows{
					size: 0,
					data: [][]any{},
				},
			},
			expectResult: false,
			expectErr:    true,
		},
		{
			name: "query_error",
			db: &testDB{
				mysqlUserErr: errors.New("db connection failed"),
			},
			expectResult: false,
			expectErr:    true,
		},
		{
			name: "scan_error",
			db: &testDB{
				mysqlUserRows: &mysqlUserMockRows{
					size: 1,
					data: [][]any{
						{"root", "%", "mysql_native_password", sql.NullString{String: "", Valid: true}},
					},
					scanErr: true,
				},
			},
			expectResult: false,
			expectErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := MySQLMetrics{db: tc.db}
			result, err := m.rootPasswordNotSet(ctx)

			if (err != nil) != tc.expectErr {
				t.Errorf("CheckRootPasswordNotSet() got error: %v, want error presence: %v", err, tc.expectErr)
			}

			if result != tc.expectResult {
				t.Errorf("CheckRootPasswordNotSet() got result: %v, want result: %v", result, tc.expectResult)
			}
		})
	}
}

func TestExposedToPublicAccess(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name         string
		db           *testDB
		expectResult bool
		expectErr    bool
	}{
		{
			name: "one_user_broad_access",
			db: &testDB{
				exposedToPublicAccessRows: &exposedToPublicAccessMockRows{
					size: 1,
					data: [][]string{
						{"root", "%"},
					},
				},
			},
			expectResult: true,
			expectErr:    false,
		},
		{
			name: "multiple_users_broad_access",
			db: &testDB{
				exposedToPublicAccessRows: &exposedToPublicAccessMockRows{
					size: 2,
					data: [][]string{
						{"root", "%"},
						{"app", "%"},
					},
				},
			},
			expectResult: true,
			expectErr:    false,
		},
		{
			name: "no_users_broad_access",
			db: &testDB{
				exposedToPublicAccessRows: &exposedToPublicAccessMockRows{
					size: 0,
					data: [][]string{},
				},
			},
			expectResult: false,
			expectErr:    false,
		},
		{
			name: "query_error",
			db: &testDB{
				exposedToPublicAccessErr: errors.New("db query failed"),
			},
			expectResult: false,
			expectErr:    true,
		},
		{
			name: "scan_error",
			db: &testDB{
				exposedToPublicAccessRows: &exposedToPublicAccessMockRows{
					size:    1,
					data:    [][]string{{"root", "%"}},
					scanErr: true,
				},
			},
			expectResult: false,
			expectErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := MySQLMetrics{db: tc.db}
			result, err := m.exposedToPublicAccess(ctx)

			if (err != nil) != tc.expectErr {
				t.Errorf("CheckBroadAccess() got error: %v, want error presence: %v", err, tc.expectErr)
			}

			if result != tc.expectResult {
				t.Errorf("CheckBroadAccess() got result: %v, want result: %v", result, tc.expectResult)
			}
		})
	}
}

func TestUnencryptedConnectionsAllowed(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name         string
		db           *testDB
		expectResult bool
		expectErr    bool
	}{
		{
			name: "secure_transport_ON",
			db: &testDB{
				requireSecureTransportRows: &globalVarMockRows{
					size: 1,
					data: [][]string{{"require_secure_transport", "ON"}},
				},
			},
			expectResult: false, // Unencrypted NOT allowed
			expectErr:    false,
		},
		{
			name: "secure_transport_OFF",
			db: &testDB{
				requireSecureTransportRows: &globalVarMockRows{
					size: 1,
					data: [][]string{{"require_secure_transport", "OFF"}},
				},
			},
			expectResult: true, // Unencrypted allowed
			expectErr:    false,
		},
		{
			name: "secure_transport_off_lowercase",
			db: &testDB{
				requireSecureTransportRows: &globalVarMockRows{
					size: 1,
					data: [][]string{{"require_secure_transport", "off"}},
				},
			},
			expectResult: true, // Unencrypted allowed
			expectErr:    false,
		},
		{
			name: "variable_not_found",
			db: &testDB{
				requireSecureTransportRows: &globalVarMockRows{size: 0},
			},
			expectResult: false,
			expectErr:    true, // Should error if variable not found
		},
		{
			name: "query_error",
			db: &testDB{
				requireSecureTransportErr: errors.New("db query failed"),
			},
			expectResult: false,
			expectErr:    true,
		},
		{
			name: "scan_error",
			db: &testDB{
				requireSecureTransportRows: &globalVarMockRows{
					size:    1,
					data:    [][]string{{"require_secure_transport", "ON"}},
					scanErr: true,
				},
			},
			expectResult: false,
			expectErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := MySQLMetrics{db: tc.db}
			result, err := m.unencryptedConnectionsAllowed(ctx)

			if (err != nil) != tc.expectErr {
				t.Errorf("unencryptedConnectionsAllowed() got error: %v, want error presence: %v", err, tc.expectErr)
			}

			if !tc.expectErr && result != tc.expectResult {
				t.Errorf("unencryptedConnectionsAllowed() got result: %v, want result: %v", result, tc.expectResult)
			}
		})
	}
}

// TestAuditingEnabled tests the auditingDisabled function.
func TestAuditingEnabled(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name         string
		db           *testDB
		expectResult bool
		expectErr    bool
	}{
		{
			name: "audit_log_ACTIVE",
			db: &testDB{
				auditLogPluginRows: &pluginStatusMockRows{size: 1, data: [][]string{{"ACTIVE"}}},
			},
			expectResult: true,
			expectErr:    false,
		},
		{
			name: "audit_log_active_lowercase",
			db: &testDB{
				auditLogPluginRows: &pluginStatusMockRows{size: 1, data: [][]string{{"active"}}},
			},
			expectResult: true,
			expectErr:    false,
		},
		{
			name: "audit_log_DISABLED",
			db: &testDB{
				auditLogPluginRows: &pluginStatusMockRows{size: 1, data: [][]string{{"DISABLED"}}},
			},
			expectResult: false,
			expectErr:    false,
		},
		{
			name: "audit_log_other_status",
			db: &testDB{
				auditLogPluginRows: &pluginStatusMockRows{size: 1, data: [][]string{{"SOME_OTHER_STATUS"}}},
			},
			expectResult: false,
			expectErr:    false,
		},
		{
			name: "audit_log_not_found",
			db: &testDB{
				auditLogPluginRows: &pluginStatusMockRows{size: 0},
			},
			expectResult: false,
			expectErr:    false,
		},
		{
			name: "query_error",
			db: &testDB{
				auditLogPluginErr: errors.New("db query failed"),
			},
			expectResult: false, // Should return error
			expectErr:    true,
		},
		{
			name: "scan_error",
			db: &testDB{
				auditLogPluginRows: &pluginStatusMockRows{size: 1, data: [][]string{{"ACTIVE"}}, scanErr: true},
			},
			expectResult: false, // Should return error
			expectErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := MySQLMetrics{db: tc.db}
			result, err := m.auditingEnabled(ctx)

			if (err != nil) != tc.expectErr {
				t.Errorf("auditingEnabled() got error: %v, want error presence: %v", err, tc.expectErr)
			}

			if !tc.expectErr && result != tc.expectResult {
				t.Errorf("auditingEnabled() got result: %v, want result: %v", result, tc.expectResult)
			}
		})
	}
}

func TestSendMetadataToDatabaseCenter(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name                 string
		m                    MySQLMetrics
		sendMetadataErr      error
		wantSendMetadataCall bool
		wantDBcenterMetrics  map[string]string
		wantErr              bool
	}{
		{
			name: "Send metadata success - secure",
			m: MySQLMetrics{
				db: &testDB{
					engineRows: &isInnoDBRows{
						count: 0,
						size:  1,
						data: []sql.NullString{
							sql.NullString{String: "InnoDB"},
							sql.NullString{String: "DEFAULT"},
						},
						shouldErr: false,
					},
					engineErr:      nil,
					bufferPoolRows: &bufferPoolRows{count: 0, size: 1, data: 134217728, shouldErr: false},
					bufferPoolErr:  nil,
					versionRows:    &versionRows{size: 1, data: []string{"8.0.35"}},
					// Mock for CheckRootPasswordNotSet - secure
					mysqlUserRows: &mysqlUserMockRows{
						size: 1,
						data: [][]any{{"root", "localhost", "caching_sha2_password", sql.NullString{String: "hash", Valid: true}}},
					},
					// Mock for CheckBroadAccess - secure
					exposedToPublicAccessRows: &exposedToPublicAccessMockRows{size: 0},
					// Mock for CheckUnencryptedConnectionsAllowed - secure
					requireSecureTransportRows: &globalVarMockRows{
						size: 1,
						data: [][]string{{"require_secure_transport", "ON"}},
					},
					// Mock for CheckAuditingDisabled - secure
					auditLogPluginRows: &pluginStatusMockRows{size: 1, data: [][]string{{"ACTIVE"}}},
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
				DBcenterClient: databasecenter.NewClient(&configpb.Configuration{}, nil),
			},
			sendMetadataErr:      nil,
			wantSendMetadataCall: true,
			wantDBcenterMetrics: map[string]string{
				databasecenter.MajorVersionKey:                    "8.0",
				databasecenter.MinorVersionKey:                    "8.0.35",
				databasecenter.NoRootPasswordKey:                  "false",
				databasecenter.ExposedToPublicAccessKey:           "false",
				databasecenter.UnencryptedConnectionsKey:          "false",
				databasecenter.DatabaseAuditingDisabledKey:        "false",
				databasecenter.NotProtectedByAutomaticFailoverKey: "true",
				databasecenter.NoAutomatedBackupPolicyKey:         "true",
				databasecenter.LastBackupOldKey:                   "true",
			},
			wantErr: false,
		},
		{
			name: "Send metadata success - insecure root, broad access",
			m: MySQLMetrics{
				db: &testDB{
					// Default Happy Path mocks
					engineRows:     &isInnoDBRows{size: 1, data: []sql.NullString{{String: "InnoDB", Valid: true}, {String: "DEFAULT", Valid: true}}},
					bufferPoolRows: &bufferPoolRows{size: 1, data: 134217728},
					versionRows:    &versionRows{size: 1, data: []string{"8.0.35"}},
					// Mock for CheckRootPasswordNotSet - insecure
					mysqlUserRows: &mysqlUserMockRows{
						size: 1,
						data: [][]any{{"root", "%", "mysql_native_password", sql.NullString{String: "", Valid: true}}},
					},
					// Mock for CheckBroadAccess - insecure
					exposedToPublicAccessRows: &exposedToPublicAccessMockRows{size: 1, data: [][]string{{"test", "%"}}},
					// Mock for CheckUnencryptedConnectionsAllowed - insecure
					requireSecureTransportRows: &globalVarMockRows{
						size: 1,
						data: [][]string{{"require_secure_transport", "OFF"}},
					},
					// Mock for CheckAuditingDisabled - insecure
					auditLogPluginRows: &pluginStatusMockRows{size: 1, data: [][]string{{"DISABLED"}}},
				},
				execute: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{StdOut: "MemTotal:        4025040 kB\n"}
				},
				WLMClient: &gcefake.TestWLM{
					WriteInsightErrs: []error{nil},
					WriteInsightResponses: []*wlm.WriteInsightResponse{
						&wlm.WriteInsightResponse{ServerResponse: googleapi.ServerResponse{HTTPStatusCode: 201}},
					},
				},
				DBcenterClient: databasecenter.NewClient(&configpb.Configuration{}, nil),
			},
			wantSendMetadataCall: true,
			wantDBcenterMetrics: map[string]string{
				databasecenter.MajorVersionKey:                    "8.0",
				databasecenter.MinorVersionKey:                    "8.0.35",
				databasecenter.NoRootPasswordKey:                  "true",
				databasecenter.ExposedToPublicAccessKey:           "true",
				databasecenter.UnencryptedConnectionsKey:          "true",
				databasecenter.DatabaseAuditingDisabledKey:        "true",
				databasecenter.NotProtectedByAutomaticFailoverKey: "true",
				databasecenter.NoAutomatedBackupPolicyKey:         "true",
				databasecenter.LastBackupOldKey:                   "true",
			},
			wantErr: false,
		},
		{
			name: "Send metadata failure, should not return error",
			m: MySQLMetrics{
				db: &testDB{
					engineRows: &isInnoDBRows{
						count: 0,
						size:  1,
						data: []sql.NullString{
							sql.NullString{String: "InnoDB"},
							sql.NullString{String: "DEFAULT"},
						},
						shouldErr: false,
					},
					engineErr:      nil,
					bufferPoolRows: &bufferPoolRows{count: 0, size: 1, data: 134217728, shouldErr: false},
					bufferPoolErr:  nil,
					versionRows:    &versionRows{size: 1, data: []string{"8.0.35"}},
					mysqlUserRows: &mysqlUserMockRows{
						size: 1,
						data: [][]any{{"root", "localhost", "caching_sha2_password", sql.NullString{String: "hash", Valid: true}}},
					},
					exposedToPublicAccessRows: &exposedToPublicAccessMockRows{size: 0},
					requireSecureTransportRows: &globalVarMockRows{
						size: 1,
						data: [][]string{{"require_secure_transport", "ON"}},
					},
					auditLogPluginRows: &pluginStatusMockRows{size: 0},
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
				DBcenterClient: databasecenter.NewClient(&configpb.Configuration{}, nil),
			},
			sendMetadataErr:      fmt.Errorf("db center error"),
			wantSendMetadataCall: true,
			wantDBcenterMetrics: map[string]string{
				databasecenter.MajorVersionKey:                    "8.0",
				databasecenter.MinorVersionKey:                    "8.0.35",
				databasecenter.NoRootPasswordKey:                  "false",
				databasecenter.ExposedToPublicAccessKey:           "false",
				databasecenter.UnencryptedConnectionsKey:          "false",
				databasecenter.DatabaseAuditingDisabledKey:        "true",
				databasecenter.NotProtectedByAutomaticFailoverKey: "true",
				databasecenter.NoAutomatedBackupPolicyKey:         "true",
				databasecenter.LastBackupOldKey:                   "true",
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockClient := &MockDatabaseCenterClient{
				sendMetadataCalled: false,
				sendMetadataErr:    tc.sendMetadataErr,
			}
			// Set the mock dbcenter client in the MySQLMetrics object
			tc.m.DBcenterClient = mockClient
			// Call the function under test
			err := tc.m.CollectDBCenterMetricsOnce(ctx)

			// Assertions
			if err != nil && !tc.wantErr {
				t.Errorf("CollectDBCenterMetricsOnce returned an unexpected error: %v", err)
			}
			if err == nil && tc.wantErr {
				t.Errorf("CollectDBCenterMetricsOnce did not return an expected error")
			}
			if mockClient.sendMetadataCalled != tc.wantSendMetadataCall {
				t.Errorf("CollectDBCenterMetricsOnce: sendMetadataCalled = %v, want %v", mockClient.sendMetadataCalled, tc.wantSendMetadataCall)
			}
			if tc.wantSendMetadataCall {
				if diff := cmp.Diff(tc.wantDBcenterMetrics, mockClient.gotMetrics.Metrics); diff != "" {
					t.Errorf("CollectDBCenterMetricsOnce() DBcenterMetrics diff (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestCollectDBCenterMetricsOnce(t *testing.T) {
	tests := []struct {
		name                 string
		m                    MySQLMetrics
		sendMetadataErr      error
		wantSendMetadataCall bool
		wantDBcenterMetrics  map[string]string
		wantErr              bool
	}{
		{
			name: "Send metadata success - secure",
			m: MySQLMetrics{
				db: &testDB{
					versionRows: &versionRows{size: 1, data: []string{"8.0.35"}},
					// Mock for CheckRootPasswordNotSet - secure
					mysqlUserRows: &mysqlUserMockRows{
						size: 1,
						data: [][]any{{"root", "localhost", "caching_sha2_password", sql.NullString{String: "hash", Valid: true}}},
					},
					// Mock for CheckBroadAccess - secure
					exposedToPublicAccessRows: &exposedToPublicAccessMockRows{size: 0},
					// Mock for CheckUnencryptedConnectionsAllowed - secure
					requireSecureTransportRows: &globalVarMockRows{
						size: 1,
						data: [][]string{{"require_secure_transport", "ON"}},
					},
					// Mock for CheckAuditingDisabled - secure
					auditLogPluginRows: &pluginStatusMockRows{size: 1, data: [][]string{{"ACTIVE"}}},
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
				DBcenterClient: databasecenter.NewClient(&configpb.Configuration{}, nil),
			},
			sendMetadataErr:      nil,
			wantSendMetadataCall: true,
			wantDBcenterMetrics: map[string]string{
				databasecenter.MajorVersionKey:                    "8.0",
				databasecenter.MinorVersionKey:                    "8.0.35",
				databasecenter.NoRootPasswordKey:                  "false",
				databasecenter.ExposedToPublicAccessKey:           "false",
				databasecenter.UnencryptedConnectionsKey:          "false",
				databasecenter.DatabaseAuditingDisabledKey:        "false",
				databasecenter.NotProtectedByAutomaticFailoverKey: "true",
				databasecenter.NoAutomatedBackupPolicyKey:         "true",
				databasecenter.LastBackupOldKey:                   "true",
			},
			wantErr: false,
		},
		{
			name: "Send metadata failure, should not return error",
			m: MySQLMetrics{
				db: &testDB{
					versionRows: &versionRows{size: 1, data: []string{"8.0.35"}},
					// Mock for CheckRootPasswordNotSet - secure
					mysqlUserRows: &mysqlUserMockRows{
						size: 1,
						data: [][]any{{"root", "localhost", "caching_sha2_password", sql.NullString{String: "hash", Valid: true}}},
					},
					// Mock for CheckBroadAccess - secure
					exposedToPublicAccessRows: &exposedToPublicAccessMockRows{size: 0},
					// Mock for CheckUnencryptedConnectionsAllowed - secure
					requireSecureTransportRows: &globalVarMockRows{
						size: 1,
						data: [][]string{{"require_secure_transport", "ON"}},
					},
					// Mock for CheckAuditingDisabled - secure
					auditLogPluginRows: &pluginStatusMockRows{size: 1, data: [][]string{{"ACTIVE"}}},
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
				DBcenterClient: databasecenter.NewClient(&configpb.Configuration{}, nil),
			},
			sendMetadataErr:      fmt.Errorf("db center error"),
			wantSendMetadataCall: true,
			wantDBcenterMetrics: map[string]string{
				databasecenter.MajorVersionKey:                    "8.0",
				databasecenter.MinorVersionKey:                    "8.0.35",
				databasecenter.NoRootPasswordKey:                  "false",
				databasecenter.ExposedToPublicAccessKey:           "false",
				databasecenter.UnencryptedConnectionsKey:          "false",
				databasecenter.DatabaseAuditingDisabledKey:        "false",
				databasecenter.NotProtectedByAutomaticFailoverKey: "true",
				databasecenter.NoAutomatedBackupPolicyKey:         "true",
				databasecenter.LastBackupOldKey:                   "true",
			},
			wantErr: false,
		},
	}

	ctx := context.Background()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockClient := &MockDatabaseCenterClient{
				sendMetadataCalled: false,
				sendMetadataErr:    tc.sendMetadataErr,
			}
			// Set the mock dbcenter client in the MySQLMetrics object
			tc.m.DBcenterClient = mockClient
			// Call the function under test
			err := tc.m.CollectDBCenterMetricsOnce(ctx)

			// Assertions
			if err != nil && !tc.wantErr {
				t.Errorf("CollectDBCenterMetricsOnce returned an unexpected error: %v", err)
			}
			if err == nil && tc.wantErr {
				t.Errorf("CollectDBCenterMetricsOnce did not return an expected error")
			}
			if mockClient.sendMetadataCalled != tc.wantSendMetadataCall {
				t.Errorf("CollectDBCenterMetricsOnce: sendMetadataCalled = %v, want %v", mockClient.sendMetadataCalled, tc.wantSendMetadataCall)
			}
			if tc.wantSendMetadataCall {
				if diff := cmp.Diff(tc.wantDBcenterMetrics, mockClient.gotMetrics.Metrics); diff != "" {
					t.Errorf("CollectDBCenterMetricsOnce() DBcenterMetrics diff (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestNotProtectedByAutoFailover(t *testing.T) {
	tests := []struct {
		name string
		m    MySQLMetrics
		want bool
	}{
		{
			name: "GroupReplicationActive",
			m: MySQLMetrics{
				db: &testDB{
					groupReplicationRows: &countMockRows{size: 1, data: 3},
				},
			},
			want: false,
		},
		{
			name: "GaleraActive",
			m: MySQLMetrics{
				db: &testDB{
					wsrepStatusRows: &globalVarMockRows{
						size: 2,
						data: [][]string{{"wsrep_ready", "ON"}, {"wsrep_cluster_size", "3"}},
					},
				},
			},
			want: false,
		},
		{
			name: "NDBClusterActive",
			m: MySQLMetrics{
				db: &testDB{
					ndbEngineRows: &globalVarMockRows{
						size: 1,
						data: [][]string{{"YES"}},
					},
				},
			},
			want: false,
		},
		{
			name: "PrimaryNoBinlog",
			m: MySQLMetrics{
				db: &testDB{
					replicaRows:   &replicaRows{size: 0},
					slaveRows:     &slaveRows{size: 0},
					logBinVarRows: &globalVarMockRows{size: 1, data: [][]string{{"log_bin", "OFF"}}},
				},
			},
			want: true,
		},
		{
			name: "PrimaryNoDumpThreads",
			m: MySQLMetrics{
				db: &testDB{
					replicaRows:     &replicaRows{size: 0},
					slaveRows:       &slaveRows{size: 0},
					logBinVarRows:   &globalVarMockRows{size: 1, data: [][]string{{"log_bin", "ON"}}},
					dumpThreadsRows: &countMockRows{size: 1, data: 0},
				},
			},
			want: true,
		},
		{
			name: "PrimaryWithReplicasAndOrchestratorUser",
			m: MySQLMetrics{
				db: &testDB{
					replicaRows:     &replicaRows{size: 0},
					slaveRows:       &slaveRows{size: 0},
					logBinVarRows:   &globalVarMockRows{size: 1, data: [][]string{{"log_bin", "ON"}}},
					dumpThreadsRows: &countMockRows{size: 1, data: 1},
					managerUserRows: &countMockRows{size: 1, data: 1},
				},
			},
			want: false,
		},
		{
			name: "PrimaryWithReplicasNoManager",
			m: MySQLMetrics{
				db: &testDB{
					replicaRows:     &replicaRows{size: 0},
					slaveRows:       &slaveRows{size: 0},
					logBinVarRows:   &globalVarMockRows{size: 1, data: [][]string{{"log_bin", "ON"}}},
					dumpThreadsRows: &countMockRows{size: 1, data: 1},
					managerUserRows: &countMockRows{size: 1, data: 0},
				},
				execute: func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{StdOut: ""}
				},
			},
			want: true,
		},
		{
			name: "ReplicaBrokenThread",
			m: MySQLMetrics{
				db: &testDB{
					replicaRows: &replicaRows{
						size: 1,
						data: []sql.NullString{
							sql.NullString{String: "No", Valid: true},
							sql.NullString{String: "Yes", Valid: true},
						},
					},
				},
			},
			want: true,
		},
		{
			name: "ReplicaHealthyWithManager",
			m: MySQLMetrics{
				db: &testDB{
					replicaRows: &replicaRows{
						size: 1,
						data: []sql.NullString{
							sql.NullString{String: "Yes", Valid: true},
							sql.NullString{String: "Yes", Valid: true},
						},
					},
					managerUserRows: &countMockRows{size: 1, data: 1},
				},
			},
			want: false,
		},
		{
			name: "LocalOrchestratorActive",
			m: MySQLMetrics{
				db: &testDB{
					replicaRows: &replicaRows{
						size: 1,
						data: []sql.NullString{
							sql.NullString{String: "Yes", Valid: true},
							sql.NullString{String: "Yes", Valid: true},
						},
					},
					managerUserRows: &countMockRows{size: 1, data: 0},
				},
				execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
					if params.Executable == "grep" && len(params.Args) > 0 && params.Args[0] == "RecoverMasterClusterFilters" {
						return commandlineexecutor.Result{StdOut: "RecoverMasterClusterFilters: [...]"}
					}
					if params.Executable == "systemctl" && len(params.Args) > 1 && params.Args[1] == "orchestrator" {
						return commandlineexecutor.Result{StdOut: "active\n"}
					}
					return commandlineexecutor.Result{Error: errors.New("command failed")}
				},
			},
			want: false,
		},
		{
			name: "LocalMHAServiceActive",
			m: MySQLMetrics{
				db: &testDB{
					replicaRows: &replicaRows{
						size: 1,
						data: []sql.NullString{
							sql.NullString{String: "Yes", Valid: true},
							sql.NullString{String: "Yes", Valid: true},
						},
					},
					managerUserRows: &countMockRows{size: 1, data: 0},
				},
				execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
					if params.Executable == "systemctl" && len(params.Args) > 1 && params.Args[1] == "mha-manager" {
						return commandlineexecutor.Result{StdOut: "active\n"}
					}
					return commandlineexecutor.Result{Error: errors.New("command failed")}
				},
			},
			want: false,
		},
		{
			name: "PrimaryDumpThreadsQueryError",
			m: MySQLMetrics{
				db: &testDB{
					replicaRows:     &replicaRows{size: 0},
					slaveRows:       &slaveRows{size: 0},
					logBinVarRows:   &globalVarMockRows{size: 1, data: [][]string{{"log_bin", "ON"}}},
					dumpThreadsErr:  errors.New("db query error"),
					managerUserRows: &countMockRows{size: 1, data: 1},
				},
			},
			want: true,
		},
		{
			name: "ReplicaSQLThreadNotRunningWithManager",
			m: MySQLMetrics{
				db: &testDB{
					replicaRows: &replicaRows{
						size: 1,
						data: []sql.NullString{
							sql.NullString{String: "Yes", Valid: true},
							sql.NullString{String: "No", Valid: true},
						},
					},
					managerUserRows: &countMockRows{size: 1, data: 1},
				},
			},
			want: true,
		},
		{
			name: "GroupReplicationScanErrorCountAboveTwo",
			m: MySQLMetrics{
				db: &testDB{
					groupReplicationRows: &countMockRows{size: 1, data: 3, shouldErr: true},
					replicaRows:          &replicaRows{size: 0},
					slaveRows:            &slaveRows{size: 0},
					logBinVarRows:        &globalVarMockRows{size: 1, data: [][]string{{"log_bin", "OFF"}}},
				},
			},
			want: true,
		},
		{
			name: "OrchestratorConfigGrepReturnsError",
			m: MySQLMetrics{
				db: &testDB{
					replicaRows: &replicaRows{
						size: 1,
						data: []sql.NullString{
							sql.NullString{String: "Yes", Valid: true},
							sql.NullString{String: "Yes", Valid: true},
						},
					},
					managerUserRows: &countMockRows{size: 1, data: 0},
				},
				execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
					if params.Executable == "grep" && len(params.Args) > 0 && params.Args[0] == "RecoverMasterClusterFilters" {
						return commandlineexecutor.Result{Error: errors.New("grep failed"), StdOut: "RecoverMasterClusterFilters: [...]"}
					}
					if params.Executable == "systemctl" && len(params.Args) > 1 && params.Args[1] == "orchestrator" {
						return commandlineexecutor.Result{StdOut: "active\n"}
					}
					return commandlineexecutor.Result{Error: errors.New("command failed")}
				},
			},
			want: true,
		},
	}

	ctx := context.Background()
	for _, tc := range tests {
		got, err := tc.m.notProtectedByAutoFailover(ctx)
		if err != nil {
			t.Errorf("notProtectedByAutoFailover() test %v returned unexpected error: %v", tc.name, err)
		}
		if got != tc.want {
			t.Errorf("notProtectedByAutoFailover() test %v = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestIsReplicaHealthy(t *testing.T) {
	tests := []struct {
		name        string
		replicaRows rowsInterface
		replicaErr  error
		slaveRows   rowsInterface
		slaveErr    error
		want        bool
		wantErr     bool
	}{
		{
			name: "BothThreadsRunning",
			replicaRows: &replicaRows{
				size: 1,
				data: []sql.NullString{
					{String: "Yes", Valid: true},
					{String: "Yes", Valid: true},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "SQLThreadNotRunning",
			replicaRows: &replicaRows{
				size: 1,
				data: []sql.NullString{
					{String: "Yes", Valid: true},
					{String: "No", Valid: true},
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "IOThreadNotRunning",
			replicaRows: &replicaRows{
				size: 1,
				data: []sql.NullString{
					{String: "No", Valid: true},
					{String: "Yes", Valid: true},
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "NeitherThreadRunning",
			replicaRows: &replicaRows{
				size: 1,
				data: []sql.NullString{
					{String: "No", Valid: true},
					{String: "No", Valid: true},
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name:       "QueryError",
			replicaErr: errors.New("query error"),
			slaveErr:   errors.New("query error"),
			want:       false,
			wantErr:    true,
		},
		{
			name:       "FallbackToSlaveStatus",
			replicaErr: errors.New("syntax error"),
			slaveRows: &slaveRows{
				size: 1,
				data: []sql.NullString{
					{String: "Yes", Valid: true},
					{String: "Yes", Valid: true},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "ScanError",
			replicaRows: &replicaRows{
				size:      1,
				data:      []sql.NullString{{String: "Yes", Valid: true}},
				shouldErr: true,
			},
			want:    false,
			wantErr: true,
		},
	}
	ctx := context.Background()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := MySQLMetrics{
				db: &testDB{
					replicaRows: tc.replicaRows,
					replicaErr:  tc.replicaErr,
					slaveRows:   tc.slaveRows,
					slaveErr:    tc.slaveErr,
				},
			}
			got, err := m.isReplicaHealthy(ctx)
			if (err != nil) != tc.wantErr {
				t.Errorf("isReplicaHealthy() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("isReplicaHealthy() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsInnoDBClusterActive(t *testing.T) {
	tests := []struct {
		name                 string
		groupReplicationRows rowsInterface
		groupReplicationErr  error
		want                 bool
	}{
		{
			name:                 "ActiveTwoNodes",
			groupReplicationRows: &countMockRows{size: 1, data: 2},
			want:                 true,
		},
		{
			name:                 "ActiveThreeNodes",
			groupReplicationRows: &countMockRows{size: 1, data: 3},
			want:                 true,
		},
		{
			name:                 "InactiveSingleNode",
			groupReplicationRows: &countMockRows{size: 1, data: 1},
			want:                 false,
		},
		{
			name:                 "InactiveZeroNodes",
			groupReplicationRows: &countMockRows{size: 1, data: 0},
			want:                 false,
		},
		{
			name:                "QueryError",
			groupReplicationErr: errors.New("db query error"),
			want:                false,
		},
		{
			name:                 "ScanErrorWithCountAboveTwo",
			groupReplicationRows: &countMockRows{size: 1, data: 3, shouldErr: true},
			want:                 false,
		},
	}
	ctx := context.Background()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := MySQLMetrics{
				db: &testDB{
					groupReplicationRows: tc.groupReplicationRows,
					groupReplicationErr:  tc.groupReplicationErr,
				},
			}
			got := m.isInnoDBClusterActive(ctx)
			if got != tc.want {
				t.Errorf("isInnoDBClusterActive() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsGaleraClusterActive(t *testing.T) {
	tests := []struct {
		name            string
		wsrepStatusRows rowsInterface
		wsrepStatusErr  error
		want            bool
	}{
		{
			name: "ActiveGaleraCluster",
			wsrepStatusRows: &globalVarMockRows{
				size: 2,
				data: [][]string{{"wsrep_ready", "ON"}, {"wsrep_cluster_size", "3"}},
			},
			want: true,
		},
		{
			name: "GaleraNotReady",
			wsrepStatusRows: &globalVarMockRows{
				size: 2,
				data: [][]string{{"wsrep_ready", "OFF"}, {"wsrep_cluster_size", "3"}},
			},
			want: false,
		},
		{
			name: "GaleraClusterSizeTooSmall",
			wsrepStatusRows: &globalVarMockRows{
				size: 2,
				data: [][]string{{"wsrep_ready", "ON"}, {"wsrep_cluster_size", "1"}},
			},
			want: false,
		},
		{
			name:           "QueryError",
			wsrepStatusErr: errors.New("db query error"),
			want:           false,
		},
	}
	ctx := context.Background()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := MySQLMetrics{
				db: &testDB{
					wsrepStatusRows: tc.wsrepStatusRows,
					wsrepStatusErr:  tc.wsrepStatusErr,
				},
			}
			got := m.isGaleraClusterActive(ctx)
			if got != tc.want {
				t.Errorf("isGaleraClusterActive() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNoAutomatedBackupPolicy(t *testing.T) {
	tests := []struct {
		name    string
		m       MySQLMetrics
		want    bool
		wantErr bool
	}{
		{
			name: "UserCronRootBackupDetected",
			m: MySQLMetrics{
				execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
					if params.Executable == "crontab" && len(params.Args) >= 2 && params.Args[1] == "root" {
						return commandlineexecutor.Result{StdOut: "0 2 * * * /usr/bin/mysqldump --all-databases > /backup.sql\n"}
					}
					return commandlineexecutor.Result{Error: errors.New("command failed")}
				},
			},
			want: false,
		},
		{
			name: "UserCronMysqlBackupDetected",
			m: MySQLMetrics{
				execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
					if params.Executable == "crontab" && len(params.Args) >= 2 && params.Args[1] == "mysql" {
						return commandlineexecutor.Result{StdOut: "0 3 * * * xtrabackup --backup --target-dir=/var/backups\n"}
					}
					return commandlineexecutor.Result{Error: errors.New("command failed")}
				},
			},
			want: false,
		},
		{
			name: "SystemCronBackupDetected",
			m: MySQLMetrics{
				execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
					if params.Executable == "grep" {
						return commandlineexecutor.Result{StdOut: "/etc/cron.d/backup:0 2 * * * root mysqldump -u root test\n"}
					}
					return commandlineexecutor.Result{Error: errors.New("command failed")}
				},
			},
			want: false,
		},
		{
			name: "SystemdTimerBackupDetected",
			m: MySQLMetrics{
				execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
					if params.Executable == "systemctl" && len(params.Args) > 0 && params.Args[0] == "list-timers" {
						return commandlineexecutor.Result{StdOut: "NEXT LEFT LAST PASSED UNIT ACTIVATES\nThu 2026-07-09 12:00:00 UTC 1h left Thu 2026-07-09 11:00:00 UTC 20min ago mysql-backup.timer mysql-backup.service\n"}
					}
					if params.Executable == "systemctl" && len(params.Args) > 0 && params.Args[0] == "cat" {
						return commandlineexecutor.Result{StdOut: "ExecStart=/usr/bin/mysqldump --all-databases\n"}
					}
					return commandlineexecutor.Result{Error: errors.New("command failed")}
				},
			},
			want: false,
		},
		{
			name: "MEBHistoryTableBackupDetected",
			m: MySQLMetrics{
				db: &testDB{
					mebHistoryRows: &countMockRows{size: 1, data: 2},
				},
			},
			want: false,
		},
		{
			name: "XtraBackupHistoryTableBackupDetected",
			m: MySQLMetrics{
				db: &testDB{
					mebHistoryRows:        &countMockRows{size: 1, data: 0},
					xtrabackupHistoryRows: &countMockRows{size: 1, data: 3},
				},
			},
			want: false,
		},
		{
			name: "MEBHistoryCountBelowThreshold",
			m: MySQLMetrics{
				db: &testDB{
					mebHistoryRows:        &countMockRows{size: 1, data: 1},
					xtrabackupHistoryRows: &countMockRows{size: 1, data: 0},
				},
			},
			want: true,
		},
		{
			name: "UserCronCommentedOut_NoPolicyDetected",
			m: MySQLMetrics{
				db: &testDB{
					mebHistoryRows:        &countMockRows{size: 1, data: 0},
					xtrabackupHistoryRows: &countMockRows{size: 1, data: 0},
				},
				execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
					if params.Executable == "crontab" {
						return commandlineexecutor.Result{StdOut: "# 0 2 * * * /usr/bin/mysqldump --all-databases > /backup.sql\n"}
					}
					return commandlineexecutor.Result{Error: errors.New("command failed")}
				},
			},
			want: true,
		},
		{
			name: "NoPolicyDetected_SignalFired",
			m: MySQLMetrics{
				db: &testDB{
					mebHistoryRows:        &countMockRows{size: 1, data: 0},
					xtrabackupHistoryRows: &countMockRows{size: 1, data: 0},
				},
				execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{Error: errors.New("command failed")}
				},
			},
			want: true,
		},
		{
			name: "ErrorsInAllChecks_SignalFired",
			m: MySQLMetrics{
				db: &testDB{
					mebHistoryErr:        errors.New("db connection error"),
					xtrabackupHistoryErr: errors.New("db connection error"),
				},
				execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{Error: errors.New("command error")}
				},
			},
			want: true,
		},
	}

	ctx := context.Background()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.m.execute == nil {
				tc.m.execute = func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{Error: errors.New("command failed")}
				}
			}
			got, err := tc.m.noAutomatedBackupPolicy(ctx)
			if (err != nil) != tc.wantErr {
				t.Errorf("noAutomatedBackupPolicy() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("noAutomatedBackupPolicy() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestContainsBackupSignature(t *testing.T) {
	tests := []struct {
		name string
		line string
		want bool
	}{
		{
			name: "MySQLDump",
			line: "0 2 * * * /usr/bin/mysqldump -u root db > /backup.sql",
			want: true,
		},
		{
			name: "XtraBackup",
			line: "0 3 * * * xtrabackup --backup --target-dir=/var/backups",
			want: true,
		},
		{
			name: "GCloudComputeDisksSnapshot",
			line: "gcloud compute disks snapshot my-disk --snapshot-names=backup",
			want: true,
		},
		{
			name: "CommentedOut",
			line: "# 0 2 * * * mysqldump -u root db > /backup.sql",
			want: false,
		},
		{
			name: "EmptyLine",
			line: "  ",
			want: false,
		},
		{
			name: "UnrelatedCommand",
			line: "0 * * * * echo 'hello world'",
			want: false,
		},
		{
			name: "CaseInsensitive",
			line: "0 2 * * * MYSQLDUMP --ALL-DATABASES",
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := containsBackupSignature(tc.line)
			if got != tc.want {
				t.Errorf("containsBackupSignature(%q) = %v, want %v", tc.line, got, tc.want)
			}
		})
	}
}

func TestBuildGrepBackupPattern(t *testing.T) {
	got := buildGrepBackupPattern()
	if !strings.Contains(got, "mysqldump") {
		t.Errorf("buildGrepBackupPattern() = %q, expected to contain mysqldump", got)
	}
	if !strings.Contains(got, "gcloud.*compute.*disks.*snapshot") {
		t.Errorf("buildGrepBackupPattern() = %q, expected to contain gcloud.*compute.*disks.*snapshot", got)
	}
}

func TestLastBackupOld(t *testing.T) {
	tests := []struct {
		name                  string
		mebHistoryRows        rowsInterface
		mebHistoryErr         error
		xtrabackupHistoryRows rowsInterface
		xtrabackupHistoryErr  error
		execute               func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result
		want                  bool
		wantErr               bool
	}{
		{
			name:           "RecentMEBBackupFound",
			mebHistoryRows: &countMockRows{size: 1, data: 1},
			want:           false,
			wantErr:        false,
		},
		{
			name:                  "RecentXtraBackupFound",
			mebHistoryRows:        &countMockRows{size: 1, data: 0},
			xtrabackupHistoryRows: &countMockRows{size: 1, data: 1},
			want:                  false,
			wantErr:               false,
		},
		{
			name:                  "RecentSystemdTimerFound",
			mebHistoryRows:        &countMockRows{size: 1, data: 0},
			xtrabackupHistoryRows: &countMockRows{size: 1, data: 0},
			execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				if params.Executable == "systemctl" && len(params.Args) > 0 && params.Args[0] == "list-timers" {
					return commandlineexecutor.Result{StdOut: "Thu 2026-07-16 12:00:00 UTC 15m left Thu 2026-07-16 10:00:00 UTC 44m ago mysql-backup.timer mysql-backup.service\n"}
				}
				if params.Executable == "systemctl" && len(params.Args) > 1 && params.Args[0] == "cat" && params.Args[1] == "mysql-backup.service" {
					return commandlineexecutor.Result{StdOut: "ExecStart=/usr/bin/mysqldump --all-databases\n"}
				}
				if params.Executable == "systemctl" && len(params.Args) > 1 && params.Args[0] == "show" && params.Args[1] == "mysql-backup.service" {
					recentTimestamp := time.Now().Add(-2 * time.Hour).Format("Mon 2006-01-02 15:04:05 MST")
					return commandlineexecutor.Result{StdOut: fmt.Sprintf("ExecMainStatus=0\nExecMainExitTimestamp=%s\n", recentTimestamp)}
				}
				return commandlineexecutor.Result{Error: errors.New("unexpected command")}
			},
			want:    false,
			wantErr: false,
		},
		{
			name:                  "NoBackupFound",
			mebHistoryRows:        &countMockRows{size: 1, data: 0},
			xtrabackupHistoryRows: &countMockRows{size: 1, data: 0},
			execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				if params.Executable == "systemctl" && len(params.Args) > 0 && params.Args[0] == "list-timers" {
					return commandlineexecutor.Result{StdOut: ""}
				}
				return commandlineexecutor.Result{Error: errors.New("unexpected command")}
			},
			want:    true,
			wantErr: false,
		},
		{
			name:                 "QueryErrorsDegradeSafely",
			mebHistoryErr:        errors.New("meb db error"),
			xtrabackupHistoryErr: errors.New("xtrabackup db error"),
			execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				return commandlineexecutor.Result{Error: errors.New("systemctl error")}
			},
			want:    true,
			wantErr: true,
		},
	}

	ctx := context.Background()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := MySQLMetrics{
				db: &testDB{
					mebHistoryRows:        tc.mebHistoryRows,
					mebHistoryErr:         tc.mebHistoryErr,
					xtrabackupHistoryRows: tc.xtrabackupHistoryRows,
					xtrabackupHistoryErr:  tc.xtrabackupHistoryErr,
				},
				execute: tc.execute,
			}
			if m.execute == nil {
				m.execute = func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{Error: errors.New("not executed")}
				}
			}
			got, err := m.lastBackupOld(ctx)
			if (err != nil) != tc.wantErr {
				t.Errorf("lastBackupOld() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("lastBackupOld() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHasRecentMEBBackup(t *testing.T) {
	tests := []struct {
		name           string
		mebHistoryRows rowsInterface
		mebHistoryErr  error
		want           bool
		wantErr        bool
	}{
		{
			name:           "CountOneOrMore",
			mebHistoryRows: &countMockRows{size: 1, data: 1},
			want:           true,
			wantErr:        false,
		},
		{
			name:           "CountZero",
			mebHistoryRows: &countMockRows{size: 1, data: 0},
			want:           false,
			wantErr:        false,
		},
		{
			name:          "QueryError",
			mebHistoryErr: errors.New("db error"),
			want:          false,
			wantErr:       true,
		},
		{
			name:           "NilRows",
			mebHistoryRows: nil,
			want:           false,
			wantErr:        true,
		},
	}

	ctx := context.Background()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := MySQLMetrics{
				db: &testDB{
					mebHistoryRows: tc.mebHistoryRows,
					mebHistoryErr:  tc.mebHistoryErr,
				},
			}
			got, err := m.hasRecentMEBBackup(ctx)
			if (err != nil) != tc.wantErr {
				t.Errorf("hasRecentMEBBackup() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("hasRecentMEBBackup() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHasRecentXtraBackup(t *testing.T) {
	tests := []struct {
		name                  string
		xtrabackupHistoryRows rowsInterface
		xtrabackupHistoryErr  error
		want                  bool
		wantErr               bool
	}{
		{
			name:                  "CountOneOrMore",
			xtrabackupHistoryRows: &countMockRows{size: 1, data: 1},
			want:                  true,
			wantErr:               false,
		},
		{
			name:                  "CountZero",
			xtrabackupHistoryRows: &countMockRows{size: 1, data: 0},
			want:                  false,
			wantErr:               false,
		},
		{
			name:                 "QueryError",
			xtrabackupHistoryErr: errors.New("db error"),
			want:                 false,
			wantErr:              true,
		},
		{
			name:                  "NilRows",
			xtrabackupHistoryRows: nil,
			want:                  false,
			wantErr:               true,
		},
	}

	ctx := context.Background()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := MySQLMetrics{
				db: &testDB{
					xtrabackupHistoryRows: tc.xtrabackupHistoryRows,
					xtrabackupHistoryErr:  tc.xtrabackupHistoryErr,
				},
			}
			got, err := m.hasRecentXtraBackup(ctx)
			if (err != nil) != tc.wantErr {
				t.Errorf("hasRecentXtraBackup() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("hasRecentXtraBackup() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHasRecentSystemdBackupTimer(t *testing.T) {
	tests := []struct {
		name    string
		execute func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result
		want    bool
		wantErr bool
	}{
		{
			name: "ActiveRecentBackupTimer",
			execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				if params.Executable == "systemctl" && len(params.Args) > 0 && params.Args[0] == "list-timers" {
					return commandlineexecutor.Result{StdOut: "mysql-backup.timer mysql-backup.service\n"}
				}
				if params.Executable == "systemctl" && len(params.Args) > 1 && params.Args[0] == "cat" && params.Args[1] == "mysql-backup.service" {
					return commandlineexecutor.Result{StdOut: "ExecStart=/usr/bin/mysqldump --all-databases\n"}
				}
				if params.Executable == "systemctl" && len(params.Args) > 1 && params.Args[0] == "show" && params.Args[1] == "mysql-backup.service" {
					recentTimestamp := time.Now().Add(-2 * time.Hour).Format("Mon 2006-01-02 15:04:05 MST")
					return commandlineexecutor.Result{StdOut: fmt.Sprintf("ExecMainStatus=0\nExecMainExitTimestamp=%s\n", recentTimestamp)}
				}
				return commandlineexecutor.Result{Error: errors.New("unexpected command")}
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "FailedExitStatus",
			execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				if params.Executable == "systemctl" && len(params.Args) > 0 && params.Args[0] == "list-timers" {
					return commandlineexecutor.Result{StdOut: "mysql-backup.timer mysql-backup.service\n"}
				}
				if params.Executable == "systemctl" && len(params.Args) > 1 && params.Args[0] == "cat" && params.Args[1] == "mysql-backup.service" {
					return commandlineexecutor.Result{StdOut: "ExecStart=/usr/bin/mysqldump --all-databases\n"}
				}
				if params.Executable == "systemctl" && len(params.Args) > 1 && params.Args[0] == "show" && params.Args[1] == "mysql-backup.service" {
					recentTimestamp := time.Now().Add(-2 * time.Hour).Format("Mon 2006-01-02 15:04:05 MST")
					return commandlineexecutor.Result{StdOut: fmt.Sprintf("ExecMainStatus=1\nExecMainExitTimestamp=%s\n", recentTimestamp)}
				}
				return commandlineexecutor.Result{Error: errors.New("unexpected command")}
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "OldBackupTimestamp",
			execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				if params.Executable == "systemctl" && len(params.Args) > 0 && params.Args[0] == "list-timers" {
					return commandlineexecutor.Result{StdOut: "mysql-backup.timer mysql-backup.service\n"}
				}
				if params.Executable == "systemctl" && len(params.Args) > 1 && params.Args[0] == "cat" && params.Args[1] == "mysql-backup.service" {
					return commandlineexecutor.Result{StdOut: "ExecStart=/usr/bin/mysqldump --all-databases\n"}
				}
				if params.Executable == "systemctl" && len(params.Args) > 1 && params.Args[0] == "show" && params.Args[1] == "mysql-backup.service" {
					oldTimestamp := time.Now().Add(-48 * time.Hour).Format("Mon 2006-01-02 15:04:05 MST")
					return commandlineexecutor.Result{StdOut: fmt.Sprintf("ExecMainStatus=0\nExecMainExitTimestamp=%s\n", oldTimestamp)}
				}
				return commandlineexecutor.Result{Error: errors.New("unexpected command")}
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "NonServiceUnitIgnored",
			execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				if params.Executable == "systemctl" && len(params.Args) > 0 && params.Args[0] == "list-timers" {
					return commandlineexecutor.Result{StdOut: "mysql-backup.timer mysql-backup.target\n"}
				}
				return commandlineexecutor.Result{Error: errors.New("unexpected command")}
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "ListTimersError",
			execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				return commandlineexecutor.Result{Error: errors.New("systemctl error")}
			},
			want:    false,
			wantErr: true,
		},
	}

	ctx := context.Background()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := MySQLMetrics{execute: tc.execute}
			got, err := m.hasRecentSystemdBackupTimer(ctx)
			if (err != nil) != tc.wantErr {
				t.Errorf("hasRecentSystemdBackupTimer() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("hasRecentSystemdBackupTimer() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsSystemdBackupService(t *testing.T) {
	tests := []struct {
		name    string
		execute func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result
		want    bool
	}{
		{
			name: "ServiceWithBackupCommand",
			execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				return commandlineexecutor.Result{StdOut: "[Service]\nExecStart=/usr/bin/mysqldump -u root\n"}
			},
			want: true,
		},
		{
			name: "ServiceWithCommentOnly",
			execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				return commandlineexecutor.Result{StdOut: "[Service]\n# ExecStart=/usr/bin/mysqldump -u root\nExecStart=/usr/bin/echo hello\n"}
			},
			want: false,
		},
		{
			name: "CatCommandError",
			execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				return commandlineexecutor.Result{Error: errors.New("cat error")}
			},
			want: false,
		},
	}

	ctx := context.Background()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := MySQLMetrics{execute: tc.execute}
			got := m.isSystemdBackupService(ctx, "mysql-backup.service")
			if got != tc.want {
				t.Errorf("isSystemdBackupService() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSystemdServiceExitTime(t *testing.T) {
	validTime := time.Date(2026, time.July, 16, 10, 0, 0, 0, time.UTC)
	validTimeStr := validTime.Format("Mon 2006-01-02 15:04:05 MST")

	tests := []struct {
		name    string
		execute func(context.Context, commandlineexecutor.Params) commandlineexecutor.Result
		want    time.Time
		wantOk  bool
	}{
		{
			name: "SuccessZeroStatus",
			execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				return commandlineexecutor.Result{StdOut: fmt.Sprintf("ExecMainStatus=0\nExecMainExitTimestamp=%s\n", validTimeStr)}
			},
			want:   validTime,
			wantOk: true,
		},
		{
			name: "NonZeroExitStatus",
			execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				return commandlineexecutor.Result{StdOut: fmt.Sprintf("ExecMainStatus=1\nExecMainExitTimestamp=%s\n", validTimeStr)}
			},
			want:   time.Time{},
			wantOk: false,
		},
		{
			name: "TimestampNA",
			execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				return commandlineexecutor.Result{StdOut: "ExecMainStatus=0\nExecMainExitTimestamp=n/a\n"}
			},
			want:   time.Time{},
			wantOk: false,
		},
		{
			name: "CommandError",
			execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				return commandlineexecutor.Result{Error: errors.New("show error")}
			},
			want:   time.Time{},
			wantOk: false,
		},
	}

	ctx := context.Background()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := MySQLMetrics{execute: tc.execute}
			got, ok := m.systemdServiceExitTime(ctx, "mysql-backup.service")
			if ok != tc.wantOk {
				t.Errorf("systemdServiceExitTime() ok = %v, wantOk %v", ok, tc.wantOk)
			}
			if ok && !got.Equal(tc.want) {
				t.Errorf("systemdServiceExitTime() got = %v, want %v", got, tc.want)
			}
		})
	}
}
