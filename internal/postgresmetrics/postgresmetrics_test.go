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

package postgresmetrics

import (
	"archive/zip"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
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
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/zipper"
)

type mockFileSystem struct {
	existsPaths map[string]bool
	dirFiles    map[string][]fs.FileInfo
}

func (m mockFileSystem) MkdirAll(string, os.FileMode) error { return nil }
func (m mockFileSystem) ReadFile(string) ([]byte, error)     { return nil, os.ErrNotExist }
func (m mockFileSystem) ReadDir(path string) ([]fs.FileInfo, error) {
	if files, ok := m.dirFiles[path]; ok {
		return files, nil
	}
	return nil, os.ErrNotExist
}
func (m mockFileSystem) Open(string) (*os.File, error) { return nil, os.ErrNotExist }
func (m mockFileSystem) OpenFile(string, int, os.FileMode) (*os.File, error) {
	return nil, os.ErrNotExist
}
func (m mockFileSystem) RemoveAll(string) error { return nil }
func (m mockFileSystem) Create(string) (*os.File, error) {
	return nil, os.ErrNotExist
}
func (m mockFileSystem) WriteStringToFile(*os.File, string) (int, error) { return 0, nil }
func (m mockFileSystem) Rename(string, string) error                     { return nil }
func (m mockFileSystem) Copy(io.Writer, io.Reader) (int64, error)         { return 0, nil }
func (m mockFileSystem) Chmod(string, os.FileMode) error                 { return nil }
func (m mockFileSystem) Stat(path string) (os.FileInfo, error) {
	if m.existsPaths[path] {
		return nil, nil // Return nil info, we only check err == nil
	}
	return nil, os.ErrNotExist
}
func (m mockFileSystem) WalkAndZip(string, zipper.Zipper, *zip.Writer) error { return nil }
func (m mockFileSystem) Seek(*os.File, int64, int) (int64, error)             { return 0, nil }

// mockFileInfo is a simple implementation of fs.FileInfo for testing
type mockFileInfo struct {
	name string
}

func (m mockFileInfo) Name() string       { return m.name }
func (m mockFileInfo) Size() int64        { return 0 }
func (m mockFileInfo) Mode() fs.FileMode  { return 0 }
func (m mockFileInfo) ModTime() time.Time { return time.Time{} }
func (m mockFileInfo) IsDir() bool        { return false }
func (m mockFileInfo) Sys() any           { return nil }

type testDB struct {
	workMemRows     rowsInterface
	workMemErr      error
	versionRows     rowsInterface
	versionErr      error
	pingErr         error
	pgauditLogRows  rowsInterface
	pgauditLogErr   error
	sslRows         rowsInterface
	sslErr          error
	hbaRulesRows    rowsInterface
	hbaRulesErr     error
	isRecoveryRows  rowsInterface
	isRecoveryErr   error
	pgdataRows      rowsInterface
	pgdataErr       error
	replicationRows rowsInterface
	replicationErr  error
	pgSettingsRows  rowsInterface
	pgSettingsErr   error
	walArchiveRows  rowsInterface
	walArchiveErr   error
}

var emptyDB = &testDB{}

type emptyRows struct{}

func (e *emptyRows) Next() bool             { return false }
func (e *emptyRows) Close() error           { return nil }
func (e *emptyRows) Scan(dest ...any) error { return sql.ErrNoRows }
func (e *emptyRows) Err() error             { return nil }

func (t *testDB) QueryContext(ctx context.Context, query string, args ...any) (rowsInterface, error) {
	if query == "SHOW work_mem" {
		if t.workMemRows == nil {
			return &emptyRows{}, nil
		}
		return t.workMemRows, t.workMemErr
	}
	if query == "SHOW server_version" {
		if t.versionRows == nil {
			return &emptyRows{}, nil
		}
		return t.versionRows, t.versionErr
	}
	if query == "SHOW ssl" {
		if t.sslRows == nil {
			return &emptyRows{}, nil
		}
		return t.sslRows, t.sslErr
	}
	if query == "SHOW pgaudit.log" {
		if t.pgauditLogRows == nil {
			return &emptyRows{}, nil
		}
		return t.pgauditLogRows, t.pgauditLogErr
	}
	if strings.Contains(query, "FROM pg_hba_file_rules()") {
		if t.hbaRulesRows == nil {
			return &emptyRows{}, nil
		}
		return t.hbaRulesRows, t.hbaRulesErr
	}
	if strings.Contains(query, "pg_stat_archiver") {
		return t.walArchiveRows, t.walArchiveErr
	}
	if query == "SELECT pg_is_in_recovery()" {
		if t.isRecoveryRows == nil {
			return &emptyRows{}, nil
		}
		return t.isRecoveryRows, t.isRecoveryErr
	}
	if query == "SHOW data_directory" {
		if t.pgdataRows == nil {
			return &emptyRows{}, nil
		}
		return t.pgdataRows, t.pgdataErr
	}
	if query == "SELECT COUNT(*) FROM pg_stat_replication" {
		if t.replicationRows == nil {
			return &emptyRows{}, nil
		}
		return t.replicationRows, t.replicationErr
	}
	if strings.Contains(query, "pg_settings") {
		if t.pgSettingsErr != nil {
			return nil, t.pgSettingsErr
		}
		if t.pgSettingsRows == nil {
			return &emptyRows{}, nil
		}
		return t.pgSettingsRows, nil
	}
	return &emptyRows{}, nil
}

func (t *testDB) Ping() error {
	return t.pingErr
}

type testRow struct {
	rows rowsInterface
	err  error
}

func (tr testRow) Scan(dest ...any) error {
	if tr.err != nil {
		return tr.err
	}
	if tr.rows == nil {
		return sql.ErrNoRows
	}
	defer tr.rows.Close()
	if !tr.rows.Next() {
		return sql.ErrNoRows
	}
	return tr.rows.Scan(dest...)
}

func (t *testDB) QueryRowContext(ctx context.Context, query string, args ...any) rowInterface {
	rows, err := t.QueryContext(ctx, query, args...)
	return testRow{rows: rows, err: err}
}

type workMemRows struct {
	count     int
	size      int
	data      string
	shouldErr bool
}

func (f *workMemRows) Scan(dest ...any) error {
	log.CtxLogger(context.Background()).Infow("fakeRows.Scan", "dest", dest, "dest[0]", dest[0], "data", f.data)
	if f.shouldErr {
		return errors.New("test-error")
	}
	*dest[0].(*string) = f.data
	return nil
}

func (f *workMemRows) Next() bool {
	f.count++
	return f.count <= f.size
}

func (f *workMemRows) Close() error {
	return nil
}

func (f *workMemRows) Err() error {
	return nil
}

type versionRows struct {
	count     int
	size      int
	data      string
	shouldErr bool
}

func (f *versionRows) Scan(dest ...any) error {
	log.CtxLogger(context.Background()).Infow("fakeRows.Scan", "dest", dest, "dest[0]", dest[0], "data", f.data)
	if f.shouldErr {
		return errors.New("test-error")
	}
	*dest[0].(*string) = f.data
	return nil
}

func (f *versionRows) Next() bool {
	f.count++
	return f.count <= f.size
}

func (f *versionRows) Close() error {
	return nil
}

func (f *versionRows) Err() error {
	return nil
}

// hbaRulesRows for pg_hba_file_rules() command
type hbaRulesRows struct {
	value   int
	read    bool
	scanErr error
	rowsErr error // To simulate errors from rows.Err()
}

func (m *hbaRulesRows) Next() bool {
	if m.rowsErr != nil {
		return false
	}
	if !m.read {
		m.read = true
		return true
	}
	return false
}

func (m *hbaRulesRows) Scan(dest ...any) error {
	if m.scanErr != nil {
		return m.scanErr
	}
	if len(dest) > 0 {
		*(dest[0].(*int)) = m.value
	}
	return nil
}

func (m *hbaRulesRows) Close() error { return nil }
func (m *hbaRulesRows) Err() error   { return m.rowsErr }

// genericMockRows for single string results from SHOW commands
type genericMockRows struct {
	value   string
	read    bool
	scanErr error
	rowsErr error // To simulate errors from rows.Err()
}

func (m *genericMockRows) Next() bool {
	if m.rowsErr != nil {
		return false
	}
	if !m.read {
		m.read = true
		return true
	}
	return false
}

func (m *genericMockRows) Scan(dest ...any) error {
	if m.scanErr != nil {
		return m.scanErr
	}
	if len(dest) > 0 {
		*(dest[0].(*string)) = m.value
	}
	return nil
}

func (m *genericMockRows) Close() error { return nil }
func (m *genericMockRows) Err() error   { return m.rowsErr }

type boolMockRows struct {
	value   bool
	read    bool
	scanErr error
	rowsErr error
}

func (m *boolMockRows) Next() bool {
	if m.rowsErr != nil {
		return false
	}
	if !m.read {
		m.read = true
		return true
	}
	return false
}

func (m *boolMockRows) Scan(dest ...any) error {
	if m.scanErr != nil {
		return m.scanErr
	}
	if len(dest) > 0 {
		*(dest[0].(*bool)) = m.value
	}
	return nil
}

func (m *boolMockRows) Close() error { return nil }
func (m *boolMockRows) Err() error   { return m.rowsErr }

type intMockRows struct {
	value   int
	read    bool
	scanErr error
	rowsErr error
}

func (m *intMockRows) Next() bool {
	if m.rowsErr != nil {
		return false
	}
	if !m.read {
		m.read = true
		return true
	}
	return false
}

func (m *intMockRows) Scan(dest ...any) error {
	if m.scanErr != nil {
		return m.scanErr
	}
	if len(dest) > 0 {
		*(dest[0].(*int)) = m.value
	}
	return nil
}

func (m *intMockRows) Close() error { return nil }
func (m *intMockRows) Err() error   { return m.rowsErr }

type pgSettingsRow struct {
	name    string
	setting string
}

type pgSettingsRows struct {
	rows  []pgSettingsRow
	index int
}

func (r *pgSettingsRows) Next() bool {
	r.index++
	return r.index <= len(r.rows)
}

func (r *pgSettingsRows) Scan(dest ...any) error {
	if r.index < 1 || r.index > len(r.rows) {
		return errors.New("out of bounds")
	}
	row := r.rows[r.index-1]
	*(dest[0].(*string)) = row.name
	*(dest[1].(*string)) = row.setting
	return nil
}

func (r *pgSettingsRows) Close() error { return nil }
func (r *pgSettingsRows) Err() error   { return nil }

type MockDatabaseCenterClient struct {
	sendMetadataCalled bool
	sendMetadataErr    error
	sentMetrics        databasecenter.DBCenterMetrics
}

func (m *MockDatabaseCenterClient) SendMetadataToDatabaseCenter(ctx context.Context, metrics databasecenter.DBCenterMetrics) error {
	m.sendMetadataCalled = true
	m.sentMetrics = metrics
	return m.sendMetadataErr
}

func TestInitDB(t *testing.T) {
	tests := []struct {
		name       string
		m          PostgresMetrics
		gceService GceInterface
		wantErr    bool
	}{
		{
			name: "HappyPath",
			m: PostgresMetrics{
				Config: &configpb.Configuration{
					PostgresConfiguration: &configpb.PostgresConfiguration{
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
			name: "HappyPathMissingSecret",
			m: PostgresMetrics{
				Config: &configpb.Configuration{
					PostgresConfiguration: &configpb.PostgresConfiguration{
						ConnectionParameters: &configpb.ConnectionParameters{
							Username: "test-user",
							Secret:   &configpb.SecretRef{ProjectId: "fake-project-id"},
						},
					},
				},
				connect: func(ctx context.Context, dataSource string) (dbInterface, error) { return emptyDB, nil },
			},
			gceService: &gcefake.TestGCE{
				GetSecretResp: []string{"fake-password"},
				GetSecretErr:  []error{errors.New("fake-error")},
			},
			wantErr: false,
		},
		{
			name: "ConfigPassword",
			m: PostgresMetrics{
				Config: &configpb.Configuration{
					PostgresConfiguration: &configpb.PostgresConfiguration{
						ConnectionParameters: &configpb.ConnectionParameters{
							Username: "test-user",
							Password: "fake-password",
						},
					},
				},
				connect: func(ctx context.Context, dataSource string) (dbInterface, error) { return emptyDB, nil },
			},
			gceService: &gcefake.TestGCE{
				GetSecretResp: []string{""},
				GetSecretErr:  []error{errors.New("fake-error")},
			},
			wantErr: false,
		},
		{
			name: "PasswordError",
			m: PostgresMetrics{
				Config: &configpb.Configuration{
					PostgresConfiguration: &configpb.PostgresConfiguration{
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
			m: PostgresMetrics{
				Config: &configpb.Configuration{
					PostgresConfiguration: &configpb.PostgresConfiguration{
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

func TestInitDBError(t *testing.T) {
	m := PostgresMetrics{
		Config: &configpb.Configuration{
			PostgresConfiguration: &configpb.PostgresConfiguration{
				ConnectionParameters: &configpb.ConnectionParameters{
					Username: "test-user",
					Password: "fake-password",
				},
			},
		},
		connect: func(ctx context.Context, dataSource string) (dbInterface, error) {
			return &testDB{pingErr: errors.New("ping-error")}, nil
		},
	}
	gceService := &gcefake.TestGCE{}
	err := m.InitDB(context.Background(), gceService)
	if err == nil {
		t.Errorf("InitDB() = nil, want error")
	}
}

func TestVersion(t *testing.T) {
	tests := []struct {
		name         string
		m            PostgresMetrics
		majorVersion string
		minorVersion string
		wantErr      bool
	}{
		{
			name: "HappyPath",
			m: PostgresMetrics{
				db: &testDB{
					versionRows: &versionRows{count: 0, size: 1, data: "16.4 (Debian 16.4-1.pgdg110+1)", shouldErr: false},
					versionErr:  nil,
				},
			},
			majorVersion: "16",
			minorVersion: "16.4",
			wantErr:      false,
		},
		{
			name: "FallbackPath",
			m: PostgresMetrics{
				db: &testDB{
					versionRows: &versionRows{count: 0, size: 1, data: "16.4", shouldErr: false},
					versionErr:  nil,
				},
			},
			majorVersion: "16",
			minorVersion: "16.4",
			wantErr:      false,
		},
		{
			name: "FallbackPath2",
			m: PostgresMetrics{
				db: &testDB{
					versionRows: &versionRows{count: 0, size: 1, data: "16", shouldErr: false},
					versionErr:  nil,
				},
			},
			majorVersion: "16",
			minorVersion: "16",
			wantErr:      false,
		},
		{
			name: "FallbackPath3",
			m: PostgresMetrics{
				db: &testDB{
					versionRows: &versionRows{count: 0, size: 1, data: "", shouldErr: false},
					versionErr:  nil,
				},
			},
			majorVersion: "",
			minorVersion: "",
			wantErr:      false,
		},
		{
			name: "VersionError",
			m: PostgresMetrics{
				db: &testDB{
					versionErr: errors.New("test-error"),
				},
			},
			majorVersion: "",
			minorVersion: "",
			wantErr:      true,
		},
	}
	ctx := context.Background()
	for _, tc := range tests {
		majorversion, minorversion, err := tc.m.version(ctx)
		if tc.wantErr {
			if err == nil {
				t.Errorf("version(%v) returned no error, want error", tc.name)
			}
			continue
		}
		if majorversion != tc.majorVersion {
			t.Errorf("version(%v) = %v, want %v", tc.name, majorversion, tc.majorVersion)
		}
		if minorversion != tc.minorVersion {
			t.Errorf("version(%v) = %v, want %v", tc.name, minorversion, tc.minorVersion)
		}
	}
}

func TestAuditingEnabled_Postgres(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name         string
		dbMock       *testDB
		expectResult bool
		expectErr    bool
	}{
		{
			name: "pgaudit_not_loaded",
			dbMock: &testDB{
				pgauditLogErr: errors.New("new error"), // undefined_object
			},
			expectResult: false, // Disabled
			expectErr:    true,
		},
		{
			name: "pgaudit_log_none",
			dbMock: &testDB{
				pgauditLogRows: &genericMockRows{value: "none"},
			},
			expectResult: false,
			expectErr:    false,
		},
		{
			name: "pgaudit_log_NONE",
			dbMock: &testDB{
				pgauditLogRows: &genericMockRows{value: "NONE"},
			},
			expectResult: false,
			expectErr:    false,
		},
		{
			name: "pgaudit_log_all",
			dbMock: &testDB{
				pgauditLogRows: &genericMockRows{value: "all"},
			},
			expectResult: true,
			expectErr:    false,
		},
		{
			name: "pgaudit_log_read_write",
			dbMock: &testDB{
				pgauditLogRows: &genericMockRows{value: "read, write"},
			},
			expectResult: true,
			expectErr:    false,
		},
		{
			name: "other_db_error",
			dbMock: &testDB{
				pgauditLogErr: errors.New("connection failed"),
			},
			expectResult: false,
			expectErr:    true,
		},
		{
			name: "scan_error",
			dbMock: &testDB{
				pgauditLogRows: &genericMockRows{value: "all", scanErr: errors.New("scan failed")},
			},
			expectResult: false,
			expectErr:    true, // Scan error IS returned
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := PostgresMetrics{db: tc.dbMock}
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

func TestUnencryptedConnectionsAllowed_Postgres(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name         string
		dbMock       *testDB
		expectResult bool
		expectErr    bool
	}{
		{
			name: "ssl_off",
			dbMock: &testDB{
				sslRows: &genericMockRows{value: "off"},
			},
			expectResult: true,
			expectErr:    false,
		},
		{
			name: "ssl_OFF",
			dbMock: &testDB{
				sslRows: &genericMockRows{value: "OFF"},
			},
			expectResult: true,
			expectErr:    false,
		},
		{
			name: "ssl_on",
			dbMock: &testDB{
				sslRows: &genericMockRows{value: "on"},
			},
			expectResult: false,
			expectErr:    false,
		},
		{
			name: "ssl_ON",
			dbMock: &testDB{
				sslRows: &genericMockRows{value: "ON"},
			},
			expectResult: false,
			expectErr:    false,
		},
		{
			name: "query_error",
			dbMock: &testDB{
				sslErr: errors.New("connection failed"),
			},
			expectResult: false,
			expectErr:    true,
		},
		{
			name: "scan_error",
			dbMock: &testDB{
				sslRows: &genericMockRows{value: "on", scanErr: errors.New("scan failed")},
			},
			expectResult: false,
			expectErr:    true,
		},
		{
			name: "no_rows",
			dbMock: &testDB{
				sslRows: &genericMockRows{read: true}, // Next() will return false immediately
			},
			expectResult: false,
			expectErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := PostgresMetrics{db: tc.dbMock}
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

func TestExposedToPublicAccess_Postgres(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name         string
		dbMock       *testDB
		expectResult bool
		expectErr    bool
	}{
		{
			name: "exposed",
			dbMock: &testDB{
				hbaRulesRows: &hbaRulesRows{value: 1}, // Simulates COUNT(*) = 1
			},
			expectResult: true,
			expectErr:    false,
		},
		{
			name: "not_exposed",
			dbMock: &testDB{
				hbaRulesRows: &hbaRulesRows{value: 0}, // Simulates COUNT(*) = 0
			},
			expectResult: false,
			expectErr:    false,
		},
		{
			name: "query_error",
			dbMock: &testDB{
				hbaRulesErr: errors.New("permission denied"),
			},
			expectResult: true, // Fails open
			expectErr:    true,
		},
		{
			name: "scan_error",
			dbMock: &testDB{
				hbaRulesRows: &hbaRulesRows{value: 1, scanErr: errors.New("scan failed")},
			},
			expectResult: false,
			expectErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := PostgresMetrics{db: tc.dbMock}
			result, err := m.exposedToPublicAccess(ctx)

			if (err != nil) != tc.expectErr {
				t.Errorf("exposedToPublicAccess() got error: %v, want error presence: %v", err, tc.expectErr)
			}

			if !tc.expectErr && result != tc.expectResult {
				t.Errorf("exposedToPublicAccess() got result: %v, want result: %v", result, tc.expectResult)
			}
		})
	}
}

func TestCollectWlmMetricsOnce(t *testing.T) {
	tests := []struct {
		name        string
		m           PostgresMetrics
		wantMetrics *workloadmanager.WorkloadMetrics
		wantErr     bool
	}{
		{
			name: "HappyPath",
			m: PostgresMetrics{
				db: &testDB{
					workMemRows: &workMemRows{count: 0, size: 1, data: "80MB", shouldErr: false},
					workMemErr:  nil,
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
				WorkloadType: workloadmanager.POSTGRES,
				Metrics: map[string]string{
					workMemKey: strconv.Itoa(80 * 1024 * 1024),
				},
			},
			wantErr: false,
		},
		{
			name: "HappyPathKB",
			m: PostgresMetrics{
				db: &testDB{
					workMemRows: &workMemRows{count: 0, size: 1, data: "64kB", shouldErr: false},
					workMemErr:  nil,
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
				WorkloadType: workloadmanager.POSTGRES,
				Metrics: map[string]string{
					workMemKey: strconv.Itoa(64 * 1024),
				},
			},
			wantErr: false,
		},
		{
			name: "HappyPathGB",
			m: PostgresMetrics{
				db: &testDB{
					workMemRows: &workMemRows{count: 0, size: 1, data: "4GB", shouldErr: false},
					workMemErr:  nil,
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
				WorkloadType: workloadmanager.POSTGRES,
				Metrics: map[string]string{
					workMemKey: strconv.Itoa(4 * 1024 * 1024 * 1024),
				},
			},
			wantErr: false,
		},
		{
			name: "GetWorkMemError",
			m: PostgresMetrics{
				db: &testDB{
					workMemErr: errors.New("test-error"),
				},
				WLMClient: &gcefake.TestWLM{
					WriteInsightErrs: []error{nil},
					WriteInsightResponses: []*wlm.WriteInsightResponse{
						&wlm.WriteInsightResponse{ServerResponse: googleapi.ServerResponse{HTTPStatusCode: 201}},
					},
				},
				DBcenterClient: databasecenter.NewClient(&configpb.Configuration{}, nil),
			},
			wantMetrics: nil,
			wantErr:     true,
		},
		{
			name: "WLMClientError",
			m: PostgresMetrics{
				db: &testDB{
					workMemRows:    &workMemRows{count: 0, size: 1, data: "64MB", shouldErr: false},
					workMemErr:     nil,
					pgauditLogRows: &genericMockRows{value: "all"},
					sslRows:        &genericMockRows{value: "off"},
					hbaRulesRows:   &hbaRulesRows{value: 1},
				},
				WLMClient: &gcefake.TestWLM{
					WriteInsightErrs: []error{errors.New("test-error")},
					WriteInsightResponses: []*wlm.WriteInsightResponse{
						&wlm.WriteInsightResponse{ServerResponse: googleapi.ServerResponse{HTTPStatusCode: 400}},
					},
				},
				DBcenterClient: databasecenter.NewClient(&configpb.Configuration{}, nil),
			},
			wantMetrics: nil,
			wantErr:     true,
		},
		{
			name: "NilWriteInsightResponse",
			m: PostgresMetrics{
				db: &testDB{
					workMemRows:    &workMemRows{count: 0, size: 1, data: "64MB", shouldErr: false},
					workMemErr:     nil,
					pgauditLogRows: &genericMockRows{value: "all"},
					sslRows:        &genericMockRows{value: "off"},
					hbaRulesRows:   &hbaRulesRows{value: 1},
				},
				WLMClient: &gcefake.TestWLM{
					WriteInsightErrs:      []error{nil},
					WriteInsightResponses: []*wlm.WriteInsightResponse{nil},
				},
				DBcenterClient: databasecenter.NewClient(&configpb.Configuration{}, nil),
			},
			wantMetrics: &workloadmanager.WorkloadMetrics{
				WorkloadType: workloadmanager.POSTGRES,
				Metrics: map[string]string{
					workMemKey: strconv.Itoa(64 * 1024 * 1024),
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
				t.Errorf("CollectMetricsOnce(%v) returned no error, want error", tc.name)
			}
			continue
		}
		if diff := cmp.Diff(tc.wantMetrics, gotMetrics, protocmp.Transform()); diff != "" {
			t.Errorf("CollectMetricsOnce(%v) returned diff (-want +got):\n%s", tc.name, diff)
		}
	}
}

func TestNew(t *testing.T) {
	ctx := context.Background()
	config := &configpb.Configuration{
		PostgresConfiguration: &configpb.PostgresConfiguration{},
	}
	mockWlmClient := &gcefake.TestWLM{}
	mockDBcenterClient := &MockDatabaseCenterClient{}

	metrics := New(ctx, config, mockWlmClient, mockDBcenterClient)

	if metrics == nil {
		t.Fatalf("New() returned nil, expected a *PostgresMetrics instance")
	}

	if metrics.Config != config {
		t.Errorf("New() returned Config %v, want %v", metrics.Config, config)
	}

	if metrics.WLMClient != mockWlmClient {
		t.Errorf("New() returned WLMClient %v, want %v", metrics.WLMClient, mockWlmClient)
	}
	if metrics.DBcenterClient != mockDBcenterClient {
		t.Errorf("New() returned DBcenterClient %v, want %v", metrics.DBcenterClient, mockDBcenterClient)
	}

	if metrics.execute == nil {
		t.Errorf("New() returned execute nil, expected a function")
	}

	if metrics.connect == nil {
		t.Errorf("New() returned connect nil, expected a function")
	}
}

func TestSendMetadataToDatabaseCenter(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name                 string
		m                    PostgresMetrics
		sendMetadataErr      error
		wantDBcenterMetrics  map[string]string
		wantSendMetadataCall bool
		wantErr              bool
	}{
		{
			name: "Send metadata success",
			m: PostgresMetrics{
				db: &testDB{
					versionRows:    &versionRows{count: 0, size: 1, data: "14.4 (Debian 14.4-1.pgdg110+1)", shouldErr: false},
					versionErr:     nil,
					pgauditLogRows: &genericMockRows{value: "all"},
					sslRows:        &genericMockRows{value: "on"},
					hbaRulesRows:   &hbaRulesRows{value: 0},
					pgSettingsRows: &pgSettingsRows{
						rows: []pgSettingsRow{
							{name: "archive_mode", setting: "on"},
							{name: "archive_command", setting: "pgbackrest --stanza=demo archive-push %p"},
						},
					},
					isRecoveryRows:  &boolMockRows{value: false},
					replicationRows: &intMockRows{value: 1},
				},
				execute: mockExecute(true, false, "", nil), // Patroni is running
				WLMClient: &gcefake.TestWLM{
					WriteInsightErrs: []error{nil},
					WriteInsightResponses: []*wlm.WriteInsightResponse{
						&wlm.WriteInsightResponse{ServerResponse: googleapi.ServerResponse{HTTPStatusCode: 201}},
					},
				},
				DBcenterClient: databasecenter.NewClient(&configpb.Configuration{}, nil),
			},
			sendMetadataErr: nil,
			wantDBcenterMetrics: map[string]string{
				databasecenter.MajorVersionKey:                    "14",
				databasecenter.MinorVersionKey:                    "14.4",
				databasecenter.ExposedToPublicAccessKey:           "false",
				databasecenter.UnencryptedConnectionsKey:          "false",
				databasecenter.DatabaseAuditingDisabledKey:        "false",
				databasecenter.NotProtectedByAutomaticFailoverKey: "false",
				databasecenter.NoAutomatedBackupPolicyKey:         "false",
				databasecenter.LastBackupOldKey:                   "true",
			},
			wantSendMetadataCall: true,
			wantErr:              false,
		},
		{
			name: "Send metadata failure, should not return error",
			m: PostgresMetrics{
				db: &testDB{
					versionRows:     &versionRows{count: 0, size: 1, data: "14.4 (Debian 14.4-1.pgdg110+1)", shouldErr: false},
					versionErr:      nil,
					pgauditLogRows:  &genericMockRows{value: "none"},
					sslRows:         &genericMockRows{value: "off"},
					hbaRulesRows:    &hbaRulesRows{value: 1},
					pgSettingsErr:   errors.New("db error"),
					isRecoveryRows:  &boolMockRows{value: false},
					replicationRows: &intMockRows{value: 0},
				},
				execute: mockExecute(false, false, "", nil), // No HA running
				WLMClient: &gcefake.TestWLM{
					WriteInsightErrs: []error{nil},
					WriteInsightResponses: []*wlm.WriteInsightResponse{
						&wlm.WriteInsightResponse{ServerResponse: googleapi.ServerResponse{HTTPStatusCode: 201}},
					},
				},
				DBcenterClient: databasecenter.NewClient(&configpb.Configuration{}, nil),
			},
			sendMetadataErr: fmt.Errorf("db center error"),
			wantDBcenterMetrics: map[string]string{
				databasecenter.MajorVersionKey:                    "14",
				databasecenter.MinorVersionKey:                    "14.4",
				databasecenter.ExposedToPublicAccessKey:           "true",
				databasecenter.UnencryptedConnectionsKey:          "true",
				databasecenter.DatabaseAuditingDisabledKey:        "true",
				databasecenter.NotProtectedByAutomaticFailoverKey: "true",
				databasecenter.NoAutomatedBackupPolicyKey:         "true",
				databasecenter.LastBackupOldKey:                   "true",
			},
			wantSendMetadataCall: true,
			wantErr:              false,
		},
		{
			name: "Archive mode off",
			m: PostgresMetrics{
				db: &testDB{
					versionRows:    &versionRows{count: 0, size: 1, data: "14.4", shouldErr: false},
					pgauditLogRows: &genericMockRows{value: "none"},
					sslRows:        &genericMockRows{value: "on"},
					hbaRulesRows:   &hbaRulesRows{value: 0},
					pgSettingsRows: &pgSettingsRows{
						rows: []pgSettingsRow{
							{name: "archive_mode", setting: "off"},
							{name: "archive_command", setting: "cp %p /path"},
						},
					},
					isRecoveryRows: &boolMockRows{value: false},
				},
				DBcenterClient: databasecenter.NewClient(&configpb.Configuration{}, nil),
			},
			wantDBcenterMetrics: map[string]string{
				databasecenter.MajorVersionKey:                    "14",
				databasecenter.MinorVersionKey:                    "14.4",
				databasecenter.ExposedToPublicAccessKey:           "false",
				databasecenter.UnencryptedConnectionsKey:          "false",
				databasecenter.DatabaseAuditingDisabledKey:        "true",
				databasecenter.NotProtectedByAutomaticFailoverKey: "true",
				databasecenter.NoAutomatedBackupPolicyKey:         "true",
				databasecenter.LastBackupOldKey:                   "true",
			},
			wantSendMetadataCall: true,
		},
		{
			name: "Archive command empty",
			m: PostgresMetrics{
				db: &testDB{
					versionRows:    &versionRows{count: 0, size: 1, data: "14.4", shouldErr: false},
					pgauditLogRows: &genericMockRows{value: "none"},
					sslRows:        &genericMockRows{value: "on"},
					hbaRulesRows:   &hbaRulesRows{value: 0},
					pgSettingsRows: &pgSettingsRows{
						rows: []pgSettingsRow{
							{name: "archive_mode", setting: "on"},
							{name: "archive_command", setting: ""},
						},
					},
					isRecoveryRows: &boolMockRows{value: false},
				},
				DBcenterClient: databasecenter.NewClient(&configpb.Configuration{}, nil),
			},
			wantDBcenterMetrics: map[string]string{
				databasecenter.MajorVersionKey:                    "14",
				databasecenter.MinorVersionKey:                    "14.4",
				databasecenter.ExposedToPublicAccessKey:           "false",
				databasecenter.UnencryptedConnectionsKey:          "false",
				databasecenter.DatabaseAuditingDisabledKey:        "true",
				databasecenter.NotProtectedByAutomaticFailoverKey: "true",
				databasecenter.NoAutomatedBackupPolicyKey:         "true",
				databasecenter.LastBackupOldKey:                   "true",
			},
			wantSendMetadataCall: true,
		},
		{
			name: "Archive command dummy colon",
			m: PostgresMetrics{
				db: &testDB{
					versionRows:    &versionRows{count: 0, size: 1, data: "14.4", shouldErr: false},
					pgauditLogRows: &genericMockRows{value: "none"},
					sslRows:        &genericMockRows{value: "on"},
					hbaRulesRows:   &hbaRulesRows{value: 0},
					pgSettingsRows: &pgSettingsRows{
						rows: []pgSettingsRow{
							{name: "archive_mode", setting: "on"},
							{name: "archive_command", setting: ":"},
						},
					},
					isRecoveryRows: &boolMockRows{value: false},
				},
				DBcenterClient: databasecenter.NewClient(&configpb.Configuration{}, nil),
			},
			wantDBcenterMetrics: map[string]string{
				databasecenter.MajorVersionKey:                    "14",
				databasecenter.MinorVersionKey:                    "14.4",
				databasecenter.ExposedToPublicAccessKey:           "false",
				databasecenter.UnencryptedConnectionsKey:          "false",
				databasecenter.DatabaseAuditingDisabledKey:        "true",
				databasecenter.NotProtectedByAutomaticFailoverKey: "true",
				databasecenter.NoAutomatedBackupPolicyKey:         "true",
				databasecenter.LastBackupOldKey:                   "true",
			},
			wantSendMetadataCall: true,
		},
		{
			name: "Archive command dummy colon with spaces",
			m: PostgresMetrics{
				db: &testDB{
					versionRows:    &versionRows{count: 0, size: 1, data: "14.4", shouldErr: false},
					pgauditLogRows: &genericMockRows{value: "none"},
					sslRows:        &genericMockRows{value: "on"},
					hbaRulesRows:   &hbaRulesRows{value: 0},
					pgSettingsRows: &pgSettingsRows{
						rows: []pgSettingsRow{
							{name: "archive_mode", setting: "on"},
							{name: "archive_command", setting: "  :  "},
						},
					},
					isRecoveryRows: &boolMockRows{value: false},
				},
				DBcenterClient: databasecenter.NewClient(&configpb.Configuration{}, nil),
			},
			wantDBcenterMetrics: map[string]string{
				databasecenter.MajorVersionKey:                    "14",
				databasecenter.MinorVersionKey:                    "14.4",
				databasecenter.ExposedToPublicAccessKey:           "false",
				databasecenter.UnencryptedConnectionsKey:          "false",
				databasecenter.DatabaseAuditingDisabledKey:        "true",
				databasecenter.NotProtectedByAutomaticFailoverKey: "true",
				databasecenter.NoAutomatedBackupPolicyKey:         "true",
				databasecenter.LastBackupOldKey:                   "true",
			},
			wantSendMetadataCall: true,
		},
		{
			name: "Archive command dummy true",
			m: PostgresMetrics{
				db: &testDB{
					versionRows:    &versionRows{count: 0, size: 1, data: "14.4", shouldErr: false},
					pgauditLogRows: &genericMockRows{value: "none"},
					sslRows:        &genericMockRows{value: "on"},
					hbaRulesRows:   &hbaRulesRows{value: 0},
					pgSettingsRows: &pgSettingsRows{
						rows: []pgSettingsRow{
							{name: "archive_mode", setting: "on"},
							{name: "archive_command", setting: "true"},
						},
					},
					isRecoveryRows: &boolMockRows{value: false},
				},
				DBcenterClient: databasecenter.NewClient(&configpb.Configuration{}, nil),
			},
			wantDBcenterMetrics: map[string]string{
				databasecenter.MajorVersionKey:                    "14",
				databasecenter.MinorVersionKey:                    "14.4",
				databasecenter.ExposedToPublicAccessKey:           "false",
				databasecenter.UnencryptedConnectionsKey:          "false",
				databasecenter.DatabaseAuditingDisabledKey:        "true",
				databasecenter.NotProtectedByAutomaticFailoverKey: "true",
				databasecenter.NoAutomatedBackupPolicyKey:         "true",
				databasecenter.LastBackupOldKey:                   "true",
			},
			wantSendMetadataCall: true,
		},
		{
			name: "Archive mode always and custom command",
			m: PostgresMetrics{
				db: &testDB{
					versionRows:    &versionRows{count: 0, size: 1, data: "14.4", shouldErr: false},
					pgauditLogRows: &genericMockRows{value: "none"},
					sslRows:        &genericMockRows{value: "on"},
					hbaRulesRows:   &hbaRulesRows{value: 0},
					pgSettingsRows: &pgSettingsRows{
						rows: []pgSettingsRow{
							{name: "archive_mode", setting: "always"},
							{name: "archive_command", setting: "/usr/bin/my_custom_script.sh %p"},
						},
					},
					isRecoveryRows: &boolMockRows{value: false},
				},
				DBcenterClient: databasecenter.NewClient(&configpb.Configuration{}, nil),
			},
			wantDBcenterMetrics: map[string]string{
				databasecenter.MajorVersionKey:                    "14",
				databasecenter.MinorVersionKey:                    "14.4",
				databasecenter.ExposedToPublicAccessKey:           "false",
				databasecenter.UnencryptedConnectionsKey:          "false",
				databasecenter.DatabaseAuditingDisabledKey:        "true",
				databasecenter.NotProtectedByAutomaticFailoverKey: "true",
				databasecenter.NoAutomatedBackupPolicyKey:         "true",
				databasecenter.LastBackupOldKey:                   "true",
			},
			wantSendMetadataCall: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockClient := &MockDatabaseCenterClient{
				sendMetadataCalled: false,
				sendMetadataErr:    tc.sendMetadataErr,
			}
			// Set the mock dbcenter client in the PostgresMetrics object
			tc.m.DBcenterClient = mockClient
			// Inject mock filesystem to prevent panic
			tc.m.fs = mockFileSystem{}
			// Inject default mock executor if nil to prevent panic
			if tc.m.execute == nil {
				tc.m.execute = func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{ExitCode: 1, Error: fmt.Errorf("exit status 1")}
				}
			}
			// Call the function under test
			err := tc.m.CollectDBCenterMetricsOnce(ctx)

			// Assertions
			if err != nil && !tc.wantErr {
				t.Errorf("CollectMetricsOnce returned an unexpected error: %v", err)
			}
			if err == nil && tc.wantErr {
				t.Errorf("CollectMetricsOnce did not return an expected error")
			}
			if mockClient.sendMetadataCalled != tc.wantSendMetadataCall {
				t.Errorf("CollectMetricsOnce: sendMetadataCalled = %v, want %v", mockClient.sendMetadataCalled, tc.wantSendMetadataCall)
			}
			if mockClient.sendMetadataCalled {
				if diff := cmp.Diff(tc.wantDBcenterMetrics, mockClient.sentMetrics.Metrics); diff != "" {
					t.Errorf("CollectDBCenterMetricsOnce metrics diff (-want +got):\n%s", diff)
				}
			}
		})
	}
}

type timeMockRows struct {
	value   time.Time
	valid   bool
	read    bool
	scanErr error
	rowsErr error
}

func (m *timeMockRows) Next() bool {
	if m.rowsErr != nil {
		return false
	}
	if !m.read {
		m.read = true
		return true
	}
	return false
}

func (m *timeMockRows) Scan(dest ...any) error {
	if m.scanErr != nil {
		return m.scanErr
	}
	if len(dest) > 0 {
		*(dest[0].(*sql.NullTime)) = sql.NullTime{Time: m.value, Valid: m.valid}
	}
	return nil
}

func (m *timeMockRows) Close() error { return nil }
func (m *timeMockRows) Err() error   { return m.rowsErr }

func TestIsLastBackupOld(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	tests := []struct {
		name         string
		m            PostgresMetrics
		expectResult bool
		expectErr    bool
	}{
		{
			name: "WAL older than 24h -> early exit violation",
			m: PostgresMetrics{
				db: &testDB{
					walArchiveRows: &timeMockRows{value: now.Add(-25 * time.Hour), valid: true},
				},
				// Mock a healthy base backup. If WAL check falls through, this would cause
				// the function to return false (healthy), failing the test and killing the mutant.
				execute: mockSuExecute(func(exe string, args []string) commandlineexecutor.Result {
					if exe == "pgbackrest" {
						return commandlineexecutor.Result{
							ExecutableFound: true,
							StdOut:          fmt.Sprintf(`[{"name":"main","backup":[{"timestamp":{"stop":%d}}]}]`, now.Add(-3*time.Hour).Unix()),
						}
					}
					return commandlineexecutor.Result{Error: errors.New("command not found")}
				}),
			},
			expectResult: true, // violation (due to stale WAL, despite healthy base backup)
			expectErr:    false,
		},
		{
			name: "WAL healthy, pgbackrest healthy -> healthy",
			m: PostgresMetrics{
				db: &testDB{
					walArchiveRows: &timeMockRows{value: now.Add(-2 * time.Hour), valid: true},
				},
				execute: mockSuExecute(func(exe string, args []string) commandlineexecutor.Result {
					if exe == "pgbackrest" {
						return commandlineexecutor.Result{
							ExecutableFound: true,
							StdOut:          fmt.Sprintf(`[{"name":"main","backup":[{"timestamp":{"stop":%d}}]}]`, now.Add(-3*time.Hour).Unix()),
						}
					}
					return commandlineexecutor.Result{Error: errors.New("command not found")}
				}),
			},
			expectResult: false, // healthy
			expectErr:    false,
		},
		{
			name: "WAL healthy, pgbackrest older than 24h -> violation",
			m: PostgresMetrics{
				db: &testDB{
					walArchiveRows: &timeMockRows{value: now.Add(-2 * time.Hour), valid: true},
				},
				execute: mockSuExecute(func(exe string, args []string) commandlineexecutor.Result {
					if exe == "pgbackrest" {
						return commandlineexecutor.Result{
							ExecutableFound: true,
							StdOut:          fmt.Sprintf(`[{"name":"main","backup":[{"timestamp":{"stop":%d}}]}]`, now.Add(-26*time.Hour).Unix()),
						}
					}
					return commandlineexecutor.Result{Error: errors.New("command not found")}
				}),
			},
			expectResult: true, // violation
			expectErr:    false,
		},
		{
			name: "WAL healthy, pgbackrest empty, walg healthy -> healthy",
			m: PostgresMetrics{
				db: &testDB{
					walArchiveRows: &timeMockRows{value: now.Add(-2 * time.Hour), valid: true},
				},
				execute: mockSuExecute(func(exe string, args []string) commandlineexecutor.Result {
					if exe == "pgbackrest" {
						return commandlineexecutor.Result{
							ExecutableFound: true,
							StdOut:          `[]`, // empty stanzas
						}
					}
					if exe == "wal-g" {
						return commandlineexecutor.Result{
							ExecutableFound: true,
							StdOut:          fmt.Sprintf(`[{"time":"%s"}]`, now.Add(-4*time.Hour).Format(time.RFC3339Nano)),
						}
					}
					return commandlineexecutor.Result{Error: errors.New("command not found")}
				}),
			},
			expectResult: false, // healthy
			expectErr:    false,
		},
		{
			name: "WAL healthy, pgbackrest backup nil, walg healthy -> healthy",
			m: PostgresMetrics{
				db: &testDB{
					walArchiveRows: &timeMockRows{value: now.Add(-2 * time.Hour), valid: true},
				},
				execute: mockSuExecute(func(exe string, args []string) commandlineexecutor.Result {
					if exe == "pgbackrest" {
						return commandlineexecutor.Result{
							ExecutableFound: true,
							StdOut:          `[{"name":"main","backup":null}]`,
						}
					}
					if exe == "wal-g" {
						return commandlineexecutor.Result{
							ExecutableFound: true,
							StdOut:          fmt.Sprintf(`[{"time":"%s"}]`, now.Add(-4*time.Hour).Format(time.RFC3339Nano)),
						}
					}
					return commandlineexecutor.Result{Error: errors.New("command not found")}
				}),
			},
			expectResult: false, // healthy
			expectErr:    false,
		},
		{
			name: "WAL healthy, pgbackrest backup nil, walg missing -> violation",
			m: PostgresMetrics{
				db: &testDB{
					walArchiveRows: &timeMockRows{value: now.Add(-2 * time.Hour), valid: true},
				},
				execute: mockSuExecute(func(exe string, args []string) commandlineexecutor.Result {
					if exe == "pgbackrest" {
						return commandlineexecutor.Result{
							ExecutableFound: true,
							StdOut:          `[{"name":"main","backup":null}]`,
						}
					}
					return commandlineexecutor.Result{Error: errors.New("command not found")}
				}),
			},
			expectResult: true, // violation
			expectErr:    false,
		},
		{
			name: "WAL healthy, pgbackrest empty, walg older than 24h -> violation",
			m: PostgresMetrics{
				db: &testDB{
					walArchiveRows: &timeMockRows{value: now.Add(-2 * time.Hour), valid: true},
				},
				execute: mockSuExecute(func(exe string, args []string) commandlineexecutor.Result {
					if exe == "pgbackrest" {
						return commandlineexecutor.Result{
							ExecutableFound: true,
							StdOut:          `[]`,
						}
					}
					if exe == "wal-g" {
						return commandlineexecutor.Result{
							ExecutableFound: true,
							StdOut:          fmt.Sprintf(`[{"time":"%s"}]`, now.Add(-26*time.Hour).Format(time.RFC3339Nano)),
						}
					}
					return commandlineexecutor.Result{Error: errors.New("command not found")}
				}),
			},
			expectResult: true, // violation
			expectErr:    false,
		},
		{
			name: "WAL healthy, pgbackrest missing, walg missing -> violation",
			m: PostgresMetrics{
				db: &testDB{
					walArchiveRows: &timeMockRows{value: now.Add(-2 * time.Hour), valid: true},
				},
				execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{Error: errors.New("command not found")} // no tools found
				},
			},
			expectResult: true, // violation
			expectErr:    false,
		},
		{
			name: "WAL missing, pgbackrest healthy -> healthy",
			m: PostgresMetrics{
				db: &testDB{
					walArchiveRows: &timeMockRows{valid: false}, // WAL returns nothing
				},
				execute: mockSuExecute(func(exe string, args []string) commandlineexecutor.Result {
					if exe == "pgbackrest" {
						return commandlineexecutor.Result{
							ExecutableFound: true,
							StdOut:          fmt.Sprintf(`[{"name":"main","backup":[{"timestamp":{"stop":%d}}]}]`, now.Add(-3*time.Hour).Unix()),
						}
					}
					return commandlineexecutor.Result{Error: errors.New("command not found")}
				}),
			},
			expectResult: false, // healthy
			expectErr:    false,
		},
		{
			name: "WAL healthy, pgbackrest failing, walg missing -> error",
			m: PostgresMetrics{
				db: &testDB{
					walArchiveRows: &timeMockRows{value: now.Add(-2 * time.Hour), valid: true},
				},
				execute: mockSuExecute(func(exe string, args []string) commandlineexecutor.Result {
					if exe == "pgbackrest" {
						return commandlineexecutor.Result{
							ExecutableFound: true,
							Error:           errors.New("corrupted config"),
						}
					}
					return commandlineexecutor.Result{Error: errors.New("command not found")}
				}),
			},
			expectResult: false, // SRE fail-safe: fail open on error
			expectErr:    true,
		},
		{
			name: "WAL healthy, pgbackrest older than 24h, walg failing -> violation (fail-closed since old backup found)",
			m: PostgresMetrics{
				db: &testDB{
					walArchiveRows: &timeMockRows{value: now.Add(-2 * time.Hour), valid: true},
				},
				execute: mockSuExecute(func(exe string, args []string) commandlineexecutor.Result {
					if exe == "pgbackrest" {
						return commandlineexecutor.Result{
							ExecutableFound: true,
							StdOut:          fmt.Sprintf(`[{"name":"main","backup":[{"timestamp":{"stop":%d}}]}]`, now.Add(-26*time.Hour).Unix()),
						}
					}
					if exe == "wal-g" {
						return commandlineexecutor.Result{
							ExecutableFound: true,
							Error:           errors.New("walg failed to connect"),
						}
					}
					return commandlineexecutor.Result{Error: errors.New("command not found")}
				}),
			},
			expectResult: true, // violation (we found a stale backup, so we report unhealthy despite WAL-G error)
			expectErr:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.m.isLastBackupOld(ctx)
			if (err != nil) != tc.expectErr {
				t.Errorf("isLastBackupOld() returned error: %v, wantErr: %v", err, tc.expectErr)
			}
			if got != tc.expectResult {
				t.Errorf("isLastBackupOld() = %v, want %v", got, tc.expectResult)
			}
		})
	}
}

func TestIsWALArchivingStale(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	maxAge := 24 * time.Hour

	tests := []struct {
		name         string
		dbMock       *testDB
		expectResult bool
		expectErr    bool
	}{
		{
			name: "healthy_not_stale",
			dbMock: &testDB{
				walArchiveRows: &timeMockRows{value: now.Add(-1 * time.Hour), valid: true},
			},
			expectResult: false,
			expectErr:    false,
		},
		{
			name: "stale",
			dbMock: &testDB{
				walArchiveRows: &timeMockRows{value: now.Add(-25 * time.Hour), valid: true},
			},
			expectResult: true,
			expectErr:    false,
		},
		{
			name: "no_rows_not_stale",
			dbMock: &testDB{
				walArchiveRows: &timeMockRows{valid: false},
			},
			expectResult: false,
			expectErr:    false,
		},
		{
			name: "db_error_propagated",
			dbMock: &testDB{
				walArchiveErr: errors.New("db connection failed"),
			},
			expectResult: false,
			expectErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := PostgresMetrics{db: tc.dbMock}
			result, err := m.isWALArchivingStale(ctx, maxAge)
			if (err != nil) != tc.expectErr {
				t.Errorf("isWALArchivingStale() got error: %v, want error presence: %v", err, tc.expectErr)
			}
			if result != tc.expectResult {
				t.Errorf("isWALArchivingStale() got result: %v, want result: %v", result, tc.expectResult)
			}
		})
	}
}

func TestNotFailoverProtected(t *testing.T) {
	ctx := context.Background()

	healthyPgAutoctlJSON := `[
		{"nodename": "node_1", "health": 1, "current_group_state": "primary"},
		{"nodename": "node_2", "health": 1, "current_group_state": "secondary"}
	]`

	unhealthyPgAutoctlJSON := `[
		{"nodename": "node_1", "health": 1, "current_group_state": "single"}
	]`

	unreachablePgAutoctlJSON := `[
		{"nodename": "node_1", "health": 1, "current_group_state": "primary"},
		{"nodename": "node_2", "health": 0, "current_group_state": "secondary"}
	]`

	tests := []struct {
		name         string
		dbMock       *testDB
		pgrepMock    commandlineexecutor.Execute
		expectResult bool
		expectErr    bool
	}{
		{
			name: "standby_node_returns_false",
			dbMock: &testDB{
				isRecoveryRows: &boolMockRows{value: true},
			},
			pgrepMock:    mockExecute(false, false, "", nil),
			expectResult: false,
		},
		{
			name: "primary_patroni_active_and_has_replicas_returns_false",
			dbMock: &testDB{
				isRecoveryRows:  &boolMockRows{value: false},
				replicationRows: &intMockRows{value: 1},
			},
			pgrepMock:    mockExecute(true, false, "", nil),
			expectResult: false,
		},
		{
			name: "primary_patroni_active_but_no_replicas_returns_true",
			dbMock: &testDB{
				isRecoveryRows:  &boolMockRows{value: false},
				replicationRows: &intMockRows{value: 0},
			},
			pgrepMock:    mockExecute(true, false, "", nil),
			expectResult: true,
		},
		{
			name: "primary_pgautoctl_active_and_healthy_and_has_replicas_returns_false",
			dbMock: &testDB{
				isRecoveryRows:  &boolMockRows{value: false},
				pgdataRows:      &genericMockRows{value: "/pgdata"},
				replicationRows: &intMockRows{value: 1},
			},
			pgrepMock:    mockExecute(false, true, healthyPgAutoctlJSON, nil),
			expectResult: false,
		},
		{
			name: "primary_pgautoctl_active_and_healthy_but_no_replicas_returns_true",
			dbMock: &testDB{
				isRecoveryRows:  &boolMockRows{value: false},
				pgdataRows:      &genericMockRows{value: "/pgdata"},
				replicationRows: &intMockRows{value: 0},
			},
			pgrepMock:    mockExecute(false, true, healthyPgAutoctlJSON, nil),
			expectResult: true,
		},
		{
			name: "primary_pgautoctl_active_but_unhealthy_returns_true",
			dbMock: &testDB{
				isRecoveryRows:  &boolMockRows{value: false},
				pgdataRows:      &genericMockRows{value: "/pgdata"},
				replicationRows: &intMockRows{value: 1}, // Even with replica, HA is unhealthy
			},
			pgrepMock:    mockExecute(false, true, unhealthyPgAutoctlJSON, nil),
			expectResult: true,
		},
		{
			name: "primary_pgautoctl_active_but_unreachable_secondary_returns_true",
			dbMock: &testDB{
				isRecoveryRows:  &boolMockRows{value: false},
				pgdataRows:      &genericMockRows{value: "/pgdata"},
				replicationRows: &intMockRows{value: 1},
			},
			pgrepMock:    mockExecute(false, true, unreachablePgAutoctlJSON, nil),
			expectResult: true,
		},
		{
			name: "primary_pgautoctl_active_but_state_cmd_fails_returns_error",
			dbMock: &testDB{
				isRecoveryRows:  &boolMockRows{value: false},
				pgdataRows:      &genericMockRows{value: "/pgdata"},
				replicationRows: &intMockRows{value: 1},
			},
			pgrepMock:    mockExecute(false, true, "", errors.New("cmd failed")),
			expectResult: false,
			expectErr:    true,
		},
		{
			name: "primary_pgautoctl_active_but_pgdata_fails_returns_error",
			dbMock: &testDB{
				isRecoveryRows:  &boolMockRows{value: false},
				pgdataErr:       errors.New("db error"),
				replicationRows: &intMockRows{value: 1},
			},
			pgrepMock:    mockExecute(false, true, healthyPgAutoctlJSON, nil),
			expectResult: false,
			expectErr:    true,
		},
		{
			name: "primary_pgautoctl_active_but_pgdata_iteration_error_returns_error",
			dbMock: &testDB{
				isRecoveryRows:  &boolMockRows{value: false},
				pgdataRows:      &genericMockRows{rowsErr: errors.New("iteration failed")},
				replicationRows: &intMockRows{value: 1},
			},
			pgrepMock:    mockExecute(false, true, healthyPgAutoctlJSON, nil),
			expectResult: false,
			expectErr:    true,
		},
		{
			name: "primary_no_ha_returns_true",
			dbMock: &testDB{
				isRecoveryRows:  &boolMockRows{value: false},
				replicationRows: &intMockRows{value: 1},
			},
			pgrepMock:    mockExecute(false, false, "", nil),
			expectResult: true,
		},
		{
			name: "primary_no_ha_skips_replication_query_on_error",
			dbMock: &testDB{
				isRecoveryRows: &boolMockRows{value: false},
				replicationErr: errors.New("replication query failed"),
			},
			pgrepMock:    mockExecute(false, false, "", nil),
			expectResult: true,
		},
		{
			name: "recovery_query_error_returns_error",
			dbMock: &testDB{
				isRecoveryErr: errors.New("query failed"),
			},
			pgrepMock:    mockExecute(false, false, "", nil),
			expectResult: false,
			expectErr:    true,
		},
		{
			name: "recovery_query_iteration_error_returns_error",
			dbMock: &testDB{
				isRecoveryRows: &boolMockRows{rowsErr: errors.New("iteration failed")},
			},
			pgrepMock:    mockExecute(false, false, "", nil),
			expectResult: false,
			expectErr:    true,
		},
		{
			name: "replication_query_error_returns_error",
			dbMock: &testDB{
				isRecoveryRows: &boolMockRows{value: false},
				replicationErr: errors.New("replication query failed"),
			},
			pgrepMock:    mockExecute(true, false, "", nil), // HA is active, so it will check replication
			expectResult: false,
			expectErr:    true,
		},
		{
			name: "replication_query_iteration_error_returns_error",
			dbMock: &testDB{
				isRecoveryRows:  &boolMockRows{value: false},
				replicationRows: &intMockRows{rowsErr: errors.New("iteration failed")},
			},
			pgrepMock:    mockExecute(true, false, "", nil), // HA is active, so it will check replication
			expectResult: false,
			expectErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := PostgresMetrics{
				db:      tc.dbMock,
				execute: tc.pgrepMock,
			}
			result, err := m.notFailoverProtected(ctx)
			if (err != nil) != tc.expectErr {
				t.Errorf("notFailoverProtected() got error: %v, want error presence: %v", err, tc.expectErr)
			}
			if !tc.expectErr && result != tc.expectResult {
				t.Errorf("notFailoverProtected() got result: %v, want result: %v", result, tc.expectResult)
			}
		})
	}
}

func mockSuExecute(mockFn func(executable string, args []string) commandlineexecutor.Result) commandlineexecutor.Execute {
	return func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
		if params.Executable != "su" {
			return commandlineexecutor.Result{Error: errors.New("command not found")}
		}
		if len(params.Args) < 4 || params.Args[0] != "-" || params.Args[1] != "postgres" || params.Args[2] != "-c" {
			return commandlineexecutor.Result{Error: errors.New("invalid su arguments")}
		}
		cmdStr := params.Args[3]
		fields := strings.Fields(cmdStr)
		if len(fields) == 0 {
			return commandlineexecutor.Result{Error: errors.New("empty command inside su")}
		}
		exe := fields[0]
		args := fields[1:]
		return mockFn(exe, args)
	}
}

func mockExecute(patroniRunning, pgAutoctlRunning bool, pgAutoctlJSON string, pgAutoctlErr error) commandlineexecutor.Execute {
	return func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
		if params.Executable == "pgrep" {
			if len(params.Args) < 2 {
				return commandlineexecutor.Result{ExitCode: 2, Error: fmt.Errorf("invalid args")}
			}
			proc := params.Args[1] // Args: ["-x", "processName"]
			var running bool
			switch proc {
			case "patroni":
				running = patroniRunning
			case "pg_autoctl":
				running = pgAutoctlRunning
			}
			if running {
				return commandlineexecutor.Result{ExitCode: 0, Error: nil}
			}
			return commandlineexecutor.Result{ExitCode: 1, Error: fmt.Errorf("exit status 1")}
		}

		if params.Executable == "su" {
			if len(params.Args) < 4 || params.Args[0] != "-" || params.Args[1] != "postgres" || params.Args[2] != "-c" {
				return commandlineexecutor.Result{Error: errors.New("invalid su arguments")}
			}
			cmdStr := params.Args[3]
			fields := strings.Fields(cmdStr)
			if len(fields) == 0 {
				return commandlineexecutor.Result{Error: errors.New("empty command inside su")}
			}
			exe := fields[0]
			if exe == "pg_autoctl" {
				if pgAutoctlErr != nil {
					return commandlineexecutor.Result{ExitCode: 1, Error: pgAutoctlErr, ExecutableFound: true}
				}
				return commandlineexecutor.Result{StdOut: pgAutoctlJSON, ExitCode: 0, Error: nil, ExecutableFound: true}
			}
		}

		return commandlineexecutor.Result{ExitCode: 127, Error: fmt.Errorf("unknown command: %s", params.Executable)}
	}
}

func TestIsProcessRunning(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		execMock    commandlineexecutor.Execute
		wantRunning bool
		wantErr     bool
	}{
		{
			name: "process_running_returns_true",
			execMock: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				return commandlineexecutor.Result{ExitCode: 0, Error: nil}
			},
			wantRunning: true,
		},
		{
			name: "process_not_running_returns_false",
			execMock: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				return commandlineexecutor.Result{ExitCode: 1, Error: errors.New("exit status 1")}
			},
			wantRunning: false,
		},
		{
			name: "pgrep_fails_returns_error",
			execMock: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				return commandlineexecutor.Result{ExitCode: 2, Error: errors.New("pgrep failed")}
			},
			wantRunning: false,
			wantErr:     true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m := &PostgresMetrics{
				execute: test.execMock,
			}
			running, err := m.isProcessRunning(ctx, "testproc")
			if (err != nil) != test.wantErr {
				t.Errorf("isProcessRunning() error = %v, wantErr %v", err, test.wantErr)
			}
			if running != test.wantRunning {
				t.Errorf("isProcessRunning() running = %v, want %v", running, test.wantRunning)
			}
		})
	}
}

func TestNoAutomatedBackupPolicy_Extended(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name         string
		dbMock       *testDB
		fsMock       mockFileSystem
		execMock     commandlineexecutor.Execute
		expectResult bool // Expected return value of noAutomatedBackupPolicy
		expectErr    bool
	}{
		{
			name: "ArchiveModeOff_ReturnsTrue",
			dbMock: &testDB{
				pgSettingsRows: &pgSettingsRows{
					rows: []pgSettingsRow{
						{name: "archive_mode", setting: "off"},
						{name: "archive_command", setting: "cp %p /path"},
					},
				},
			},
			execMock: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				return commandlineexecutor.Result{ExitCode: 0, Error: nil} // Mock active backup to kill mutants
			},
			expectResult: true, // Should still be true (no policy) because WAL archiving is disabled
		},
		{
			name: "ArchiveCommandInvalid_ReturnsTrue",
			dbMock: &testDB{
				pgSettingsRows: &pgSettingsRows{
					rows: []pgSettingsRow{
						{name: "archive_mode", setting: "on"},
						{name: "archive_command", setting: ":"},
					},
				},
			},
			execMock: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				return commandlineexecutor.Result{ExitCode: 0, Error: nil} // Mock active backup to kill mutants
			},
			expectResult: true, // Should still be true (no policy) because archive command is invalid
		},
		{
			name: "pgBackRestActiveViaConfig_ReturnsFalse",
			dbMock: &testDB{
				pgSettingsRows: &pgSettingsRows{
					rows: []pgSettingsRow{
						{name: "archive_mode", setting: "on"},
						{name: "archive_command", setting: "cp %p /path"},
					},
				},
			},
			fsMock: mockFileSystem{
				existsPaths: map[string]bool{
					"/etc/pgbackrest/pgbackrest.conf": true,
				},
			},
			expectResult: false, // Policy present
		},
		{
			name: "BarmanActiveViaConfig_ReturnsFalse",
			dbMock: &testDB{
				pgSettingsRows: &pgSettingsRows{
					rows: []pgSettingsRow{
						{name: "archive_mode", setting: "on"},
						{name: "archive_command", setting: "cp %p /path"},
					},
				},
			},
			fsMock: mockFileSystem{
				existsPaths: map[string]bool{
					"/etc/barman.conf": true,
				},
			},
			expectResult: false, // Policy present
		},
		{
			name: "BarmanActiveViaArchiveCommand_ReturnsFalse",
			dbMock: &testDB{
				pgSettingsRows: &pgSettingsRows{
					rows: []pgSettingsRow{
						{name: "archive_mode", setting: "on"},
						{name: "archive_command", setting: "/usr/bin/barman-wal-archive %p"},
					},
				},
			},
			expectResult: false, // Policy present
		},
		{
			name: "WalGActiveViaEnvDir_ReturnsFalse",
			dbMock: &testDB{
				pgSettingsRows: &pgSettingsRows{
					rows: []pgSettingsRow{
						{name: "archive_mode", setting: "on"},
						{name: "archive_command", setting: "cp %p /path"},
					},
				},
			},
			fsMock: mockFileSystem{
				dirFiles: map[string][]fs.FileInfo{
					"/etc/wal-g.d/env/": {mockFileInfo{name: "WALE_S3_PREFIX"}},
				},
			},
			expectResult: false, // Policy present
		},
		{
			name: "CronActive_ReturnsFalse",
			dbMock: &testDB{
				pgSettingsRows: &pgSettingsRows{
					rows: []pgSettingsRow{
						{name: "archive_mode", setting: "on"},
						{name: "archive_command", setting: "cp %p /path"},
					},
				},
			},
			execMock: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				if strings.Contains(strings.Join(params.Args, " "), "crontab") {
					return commandlineexecutor.Result{ExitCode: 0, Error: nil} // Cron found
				}
				return commandlineexecutor.Result{ExitCode: 1, Error: fmt.Errorf("exit status 1")} // Systemd not found
			},
			expectResult: false, // Policy present
		},
		{
			name: "SystemdActive_ReturnsFalse",
			dbMock: &testDB{
				pgSettingsRows: &pgSettingsRows{
					rows: []pgSettingsRow{
						{name: "archive_mode", setting: "on"},
						{name: "archive_command", setting: "cp %p /path"},
					},
				},
			},
			execMock: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				if strings.Contains(strings.Join(params.Args, " "), "systemctl") {
					return commandlineexecutor.Result{ExitCode: 0, Error: nil} // Systemd found
				}
				return commandlineexecutor.Result{ExitCode: 1, Error: fmt.Errorf("exit status 1")} // Cron not found
			},
			expectResult: false, // Policy present
		},
		{
			name: "NoBackupPolicy_ReturnsTrue",
			dbMock: &testDB{
				pgSettingsRows: &pgSettingsRows{
					rows: []pgSettingsRow{
						{name: "archive_mode", setting: "on"},
						{name: "archive_command", setting: "cp %p /path"},
					},
				},
			},
			execMock: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				return commandlineexecutor.Result{ExitCode: 1, Error: fmt.Errorf("exit status 1")} // Both cron/systemd return 1 (not found)
			},
			expectResult: true, // No policy!
		},
		{
			name: "CronError_SRE_FailSafe_ReturnsFalse",
			dbMock: &testDB{
				pgSettingsRows: &pgSettingsRows{
					rows: []pgSettingsRow{
						{name: "archive_mode", setting: "on"},
						{name: "archive_command", setting: "cp %p /path"},
					},
				},
			},
			execMock: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				if strings.Contains(strings.Join(params.Args, " "), "crontab") {
					return commandlineexecutor.Result{ExitCode: 2, Error: errors.New("cron execution error")} // Cron error
				}
				return commandlineexecutor.Result{ExitCode: 1, Error: fmt.Errorf("exit status 1")}
			},
			expectResult: false, // Policy present (fail-safe suppresses alert)
		},
		{
			name: "SystemdError_SRE_FailSafe_ReturnsFalse",
			dbMock: &testDB{
				pgSettingsRows: &pgSettingsRows{
					rows: []pgSettingsRow{
						{name: "archive_mode", setting: "on"},
						{name: "archive_command", setting: "cp %p /path"},
					},
				},
			},
			execMock: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				if strings.Contains(strings.Join(params.Args, " "), "systemctl") {
					return commandlineexecutor.Result{ExitCode: 126, Error: errors.New("systemctl permission error")} // Systemd error
				}
				return commandlineexecutor.Result{ExitCode: 1, Error: fmt.Errorf("exit status 1")} // Cron not found
			},
			expectResult: false, // Policy present (fail-safe suppresses alert)
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := PostgresMetrics{
				db:      tc.dbMock,
				fs:      tc.fsMock,
				execute: tc.execMock,
			}
			if m.execute == nil {
				// Default mock executor that returns 1 (not found) for safety
				m.execute = func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
					return commandlineexecutor.Result{ExitCode: 1, Error: fmt.Errorf("exit status 1")}
				}
			}
			result, err := m.noAutomatedBackupPolicy(ctx)
			if (err != nil) != tc.expectErr {
				t.Errorf("noAutomatedBackupPolicy() got error: %v, want error presence: %v", err, tc.expectErr)
			}
			if !tc.expectErr && result != tc.expectResult {
				t.Errorf("noAutomatedBackupPolicy() got result: %v, want result: %v", result, tc.expectResult)
			}
		})
	}
}

func TestRunCommandAsPostgres(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		executable  string
		args        []string
		wantCmdStr  string
	}{
		{
			name:       "no arguments",
			executable: "mycmd",
			args:       nil,
			wantCmdStr: "mycmd",
		},
		{
			name:       "simple arguments",
			executable: "mycmd",
			args:       []string{"arg1", "arg2"},
			wantCmdStr: "mycmd 'arg1' 'arg2'",
		},
		{
			name:       "arguments with spaces",
			executable: "mycmd",
			args:       []string{"arg with spaces", "another"},
			wantCmdStr: "mycmd 'arg with spaces' 'another'",
		},
		{
			name:       "arguments with single quotes",
			executable: "mycmd",
			args:       []string{"arg's", "simple"},
			wantCmdStr: "mycmd 'arg'\\''s' 'simple'",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var capturedParams commandlineexecutor.Params
			m := PostgresMetrics{
				execute: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
					capturedParams = params
					return commandlineexecutor.Result{ExecutableFound: true}
				},
			}

			_, _ = m.runCommandAsPostgres(ctx, tc.executable, tc.args)

			if len(capturedParams.Args) < 4 {
				t.Fatalf("expected at least 4 arguments for su, got %v", capturedParams.Args)
			}
			gotCmdStr := capturedParams.Args[3]
			if gotCmdStr != tc.wantCmdStr {
				t.Errorf("runCommandAsPostgres() cmdStr = %q, want %q", gotCmdStr, tc.wantCmdStr)
			}
		})
	}
}
