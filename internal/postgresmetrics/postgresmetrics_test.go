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
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/api/googleapi"
	"google.golang.org/protobuf/testing/protocmp"
	"github.com/GoogleCloudPlatform/workloadagent/internal/databasecenter"
	"github.com/GoogleCloudPlatform/workloadagent/internal/workloadmanager"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	gcefake "github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce/fake"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce/wlm"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
)

type testDB struct {
	workMemRows    rowsInterface
	workMemErr     error
	versionRows    rowsInterface
	versionErr     error
	pingErr        error
	pgauditLogRows rowsInterface
	pgauditLogErr  error
	sslRows        rowsInterface
	sslErr         error
	hbaRulesRows   rowsInterface
	hbaRulesErr    error
}

var emptyDB = &testDB{}

func (t *testDB) QueryContext(ctx context.Context, query string, args ...any) (rowsInterface, error) {
	if query == "SHOW work_mem" {
		return t.workMemRows, t.workMemErr
	}
	if query == "SHOW server_version" {
		return t.versionRows, t.versionErr
	}
	if query == "SHOW ssl" {
		return t.sslRows, t.sslErr
	}
	if query == "SHOW pgaudit.log" {
		return t.pgauditLogRows, t.pgauditLogErr
	}
	if strings.Contains(query, "FROM pg_hba_file_rules()") {
		return t.hbaRulesRows, t.hbaRulesErr
	}
	return nil, nil
}

func (t *testDB) Ping() error {
	return t.pingErr
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

type MockDatabaseCenterClient struct {
	sendMetadataCalled bool
	sendMetadataErr    error
}

func (m *MockDatabaseCenterClient) SendMetadataToDatabaseCenter(ctx context.Context, metrics databasecenter.DBCenterMetrics) error {
	m.sendMetadataCalled = true
	return m.sendMetadataErr
}

func TestInitDB(t *testing.T) {
	tests := []struct {
		name       string
		m          PostgresMetrics
		gceService gceInterface
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

func TestCollectMetricsOnce(t *testing.T) {
	tests := []struct {
		name        string
		m           PostgresMetrics
		wantMetrics *workloadmanager.WorkloadMetrics
		wantVersion string
		wantErr     bool
	}{
		{
			name: "HappyPath",
			m: PostgresMetrics{
				db: &testDB{
					workMemRows:    &workMemRows{count: 0, size: 1, data: "80MB", shouldErr: false},
					workMemErr:     nil,
					versionRows:    &versionRows{count: 0, size: 1, data: "14.4 (Debian 14.4-1.pgdg110+1)", shouldErr: false},
					versionErr:     nil,
					pgauditLogRows: &genericMockRows{value: "all"},
					sslRows:        &genericMockRows{value: "off"},
					hbaRulesRows:   &hbaRulesRows{value: 1},
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
			wantVersion: "14.4",
			wantErr:     false,
		},
		{
			name: "HappyPathKB",
			m: PostgresMetrics{
				db: &testDB{
					workMemRows:    &workMemRows{count: 0, size: 1, data: "64kB", shouldErr: false},
					workMemErr:     nil,
					versionRows:    &versionRows{count: 0, size: 1, data: "14.4 (Debian 14.4-1.pgdg110+1)", shouldErr: false},
					versionErr:     nil,
					pgauditLogRows: &genericMockRows{value: "all"},
					sslRows:        &genericMockRows{value: "off"},
					hbaRulesRows:   &hbaRulesRows{value: 1},
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
			wantVersion: "14.4",
			wantErr:     false,
		},
		{
			name: "HappyPathGB",
			m: PostgresMetrics{
				db: &testDB{
					workMemRows:    &workMemRows{count: 0, size: 1, data: "4GB", shouldErr: false},
					workMemErr:     nil,
					versionRows:    &versionRows{count: 0, size: 1, data: "14.4 (Debian 14.4-1.pgdg110+1)", shouldErr: false},
					versionErr:     nil,
					pgauditLogRows: &genericMockRows{value: "all"},
					sslRows:        &genericMockRows{value: "off"},
					hbaRulesRows:   &hbaRulesRows{value: 1},
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
			wantVersion: "14.4",
			wantErr:     false,
		},
		{
			name: "GetWorkMemError",
			m: PostgresMetrics{
				db: &testDB{
					workMemErr:     errors.New("test-error"),
					versionRows:    &versionRows{count: 0, size: 1, data: "14.4 (Debian 14.4-1.pgdg110+1)", shouldErr: false},
					versionErr:     nil,
					pgauditLogRows: &genericMockRows{value: "all"},
					sslRows:        &genericMockRows{value: "off"},
					hbaRulesRows:   &hbaRulesRows{value: 1},
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
			wantVersion: "14.4",
			wantErr:     true,
		},
		{
			name: "GetVersionError",
			m: PostgresMetrics{
				db: &testDB{
					workMemRows:    &workMemRows{count: 0, size: 1, data: "64MB", shouldErr: false},
					workMemErr:     nil,
					versionErr:     errors.New("test-error"),
					pgauditLogRows: &genericMockRows{value: "all"},
					sslRows:        &genericMockRows{value: "off"},
					hbaRulesRows:   &hbaRulesRows{value: 1},
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
					workMemKey: strconv.Itoa(64 * 1024 * 1024),
				},
			},
			wantVersion: "",
			wantErr:     false,
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
		gotMetrics, err := tc.m.CollectMetricsOnce(ctx, true)
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
					workMemRows:    &workMemRows{count: 0, size: 1, data: "80MB", shouldErr: false},
					workMemErr:     nil,
					versionRows:    &versionRows{count: 0, size: 1, data: "14.4 (Debian 14.4-1.pgdg110+1)", shouldErr: false},
					versionErr:     nil,
					pgauditLogRows: &genericMockRows{value: "all"},
					sslRows:        &genericMockRows{value: "on"},
					hbaRulesRows:   &hbaRulesRows{value: 0},
				},
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
				databasecenter.MajorVersionKey:             "14",
				databasecenter.MinorVersionKey:             "14.4",
				databasecenter.ExposedToPublicAccessKey:    "false",
				databasecenter.UnencryptedConnectionsKey:   "false",
				databasecenter.DatabaseAuditingDisabledKey: "false",
			},
			wantSendMetadataCall: true,
			wantErr:              false,
		},
		{
			name: "Send metadata failure, should not return error",
			m: PostgresMetrics{
				db: &testDB{
					workMemRows:    &workMemRows{count: 0, size: 1, data: "80MB", shouldErr: false},
					workMemErr:     nil,
					versionRows:    &versionRows{count: 0, size: 1, data: "14.4 (Debian 14.4-1.pgdg110+1)", shouldErr: false},
					versionErr:     nil,
					pgauditLogRows: &genericMockRows{value: "none"},
					sslRows:        &genericMockRows{value: "off"},
					hbaRulesRows:   &hbaRulesRows{value: 1},
				},
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
				databasecenter.MajorVersionKey:             "14",
				databasecenter.MinorVersionKey:             "14.4",
				databasecenter.ExposedToPublicAccessKey:    "true",
				databasecenter.UnencryptedConnectionsKey:   "true",
				databasecenter.DatabaseAuditingDisabledKey: "true",
			},
			wantSendMetadataCall: true,
			wantErr:              false,
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
			// Call the function under test
			metrics, err := tc.m.CollectMetricsOnce(ctx, true)

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
			if metrics == nil {
				t.Errorf("CollectMetricsOnce returned nil metrics")
			}
		})
	}
}
