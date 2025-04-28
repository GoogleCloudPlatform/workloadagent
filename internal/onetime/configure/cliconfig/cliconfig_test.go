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

package cliconfig

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

// mockMarshaller implements Marshaller for testing.
type mockMarshaller struct {
	ReturnError error
	Called      bool
	InputProto  proto.Message
}

func (m *mockMarshaller) Marshal(msg proto.Message) ([]byte, error) {
	m.Called = true
	m.InputProto = proto.Clone(msg)
	if m.ReturnError != nil {
		return nil, m.ReturnError
	}
	// Return dummy data, not relevant for this error test
	return []byte("{}"), nil
}

func fakeWriteFileSuccess() WriteConfigFile {
	return func(name string, content []byte, perm os.FileMode) error {
		return nil
	}
}

func fakeWriteFileFail() WriteConfigFile {
	return func(name string, content []byte, perm os.FileMode) error {
		return cmpopts.AnyError
	}
}

func TestWriteFileErrors(t *testing.T) {
	tempPath := path.Join(t.TempDir(), "/configuration.json")
	cp := &cpb.CloudProperties{
		ProjectId:        "test-project",
		InstanceId:       "1234567890",
		Zone:             "us-central1-a",
		InstanceName:     "test-instance",
		Image:            "test-image",
		NumericProjectId: "1234567890",
		Region:           "us-central1",
		MachineType:      "test-machine-type",
	}
	cfg := &cpb.Configuration{
		CloudProperties: cp,
		OracleConfiguration: &cpb.OracleConfiguration{
			Enabled: proto.Bool(true),
		},
		SqlserverConfiguration: &cpb.SQLServerConfiguration{
			Enabled: proto.Bool(true),
		},
	}
	tests := []struct {
		name      string
		path      string
		cfg       *cpb.Configuration
		configure *Configure
		wantErr   error
	}{
		{
			name: "Success",
			path: tempPath,
			cfg:  cfg,
			configure: &Configure{
				Configuration: cfg,
				Path:          tempPath,
				marshaller:    &mockMarshaller{},
				fileWriter:    fakeWriteFileSuccess(),
			},
		},
		{
			name: "NilConfigFailure",
			path: tempPath,
			cfg:  nil,
			configure: &Configure{
				Configuration: nil,
				Path:          tempPath,
				marshaller:    &mockMarshaller{},
				fileWriter:    fakeWriteFileSuccess(),
			},
			wantErr: cmpopts.AnyError,
		},
		{
			name: "MarshalFailure",
			path: tempPath,
			cfg:  cfg,
			configure: &Configure{
				Configuration: cfg,
				Path:          tempPath,
				marshaller:    &mockMarshaller{ReturnError: cmpopts.AnyError},
				fileWriter:    fakeWriteFileSuccess(),
			},
			wantErr: cmpopts.AnyError,
		},
		{
			name: "WriteFileFailure",
			path: tempPath,
			cfg:  cfg,
			configure: &Configure{
				Configuration: cfg,
				Path:          tempPath,
				marshaller:    &mockMarshaller{},
				fileWriter:    fakeWriteFileFail(),
			},
			wantErr: cmpopts.AnyError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotErr := tc.configure.WriteFile(context.Background())
			if !cmp.Equal(gotErr, tc.wantErr, cmpopts.EquateErrors()) {
				t.Errorf("writeFile(%v, %v)=%v, want %v", tc.configure, tc.path, gotErr, tc.wantErr)
			}
		})
	}
}

func TestValidateOracle(t *testing.T) {
	tests := []struct {
		name           string
		configToModify *Configure
		want           *Configure
	}{
		{
			name: "ValidOracleConfig",
			configToModify: &Configure{
				Configuration: &cpb.Configuration{},
			},
			want: &Configure{
				Configuration: &cpb.Configuration{
					OracleConfiguration: &cpb.OracleConfiguration{},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.configToModify.ValidateOracle()
			// Compare the configurations.
			if diff := cmp.Diff(tc.want, tc.configToModify, protocmp.Transform(), cmpopts.IgnoreUnexported(Configure{})); diff != "" {
				t.Errorf("ValidateOracle() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestValidateOracleDiscovery(t *testing.T) {
	tests := []struct {
		name           string
		configToModify *Configure
		want           *Configure
	}{
		{
			name: "ValidOracleDiscoveryConfig",
			configToModify: &Configure{
				Configuration: &cpb.Configuration{},
			},
			want: &Configure{
				Configuration: &cpb.Configuration{
					OracleConfiguration: &cpb.OracleConfiguration{
						OracleDiscovery: &cpb.OracleDiscovery{},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.configToModify.ValidateOracleDiscovery()
			// Compare the configurations.
			if diff := cmp.Diff(tc.want, tc.configToModify, protocmp.Transform(), cmpopts.IgnoreUnexported(Configure{})); diff != "" {
				t.Errorf("ValidateOracleDiscovery() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestValidateOracleMetrics(t *testing.T) {
	tests := []struct {
		name           string
		configToModify *Configure
		want           *Configure
	}{
		{
			name: "ValidOracleMetricsConfig",
			configToModify: &Configure{
				Configuration: &cpb.Configuration{},
			},
			want: &Configure{
				Configuration: &cpb.Configuration{
					OracleConfiguration: &cpb.OracleConfiguration{
						OracleMetrics: &cpb.OracleMetrics{},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.configToModify.ValidateOracleMetrics()
			// Compare the configurations.
			if diff := cmp.Diff(tc.want, tc.configToModify, protocmp.Transform(), cmpopts.IgnoreUnexported(Configure{})); diff != "" {
				t.Errorf("ValidateOracleMetrics() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestValidateOracleMetricsConnection(t *testing.T) {
	tests := []struct {
		name           string
		configToModify *Configure
		want           *Configure
	}{
		{
			name: "ValidOracleMetricsConnectionConfig",
			configToModify: &Configure{
				Configuration: &cpb.Configuration{},
			},
			want: &Configure{
				Configuration: &cpb.Configuration{
					OracleConfiguration: &cpb.OracleConfiguration{
						OracleMetrics: &cpb.OracleMetrics{
							ConnectionParameters: []*cpb.ConnectionParameters{},
						},
					},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.configToModify.ValidateOracleMetricsConnection()
			// Compare the configurations.
			if diff := cmp.Diff(tc.want, tc.configToModify, protocmp.Transform(), cmpopts.IgnoreUnexported(Configure{})); diff != "" {
				t.Errorf("ValidateOracleMetricsConnection() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestValidateSQLServer(t *testing.T) {
	tests := []struct {
		name           string
		configToModify *Configure
		want           *Configure
	}{
		{
			name: "ValidSQLServerConfig",
			configToModify: &Configure{
				Configuration: &cpb.Configuration{},
			},
			want: &Configure{
				Configuration: &cpb.Configuration{
					SqlserverConfiguration: &cpb.SQLServerConfiguration{},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.configToModify.ValidateSQLServer()
			// Compare the configurations.
			if diff := cmp.Diff(tc.want, tc.configToModify, protocmp.Transform(), cmpopts.IgnoreUnexported(Configure{})); diff != "" {
				t.Errorf("ValidateSQLServer() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestValidateSQLServerCollectionConfig(t *testing.T) {
	tests := []struct {
		name           string
		configToModify *Configure
		want           *Configure
	}{
		{
			name: "ValidSQLServerCollectionConfig",
			configToModify: &Configure{
				Configuration: &cpb.Configuration{},
			},
			want: &Configure{
				Configuration: &cpb.Configuration{
					SqlserverConfiguration: &cpb.SQLServerConfiguration{
						CollectionConfiguration: &cpb.SQLServerConfiguration_CollectionConfiguration{},
					},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.configToModify.ValidateSQLServerCollectionConfig()
			// Compare the configurations.
			if diff := cmp.Diff(tc.want, tc.configToModify, protocmp.Transform(), cmpopts.IgnoreUnexported(Configure{})); diff != "" {
				t.Errorf("ValidateSQLServerCollectionConfig() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestValidateRedis(t *testing.T) {
	tests := []struct {
		name           string
		configToModify *Configure
		want           *Configure
	}{
		{
			name: "ValidRedisConfig",
			configToModify: &Configure{
				Configuration: &cpb.Configuration{},
			},
			want: &Configure{
				Configuration: &cpb.Configuration{
					RedisConfiguration: &cpb.RedisConfiguration{},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.configToModify.ValidateRedis()
			// Compare the configurations.
			if diff := cmp.Diff(tc.want, tc.configToModify, protocmp.Transform(), cmpopts.IgnoreUnexported(Configure{})); diff != "" {
				t.Errorf("ValidateRedis() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestValidateRedisConnectionParams(t *testing.T) {
	tests := []struct {
		name           string
		configToModify *Configure
		want           *Configure
	}{
		{
			name: "ValidRedisConnectionParams",
			configToModify: &Configure{
				Configuration: &cpb.Configuration{},
			},
			want: &Configure{
				Configuration: &cpb.Configuration{
					RedisConfiguration: &cpb.RedisConfiguration{
						ConnectionParameters: &cpb.ConnectionParameters{},
					},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.configToModify.ValidateRedisConnectionParams()
			// Compare the configurations.
			if diff := cmp.Diff(tc.want, tc.configToModify, protocmp.Transform(), cmpopts.IgnoreUnexported(Configure{})); diff != "" {
				t.Errorf("ValidateRedisConnectionParams() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestValidateMySQL(t *testing.T) {
	tests := []struct {
		name           string
		configToModify *Configure
		want           *Configure
	}{
		{
			name: "ValidMySQLConfig",
			configToModify: &Configure{
				Configuration: &cpb.Configuration{},
			},
			want: &Configure{
				Configuration: &cpb.Configuration{
					MysqlConfiguration: &cpb.MySQLConfiguration{},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.configToModify.ValidateMySQL()
			// Compare the configurations.
			if diff := cmp.Diff(tc.want, tc.configToModify, protocmp.Transform(), cmpopts.IgnoreUnexported(Configure{})); diff != "" {
				t.Errorf("ValidateMySQL() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestValidateMySQLConnectionParams(t *testing.T) {
	tests := []struct {
		name           string
		configToModify *Configure
		want           *Configure
	}{
		{
			name: "ValidMySQLConnectionParams",
			configToModify: &Configure{
				Configuration: &cpb.Configuration{},
			},
			want: &Configure{
				Configuration: &cpb.Configuration{
					MysqlConfiguration: &cpb.MySQLConfiguration{
						ConnectionParameters: &cpb.ConnectionParameters{},
					},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.configToModify.ValidateMySQLConnectionParams()
			// Compare the configurations.
			if diff := cmp.Diff(tc.want, tc.configToModify, protocmp.Transform(), cmpopts.IgnoreUnexported(Configure{})); diff != "" {
				t.Errorf("ValidateMySQLConnectionParams() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
