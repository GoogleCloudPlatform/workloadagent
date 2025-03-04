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

package configuration

import (
	"errors"
	"testing"

	dpb "google.golang.org/protobuf/types/known/durationpb"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"go.uber.org/zap/zapcore"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

var (
	defaultCloudProps = &cpb.CloudProperties{
		ProjectId:        "test-project",
		NumericProjectId: "123456789",
		InstanceId:       "test-instance",
		Zone:             "test-zone",
		InstanceName:     "test-instance-name",
		Image:            "test-image",
	}
)

func TestLoad(t *testing.T) {
	defaultCfg, err := defaultConfig(defaultCloudProps)
	if err != nil {
		t.Fatalf("Failed to get default config: %v", err)
	}
	tests := []struct {
		name     string
		path     string
		readFunc ReadConfigFile
		want     *cpb.Configuration
		wantErr  bool
	}{
		{
			name: "FileReadError",
			readFunc: func(p string) ([]byte, error) {
				return nil, cmpopts.AnyError
			},
			want: defaultCfg,
		},
		{
			name: "EmptyConfigFile",
			readFunc: func(p string) ([]byte, error) {
				return nil, nil
			},
			want: defaultCfg,
		},
		{
			name: "ConfigFileWithContents",
			readFunc: func(p string) ([]byte, error) {
				fileContent := `{"log_to_cloud": false, "cloud_properties": {"project_id": "config-project-id", "instance_id": "config-instance-id", "zone": "config-zone" } }`
				return []byte(fileContent), nil
			},
			want: &cpb.Configuration{
				CloudProperties: &cpb.CloudProperties{
					ProjectId:        "config-project-id",
					InstanceId:       "config-instance-id",
					Zone:             "config-zone",
					Image:            "test-image",
					NumericProjectId: "123456789",
					InstanceName:     "test-instance-name",
				},
				DataWarehouseEndpoint:  "https://workloadmanager-datawarehouse.googleapis.com/",
				AgentProperties:        &cpb.AgentProperties{Name: AgentName, Version: AgentVersion},
				LogLevel:               cpb.Configuration_INFO,
				LogToCloud:             proto.Bool(false),
				OracleConfiguration:    defaultCfg.OracleConfiguration,
				SqlserverConfiguration: defaultCfg.SqlserverConfiguration,
			},
		},
		{
			name: "MalformedConfigurationJsonFile",
			readFunc: func(p string) ([]byte, error) {
				fileContent := `{"log_to_cloud": true, "cloud_properties": {"project_id": "config-project-id", "instance_id": "config-instance-id", "zone": "config-zone", } }`
				return []byte(fileContent), nil
			},
			wantErr: true,
		},
		{
			name: "OverrideDefaultConfig",
			readFunc: func(p string) ([]byte, error) {
				fileContent := `{
					"log_level": "DEBUG",
					"log_to_cloud": false,
					"cloud_properties": {
						"project_id": "config-project-id",
						"instance_id": "config-instance-id",
						"zone": "config-zone",
						"image": "config-image"
					},
					"oracle_configuration": {
						"enabled": true,
						"oracle_discovery": {
							"enabled": true,
							"update_frequency": "240s"
						},
						"oracle_metrics": {
							"enabled": true,
							"connection_parameters": [
								{
									"username": "testuser",
									"service_name": "orcl",
									"secret": {
										"project_id": "testproject",
										"secret_name": "testsecret"
									}
								}
							],
							"collection_frequency": "120s",
							"query_timeout": "10s",
							"max_execution_threads": 20,
							"queries": [
								{
									"name": "pga_memory_queries",
									"disabled": true
								}
							]
						}
					},
					"sqlserver_configuration": {
						"enabled": true,
						"collection_configuration": {
							"collect_guest_os_metrics":true,
							"collect_sql_metrics":true,
							"collection_frequency": "600s"
						},
						"credential_configurations": [
							{
								"connection_parameters": [
									{
										"host":"test-host",
										"username":"test-user",
										"secret": {
											"project_id":"test-project",
											"secret_name":"test-secret"
										},
										"port":1433
									}
								],
								"local_collection":true
							}
						],
						"collection_timeout":"5s",
						"max_retries":5,
						"retry_frequency":"600s"
					}
				}`
				return []byte(fileContent), nil
			},
			want: &cpb.Configuration{
				CloudProperties: &cpb.CloudProperties{
					ProjectId:        "config-project-id",
					InstanceId:       "config-instance-id",
					Zone:             "config-zone",
					Image:            "config-image",
					NumericProjectId: "123456789",
					InstanceName:     "test-instance-name",
				},
				DataWarehouseEndpoint: "https://workloadmanager-datawarehouse.googleapis.com/",
				AgentProperties:       &cpb.AgentProperties{Name: AgentName, Version: AgentVersion},
				LogLevel:              cpb.Configuration_DEBUG,
				LogToCloud:            proto.Bool(false),
				OracleConfiguration: &cpb.OracleConfiguration{
					Enabled: proto.Bool(true),
					OracleDiscovery: &cpb.OracleDiscovery{
						Enabled:         proto.Bool(true),
						UpdateFrequency: &dpb.Duration{Seconds: 240},
					},
					OracleMetrics: &cpb.OracleMetrics{
						Enabled: proto.Bool(true),
						ConnectionParameters: []*cpb.ConnectionParameters{
							&cpb.ConnectionParameters{
								Username:    "testuser",
								ServiceName: "orcl",
								Secret: &cpb.SecretRef{
									ProjectId:  "testproject",
									SecretName: "testsecret",
								},
							},
						},
						CollectionFrequency: &dpb.Duration{Seconds: 120},
						QueryTimeout:        &dpb.Duration{Seconds: 10},
						MaxExecutionThreads: 20,
						Queries: append(defaultCfg.GetOracleConfiguration().GetOracleMetrics().GetQueries(), &cpb.Query{
							Name:     "pga_memory_queries",
							Disabled: proto.Bool(true),
						}),
					},
				},
				SqlserverConfiguration: &cpb.SQLServerConfiguration{
					Enabled: proto.Bool(true),
					CollectionConfiguration: &cpb.SQLServerConfiguration_CollectionConfiguration{
						CollectionFrequency:   &dpb.Duration{Seconds: 600},
						CollectGuestOsMetrics: true, CollectSqlMetrics: true,
					},
					CredentialConfigurations: []*cpb.SQLServerConfiguration_CredentialConfiguration{
						&cpb.SQLServerConfiguration_CredentialConfiguration{
							GuestConfigurations: &cpb.SQLServerConfiguration_CredentialConfiguration_LocalCollection{
								LocalCollection: true,
							},
							ConnectionParameters: []*cpb.ConnectionParameters{
								&cpb.ConnectionParameters{
									Host:     "test-host",
									Username: "test-user",
									Secret: &cpb.SecretRef{
										ProjectId:  "test-project",
										SecretName: "test-secret",
									},
									Port: 1433,
								}}}},
					CollectionTimeout: &dpb.Duration{Seconds: 5},
					MaxRetries:        5,
					RetryFrequency:    &dpb.Duration{Seconds: 600},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := Load(test.path, test.readFunc, defaultCloudProps)
			if (err != nil) != test.wantErr {
				t.Fatalf("Read(%s) returned error: %v, want error: %v", test.path, err, test.wantErr)
			}
			if diff := cmp.Diff(test.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("Load(%s) returned unexpected diff (-want +got):\n%s", test.path, diff)
			}
		})
	}
}

func TestLogLevelToZapcore(t *testing.T) {
	tests := []struct {
		name  string
		level cpb.Configuration_LogLevel
		want  zapcore.Level
	}{
		{
			name:  "INFO",
			level: cpb.Configuration_INFO,
			want:  zapcore.InfoLevel,
		},
		{
			name:  "DEBUG",
			level: cpb.Configuration_DEBUG,
			want:  zapcore.DebugLevel,
		},
		{
			name:  "WARNING",
			level: cpb.Configuration_WARNING,
			want:  zapcore.WarnLevel,
		},
		{
			name:  "ERROR",
			level: cpb.Configuration_ERROR,
			want:  zapcore.ErrorLevel,
		},
		{
			name:  "UNKNOWN",
			level: cpb.Configuration_UNDEFINED,
			want:  zapcore.InfoLevel,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := LogLevelToZapcore(test.level)
			if got != test.want {
				t.Errorf("LogLevelToZapcore(%v) = %v, want: %v", test.level, got, test.want)
			}
		})
	}
}

func TestValidateOracleConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config *cpb.Configuration
		want   error
	}{
		{
			name: "Valid configuration",
			config: &cpb.Configuration{
				OracleConfiguration: &cpb.OracleConfiguration{
					Enabled: proto.Bool(true),
					OracleMetrics: &cpb.OracleMetrics{
						Enabled: proto.Bool(true),
						ConnectionParameters: []*cpb.ConnectionParameters{
							{
								Username:    "testuser",
								ServiceName: "orcl",
								Secret: &cpb.SecretRef{
									ProjectId:  "testproject",
									SecretName: "testsecret",
								},
							},
						},
					},
				},
			},
			want: nil,
		},
		{
			name: "Oracle Metrics enabled but no connection parameters",
			config: &cpb.Configuration{
				OracleConfiguration: &cpb.OracleConfiguration{
					Enabled: proto.Bool(true),
					OracleMetrics: &cpb.OracleMetrics{
						Enabled: proto.Bool(true),
					},
				},
			},
			want: errMissingConnectionParameters,
		},
		{
			name: "Oracle Metrics enabled but username not provided",
			config: &cpb.Configuration{
				OracleConfiguration: &cpb.OracleConfiguration{
					Enabled: proto.Bool(true),
					OracleMetrics: &cpb.OracleMetrics{
						Enabled: proto.Bool(true),
						ConnectionParameters: []*cpb.ConnectionParameters{
							{
								ServiceName: "orcl",
								Secret: &cpb.SecretRef{
									ProjectId:  "testproject",
									SecretName: "testsecret",
								},
							},
						},
					},
				},
			},
			want: errMissingUsername,
		},
		{
			name: "Oracle Metrics enabled but service_name not provided",
			config: &cpb.Configuration{
				OracleConfiguration: &cpb.OracleConfiguration{
					Enabled: proto.Bool(true),
					OracleMetrics: &cpb.OracleMetrics{
						Enabled: proto.Bool(true),
						ConnectionParameters: []*cpb.ConnectionParameters{
							{
								Username: "testuser",
								Secret: &cpb.SecretRef{
									ProjectId:  "testproject",
									SecretName: "testsecret",
								},
							},
						},
					},
				},
			},
			want: errMissingServiceName,
		},
		{
			name: "Oracle Metrics enabled but secret not provided",
			config: &cpb.Configuration{
				OracleConfiguration: &cpb.OracleConfiguration{
					Enabled: proto.Bool(true),
					OracleMetrics: &cpb.OracleMetrics{
						Enabled: proto.Bool(true),
						ConnectionParameters: []*cpb.ConnectionParameters{
							{
								Username:    "testuser",
								ServiceName: "orcl",
							},
						},
					},
				},
			},
			want: errMissingSecret,
		},
		{
			name: "Oracle Metrics enabled but project ID not provided",
			config: &cpb.Configuration{
				OracleConfiguration: &cpb.OracleConfiguration{
					Enabled: proto.Bool(true),
					OracleMetrics: &cpb.OracleMetrics{
						Enabled: proto.Bool(true),
						ConnectionParameters: []*cpb.ConnectionParameters{
							{
								Username:    "testuser",
								ServiceName: "orcl",
								Secret:      &cpb.SecretRef{SecretName: "testsecret"},
							},
						},
					},
				},
			},
			want: errMissingProjectID,
		},
		{
			name: "Oracle Metrics enabled but secret name not provided",
			config: &cpb.Configuration{
				OracleConfiguration: &cpb.OracleConfiguration{
					Enabled: proto.Bool(true),
					OracleMetrics: &cpb.OracleMetrics{
						Enabled: proto.Bool(true),
						ConnectionParameters: []*cpb.ConnectionParameters{
							{
								Username:    "testuser",
								ServiceName: "orcl",
								Secret:      &cpb.SecretRef{ProjectId: "testproject"},
							},
						},
					},
				},
			},
			want: errMissingSecretName,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateOracleConfiguration(tc.config)
			if !errors.Is(err, tc.want) {
				t.Errorf("validateOracleConfiguration() got %v, want: %v", err, tc.want)
			}
		})
	}
}

func TestValidateSQLServerConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config *cpb.Configuration
		want   error
	}{
		{
			name: "Valid configuration",
			config: &cpb.Configuration{
				SqlserverConfiguration: &cpb.SQLServerConfiguration{
					Enabled: proto.Bool(true),
					CollectionConfiguration: &cpb.SQLServerConfiguration_CollectionConfiguration{
						CollectionFrequency: &dpb.Duration{
							Seconds: 3600,
						}},
					CredentialConfigurations: []*cpb.SQLServerConfiguration_CredentialConfiguration{},
					CollectionTimeout:        &dpb.Duration{Seconds: 10},
					RetryFrequency:           &dpb.Duration{Seconds: 3600},
				},
			},
			want: nil,
		},
		{
			name: "Collection configuration not provided",
			config: &cpb.Configuration{
				SqlserverConfiguration: &cpb.SQLServerConfiguration{
					Enabled:                  proto.Bool(true),
					CredentialConfigurations: []*cpb.SQLServerConfiguration_CredentialConfiguration{},
					CollectionTimeout:        &dpb.Duration{Seconds: 10},
					RetryFrequency:           &dpb.Duration{Seconds: 3600},
				},
			},
			want: sqlServerConfigurationErrors["errMissingCollectionConfiguration"],
		},
		{
			name: "invalid collection frequency",
			config: &cpb.Configuration{
				SqlserverConfiguration: &cpb.SQLServerConfiguration{
					Enabled: proto.Bool(true),
					CollectionConfiguration: &cpb.SQLServerConfiguration_CollectionConfiguration{
						CollectionFrequency: &dpb.Duration{Seconds: -1},
					},
					CredentialConfigurations: []*cpb.SQLServerConfiguration_CredentialConfiguration{},
					CollectionTimeout:        &dpb.Duration{Seconds: 10},
					RetryFrequency:           &dpb.Duration{Seconds: 3600},
				},
			},
			want: sqlServerConfigurationErrors["errInvalidCollectionFrequency"],
		},
		{
			name: "invalid collection frequency with other fields set",
			config: &cpb.Configuration{
				SqlserverConfiguration: &cpb.SQLServerConfiguration{
					Enabled: proto.Bool(true),
					CollectionConfiguration: &cpb.SQLServerConfiguration_CollectionConfiguration{
						CollectGuestOsMetrics: true,
						CollectSqlMetrics:     true,
						CollectionFrequency:   &dpb.Duration{Seconds: -1},
					},
					CredentialConfigurations: []*cpb.SQLServerConfiguration_CredentialConfiguration{},
					CollectionTimeout:        &dpb.Duration{Seconds: 10},
					RetryFrequency:           &dpb.Duration{Seconds: 3600},
				},
			},
			want: sqlServerConfigurationErrors["errInvalidCollectionFrequency"],
		},
		{
			name: "Credential configurations not provided",
			config: &cpb.Configuration{
				SqlserverConfiguration: &cpb.SQLServerConfiguration{
					Enabled:                 proto.Bool(true),
					CollectionConfiguration: &cpb.SQLServerConfiguration_CollectionConfiguration{},
					CollectionTimeout:       &dpb.Duration{Seconds: 10},
					RetryFrequency:          &dpb.Duration{Seconds: 3600},
				},
			},
			want: sqlServerConfigurationErrors["errMissingCredentialConfigurations"],
		},
		{
			name: "invalid collection timeout",
			config: &cpb.Configuration{
				SqlserverConfiguration: &cpb.SQLServerConfiguration{
					Enabled: proto.Bool(true),
					CollectionConfiguration: &cpb.SQLServerConfiguration_CollectionConfiguration{
						CollectionFrequency: &dpb.Duration{Seconds: 3600},
					},
					CredentialConfigurations: []*cpb.SQLServerConfiguration_CredentialConfiguration{},
					CollectionTimeout:        &dpb.Duration{Seconds: -1},
					RetryFrequency:           &dpb.Duration{Seconds: 3600},
				},
			},
			want: sqlServerConfigurationErrors["errInvalidCollectionTimeout"],
		},
		{
			name: "invalid retry frequency",
			config: &cpb.Configuration{
				SqlserverConfiguration: &cpb.SQLServerConfiguration{
					Enabled: proto.Bool(true),
					CollectionConfiguration: &cpb.SQLServerConfiguration_CollectionConfiguration{
						CollectionFrequency: &dpb.Duration{Seconds: 3600},
					},
					CredentialConfigurations: []*cpb.SQLServerConfiguration_CredentialConfiguration{},
					CollectionTimeout:        &dpb.Duration{Seconds: 10},
					RetryFrequency:           &dpb.Duration{Seconds: -1},
				},
			},
			want: sqlServerConfigurationErrors["errInvalidRetryFrequency"],
		},
		{
			name: "invalid max retries",
			config: &cpb.Configuration{
				SqlserverConfiguration: &cpb.SQLServerConfiguration{
					Enabled: proto.Bool(true),
					CollectionConfiguration: &cpb.SQLServerConfiguration_CollectionConfiguration{
						CollectionFrequency: &dpb.Duration{Seconds: 3600},
					},
					CredentialConfigurations: []*cpb.SQLServerConfiguration_CredentialConfiguration{},
					CollectionTimeout:        &dpb.Duration{Seconds: 10},
					RetryFrequency:           &dpb.Duration{Seconds: 3600},
					MaxRetries:               -1,
				},
			},
			want: sqlServerConfigurationErrors["errInvalidMaxRetries"],
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSQLServerConfiguration(tc.config)
			if !errors.Is(err, tc.want) {
				t.Errorf("validateSQLServerConfiguration() got %v, want: %v", err, tc.want)
			}
		})
	}
}
