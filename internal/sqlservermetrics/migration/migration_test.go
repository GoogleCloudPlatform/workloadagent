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

package migration

import (
	"os"
	"path"
	"testing"
	"time"

	durationpb "google.golang.org/protobuf/types/known/durationpb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	sqlserverpb "github.com/GoogleCloudPlatform/workloadagent/protos/sqlserveragent"
)

func TestMigrateSQLServerConfigurations(t *testing.T) {
	tests := []struct {
		name                      string
		wantBackupFileNotExistErr bool
		wantReadBackupFileErr     bool
		wantReadConfigFileErr     bool
	}{
		{
			name: "success",
		},
		{
			name:                      "backup_file_not_exist_error",
			wantBackupFileNotExistErr: true,
			wantReadBackupFileErr:     true,
		},
		{
			name:                  "read_backup_file_error",
			wantReadBackupFileErr: true,
		},
		{
			name:                  "read_config_file_error",
			wantReadConfigFileErr: true,
		},
	}
	contentBackup := []byte(`{
		"log_level": "DEBUG",
		"log_to_cloud": true,
		"disable_log_usage": false,
		"collection_configuration": {
			"collect_guest_os_metrics": true,
			"collect_sql_metrics": true,
			"sql_metrics_collection_interval_in_seconds": 1234,
			"guest_os_metrics_collection_interval_in_seconds": 5678
		},
		"credential_configuration": [
			{
				"instance_name": "test_instance_name",
				"instance_id": "test_instance_id",
				"sql_configurations": [
					{
						"host": "test_host",
						"user_name": "test_user_name",
						"secret_name": "test_secret_name",
						"port_number": 1234
					}
				],
				"local_collection": true
			}
		],
		"collection_timeout_seconds": 10,
		"max_retries": 3,
		"retry_interval_in_seconds": 3600
	}`)
	contentConfiguration := []byte(`{
		"log_level": "INFO",
		"log_to_cloud": false
	}`)

	want := &configpb.Configuration{
		LogLevel:   configpb.Configuration_DEBUG,
		LogToCloud: proto.Bool(true),
		AgentProperties: &configpb.AgentProperties{
			LogUsageMetrics: true,
		},
		SqlserverConfiguration: &configpb.SQLServerConfiguration{
			Enabled: proto.Bool(true),
			CollectionConfiguration: &configpb.SQLServerConfiguration_CollectionConfiguration{
				CollectGuestOsMetrics: true,
				CollectSqlMetrics:     true,
				CollectionFrequency:   durationpb.New(time.Duration(1234) * time.Second),
			},
			CredentialConfigurations: []*configpb.SQLServerConfiguration_CredentialConfiguration{
				&configpb.SQLServerConfiguration_CredentialConfiguration{
					ConnectionParameters: []*configpb.ConnectionParameters{
						&configpb.ConnectionParameters{
							Username: "test_user_name",
							Host:     "test_host",
							Port:     1234,
							Secret: &configpb.SecretRef{
								SecretName: "test_secret_name",
							},
						},
					},
					GuestConfigurations: &configpb.SQLServerConfiguration_CredentialConfiguration_LocalCollection{
						LocalCollection: true,
					},
				},
			},
			CollectionTimeout: durationpb.New(time.Duration(10) * time.Second),
			MaxRetries:        3,
			RetryFrequency:    durationpb.New(time.Duration(3600) * time.Second),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backupPath := path.Join(t.TempDir(), "backup.json")
			configPath := path.Join(t.TempDir(), "configuration.json")
			if !tc.wantBackupFileNotExistErr {
				// Create temp invalid backup file.
				if err := os.WriteFile(backupPath, []byte("invalid_json"), 0644); err != nil {
					t.Fatal(err)
				}
			}
			if !tc.wantReadBackupFileErr {
				// Write valid backup configuration into file.
				if err := os.WriteFile(backupPath, contentBackup, 0644); err != nil {
					t.Fatal(err)
				}
			}
			// Create temp invalid configuration file.
			if err := os.WriteFile(configPath, []byte("invalid_json"), 0644); err != nil {
				t.Fatal(err)
			}
			if !tc.wantReadConfigFileErr {
				// Write valid configuration into file.
				if err := os.WriteFile(configPath, contentConfiguration, 0644); err != nil {
					t.Fatal(err)
				}
			}
			err := migrateSQLServerConfigurations(backupPath, configPath)
			if tc.wantBackupFileNotExistErr {
				if err != nil {
					t.Fatalf("MigrateSQLServerConfigurations() returned unexpected error: %v", err)
				}
				return
			}
			if tc.wantReadBackupFileErr {
				if err == nil {
					t.Fatalf("MigrateSQLServerConfigurations() returned nil, want ReadBackupFile error")
				}
				return
			}
			if tc.wantReadConfigFileErr {
				if err == nil {
					t.Fatalf("MigrateSQLServerConfigurations() returned nil, want ReadConfigFile error")
				}
				return
			}
			if err != nil {
				t.Fatalf("MigrateSQLServerConfigurations() returned an unexpected error: %v", err)
			}

			cfg, err := os.ReadFile(configPath)
			if err != nil {
				t.Fatalf("Failed to read configuration file: %v", err)
			}
			got := &configpb.Configuration{}
			if err = protojson.Unmarshal(cfg, got); err != nil {
				t.Fatalf("Failed to unmarshal configuration file: %v", err)
			}
			if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
				t.Errorf("MigrateSQLServerConfigurations() returned an unexpected diff (-want +got): %v", diff)
			}
		})
	}
}

func TestMigrate(t *testing.T) {
	tests := []struct {
		name    string
		backCfg *sqlserverpb.Configuration
		config  *configpb.Configuration
		want    *configpb.Configuration
	}{
		{
			name: "test_migrate_local_collection",
			backCfg: &sqlserverpb.Configuration{
				LogLevel:        "DEBUG",
				LogToCloud:      true,
				DisableLogUsage: false,
				CollectionConfiguration: &sqlserverpb.CollectionConfiguration{
					CollectGuestOsMetrics:                     true,
					CollectSqlMetrics:                         true,
					SqlMetricsCollectionIntervalInSeconds:     1234,
					GuestOsMetricsCollectionIntervalInSeconds: 5678,
				},
				CredentialConfiguration: []*sqlserverpb.CredentialConfiguration{
					&sqlserverpb.CredentialConfiguration{
						SqlConfigurations: []*sqlserverpb.CredentialConfiguration_SqlCredentials{
							&sqlserverpb.CredentialConfiguration_SqlCredentials{
								Host:       "test_host",
								UserName:   "test_user_name",
								SecretName: "test_secret_name",
								PortNumber: 1234,
							},
						},
						GuestConfigurations: &sqlserverpb.CredentialConfiguration_LocalCollection{
							LocalCollection: true,
						},
					},
				},
				CollectionTimeoutSeconds: 10,
				MaxRetries:               3,
				RetryIntervalInSeconds:   3600,
			},
			config: &configpb.Configuration{
				LogLevel:   configpb.Configuration_INFO,
				LogToCloud: proto.Bool(false),
			},
			want: &configpb.Configuration{
				LogLevel:   configpb.Configuration_DEBUG,
				LogToCloud: proto.Bool(true),
				AgentProperties: &configpb.AgentProperties{
					LogUsageMetrics: true,
				},
				SqlserverConfiguration: &configpb.SQLServerConfiguration{
					Enabled: proto.Bool(true),
					CollectionConfiguration: &configpb.SQLServerConfiguration_CollectionConfiguration{
						CollectGuestOsMetrics: true,
						CollectSqlMetrics:     true,
						CollectionFrequency:   durationpb.New(time.Duration(1234) * time.Second),
					},
					CredentialConfigurations: []*configpb.SQLServerConfiguration_CredentialConfiguration{
						&configpb.SQLServerConfiguration_CredentialConfiguration{
							ConnectionParameters: []*configpb.ConnectionParameters{
								&configpb.ConnectionParameters{
									Username: "test_user_name",
									Host:     "test_host",
									Port:     1234,
									Secret: &configpb.SecretRef{
										SecretName: "test_secret_name",
									},
								},
							},
							GuestConfigurations: &configpb.SQLServerConfiguration_CredentialConfiguration_LocalCollection{
								LocalCollection: true,
							},
						},
					},
					CollectionTimeout: durationpb.New(time.Duration(10) * time.Second),
					MaxRetries:        3,
					RetryFrequency:    durationpb.New(time.Duration(3600) * time.Second),
				},
			},
		},
		{
			name: "test_migrate_remote",
			backCfg: &sqlserverpb.Configuration{
				LogLevel:   "DEBUG",
				LogToCloud: true,
				CollectionConfiguration: &sqlserverpb.CollectionConfiguration{
					CollectGuestOsMetrics:                     true,
					CollectSqlMetrics:                         true,
					SqlMetricsCollectionIntervalInSeconds:     1234,
					GuestOsMetricsCollectionIntervalInSeconds: 5678,
				},
				CredentialConfiguration: []*sqlserverpb.CredentialConfiguration{
					&sqlserverpb.CredentialConfiguration{
						InstanceName: "test_instance_name",
						InstanceId:   "test_instance_id",
						SqlConfigurations: []*sqlserverpb.CredentialConfiguration_SqlCredentials{
							&sqlserverpb.CredentialConfiguration_SqlCredentials{
								Host:       "test_host",
								UserName:   "test_user_name",
								SecretName: "test_secret_name",
								PortNumber: 1234,
							},
						},
						GuestConfigurations: &sqlserverpb.CredentialConfiguration_RemoteWin{
							RemoteWin: &sqlserverpb.CredentialConfiguration_GuestCredentialsRemoteWin{
								ServerName:      "test_server_name",
								GuestUserName:   "test_guest_user_name",
								GuestSecretName: "test_guest_secret_name",
							},
						},
					},
					&sqlserverpb.CredentialConfiguration{
						InstanceName: "test_instance_name",
						InstanceId:   "test_instance_id",
						SqlConfigurations: []*sqlserverpb.CredentialConfiguration_SqlCredentials{
							&sqlserverpb.CredentialConfiguration_SqlCredentials{
								Host:       "test_host",
								UserName:   "test_user_name",
								SecretName: "test_secret_name",
								PortNumber: 1234,
							},
						},
						GuestConfigurations: &sqlserverpb.CredentialConfiguration_RemoteLinux{
							RemoteLinux: &sqlserverpb.CredentialConfiguration_GuestCredentialsRemoteLinux{
								ServerName:             "test_server_name",
								GuestUserName:          "test_guest_user_name",
								GuestPortNumber:        1234,
								LinuxSshPrivateKeyPath: "test_linux_ssh_private_key_path",
							},
						},
					},
				},
				CollectionTimeoutSeconds: 10,
				MaxRetries:               3,
				RetryIntervalInSeconds:   3600,
				RemoteCollection:         true,
			},
			config: &configpb.Configuration{
				LogLevel:   configpb.Configuration_INFO,
				LogToCloud: proto.Bool(false),
			},
			want: &configpb.Configuration{
				LogLevel:   configpb.Configuration_DEBUG,
				LogToCloud: proto.Bool(true),
				AgentProperties: &configpb.AgentProperties{
					LogUsageMetrics: true,
				},
				SqlserverConfiguration: &configpb.SQLServerConfiguration{
					Enabled: proto.Bool(true),
					CollectionConfiguration: &configpb.SQLServerConfiguration_CollectionConfiguration{
						CollectGuestOsMetrics: true,
						CollectSqlMetrics:     true,
						CollectionFrequency:   durationpb.New(time.Duration(1234) * time.Second),
					},
					CredentialConfigurations: []*configpb.SQLServerConfiguration_CredentialConfiguration{
						&configpb.SQLServerConfiguration_CredentialConfiguration{
							ConnectionParameters: []*configpb.ConnectionParameters{
								&configpb.ConnectionParameters{
									Username: "test_user_name",
									Host:     "test_host",
									Port:     1234,
									Secret: &configpb.SecretRef{
										SecretName: "test_secret_name",
									},
								},
							},
							GuestConfigurations: &configpb.SQLServerConfiguration_CredentialConfiguration_RemoteWin{
								RemoteWin: &configpb.SQLServerConfiguration_CredentialConfiguration_GuestCredentialsRemoteWin{
									ConnectionParameters: &configpb.ConnectionParameters{
										Host:     "test_server_name",
										Username: "test_guest_user_name",
										Secret: &configpb.SecretRef{
											SecretName: "test_guest_secret_name",
										},
									},
								},
							},
							VmProperties: &configpb.CloudProperties{
								InstanceName: "test_instance_name",
								InstanceId:   "test_instance_id",
							},
						},
						&configpb.SQLServerConfiguration_CredentialConfiguration{
							ConnectionParameters: []*configpb.ConnectionParameters{
								&configpb.ConnectionParameters{
									Username: "test_user_name",
									Host:     "test_host",
									Port:     1234,
									Secret: &configpb.SecretRef{
										SecretName: "test_secret_name",
									},
								},
							},
							GuestConfigurations: &configpb.SQLServerConfiguration_CredentialConfiguration_RemoteLinux{
								RemoteLinux: &configpb.SQLServerConfiguration_CredentialConfiguration_GuestCredentialsRemoteLinux{
									ConnectionParameters: &configpb.ConnectionParameters{
										Host:     "test_server_name",
										Username: "test_guest_user_name",
										Port:     1234,
									},
									LinuxSshPrivateKeyPath: "test_linux_ssh_private_key_path",
								},
							},
							VmProperties: &configpb.CloudProperties{
								InstanceName: "test_instance_name",
								InstanceId:   "test_instance_id",
							},
						},
					},
					CollectionTimeout: durationpb.New(time.Duration(10) * time.Second),
					MaxRetries:        3,
					RetryFrequency:    durationpb.New(time.Duration(3600) * time.Second),
					RemoteCollection:  true,
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := &migration{}
			m.Migrate(tc.backCfg, tc.config)
			if diff := cmp.Diff(tc.want, tc.config, protocmp.Transform()); diff != "" {
				t.Errorf("Migrate(%v, %v) returned an unexpected diff (-want +got): %v", tc.backCfg, tc.config, diff)
			}
		})
	}
}

func TestLoadConfiguration(t *testing.T) {
	tests := []struct {
		name             string
		content          []byte
		want             *configpb.Configuration
		wantReadFileErr  bool
		wantUnmarshalErr bool
	}{
		{
			name: "success",
			content: []byte(`{
				"log_level": "DEBUG",
				"log_to_cloud": true
			}`),
			want: &configpb.Configuration{
				LogLevel:   configpb.Configuration_DEBUG,
				LogToCloud: proto.Bool(true),
			},
		},
		{
			name:            "read_file_error",
			wantReadFileErr: true,
			want:            nil,
		},
		{
			name:             "unmarshal_error",
			content:          []byte("invalid_json"),
			wantUnmarshalErr: true,
			want:             nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := &migration{}
			tempFilePath := path.Join(t.TempDir(), "configuration.json")
			if !tc.wantReadFileErr {
				if err := os.WriteFile(tempFilePath, tc.content, 0644); err != nil {
					t.Fatal(err)
				}
			}
			got, err := m.LoadConfiguration(tempFilePath)
			if tc.wantReadFileErr {
				if err == nil {
					t.Fatalf("LoadConfiguration() returned nil error, want ReadFile error")
				}
				return
			}
			if tc.wantUnmarshalErr {
				if err == nil {
					t.Fatalf("LoadConfiguration() returned nil error, want Unmarshal error")
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadConfiguration() returned an unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("LoadConfiguration()) returned an unexpected diff (-want +got): %v", diff)
			}
		})
	}
}

func TestLoadConfigBackup(t *testing.T) {
	tests := []struct {
		name             string
		content          []byte
		want             *sqlserverpb.Configuration
		wantReadFileErr  bool
		wantUnmarshalErr bool
	}{
		{
			name: "success",
			content: []byte(`{
				"log_level": "DEBUG",
				"log_to_cloud": true,
				"disable_log_usage": false,
				"collection_configuration": {
					"collect_guest_os_metrics": true,
					"collect_sql_metrics": true,
					"sql_metrics_collection_interval_in_seconds": 1234,
					"guest_os_metrics_collection_interval_in_seconds": 5678
				},
				"credential_configuration": [
					{
						"instance_name": "test_instance_name",
						"instance_id": "test_instance_id",
						"sql_configurations": [
							{
								"host": "test_host",
								"user_name": "test_user_name",
								"secret_name": "test_secret_name",
								"port_number": 1234
							}
						],
						"local_collection": true
					}
				]
			}`),
			want: &sqlserverpb.Configuration{
				LogLevel:        "DEBUG",
				LogToCloud:      true,
				DisableLogUsage: false,
				CollectionConfiguration: &sqlserverpb.CollectionConfiguration{
					CollectGuestOsMetrics:                     true,
					CollectSqlMetrics:                         true,
					SqlMetricsCollectionIntervalInSeconds:     1234,
					GuestOsMetricsCollectionIntervalInSeconds: 5678,
				},
				CredentialConfiguration: []*sqlserverpb.CredentialConfiguration{
					&sqlserverpb.CredentialConfiguration{
						InstanceName: "test_instance_name",
						InstanceId:   "test_instance_id",
						SqlConfigurations: []*sqlserverpb.CredentialConfiguration_SqlCredentials{
							&sqlserverpb.CredentialConfiguration_SqlCredentials{
								Host:       "test_host",
								UserName:   "test_user_name",
								SecretName: "test_secret_name",
								PortNumber: 1234,
							},
						},
						GuestConfigurations: &sqlserverpb.CredentialConfiguration_LocalCollection{
							LocalCollection: true,
						},
					},
				},
			},
		},
		{
			name:            "read_file_error",
			wantReadFileErr: true,
			want:            nil,
		},
		{
			name:             "unmarshal_error",
			content:          []byte("invalid_json"),
			wantUnmarshalErr: true,
			want:             nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := &migration{}
			tempFilePath := path.Join(t.TempDir(), "configuration.json")
			if !tc.wantReadFileErr {
				if err := os.WriteFile(tempFilePath, tc.content, 0644); err != nil {
					t.Fatal(err)
				}
			}
			got, err := m.LoadConfigBackup(tempFilePath)
			if tc.wantReadFileErr {
				if err == nil {
					t.Fatalf("LoadConfigBackup() returned nil error, want ReadFile error")
				}
				return
			}
			if tc.wantUnmarshalErr {
				if err == nil {
					t.Fatalf("LoadConfigBackup() returned nil error, want Unmarshal error")
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadConfigBackup() returned an unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("LoadConfigBackup()) returned an unexpected diff (-want +got): %v", diff)
			}
		})
	}
}

func TestLogLevel(t *testing.T) {
	tests := []struct {
		name  string
		level string
		want  configpb.Configuration_LogLevel
	}{
		{
			name:  "test_debug_level",
			level: "DEBUG",
			want:  configpb.Configuration_DEBUG,
		},
		{
			name:  "test_warning_level",
			level: "WARNING",
			want:  configpb.Configuration_WARNING,
		},
		{
			name:  "test_error_level",
			level: "ERROR",
			want:  configpb.Configuration_ERROR,
		},
		{
			name:  "test_info_level",
			level: "INFO",
			want:  configpb.Configuration_INFO,
		},
		{
			name:  "test_default_level",
			level: "DEFAULT",
			want:  configpb.Configuration_INFO,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := &migration{}
			got := m.logLevel(tc.level)
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("logLevel(%q) returned an unexpected diff (-want +got): %v", tc.level, diff)
			}
		})
	}
}

func TestCollectionConfiguration(t *testing.T) {
	tests := []struct {
		name   string
		config *sqlserverpb.CollectionConfiguration
		want   *configpb.SQLServerConfiguration_CollectionConfiguration
	}{
		{
			name: "test_collection_configuration",
			config: &sqlserverpb.CollectionConfiguration{
				CollectGuestOsMetrics:                     true,
				CollectSqlMetrics:                         true,
				SqlMetricsCollectionIntervalInSeconds:     1234,
				GuestOsMetricsCollectionIntervalInSeconds: 5678,
			},
			want: &configpb.SQLServerConfiguration_CollectionConfiguration{
				CollectGuestOsMetrics: true,
				CollectSqlMetrics:     true,
				CollectionFrequency:   durationpb.New(time.Duration(1234) * time.Second),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := &migration{}
			got := m.collectionConfiguration(tc.config)
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("collectionConfiguration(%v) returned an unexpected diff (-want +got): %v", tc.config, diff)
			}
		})
	}
}
func TestSql(t *testing.T) {
	tests := []struct {
		name   string
		config *sqlserverpb.CredentialConfiguration_SqlCredentials
		want   *configpb.ConnectionParameters
	}{
		{
			name: "test_sql",
			config: &sqlserverpb.CredentialConfiguration_SqlCredentials{
				UserName:   "test_user_name",
				Host:       "test_host",
				PortNumber: 1234,
				SecretName: "test_secret_name",
			},
			want: &configpb.ConnectionParameters{
				Username: "test_user_name",
				Host:     "test_host",
				Port:     1234,
				Secret: &configpb.SecretRef{
					SecretName: "test_secret_name",
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := &migration{}
			got := m.sql(tc.config)
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("sql(%v) returned an unexpected diff (-want +got): %v", tc.config, diff)
			}
		})
	}
}

func TestGuestRemoteWin(t *testing.T) {
	tests := []struct {
		name   string
		config *sqlserverpb.CredentialConfiguration_GuestCredentialsRemoteWin
		want   *configpb.SQLServerConfiguration_CredentialConfiguration_GuestCredentialsRemoteWin
	}{
		{
			name: "test_guest_remote_win",
			config: &sqlserverpb.CredentialConfiguration_GuestCredentialsRemoteWin{
				ServerName:      "test_server_name",
				GuestUserName:   "test_guest_user_name",
				GuestSecretName: "test_guest_secret_name",
			},
			want: &configpb.SQLServerConfiguration_CredentialConfiguration_GuestCredentialsRemoteWin{
				ConnectionParameters: &configpb.ConnectionParameters{
					Host:     "test_server_name",
					Username: "test_guest_user_name",
					Secret: &configpb.SecretRef{
						SecretName: "test_guest_secret_name",
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := &migration{}
			got := m.guestRemoteWin(tc.config)
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("guestRemoteWin(%v) returned an unexpected diff (-want +got): %v", tc.config, diff)
			}
		})
	}
}

func TestGuestRemoteLinux(t *testing.T) {
	tests := []struct {
		name   string
		config *sqlserverpb.CredentialConfiguration_GuestCredentialsRemoteLinux
		want   *configpb.SQLServerConfiguration_CredentialConfiguration_GuestCredentialsRemoteLinux
	}{
		{
			name: "test_guest_remote_linux",
			config: &sqlserverpb.CredentialConfiguration_GuestCredentialsRemoteLinux{
				ServerName:             "test_server_name",
				GuestUserName:          "test_guest_user_name",
				GuestPortNumber:        1234,
				LinuxSshPrivateKeyPath: "test_linux_ssh_private_key_path",
			},
			want: &configpb.SQLServerConfiguration_CredentialConfiguration_GuestCredentialsRemoteLinux{
				ConnectionParameters: &configpb.ConnectionParameters{
					Host:     "test_server_name",
					Username: "test_guest_user_name",
					Port:     1234,
				},
				LinuxSshPrivateKeyPath: "test_linux_ssh_private_key_path",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := &migration{}
			got := m.guestRemoteLinux(tc.config)
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("guestRemoteLinux(%v) returned an unexpected diff (-want +got): %v", tc.config, diff)
			}
		})
	}
}
