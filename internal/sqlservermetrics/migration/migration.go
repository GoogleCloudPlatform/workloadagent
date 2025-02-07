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

// Package migration provides functions to migrate old sql-server-agent configurations to the new configuration format.
package migration

import (
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"time"

	durationpb "google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	sqlserverpb "github.com/GoogleCloudPlatform/workloadagent/protos/sqlserveragent"
)

const (
	// ConfigPathLinux is the path to the config file on Linux.
	configPathLinux = "/etc/google-cloud-workload-agent/configuration.json"
	// ConfigPathWindows is the path to the config file on Windows.
	configPathWindows = "C:\\Program Files\\Google\\google-cloud-workload-agent\\conf\\configuration.json"
	// ConfigBackupPathLinux is the path to the config backup file on Linux.
	configBackupPathLinux = "/etc/google-cloud-workload-agent/cfg_sqlserver_backup.json"
	// ConfigBackupPathWindows is the path to the config backup file on Windows.
	configBackupPathWindows = "C:\\Program Files\\Google\\google-cloud-workload-agent\\conf\\cfg_sqlserver_backup.json"
)

type migration struct {
}

// Migrate is the entry point for the migration process.
func Migrate() error {
	backupPath := configBackupPathLinux
	configPath := configPathLinux
	if runtime.GOOS == "windows" {
		backupPath = configBackupPathWindows
		configPath = configPathWindows
	}
	return migrateSQLServerConfigurations(backupPath, configPath)
}

// MigrateSQLServerConfigurations migrates the old sql-server-agent configurations to the new configuration format.
func migrateSQLServerConfigurations(backupPath, configPath string) error {
	// return nil if config backup does not exist
	if _, err := os.Stat(backupPath); err != nil {
		return nil
	}
	m := &migration{}
	backCfg, err := m.LoadConfigBackup(backupPath)
	if err != nil {
		return err
	}
	config, err := m.LoadConfiguration(configPath)
	if err != nil {
		return err
	}
	// Migrate the old sql-server-agent configurations to the new configuration format.
	m.Migrate(backCfg, config)
	// Save to the config file.
	configBytes, err := protojson.MarshalOptions{
		Multiline: true,
		Indent:    "  ",
	}.Marshal(config)

	if err != nil {
		return err
	}
	if err := ioutil.WriteFile(configPath, configBytes, 0644); err != nil {
		return err
	}
	// Delete the backup file.
	return os.Remove(backupPath)
}

func (m *migration) Migrate(backCfg *sqlserverpb.Configuration, config *configpb.Configuration) {
	config.LogLevel = m.logLevel(backCfg.GetLogLevel())
	config.LogToCloud = proto.Bool(backCfg.GetLogToCloud())
	config.AgentProperties = &configpb.AgentProperties{
		LogUsageMetrics: !backCfg.GetDisableLogUsage(),
	}
	config.SqlserverConfiguration = &configpb.SQLServerConfiguration{
		Enabled:                 proto.Bool(true),
		CollectionConfiguration: m.collectionConfiguration(backCfg.GetCollectionConfiguration()),
		CollectionTimeout:       durationpb.New(time.Duration(backCfg.GetCollectionTimeoutSeconds()) * time.Second),
		MaxRetries:              int32(backCfg.GetMaxRetries()),
		RetryFrequency:          durationpb.New(time.Duration(backCfg.GetRetryIntervalInSeconds()) * time.Second),
		RemoteCollection:        backCfg.GetRemoteCollection(),
	}
	credentialConfigrations := []*configpb.SQLServerConfiguration_CredentialConfiguration{}
	for _, credCfg := range backCfg.GetCredentialConfiguration() {
		c := &configpb.SQLServerConfiguration_CredentialConfiguration{}

		connectionParameters := []*configpb.ConnectionParameters{}
		for _, sqlCfg := range credCfg.GetSqlConfigurations() {
			connectionParameters = append(connectionParameters, m.sql(sqlCfg))
		}
		c.ConnectionParameters = connectionParameters

		switch credCfg.GetGuestConfigurations().(type) {
		case *sqlserverpb.CredentialConfiguration_LocalCollection:
			c.GuestConfigurations = &configpb.SQLServerConfiguration_CredentialConfiguration_LocalCollection{
				LocalCollection: credCfg.GetLocalCollection(),
			}
		case *sqlserverpb.CredentialConfiguration_RemoteWin:
			c.GuestConfigurations = &configpb.SQLServerConfiguration_CredentialConfiguration_RemoteWin{
				RemoteWin: m.guestRemoteWin(credCfg.GetRemoteWin()),
			}
			c.VmProperties = &configpb.CloudProperties{
				InstanceName: credCfg.GetInstanceName(),
				InstanceId:   credCfg.GetInstanceId(),
			}
		case *sqlserverpb.CredentialConfiguration_RemoteLinux:
			c.GuestConfigurations = &configpb.SQLServerConfiguration_CredentialConfiguration_RemoteLinux{
				RemoteLinux: m.guestRemoteLinux(credCfg.GetRemoteLinux()),
			}
			c.VmProperties = &configpb.CloudProperties{
				InstanceName: credCfg.GetInstanceName(),
				InstanceId:   credCfg.GetInstanceId(),
			}
		}
		credentialConfigrations = append(credentialConfigrations, c)
	}
	config.SqlserverConfiguration.CredentialConfigurations = credentialConfigrations
}

func (m *migration) LoadConfiguration(path string) (*configpb.Configuration, error) {
	// Read config file from file system.
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to load the configuration file. filepath: %v, error: %v", path, err)
	}
	cfg := &configpb.Configuration{}
	if err := protojson.Unmarshal(b, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
func (m *migration) LoadConfigBackup(path string) (*sqlserverpb.Configuration, error) {
	// Read config file from file system.
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to load the configuration backup file. filepath: %v, error: %v", path, err)
	}
	cfg := &sqlserverpb.Configuration{}
	if err := protojson.Unmarshal(b, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (m *migration) logLevel(level string) configpb.Configuration_LogLevel {
	switch level {
	case "DEBUG":
		return configpb.Configuration_DEBUG
	case "WARNING":
		return configpb.Configuration_WARNING
	case "ERROR":
		return configpb.Configuration_ERROR
	default:
		return configpb.Configuration_INFO
	}
}

func (m *migration) collectionConfiguration(config *sqlserverpb.CollectionConfiguration) *configpb.SQLServerConfiguration_CollectionConfiguration {
	return &configpb.SQLServerConfiguration_CollectionConfiguration{
		CollectGuestOsMetrics: config.GetCollectGuestOsMetrics(),
		CollectSqlMetrics:     config.GetCollectSqlMetrics(),
		CollectionFrequency:   durationpb.New(time.Duration(config.GetSqlMetricsCollectionIntervalInSeconds()) * time.Second),
	}
}

func (m *migration) sql(config *sqlserverpb.CredentialConfiguration_SqlCredentials) *configpb.ConnectionParameters {
	return &configpb.ConnectionParameters{
		Username: config.GetUserName(),
		Host:     config.GetHost(),
		Port:     config.GetPortNumber(),
		Secret: &configpb.SecretRef{
			SecretName: config.GetSecretName(),
		},
	}
}

func (m *migration) guestRemoteWin(config *sqlserverpb.CredentialConfiguration_GuestCredentialsRemoteWin) *configpb.SQLServerConfiguration_CredentialConfiguration_GuestCredentialsRemoteWin {
	return &configpb.SQLServerConfiguration_CredentialConfiguration_GuestCredentialsRemoteWin{
		ConnectionParameters: &configpb.ConnectionParameters{
			Username: config.GetGuestUserName(),
			Host:     config.GetServerName(),
			Secret: &configpb.SecretRef{
				SecretName: config.GetGuestSecretName(),
			},
		},
	}
}

func (m *migration) guestRemoteLinux(config *sqlserverpb.CredentialConfiguration_GuestCredentialsRemoteLinux) *configpb.SQLServerConfiguration_CredentialConfiguration_GuestCredentialsRemoteLinux {
	return &configpb.SQLServerConfiguration_CredentialConfiguration_GuestCredentialsRemoteLinux{
		ConnectionParameters: &configpb.ConnectionParameters{
			Username: config.GetGuestUserName(),
			Host:     config.GetServerName(),
			Port:     config.GetGuestPortNumber(),
		},
		LinuxSshPrivateKeyPath: config.GetLinuxSshPrivateKeyPath(),
	}
}
