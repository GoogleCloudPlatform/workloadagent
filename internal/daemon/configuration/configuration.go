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

// Package configuration provides configuration reading capabilities.
package configuration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"go.uber.org/zap/zapcore"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/parametermanager"

	dpb "google.golang.org/protobuf/types/known/durationpb"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"

	_ "embed"
)

//go:embed defaultconfigs/oraclemetrics/default_queries.json
var defaultOracleQueriesContent []byte

//go:embed defaultconfigs/configuration.json
var defaultConfigurationJSONContent []byte

// ReadConfigFile abstracts os.ReadFile function for testability.
type ReadConfigFile func(string) ([]byte, error)

// WriteConfigFile abstracts os.WriteFile function for testability.
type WriteConfigFile func(string, []byte, os.FileMode) error

// StatFile abstracts os.Stat function for testability.
type StatFile func(string) (os.FileInfo, error)

// MkdirAll abstracts os.MkdirAll function for testability.
type MkdirAll func(string, os.FileMode) error

var (
	// AgentBuildChange is the change number that the agent was built at
	// this will be replaced using "-X github.com/GoogleCloudPlatform/workloadagent/internal/configuration.AgentBuildChange=$CLNUMBER" by the build process
	AgentBuildChange = `0`

	errMissingConnectionParameters = errors.New("connection_parameters are required")
	errMissingSecret               = errors.New("secret is required")
	errMissingUsername             = errors.New("username is required")
	errMissingServiceName          = errors.New("service_name is required")
	errMissingProjectID            = errors.New("project_id is required")
	errMissingSecretName           = errors.New("secret_name is required")

	sqlServerConfigurationErrors = map[string]error{
		"errMissingCollectionConfiguration":  errors.New("collection_configuration is required"),
		"errMissingCredentialConfigurations": errors.New("credential_configurations are required"),
		"errInvalidCollectionFrequency":      errors.New("collection_frequency is invalid"),
		"errInvalidCollectionTimeout":        errors.New("collection_timeout is invalid"),
		"errInvalidRetryFrequency":           errors.New("retry_frequency is invalid"),
		"errInvalidMaxRetries":               errors.New("max_retries is invalid"),
	}
	fetchParameter = parametermanager.FetchParameter
)

const (
	// AgentName is a short-hand name of the agent.
	AgentName = `workloadagent`

	

	// AgentVersion is the version of the agent.
	AgentVersion = `1.3`
	

	// LinuxConfigPath is the default path to agent configuration file on linux.
	LinuxConfigPath = `/etc/google-cloud-workload-agent/configuration.json`
	// WindowsConfigPath is the default path to agent configuration file on linux.
	WindowsConfigPath = `C:\Program Files\Google\google-cloud-workload-agent\conf\configuration.json`
	// DefaultOracleDiscoveryFrequency is the default frequency for Oracle discovery.
	DefaultOracleDiscoveryFrequency = time.Hour
	// DefaultOracleMetricsFrequency is the default frequency for Oracle metrics collection.
	DefaultOracleMetricsFrequency = time.Minute
	// DefaultOracleMetricsMaxThreads is the default maximum number of threads for Oracle metrics collection.
	DefaultOracleMetricsMaxThreads = 10
	// DefaultOracleMetricsQueryTimeout is the default timeout for Oracle metrics queries.
	DefaultOracleMetricsQueryTimeout = 10 * time.Second
	// DefaultSQLServerCollectionTimeout is the default timeout for SQL Server Configuration.
	DefaultSQLServerCollectionTimeout = 10 * time.Second
	// DefaultSQLServerMaxRetries is the default maximum number of retries for SQL Server Configuration.
	DefaultSQLServerMaxRetries = 3
	// DefaultSQLServerRetryFrequency is the default frequency for retrying SQL Server Configuration.
	DefaultSQLServerRetryFrequency = time.Hour
	// DefaultSQLServerCollectionFrequency is the default frequency for SQL Server collection.
	DefaultSQLServerCollectionFrequency = time.Hour
	// DefaultSQLServerDBCenterMetricsCollectionFrequency is the default frequency for SQL Server DB Center metrics collection
	DefaultSQLServerDBCenterMetricsCollectionFrequency = 1 * time.Hour
	// DefaultRedisPort is the default port for Redis.
	DefaultRedisPort = 6379
)

// ConfigFromFile returns the configuration from the given file path.
func ConfigFromFile(path string, read ReadConfigFile) (*cpb.Configuration, error) {
	content, err := read(path)
	if err != nil {
		return nil, fmt.Errorf("configuration file cannot be read: %v", err)
	}
	if len(content) == 0 {
		return nil, fmt.Errorf("configuration file is empty")
	}
	cfgFromFile := &cpb.Configuration{}
	unmarshalOptions := protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}
	if err = unmarshalOptions.Unmarshal(content, cfgFromFile); err != nil {
		return nil, fmt.Errorf("parsing JSON content from %s configuration file: %w", path, err)
	}
	return cfgFromFile, nil
}

// ConfigPath returns the default configuration file path based on the operating system.
func ConfigPath() string {
	if runtime.GOOS == "windows" {
		return WindowsConfigPath
	}
	return LinuxConfigPath
}

// Load loads the configuration from a JSON file and applies defaults for missing fields.
func Load(path string, read ReadConfigFile, cloudProps *cpb.CloudProperties, pmClient *parametermanager.Client) (*cpb.Configuration, string, error) {
	if path == "" {
		path = ConfigPath()
	}

	defaultCfg, err := defaultConfig(cloudProps)
	if err != nil {
		return nil, "", fmt.Errorf("generating default configuration: %w", err)
	}

	userCfg, err := ConfigFromFile(path, read)
	if err != nil {
		return nil, "", fmt.Errorf("gathering configuration from file: %w", err)
	}

	userCfg, pmVersion := mergePMConfig(context.Background(), pmClient, userCfg)

	if err := validateOracleConfiguration(userCfg); err != nil {
		return nil, "", fmt.Errorf("validating Oracle configuration: %w", err)
	}

	if err := validateSQLServerConfiguration(userCfg); err != nil {
		return nil, "", fmt.Errorf("validating SQL Server configuration: %w", err)
	}

	defaultOracleQueries := defaultCfg.GetOracleConfiguration().GetOracleMetrics().GetQueries()
	userOracleQueries := userCfg.GetOracleConfiguration().GetOracleMetrics().GetQueries()
	mergedOracleQueries := mergeQueries(defaultOracleQueries, userOracleQueries)

	proto.Merge(defaultCfg, userCfg)

	defaultCfg.GetOracleConfiguration().GetOracleMetrics().Queries = mergedOracleQueries
	return defaultCfg, pmVersion, nil
}

func validateOracleConfiguration(config *cpb.Configuration) error {
	if !config.GetOracleConfiguration().GetEnabled() {
		return nil
	}
	if config.GetOracleConfiguration().GetOracleMetrics().GetEnabled() {
		if config.GetOracleConfiguration().GetOracleMetrics().GetConnectionParameters() == nil {
			return errMissingConnectionParameters
		}
		for _, cp := range config.GetOracleConfiguration().GetOracleMetrics().GetConnectionParameters() {
			if cp.GetUsername() == "" {
				return errMissingUsername
			}
			if cp.GetServiceName() == "" {
				return errMissingServiceName
			}
			if cp.GetSecret() == nil {
				return errMissingSecret
			}
			if cp.GetSecret().GetProjectId() == "" {
				return errMissingProjectID
			}
			if cp.GetSecret().GetSecretName() == "" {
				return errMissingSecretName
			}
		}
	}
	if config.GetOracleConfiguration().GetOracleHandlers() != nil {
		if config.GetOracleConfiguration().GetOracleHandlers().GetConnectionParameters() == nil {
			return errMissingConnectionParameters
		}
		if config.GetOracleConfiguration().GetOracleHandlers().GetConnectionParameters().GetUsername() == "" {
			return errMissingUsername
		}
		if config.GetOracleConfiguration().GetOracleHandlers().GetConnectionParameters().GetServiceName() == "" {
			return errMissingServiceName
		}
		if config.GetOracleConfiguration().GetOracleHandlers().GetConnectionParameters().GetSecret() == nil {
			return errMissingSecret
		}
		if config.GetOracleConfiguration().GetOracleHandlers().GetConnectionParameters().GetSecret().GetProjectId() == "" {
			return errMissingProjectID
		}
		if config.GetOracleConfiguration().GetOracleHandlers().GetConnectionParameters().GetSecret().GetSecretName() == "" {
			return errMissingSecretName
		}
	}
	return nil
}

func validateSQLServerConfiguration(config *cpb.Configuration) error {
	if !config.GetSqlserverConfiguration().GetEnabled() {
		return nil
	}

	if config.GetSqlserverConfiguration().GetCollectionConfiguration() == nil {
		return sqlServerConfigurationErrors["errMissingCollectionConfiguration"]
	}

	if config.GetSqlserverConfiguration().GetCollectionConfiguration().GetCollectionFrequency() != nil && config.GetSqlserverConfiguration().GetCollectionConfiguration().GetCollectionFrequency().GetSeconds() <= 0 {
		return sqlServerConfigurationErrors["errInvalidCollectionFrequency"]
	}

	if config.GetSqlserverConfiguration().GetCredentialConfigurations() == nil {
		return sqlServerConfigurationErrors["errMissingCredentialConfigurations"]
	}
	if config.GetSqlserverConfiguration().GetCollectionTimeout() != nil && config.GetSqlserverConfiguration().GetCollectionTimeout().GetSeconds() <= 0 {
		return sqlServerConfigurationErrors["errInvalidCollectionTimeout"]
	}
	if config.GetSqlserverConfiguration().GetRetryFrequency() != nil && config.GetSqlserverConfiguration().GetRetryFrequency().GetSeconds() <= 0 {
		return sqlServerConfigurationErrors["errInvalidRetryFrequency"]
	}
	if config.GetSqlserverConfiguration().GetMaxRetries() < 0 {
		return sqlServerConfigurationErrors["errInvalidMaxRetries"]
	}
	return nil
}

// defaultConfig returns the default configuration.
func defaultConfig(cloudProps *cpb.CloudProperties) (*cpb.Configuration, error) {
	oracleQueries, err := defaultOracleQueries()
	if err != nil {
		usagemetrics.Error(usagemetrics.MalformedDefaultOracleQueriesFile)
		return nil, fmt.Errorf("parsing JSON content containing Oracle queries from the embedded default_queries.json file: %w", err)
	}
	sort.Slice(oracleQueries, func(i, j int) bool {
		return oracleQueries[i].GetName() < oracleQueries[j].GetName()
	})
	return &cpb.Configuration{
		AgentProperties:       &cpb.AgentProperties{Name: AgentName, Version: AgentVersion},
		LogToCloud:            proto.Bool(true),
		LogLevel:              cpb.Configuration_INFO,
		CloudProperties:       cloudProps,
		DataWarehouseEndpoint: "https://workloadmanager-datawarehouse.googleapis.com/",
		CommonDiscovery: &cpb.CommonDiscovery{
			Enabled: proto.Bool(true),
		},
		OracleConfiguration: &cpb.OracleConfiguration{
			Enabled: proto.Bool(false),
			OracleDiscovery: &cpb.OracleDiscovery{
				Enabled:         proto.Bool(true),
				UpdateFrequency: dpb.New(time.Duration(DefaultOracleDiscoveryFrequency)),
			},
			OracleMetrics: &cpb.OracleMetrics{
				Enabled:             proto.Bool(false),
				CollectionFrequency: dpb.New(time.Duration(DefaultOracleMetricsFrequency)),
				QueryTimeout:        dpb.New(time.Duration(DefaultOracleMetricsQueryTimeout)),
				MaxExecutionThreads: DefaultOracleMetricsMaxThreads,
				Queries:             oracleQueries,
			},
		},
		SqlserverConfiguration: &cpb.SQLServerConfiguration{
			Enabled: proto.Bool(false),
			CollectionConfiguration: &cpb.SQLServerConfiguration_CollectionConfiguration{
				CollectionFrequency:                dpb.New(time.Duration(DefaultSQLServerCollectionFrequency)),
				DbcenterMetricsCollectionFrequency: dpb.New(time.Duration(DefaultSQLServerDBCenterMetricsCollectionFrequency)),
			},
			CredentialConfigurations: []*cpb.SQLServerConfiguration_CredentialConfiguration{},
			CollectionTimeout:        dpb.New(time.Duration(DefaultSQLServerCollectionTimeout)),
			MaxRetries:               DefaultSQLServerMaxRetries,
			RetryFrequency:           dpb.New(time.Duration(DefaultSQLServerRetryFrequency)),
		},
	}, nil
}

// mergeQueries merges default queries with user-provided queries based on Query.name
// If a query with the same name exists in both defaultQueries and userQueries,
// the userQuery will overwrite the defaultQuery.
func mergeQueries(defaultQueries, userQueries []*cpb.Query) []*cpb.Query {
	queryMap := make(map[string]*cpb.Query)

	for _, q := range defaultQueries {
		queryMap[q.GetName()] = q
	}

	for _, userQuery := range userQueries {
		queryMap[userQuery.GetName()] = userQuery
	}

	mergedQueries := make([]*cpb.Query, 0, len(queryMap))
	for _, q := range queryMap {
		mergedQueries = append(mergedQueries, q)
	}

	return mergedQueries
}

func defaultOracleQueries() ([]*cpb.Query, error) {
	config := &cpb.OracleMetrics{}
	err := protojson.Unmarshal(defaultOracleQueriesContent, config)
	if err != nil {
		return nil, fmt.Errorf("parsing JSON file containing Oracle queries %q: %w", defaultOracleQueriesContent, err)
	}
	return config.GetQueries(), nil
}

// LogLevelToZapcore returns the zapcore equivalent of the configuration log level.
func LogLevelToZapcore(level cpb.Configuration_LogLevel) zapcore.Level {
	switch level {
	case cpb.Configuration_DEBUG:
		return zapcore.DebugLevel
	case cpb.Configuration_INFO:
		return zapcore.InfoLevel
	case cpb.Configuration_WARNING:
		return zapcore.WarnLevel
	case cpb.Configuration_ERROR:
		return zapcore.ErrorLevel
	default:
		log.Logger.Warnw("Unsupported log level, defaulting to INFO", "level", level.String())
		return zapcore.InfoLevel
	}
}

// EnsureConfigExists ensures that the configuration file exists.
// If the file does not exist, it creates the file and its parent directories.
func EnsureConfigExists() error {
	return ensureConfigExists(nil, nil, nil)
}

func ensureConfigExists(stat StatFile, mkdirall MkdirAll, write WriteConfigFile) error {
	path := ConfigPath()

	if stat == nil {
		stat = os.Stat
	}

	if mkdirall == nil {
		mkdirall = os.MkdirAll
	}

	if write == nil {
		write = os.WriteFile
	}

	_, err := stat(path)
	if err == nil {
		return nil
	}

	// We expect to see os.ErrNotExist if the file does not exist.
	// Any other error is unexpected and should be returned.
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to stat configuration file: %w", err)
	}

	dir := filepath.Dir(path)
	if err := mkdirall(dir, 0755); err != nil {
		return fmt.Errorf("failed to create configuration file directory %s: %w", dir, err)
	}

	if err := write(path, defaultConfigurationJSONContent, 0644); err != nil {
		return fmt.Errorf("failed to write default configuration to file %s: %w", path, err)
	}

	log.Logger.Infow("Default configuration file created", "path", path)
	return nil
}

// WriteConfigToFile writes the contents of a configuration struct to a file at the given path.
func WriteConfigToFile(config *cpb.Configuration, path string, write WriteConfigFile) error {
	content, err := protojson.MarshalOptions{
		Multiline:     true,
		UseProtoNames: true,
	}.Marshal(config)
	if err != nil {
		log.Logger.Errorf("Failed to marshal configuration to JSON: %v", err)
		return err
	}
	return write(path, content, 0644)
}

func mergePMConfig(ctx context.Context, pmClient *parametermanager.Client, config *cpb.Configuration) (*cpb.Configuration, string) {
	pmConfig := config.GetParameterManagerConfig()
	if pmConfig == nil {
		return config, ""
	}
	log.Logger.Info("Parameter manager config found, attempting to fetch remote config")
	resource, err := fetchParameter(ctx, pmClient, pmConfig.GetProject(), pmConfig.GetLocation(), pmConfig.GetParameterName(), pmConfig.GetParameterVersion())
	if err != nil {
		log.Logger.Errorw("Failed to fetch configuration from Parameter Manager", "error", err)
		return config, ""
	}
	remoteConfig := &cpb.Configuration{}
	if err := protojson.Unmarshal([]byte(resource.Data), remoteConfig); err != nil {
		log.Logger.Errorw("Failed to unmarshal Parameter Manager payload", "error", err)
		return config, ""
	}
	proto.Merge(config, remoteConfig)
	log.Logger.Info("Configuration successfully merged with Parameter Manager payload")
	return config, resource.Version
}
