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
	_ "embed"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sort"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"go.uber.org/zap/zapcore"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"

	dpb "google.golang.org/protobuf/types/known/durationpb"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

//go:embed defaultconfigs/oraclemetrics/default_queries.json
var defaultOracleQueriesContent []byte

// ReadConfigFile abstracts os.ReadFile function for testability.
type ReadConfigFile func(string) ([]byte, error)

// WriteConfigFile abstracts os.WriteFile function for testability.
type WriteConfigFile func(string, []byte, os.FileMode) error

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
)

const (
	// AgentName is a short-hand name of the agent.
	AgentName = `workloadagent`

	

	// AgentVersion is the version of the agent.
	AgentVersion = `1.1`
	

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
	// DefaultRedisPort is the default port for Redis.
	DefaultRedisPort = 6379
)

// ConfigFromFile returns the configuration from the given file path.
func ConfigFromFile(path string, read ReadConfigFile) (*cpb.Configuration, error) {
	emptyConfig := &cpb.Configuration{}
	content, err := read(path)
	if err != nil {
		log.Logger.Warnw("Configuration file cannot be read; Using defaults", "error", err)
		return emptyConfig, nil
	}
	if len(content) == 0 {
		log.Logger.Warnw("Configuration file is empty; Using defaults", "path", path)
		return emptyConfig, nil
	}

	cfgFromFile := &cpb.Configuration{}
	err = protojson.Unmarshal(content, cfgFromFile)
	if err != nil {
		return nil, fmt.Errorf("parsing JSON content from %s configuration file: %w", path, err)
	}

	return cfgFromFile, nil
}

// Load loads the configuration from a JSON file and applies defaults for missing fields.
func Load(path string, read ReadConfigFile, cloudProps *cpb.CloudProperties) (*cpb.Configuration, error) {
	if path == "" {
		path = LinuxConfigPath
		if runtime.GOOS == "windows" {
			path = WindowsConfigPath
		}
	}

	defaultCfg, err := defaultConfig(cloudProps)
	if err != nil {
		return nil, fmt.Errorf("generating default configuration: %w", err)
	}

	userCfg, err := ConfigFromFile(path, read)
	if err != nil {
		return nil, fmt.Errorf("gathering configuration from file: %w", err)
	}

	if err := validateOracleConfiguration(userCfg); err != nil {
		return nil, fmt.Errorf("validating Oracle configuration: %w", err)
	}

	if err := validateSQLServerConfiguration(userCfg); err != nil {
		return nil, fmt.Errorf("validating SQL Server configuration: %w", err)
	}

	defaultOracleQueries := defaultCfg.GetOracleConfiguration().GetOracleMetrics().GetQueries()
	userOracleQueries := userCfg.GetOracleConfiguration().GetOracleMetrics().GetQueries()
	mergedOracleQueries := mergeQueries(defaultOracleQueries, userOracleQueries)

	proto.Merge(defaultCfg, userCfg)

	defaultCfg.GetOracleConfiguration().GetOracleMetrics().Queries = mergedOracleQueries
	return defaultCfg, nil
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
				CollectionFrequency: dpb.New(time.Duration(DefaultSQLServerCollectionFrequency)),
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
