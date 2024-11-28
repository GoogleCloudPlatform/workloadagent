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
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"go.uber.org/zap/zapcore"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/log"

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
)

const (
	// AgentName is a short-hand name of the agent.
	AgentName = `workloadagent`

	

	// AgentVersion is the version of the agent.
	AgentVersion = `1.0`
	

	// LinuxConfigPath is the default path to agent configuration file on linux.
	LinuxConfigPath = `/etc/google-cloud-workload-agent/configuration.json`
	// WindowsConfigPath is the default path to agent configuration file on linux.
	WindowsConfigPath = `C:\Program Files\Google\google-cloud-workload-agent\conf\configuration.json`
)

// Load loads the configuration from a JSON file and applies defaults for missing fields.
func Load(path string, read ReadConfigFile, cloudProps *cpb.CloudProperties) (*cpb.Configuration, error) {
	if path == "" {
		path = LinuxConfigPath
		if runtime.GOOS == "windows" {
			path = WindowsConfigPath
		}
	}

	cfg, err := defaultConfig(cloudProps)
	if err != nil {
		return nil, fmt.Errorf("generating default configuration: %w", err)
	}

	content, err := read(path)
	if err != nil {
		log.Logger.Warnw("Configuration file cannot be read; Using defaults", "error", err)
		return cfg, nil
	}
	if len(content) == 0 {
		log.Logger.Warnw("Configuration file is empty; Using defaults", "path", path)
		return cfg, nil
	}

	cfgFromFile := &cpb.Configuration{}
	err = protojson.Unmarshal(content, cfgFromFile)
	if err != nil {
		return nil, fmt.Errorf("parsing JSON content from %s configuration file: %w", path, err)
	}

	if err := validateOracleConfiguration(cfgFromFile); err != nil {
		return nil, fmt.Errorf("validating configuration: %w", err)
	}

	proto.Merge(cfg, cfgFromFile)
	return cfg, nil
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

// defaultConfig returns the default configuration.
func defaultConfig(cloudProps *cpb.CloudProperties) (*cpb.Configuration, error) {
	oracleQueries, err := defaultOracleQueries()
	if err != nil {
		usagemetrics.Error(usagemetrics.MalformedDefaultOracleQueriesFile)
		return nil, fmt.Errorf("parsing JSON content containing Oracle queries from the embedded default_queries.json file: %w", err)
	}
	return &cpb.Configuration{
		AgentProperties: &cpb.AgentProperties{Name: AgentName, Version: AgentVersion},
		LogToCloud:      proto.Bool(true),
		LogLevel:        cpb.Configuration_INFO,
		CloudProperties: cloudProps,
		OracleConfiguration: &cpb.OracleConfiguration{
			Enabled: proto.Bool(false),
			OracleDiscovery: &cpb.OracleDiscovery{
				UpdateFrequency: dpb.New(time.Duration(3 * 60 * 60 * time.Second)),
			},
			OracleMetrics: &cpb.OracleMetrics{
				Enabled:             proto.Bool(false),
				CollectionFrequency: dpb.New(time.Duration(time.Minute)),
				QueryTimeout:        dpb.New(time.Duration(10 * time.Second)),
				MaxExecutionThreads: 10,
				Queries:             oracleQueries,
			},
		},
	}, nil
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
