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

// Package oracle implements the oracle subcommand.
package oracle

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"

	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

// Config holds the configuration for the Oracle subcommand.
type Config struct {
	OracleConfiguration *cpb.OracleConfiguration
	CloudProperties     *cpb.CloudProperties
	Path                string
}

// NewConfig creates a new Config.
func NewConfig(cfg *cpb.Configuration, cloudProps *cpb.CloudProperties, path string) *Config {
	return &Config{
		OracleConfiguration: cfg.GetOracleConfiguration(),
		CloudProperties:     cloudProps,
		Path:                path,
	}
}

// NewWAConfig creates a new Configuration.
func NewWAConfig(cloudProps *cpb.CloudProperties) (*cpb.Configuration, error) {
	config, err := configuration.Load(configPath(), os.ReadFile, cloudProps)
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration: %w", err)
	}
	return config, nil
}

// NewCommand creates a new 'oracle' command.
func NewCommand(cloudProps *cpb.CloudProperties) *cobra.Command {
	var enabled bool
	var ocfg *Config
	var waConfig *cpb.Configuration

	oracleCmd := &cobra.Command{
		Use:   "oracle",
		Short: "Configure Oracle settings",
		Long: `Configure Oracle settings for the Google Cloud Agent for Compute Workloads.

This command allows you to enable and configure various features
for monitoring Oracle databases, including discovery and metrics collection.`,
		Run: func(cmd *cobra.Command, args []string) {
			if ocfg == nil || ocfg.OracleConfiguration == nil {
				fmt.Println("Oracle configuration is not initialized.")
				return
			}
			ocfg.OracleConfiguration.Enabled = &enabled
			// TODO: We'll add/remove this logic once we have a clearer design.
			if enabled {
				fmt.Println("Oracle Configuration is Enabled.  Performing Oracle-specific tasks...")
			} else {
				fmt.Println("Oracle Configuration is Disabled.")
			}
		},
		// TODO: Remove PersistentPreRunE and PersistentPostRunE.
		// PersistentPreRunE is called before each cli command is run.
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			var err error
			waConfig, err = NewWAConfig(cloudProps)
			if err != nil {
				return err
			}
			if waConfig == nil {
				return fmt.Errorf("unable to load configuration from a json file")
			}

			path := configPath()
			ocfg = NewConfig(waConfig, cloudProps, path)
			return nil
		},
		// PersistentPostRunE is called after each cli command is run.
		PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			return ocfg.writeFile(ctx, waConfig)
		},
	}

	oracleCmd.Flags().BoolVarP(&enabled, "enabled", "e", false, "Enable Oracle configuration")

	oracleCmd.AddCommand(DiscoveryCommand(&ocfg))
	oracleCmd.AddCommand(MetricsCommand(&ocfg))

	return oracleCmd
}

func (c *Config) writeFile(ctx context.Context, wca *cpb.Configuration) error {
	wca.OracleConfiguration = c.OracleConfiguration
	file, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(wca)
	if err != nil {
		return fmt.Errorf("unable to marshal configuration.json: %w", err)
	}

	var fileBuf bytes.Buffer
	json.Indent(&fileBuf, file, "", "  ")
	err = os.WriteFile(c.Path, fileBuf.Bytes(), 0644)
	if err != nil {
		return fmt.Errorf("unable to write configuration.json: %w", err)
	}
	log.CtxLogger(ctx).Info("Successfully Updated configuration.json")
	return nil
}

// configPath determines the configuration path based on the OS.
func configPath() string {
	if runtime.GOOS == "windows" {
		return configuration.WindowsConfigPath
	}
	return configuration.LinuxConfigPath
}
