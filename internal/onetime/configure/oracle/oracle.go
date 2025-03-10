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
	Ctx                 context.Context
	OracleConfiguration *cpb.OracleConfiguration
	CloudProperties     *cpb.CloudProperties
	Path                string
}

var (
	enabled bool
	// ocfg needs to be declared outside NewCommand to initialize context once
	ocfg   *Config
	config *cpb.Configuration
)

// NewCommand creates a new 'oracle' command.
func NewCommand(cloudProps *cpb.CloudProperties) *cobra.Command {
	var err error
	if ocfg == nil {
		ctx := context.Background()
		path := getConfigPath()

		config, err = configuration.Load(path, os.ReadFile, cloudProps)
		if err != nil {
			log.CtxLogger(ctx).Errorw("Unable to load configuration from a json file", err)
			fmt.Println("Error loading configuration:", err)
			return nil
		}
		log.CtxLogger(ctx).Info("Loads the configuration from a json file and applies defaults for missing fields.")

		ocfg = &Config{
			Ctx:                 ctx, // Initialize context here
			OracleConfiguration: config.GetOracleConfiguration(),
			CloudProperties:     cloudProps,
			Path:                path,
		}
	}

	oracleCmd := &cobra.Command{
		Use:   "oracle",
		Short: "Configure Oracle settings",
		Long: `Configure Oracle settings for the Google Cloud Agent for Compute Workloads.

This command allows you to enable and configure various features
for monitoring Oracle databases, including discovery and metrics collection.`,
		Run: func(cmd *cobra.Command, args []string) {
			if enabled {
				ocfg.OracleConfiguration.Enabled = &enabled
				fmt.Println("Oracle Configuration is Enabled.  Performing Oracle-specific tasks...")
			} else {
				ocfg.OracleConfiguration.Enabled = &enabled
				fmt.Println("Oracle Configuration is Disabled.")
			}
		},
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			fmt.Println("Oracle PersistentPostRun")
			ocfg.writeFile()
		},
	}

	oracleCmd.Flags().BoolVarP(&enabled, "enabled", "e", false, "Enable Oracle configuration")

	oracleCmd.AddCommand(DiscoveryCommand(ocfg))
	oracleCmd.AddCommand(MetricsCommand())

	return oracleCmd
}

func (c *Config) writeFile() {
	config.OracleConfiguration = c.OracleConfiguration
	file, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(config)
	if err != nil {
		log.CtxLogger(c.Ctx).Errorw("Unable to marshal configuration.json", err)
		fmt.Println("Unable to marshal configuration.json")
		return
	}

	var fileBuf bytes.Buffer
	json.Indent(&fileBuf, file, "", "  ")
	err = os.WriteFile(c.Path, fileBuf.Bytes(), 0644)
	if err != nil {
		log.CtxLogger(c.Ctx).Errorw("Unable to write configuration.json", err)
		fmt.Println("Unable to write configuration.json")
		return
	}
}

// getConfigPath determines the configuration path based on the OS.
func getConfigPath() string {
	if runtime.GOOS == "windows" {
		return configuration.WindowsConfigPath
	}
	return configuration.LinuxConfigPath
}
