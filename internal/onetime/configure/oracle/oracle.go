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
	"fmt"

	"github.com/spf13/cobra"
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime/configure/cliconfig"
)

// NewCommand creates a new 'oracle' command.
func NewCommand(configure *cliconfig.Configure) *cobra.Command {
	var enabled bool

	oracleCmd := &cobra.Command{
		Use:   "oracle",
		Short: "Configure Oracle settings",
		Long: `Configure Oracle settings for the Google Cloud Agent for Compute Workloads.

This command allows you to enable and configure various features
for monitoring Oracle databases, including discovery and metrics collection.`,
		Run: func(cmd *cobra.Command, args []string) {
			if cmd.Flags().Changed("enabled") {
				fmt.Println("Oracle Enabled: ", enabled)
				configure.Configuration.OracleConfiguration.Enabled = &enabled
				configure.OracleConfigModified = true
			}
			// TODO: We'll add/remove this logic once we have a clearer design.
			if enabled {
				fmt.Println("Oracle Configuration is Enabled.  Performing Oracle-specific tasks...")
			} else {
				fmt.Println("Oracle Configuration is Disabled.")
			}
		},
	}

	oracleCmd.Flags().BoolVar(&enabled, "enabled", false, "Enable Oracle configuration")

	oracleCmd.AddCommand(DiscoveryCommand(configure))
	oracleCmd.AddCommand(MetricsCommand(configure))

	return oracleCmd
}
