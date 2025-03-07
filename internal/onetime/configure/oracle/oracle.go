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
)

var (
	enabled bool
)

// NewCommand creates a new 'oracle' command.
func NewCommand() *cobra.Command {
	oracleCmd := &cobra.Command{
		Use:   "oracle",
		Short: "Configure Oracle settings",
		Long: `Configure Oracle settings for the Google Cloud Agent for Compute Workloads.

This command allows you to enable and configure various features
for monitoring Oracle databases, including discovery and metrics collection.`,
		Run: func(cmd *cobra.Command, args []string) {
			if enabled {
				fmt.Println("Oracle Configuration is Enabled.  Performing Oracle-specific tasks...")

				// Detailed configuration if enabled
				fmt.Println("Oracle Configuration:")
				fmt.Println("  Enabled:", enabled)
				fmt.Println("  Discovery:")
				fmt.Println("    Enabled:", enableDiscovery)
				fmt.Println("    Update Frequency:", discoveryFrequency)
				fmt.Println("  Metrics:")
				fmt.Println("    Enabled:", enableMetrics)
				fmt.Println("    Collection Frequency:", metricsFrequency)
				fmt.Println("    Connections:", metricsConnections)
				fmt.Println("    Queries:", metricsQueries)
				fmt.Println("    Max Threads:", metricsMaxThreads)
				fmt.Println("    Query Timeout:", metricsQueryTimeout)
			} else {
				fmt.Println("Oracle Configuration is Disabled.")
			}
		},
	}
	oracleCmd.Flags().BoolVarP(&enabled, "enabled", "e", false, "Enable Oracle configuration")

	oracleCmd.AddCommand(DiscoveryCommand())
	oracleCmd.AddCommand(MetricsCommand())

	return oracleCmd
}
