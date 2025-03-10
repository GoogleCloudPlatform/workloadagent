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

package oracle

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// NOTE: These variables value is derived from command-line flags and may be
// subject to modification by other functions within this package.
var (
	enableMetrics       bool
	metricsFrequency    time.Duration
	metricsConnections  []string
	metricsQueries      []string
	metricsMaxThreads   int64
	metricsQueryTimeout time.Duration
)

// MetricsCommand creates a new 'metrics' subcommand for Oracle.
func MetricsCommand() *cobra.Command {
	metricsCmd := &cobra.Command{
		Use:   "metrics",
		Short: "Configure Oracle metrics collection",
		Long: `Configure Oracle metrics collection settings.

This includes enabling metrics, setting the collection frequency,
managing connection parameters, and adding/removing SQL queries.`,
		Run: func(cmd *cobra.Command, args []string) {
			if enableMetrics {
				fmt.Println("Oracle Metrics is Enabled.")
				fmt.Println("Update Frequency:", metricsFrequency)
				fmt.Println("Max Threads:", metricsMaxThreads)
				fmt.Println("Query Timeout:", metricsQueryTimeout)
			} else {
				fmt.Println("Oracle Metrics is Disabled.")
			}
		},
	}

	metricsCmd.Flags().BoolVarP(&enableMetrics, "enabled", "e", false, "Enable Oracle metrics")
	metricsCmd.Flags().DurationVar(&metricsFrequency, "frequency", 1*time.Minute, "Metrics update frequency (e.g., 5m, 1h)")
	metricsCmd.Flags().Int64Var(&metricsMaxThreads, "max-threads", 10, "Maximum number of threads to use for metrics collection")
	metricsCmd.Flags().DurationVar(&metricsQueryTimeout, "query-timeout", 10*time.Second, "Query timeout")

	// Add subcommands for managing connections and queries
	metricsCmd.AddCommand(newMetricsConnectionAddCmd())
	metricsCmd.AddCommand(newMetricsQueryAddCmd())

	return metricsCmd
}

// newMetricsConnectionAddCmd adds a new database connection
func newMetricsConnectionAddCmd() *cobra.Command {
	connectionAddCmd := &cobra.Command{
		Use:   "connection-add",
		Short: "Add a database connection",
		RunE: func(cmd *cobra.Command, args []string) error {
			username, err := cmd.Flags().GetString("username")
			if err != nil {
				return fmt.Errorf("error getting username: %w", err)
			}
			password, err := cmd.Flags().GetString("password")
			if err != nil {
				return fmt.Errorf("error getting password: %w", err)
			}
			host, err := cmd.Flags().GetString("host")
			if err != nil {
				return fmt.Errorf("error getting host: %w", err)
			}
			port, err := cmd.Flags().GetInt("port")
			if err != nil {
				return fmt.Errorf("error getting port: %w", err)
			}
			serviceName, err := cmd.Flags().GetString("service-name")
			if err != nil {
				return fmt.Errorf("error getting service-name: %w", err)
			}

			connectionString := fmt.Sprintf("%s:%s@%s:%d/%s", username, password, host, port, serviceName)
			metricsConnections = append(metricsConnections, connectionString)
			fmt.Println("Added connection:", connectionString)
			return nil
		},
	}

	// Add flags for the connection
	connectionAddCmd.Flags().StringP("username", "u", "", "Database username")
	connectionAddCmd.Flags().StringP("password", "p", "", "Database password")
	connectionAddCmd.Flags().String("host", "", "Database host")
	connectionAddCmd.Flags().Int("port", 1521, "Database port")
	connectionAddCmd.Flags().String("service-name", "", "Oracle service name")

	connectionAddCmd.MarkFlagRequired("username")
	connectionAddCmd.MarkFlagRequired("password")
	connectionAddCmd.MarkFlagRequired("host")
	connectionAddCmd.MarkFlagRequired("port")
	connectionAddCmd.MarkFlagRequired("service-name")

	return connectionAddCmd
}

// metricsQueryAddCmd adds a new query in the metricsQueries
func newMetricsQueryAddCmd() *cobra.Command {
	queryAddCmd := &cobra.Command{
		Use:   "query-add",
		Short: "Add a metrics query",
		RunE: func(cmd *cobra.Command, args []string) error {
			query, err := cmd.Flags().GetString("query")
			if err != nil {
				return fmt.Errorf("error getting query: %w", err)
			}
			metricsQueries = append(metricsQueries, query)
			fmt.Println("metricsQueries:", strings.Join(metricsQueries, ", "))
			return nil
		},
	}

	// Add flags for the query
	queryAddCmd.Flags().StringP("query", "q", "", "Query to add")

	return queryAddCmd
}
