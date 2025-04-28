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
	"time"

	"github.com/spf13/cobra"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/configuration"
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime/configure/cliconfig"

	dpb "google.golang.org/protobuf/types/known/durationpb"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

// MetricsCommand creates a new 'metrics' subcommand for Oracle.
func MetricsCommand(cfg *cliconfig.Configure) *cobra.Command {
	var (
		enableMetrics       bool
		metricsFrequency    time.Duration
		metricsMaxThreads   int64
		metricsQueryTimeout time.Duration
	)

	metricsCmd := &cobra.Command{
		Use:   "metrics",
		Short: "Configure Oracle metrics collection",
		Long: `Configure Oracle metrics collection settings.

This includes enabling metrics, setting the collection frequency,
managing connection parameters, and adding/removing SQL queries.`,
		Run: func(cmd *cobra.Command, args []string) {
			cfg.ValidateOracleMetrics()

			if cmd.Flags().Changed("frequency") {
				msg := fmt.Sprintf("Oracle Metrics Frequency: %v", metricsFrequency)
				cfg.LogToBoth(cmd.Context(), msg)
				cfg.Configuration.OracleConfiguration.OracleMetrics.CollectionFrequency = dpb.New(metricsFrequency)
				cfg.OracleConfigModified = true
			}
			if cmd.Flags().Changed("max-threads") {
				msg := fmt.Sprintf("Oracle Metrics Max Threads: %v", metricsMaxThreads)
				cfg.LogToBoth(cmd.Context(), msg)
				cfg.Configuration.OracleConfiguration.OracleMetrics.MaxExecutionThreads = metricsMaxThreads
				cfg.OracleConfigModified = true
			}
			if cmd.Flags().Changed("query-timeout") {
				msg := fmt.Sprintf("Oracle Metrics Query Timeout: %v", metricsQueryTimeout)
				cfg.LogToBoth(cmd.Context(), msg)
				cfg.Configuration.OracleConfiguration.OracleMetrics.QueryTimeout = dpb.New(metricsQueryTimeout)
				cfg.OracleConfigModified = true
			}

			if cmd.Flags().Changed("enabled") {
				// If metrics are enabled, but there are no connection parameters, disable metrics.
				if enableMetrics && (cfg.Configuration.OracleConfiguration.GetOracleMetrics().GetConnectionParameters() == nil ||
					len(cfg.Configuration.OracleConfiguration.GetOracleMetrics().GetConnectionParameters()) == 0) {
					cfg.LogToBoth(cmd.Context(), "Metrics Enabled, but no connection parameters found.  Disabling metrics.")
					enableMetrics = false
				}
				msg := fmt.Sprintf("Oracle Metrics Enabled: %v", enableMetrics)
				cfg.LogToBoth(cmd.Context(), msg)
				cfg.Configuration.OracleConfiguration.OracleMetrics.Enabled = &enableMetrics
				cfg.OracleConfigModified = true
			}
		},
	}

	// Add flags for the metrics
	metricsCmd.Flags().BoolVar(&enableMetrics, "enabled", false, "Enable Oracle metrics")
	metricsCmd.Flags().DurationVar(&metricsFrequency, "frequency", time.Duration(configuration.DefaultOracleMetricsFrequency), "Metrics update frequency (e.g., 5m, 1h)")
	metricsCmd.Flags().Int64Var(&metricsMaxThreads, "max-threads", int64(configuration.DefaultOracleMetricsMaxThreads), "Maximum number of threads to use for metrics collection")
	metricsCmd.Flags().DurationVar(&metricsQueryTimeout, "query-timeout", time.Duration(configuration.DefaultOracleMetricsQueryTimeout), "Query timeout")

	// Add subcommands for managing connections and queries
	metricsCmd.AddCommand(newMetricsConnectionAddCmd(cfg))

	return metricsCmd
}

// newMetricsConnectionAddCmd adds a new database connection
func newMetricsConnectionAddCmd(cfg *cliconfig.Configure) *cobra.Command {
	connectionAddCmd := &cobra.Command{
		Use:   "connection-add",
		Short: "Add a database connection",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg.ValidateOracleMetricsConnection()

			username, err := cmd.Flags().GetString("username")
			if err != nil {
				return fmt.Errorf("error getting username: %w", err)
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
			projectID, err := cmd.Flags().GetString("project-id")
			if err != nil {
				return fmt.Errorf("error getting project-id: %w", err)
			}
			secretName, err := cmd.Flags().GetString("secret-name")
			if err != nil {
				return fmt.Errorf("error getting secret-name: %w", err)
			}

			newConn := &cpb.ConnectionParameters{
				Username:    username,
				Host:        host,
				Port:        int32(port),
				ServiceName: serviceName,
				Secret: &cpb.SecretRef{
					ProjectId:  projectID,
					SecretName: secretName,
				},
			}

			msg := fmt.Sprintf("Oracle Metrics Connection Added: %v", newConn)
			cfg.LogToBoth(cmd.Context(), msg)
			cfg.Configuration.OracleConfiguration.OracleMetrics.ConnectionParameters = append(cfg.Configuration.OracleConfiguration.OracleMetrics.ConnectionParameters, newConn)
			cfg.OracleConfigModified = true
			return nil
		},
	}

	// Add flags for the connection
	connectionAddCmd.Flags().String("username", "", "Database username")
	connectionAddCmd.Flags().String("host", "", "Database host")
	connectionAddCmd.Flags().Int("port", 1521, "Database port")
	connectionAddCmd.Flags().String("service-name", "", "Oracle service name")
	connectionAddCmd.Flags().String("project-id", "", "Project ID")
	connectionAddCmd.Flags().String("secret-name", "", "Secret name")

	connectionAddCmd.MarkFlagRequired("username")
	connectionAddCmd.MarkFlagRequired("host")
	connectionAddCmd.MarkFlagRequired("service-name")
	connectionAddCmd.MarkFlagRequired("project-id")
	connectionAddCmd.MarkFlagRequired("secret-name")

	return connectionAddCmd
}
