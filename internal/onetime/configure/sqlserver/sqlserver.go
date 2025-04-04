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

// Package sqlserver implements the sqlserver subcommand.
package sqlserver

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/configuration"
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime/configure/cliconfig"

	dpb "google.golang.org/protobuf/types/known/durationpb"
)

// NewCommand creates a new 'sqlserver' command.
func NewCommand(cfg *cliconfig.Configure) *cobra.Command {
	var (
		enabled           bool
		collectionTimeout time.Duration
		maxRetries        int32
		retryFrequency    time.Duration
		remoteCollection  bool
	)

	sqlserverCmd := &cobra.Command{
		Use:   "sqlserver",
		Short: "Configure SQL Server settings",
		Long: `Configure SQL Server settings for the Google Cloud Agent for Compute Workloads.

This command allows you to enable and configure various features for monitoring SQL Server databases.`,
		Run: func(cmd *cobra.Command, args []string) {
			if cmd.Flags().Changed("enabled") {
				fmt.Println("SQL Server Enabled: ", enabled)
				cfg.Configuration.SqlserverConfiguration.Enabled = &enabled
				cfg.SQLServerConfigModified = true
			}
			if cmd.Flags().Changed("collection-timeout") {
				fmt.Println("SQL Server Collection Timeout: ", collectionTimeout)
				cfg.Configuration.SqlserverConfiguration.CollectionTimeout = dpb.New(collectionTimeout)
				cfg.SQLServerConfigModified = true
			}
			if cmd.Flags().Changed("max-retries") {
				fmt.Println("SQL Server Max Retries: ", maxRetries)
				cfg.Configuration.SqlserverConfiguration.MaxRetries = maxRetries
				cfg.SQLServerConfigModified = true
			}
			if cmd.Flags().Changed("retry-frequency") {
				fmt.Println("SQL Server Retry Frequency: ", retryFrequency)
				cfg.Configuration.SqlserverConfiguration.RetryFrequency = dpb.New(retryFrequency)
				cfg.SQLServerConfigModified = true
			}
			if cmd.Flags().Changed("remote-collection") {
				fmt.Println("SQL Server Remote Collection: ", remoteCollection)
				cfg.Configuration.SqlserverConfiguration.RemoteCollection = remoteCollection
				cfg.SQLServerConfigModified = true
			}
		},
	}

	// Add flags for the metrics
	sqlserverCmd.Flags().BoolVar(&enabled, "enabled", false, "Enable SQL Server metrics")
	sqlserverCmd.Flags().DurationVar(&collectionTimeout, "collection-timeout", time.Duration(configuration.DefaultSQLServerCollectionTimeout), "Query timeout for metrics collection (e.g., 5m, 1h)")
	sqlserverCmd.Flags().Int32Var(&maxRetries, "max-retries", int32(configuration.DefaultSQLServerMaxRetries), "Maximum number of attempts to submit collected metric data to Workload Manager")
	sqlserverCmd.Flags().DurationVar(&retryFrequency, "retry-frequency", time.Duration(configuration.DefaultSQLServerRetryFrequency), "Duration to wait before retrying metrics submission to Workload Manager in the event of a failure")
	sqlserverCmd.Flags().BoolVar(&remoteCollection, "remote-collection", false, "Enable remote collection")

	sqlserverCmd.AddCommand(CollectionConfigCommand(cfg))
	return sqlserverCmd
}
