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

package sqlserver

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/configuration"
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime/configure/cliconfig"

	dpb "google.golang.org/protobuf/types/known/durationpb"
)

// CollectionConfigCommand creates a new 'collection-config' subcommand for SQL Server.
func CollectionConfigCommand(configure *cliconfig.Configure) *cobra.Command {
	var (
		collectGuestOSMetrics bool
		collectSQLMetrics     bool
		collectionFrequency   time.Duration
	)
	collectionConfigCmd := &cobra.Command{
		Use:   "collection-config",
		Short: "Configure SQL Server collection settings",
		Run: func(cmd *cobra.Command, args []string) {
			if cmd.Flags().Changed("collect-guest-os-metrics") {
				fmt.Println("Collect Guest OS Metrics: ", collectGuestOSMetrics)
				configure.Configuration.SqlserverConfiguration.CollectionConfiguration.CollectGuestOsMetrics = collectGuestOSMetrics
				configure.SQLServerConfigModified = true
			}
			if cmd.Flags().Changed("collect-sql-metrics") {
				fmt.Println("Collect SQL Metrics: ", collectSQLMetrics)
				configure.Configuration.SqlserverConfiguration.CollectionConfiguration.CollectSqlMetrics = collectSQLMetrics
				configure.SQLServerConfigModified = true
			}
			if cmd.Flags().Changed("collection-frequency") {
				fmt.Println("Collection Frequency: ", collectionFrequency)
				configure.Configuration.SqlserverConfiguration.CollectionConfiguration.CollectionFrequency = dpb.New(collectionFrequency)
				configure.SQLServerConfigModified = true
			}
		},
	}

	collectionConfigCmd.Flags().BoolVar(&collectGuestOSMetrics, "collect-guest-os-metrics", true, "Enables guest os collection")
	collectionConfigCmd.Flags().BoolVar(&collectSQLMetrics, "collect-sql-metrics", true, "Enables SQL Server collection")
	collectionConfigCmd.Flags().DurationVar(&collectionFrequency, "collection-frequency", time.Duration(configuration.DefaultSQLServerCollectionFrequency), "Collection frequency (e.g., 5m, 1h)")

	return collectionConfigCmd
}
