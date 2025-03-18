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

	durationpb "google.golang.org/protobuf/types/known/durationpb"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

// MetricsCommand creates a new 'metrics' subcommand for Oracle.
func MetricsCommand(ocfgPtr **Config) *cobra.Command {
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
			ocfg := *ocfgPtr // Dereference the pointer to get the Config
			if ocfg == nil || ocfg.OracleConfiguration == nil || ocfg.OracleConfiguration.OracleMetrics == nil {
				fmt.Println("Error: Oracle configuration or metrics is not initialized.")
				return
			}

			ocfg.OracleConfiguration.OracleMetrics.CollectionFrequency = durationpb.New(metricsFrequency)
			ocfg.OracleConfiguration.OracleMetrics.MaxExecutionThreads = metricsMaxThreads
			ocfg.OracleConfiguration.OracleMetrics.QueryTimeout = durationpb.New(metricsQueryTimeout)
			if enableMetrics && ocfg.OracleConfiguration.GetOracleMetrics().GetConnectionParameters() == nil {
				fmt.Println("Metrics remained disabled because connection parameters are not set.")
			} else {
				ocfg.OracleConfiguration.OracleMetrics.Enabled = &enableMetrics
			}
		},
	}

	var em = false
	var mf = time.Duration(1 * time.Minute)
	var mt = int64(10)
	var qt = time.Duration(10 * time.Second)
	ocfg := *ocfgPtr
	if ocfg != nil && ocfg.OracleConfiguration.GetOracleMetrics() != nil {
		// Set the default values for the flags from the configuration.
		em = ocfg.OracleConfiguration.GetOracleMetrics().GetEnabled()
		mf = ocfg.OracleConfiguration.GetOracleMetrics().GetCollectionFrequency().AsDuration()
		mt = ocfg.OracleConfiguration.GetOracleMetrics().GetMaxExecutionThreads()
		qt = ocfg.OracleConfiguration.GetOracleMetrics().GetQueryTimeout().AsDuration()
	}

	metricsCmd.Flags().BoolVarP(&enableMetrics, "enabled", "e", em, "Enable Oracle metrics")
	metricsCmd.Flags().DurationVar(&metricsFrequency, "frequency", mf, "Metrics update frequency (e.g., 5m, 1h)")
	metricsCmd.Flags().Int64Var(&metricsMaxThreads, "max-threads", mt, "Maximum number of threads to use for metrics collection")
	metricsCmd.Flags().DurationVar(&metricsQueryTimeout, "query-timeout", qt, "Query timeout")

	// Add subcommands for managing connections and queries
	metricsCmd.AddCommand(newMetricsConnectionAddCmd(ocfgPtr))
	metricsCmd.AddCommand(newMetricsQueryAddCmd(ocfgPtr))

	return metricsCmd
}

// newMetricsConnectionAddCmd adds a new database connection
func newMetricsConnectionAddCmd(ocfgPtr **Config) *cobra.Command {
	connectionAddCmd := &cobra.Command{
		Use:   "connection-add",
		Short: "Add a database connection",
		RunE: func(cmd *cobra.Command, args []string) error {
			ocfg := *ocfgPtr // Dereference the original pointer
			if ocfg == nil || ocfg.OracleConfiguration == nil || ocfg.OracleConfiguration.OracleMetrics == nil {
				return fmt.Errorf("error: Oracle configuration or metrics is not initialized")
			}

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
			projectID, err := cmd.Flags().GetString("project-id")
			if err != nil {
				return fmt.Errorf("error getting project-id: %w", err)
			}
			secretName, err := cmd.Flags().GetString("secret-name")
			if err != nil {
				return fmt.Errorf("error getting secret-name: %w", err)
			}

			newConn := cpb.ConnectionParameters{
				Username:    username,
				Password:    password,
				Host:        host,
				Port:        int32(port),
				ServiceName: serviceName,
				Secret: &cpb.SecretRef{
					ProjectId:  projectID,
					SecretName: secretName,
				},
			}

			connectionString := fmt.Sprintf("%s:%s@%s:%d/%s", username, password, host, port, serviceName)
			ocfg.OracleConfiguration.OracleMetrics.ConnectionParameters = append(ocfg.OracleConfiguration.OracleMetrics.ConnectionParameters, &newConn)
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
	connectionAddCmd.Flags().String("project-id", "", "Project ID")
	connectionAddCmd.Flags().String("secret-name", "", "Secret name")

	connectionAddCmd.MarkFlagRequired("username")
	connectionAddCmd.MarkFlagRequired("password")
	connectionAddCmd.MarkFlagRequired("host")
	connectionAddCmd.MarkFlagRequired("service-name")
	connectionAddCmd.MarkFlagRequired("project-id")
	connectionAddCmd.MarkFlagRequired("secret-name")

	return connectionAddCmd
}

// newMetricsQueryAddCmd adds a new query in the metricsQueries
func newMetricsQueryAddCmd(ocfgPtr **Config) *cobra.Command {
	queryAddCmd := &cobra.Command{
		Use:   "query-add",
		Short: "Add a metrics query",
		RunE: func(cmd *cobra.Command, arg []string) error {
			ocfg := *ocfgPtr // Dereference the original pointer
			if ocfg == nil || ocfg.OracleConfiguration == nil || ocfg.OracleConfiguration.OracleMetrics == nil {
				return fmt.Errorf("error: Oracle configuration or metrics is not initialized")
			}

			name, err := cmd.Flags().GetString("name")
			if err != nil {
				return fmt.Errorf("error getting name: %w", err)
			}
			sql, err := cmd.Flags().GetString("sql")
			if err != nil {
				return fmt.Errorf("error getting sql: %w", err)
			}
			disabled, err := cmd.Flags().GetBool("disabled")
			if err != nil {
				return fmt.Errorf("error getting disabled: %w", err)
			}
			databaseRoleStr, err := cmd.Flags().GetString("role")
			if err != nil {
				return fmt.Errorf("error getting database role: %w", err)
			}

			var databaseRole cpb.Query_DatabaseRole
			switch databaseRoleStr {
			case "UNSPECIFIED":
				databaseRole = cpb.Query_UNSPECIFIED
			case "PRIMARY":
				databaseRole = cpb.Query_PRIMARY
			case "STANDBY":
				databaseRole = cpb.Query_STANDBY
			case "BOTH":
				databaseRole = cpb.Query_BOTH
			default:
				return fmt.Errorf("invalid database role: %s", databaseRoleStr)
			}

			// Create a new Query object.
			newQuery := &cpb.Query{
				Name:         name,
				Sql:          sql,
				Disabled:     &disabled,
				DatabaseRole: databaseRole,
				// TODO: Add support/subcommand for columns
				Columns: []*cpb.Column{},
			}

			ocfg.OracleConfiguration.OracleMetrics.Queries = append(ocfg.OracleConfiguration.OracleMetrics.Queries, newQuery)

			return nil
		},
	}

	// Add flags for the query
	queryAddCmd.Flags().String("name", "", "Query name")
	queryAddCmd.Flags().String("sql", "", "Query SQL")
	queryAddCmd.Flags().BoolP("disabled", "d", true, "Disable query")
	queryAddCmd.Flags().String("role", "UNSPECIFIED", "Database role (UNSPECIFIED, PRIMARY, STANDBY, BOTH)")

	queryAddCmd.MarkFlagRequired("name")
	queryAddCmd.MarkFlagRequired("sql")

	return queryAddCmd
}
