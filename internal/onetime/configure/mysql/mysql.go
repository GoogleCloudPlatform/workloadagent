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

// Package mysql implements the mysql subcommand.
package mysql

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime/configure/cliconfig"

	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

// NewCommand creates a new 'mysql' command.
func NewCommand(cfg *cliconfig.Configure) *cobra.Command {
	var enabled bool

	mysqlCmd := &cobra.Command{
		Use:   "mysql",
		Short: "Configure MySQL settings",
		Long: `Configure MySQL settings for the Google Cloud Agent for Compute Workloads.

This command allows you to enable and configure various features
for monitoring MySQL databases, including discovery and metrics collection.`,
		Run: func(cmd *cobra.Command, args []string) {
			if cmd.Flags().Changed("enabled") {
				fmt.Println("MySQL Enabled: ", enabled)
				cfg.Configuration.MysqlConfiguration.Enabled = &enabled
				cfg.MySQLConfigModified = true
			}
		},
	}

	mysqlCmd.Flags().BoolVar(&enabled, "enabled", false, "Enable MySQL configuration")

	mysqlCmd.AddCommand(newConnectionParamsCmd(cfg))

	return mysqlCmd
}

// newConnectionParamsCmd adds a connection parameters to request for a MySQL database.
func newConnectionParamsCmd(cfg *cliconfig.Configure) *cobra.Command {
	connectionParamsCmd := &cobra.Command{
		Use:   "connection-params",
		Short: "Connection Parameters to request for a MySQL database",
		RunE: func(cmd *cobra.Command, args []string) error {
			username, err := cmd.Flags().GetString("username")
			if err != nil {
				return fmt.Errorf("error getting username: %w", err)
			}
			projectID, err := cmd.Flags().GetString("project-id")
			if err != nil {
				return fmt.Errorf("error getting project-id: %w", err)
			}
			secretName, err := cmd.Flags().GetString("secret-name")
			if err != nil {
				return fmt.Errorf("error getting secret-name: %w", err)
			}
			password, err := cmd.Flags().GetString("password")
			if err != nil {
				return fmt.Errorf("error getting password: %w", err)
			}

			newConn := &cpb.ConnectionParameters{
				Username: username,
				Secret: &cpb.SecretRef{
					ProjectId:  projectID,
					SecretName: secretName,
				},
				Password: password,
			}

			fmt.Println("Adding New connection: ", newConn)
			cfg.Configuration.MysqlConfiguration.ConnectionParameters = newConn
			cfg.MySQLConfigModified = true
			return nil
		},
	}

	// Add flags for the connection
	connectionParamsCmd.Flags().String("username", "", "Database username")
	connectionParamsCmd.Flags().String("project-id", "", "Project ID")
	connectionParamsCmd.Flags().String("secret-name", "", "Secret name")
	connectionParamsCmd.Flags().String("password", "", "Password")

	connectionParamsCmd.MarkFlagRequired("username")

	return connectionParamsCmd
}
