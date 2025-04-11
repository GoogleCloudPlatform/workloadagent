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
			if cfg.Configuration.MysqlConfiguration == nil {
				cfg.Configuration.MysqlConfiguration = &cpb.MySQLConfiguration{}
			}
			if cmd.Flags().Changed("enabled") {
				msg := fmt.Sprintf("MySQL Enabled: %v", enabled)
				cfg.LogToBoth(cmd.Context(), msg)
				cfg.Configuration.MysqlConfiguration.Enabled = &enabled
				cfg.MySQLConfigModified = true
			}
		},
	}

	mysqlCmd.Flags().BoolVar(&enabled, "enabled", false, "Enable MySQL configuration")

	mysqlCmd.AddCommand(newConnectionParamsCmd(cfg))

	return mysqlCmd
}

// newConnectionParamsCmd adds connection parameters for a MySQL database.
func newConnectionParamsCmd(cfg *cliconfig.Configure) *cobra.Command {
	var username, projectID, secretName, password string
	cpCmd := &cobra.Command{
		Use:   "connection-params",
		Short: "Add connection parameters for a MySQL database.",
		Long: `Sets the username, password, and Secret Manager details
for connecting to the MySQL database specified in the main configuration.

Existing connection parameters will be overwritten by the provided flags.

WARNING: Using the --password flag is not recommended for security reasons
as it can expose the password in shell history or logs. Please prefer storing
the password in Google Cloud Secret Manager and using the --project-id and
--secret-name flags instead.`,
		Run: func(cmd *cobra.Command, args []string) {
			if cfg.Configuration.MysqlConfiguration == nil {
				cfg.Configuration.MysqlConfiguration = &cpb.MySQLConfiguration{}
			}
			if cfg.Configuration.MysqlConfiguration.ConnectionParameters == nil {
				cfg.Configuration.MysqlConfiguration.ConnectionParameters = &cpb.ConnectionParameters{}
			}
			cp := cfg.Configuration.MysqlConfiguration.ConnectionParameters

			if cmd.Flags().Changed("username") {
				msg := fmt.Sprintf("Setting MySQL Username: %v", username)
				cfg.LogToBoth(cmd.Context(), msg)
				cp.Username = username
				cfg.MySQLConfigModified = true
			}
			if cmd.Flags().Changed("password") {
				msg := fmt.Sprintf("Setting MySQL Password: %v", password)
				cfg.LogToBoth(cmd.Context(), msg)
				cp.Password = password
				cfg.MySQLConfigModified = true
			}

			spChanged := cmd.Flags().Changed("project-id")
			snChanged := cmd.Flags().Changed("secret-name")
			if (spChanged || snChanged) && (cp.Secret == nil) {
				cp.Secret = &cpb.SecretRef{}
			}
			if spChanged {
				msg := fmt.Sprintf("Setting MySQL Project ID: %v", projectID)
				cfg.LogToBoth(cmd.Context(), msg)
				cp.Secret.ProjectId = projectID
				cfg.MySQLConfigModified = true
			}
			if snChanged {
				msg := fmt.Sprintf("Setting MySQL Secret Name: %v", secretName)
				cfg.LogToBoth(cmd.Context(), msg)
				cp.Secret.SecretName = secretName
				cfg.MySQLConfigModified = true
			}
		},
	}

	// Add flags for the connection
	cpCmd.Flags().StringVar(&username, "username", "", "Database username")
	cpCmd.Flags().StringVar(&projectID, "project-id", "", "Project ID")
	cpCmd.Flags().StringVar(&secretName, "secret-name", "", "Secret name")
	cpCmd.Flags().StringVar(&password, "password", "", "Password")

	return cpCmd
}
