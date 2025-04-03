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

	return mysqlCmd
}
