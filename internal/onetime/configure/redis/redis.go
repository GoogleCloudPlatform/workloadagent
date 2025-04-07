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

// Package redis implements the redis subcommand.
package redis

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/configuration"
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime/configure/cliconfig"

	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

// NewCommand creates a new 'redis' command.
func NewCommand(cfg *cliconfig.Configure) *cobra.Command {
	var enabled bool

	redisCmd := &cobra.Command{
		Use:   "redis",
		Short: "Configure Redis settings",
		Long: `Configure Redis settings for the Google Cloud Agent for Compute Workloads.

This command allows you to enable and configure various features
for monitoring Redis databases, including discovery and metrics collection.`,
		Run: func(cmd *cobra.Command, args []string) {
			if cmd.Flags().Changed("enabled") {
				fmt.Println("Redis Enabled: ", enabled)
				cfg.Configuration.RedisConfiguration.Enabled = &enabled
				cfg.RedisConfigModified = true
			}
		},
	}

	redisCmd.Flags().BoolVar(&enabled, "enabled", false, "Enable Redis configuration")

	redisCmd.AddCommand(newConnectionParamsCmd(cfg))

	return redisCmd
}

// newConnectionParamsCmd adds connection parameters for a Redis database.
func newConnectionParamsCmd(cfg *cliconfig.Configure) *cobra.Command {
	var (
		port                            int
		projectID, secretName, password string
	)

	cpCmd := &cobra.Command{
		Use:   "connection-params",
		Short: "Add connection parameters for a Redis database.",
		Long: `Sets the port, password, and Secret Manager details
for connecting to the Redis database specified in the main configuration.

Existing connection parameters will be overwritten by the provided flags.

WARNING: Using the --password flag is not recommended for security reasons
as it can expose the password in shell history or logs. Please prefer storing
the password in Google Cloud Secret Manager and using the --project-id and
--secret-name flags instead.`,
		Run: func(cmd *cobra.Command, args []string) {
			if cfg.Configuration.RedisConfiguration.ConnectionParameters == nil {
				cfg.Configuration.RedisConfiguration.ConnectionParameters = &cpb.ConnectionParameters{}
			}
			cp := cfg.Configuration.RedisConfiguration.ConnectionParameters

			if cmd.Flags().Changed("port") {
				fmt.Println("Setting Redis Port:", port)
				cp.Port = int32(port)
				cfg.RedisConfigModified = true
			}
			if cmd.Flags().Changed("password") {
				fmt.Println("Setting Redis Password:", password)
				cp.Password = password
				cfg.RedisConfigModified = true
			}

			spChanged := cmd.Flags().Changed("project-id")
			snChanged := cmd.Flags().Changed("secret-name")
			if (spChanged || snChanged) && (cp.Secret == nil) {
				cp.Secret = &cpb.SecretRef{}
			}
			if spChanged {
				fmt.Println("Setting Redis Project ID:", projectID)
				cp.Secret.ProjectId = projectID
				cfg.RedisConfigModified = true
			}
			if snChanged {
				fmt.Println("Setting Redis Secret Name:", secretName)
				cp.Secret.SecretName = secretName
				cfg.RedisConfigModified = true
			}
		},
	}

	// Add flags for the connection
	cpCmd.Flags().IntVar(&port, "port", configuration.DefaultRedisPort, "Port")
	cpCmd.Flags().StringVar(&projectID, "project-id", "", "Project ID")
	cpCmd.Flags().StringVar(&secretName, "secret-name", "", "Secret name")
	cpCmd.Flags().StringVar(&password, "password", "", "Password")

	return cpCmd
}
