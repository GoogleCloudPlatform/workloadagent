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
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime/configure/cliconfig"
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

	return redisCmd
}
