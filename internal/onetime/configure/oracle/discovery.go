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
	"time"

	dpb "google.golang.org/protobuf/types/known/durationpb"
	"github.com/spf13/cobra"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/configuration"
)

// DiscoveryCommand creates a new 'discovery' subcommand for Oracle.
func DiscoveryCommand(ocfg *Config) *cobra.Command {
	var (
		enableDiscovery    bool
		discoveryFrequency time.Duration
	)
	discoveryCmd := &cobra.Command{
		Use:   "discovery",
		Short: "Configure Oracle discovery",
		Long: `Configure Oracle discovery settings.

This command allows you to enable or disable Oracle discovery and set the update frequency.`,
		Run: func(cmd *cobra.Command, args []string) {
			if cmd.Flags().Changed("enabled") {
				ocfg.OracleConfiguration.OracleDiscovery.Enabled = &enableDiscovery
				ocfg.ConfigModified = true
			}

			if cmd.Flags().Changed("frequency") {
				ocfg.OracleConfiguration.OracleDiscovery.UpdateFrequency = dpb.New(discoveryFrequency)
				ocfg.ConfigModified = true
			}
		},
	}

	// Add flags for the discovery
	discoveryCmd.Flags().BoolVar(&enableDiscovery, "enabled", false, "Enable Oracle discovery")
	discoveryCmd.Flags().DurationVar(&discoveryFrequency, "frequency", time.Duration(configuration.DefaultOracleDiscoveryFrequency), "Update discovery frequency (e.g., 5m, 1h)")

	return discoveryCmd
}
