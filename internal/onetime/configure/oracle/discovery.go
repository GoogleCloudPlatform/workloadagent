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

	durationpb "google.golang.org/protobuf/types/known/durationpb"
	"github.com/spf13/cobra"
)

// NOTE: These variables value is derived from command-line flags and may be
// subject to modification by other functions within this package.
// TODO: Remove this global variable.
var (
	enableDiscovery    bool
	discoveryFrequency time.Duration
)

// DiscoveryCommand creates a new 'discovery' subcommand for Oracle.
func DiscoveryCommand(ocfg *Config) *cobra.Command {
	discoveryCmd := &cobra.Command{
		Use:   "discovery",
		Short: "Configure Oracle discovery",
		Long: `Configure Oracle discovery settings.

This command allows you to enable or disable Oracle discovery and set the update frequency.`,
		Run: func(cmd *cobra.Command, args []string) {
			if enableDiscovery {
				fmt.Println("Oracle Discovery is Enabled.")
				fmt.Printf("  Update Frequency: %s\n", discoveryFrequency)
				// Modify the oracle discovery configuration only when enabled.
				ocfg.OracleConfiguration.OracleDiscovery.UpdateFrequency = durationpb.New(discoveryFrequency)
			} else {
				fmt.Println("Oracle Discovery is Disabled.")
			}
		},
	}

	var ed = false
	var df = time.Duration(3 * time.Hour)
	if ocfg != nil && ocfg.OracleConfiguration.GetOracleDiscovery() != nil {
		// Set the default values for the flags from the configuration.
		ed = ocfg.OracleConfiguration.GetOracleDiscovery().GetEnabled()
		df = ocfg.OracleConfiguration.GetOracleDiscovery().GetUpdateFrequency().AsDuration()
	}
	discoveryCmd.Flags().BoolVarP(&enableDiscovery, "enabled", "e", ed, "Enable Oracle discovery")
	discoveryCmd.Flags().DurationVar(&discoveryFrequency, "frequency", df, "Update discovery frequency (e.g., 5m, 1h)")

	return discoveryCmd
}
