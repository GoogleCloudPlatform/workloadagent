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
)

// NOTE: These variables value is derived from command-line flags and may be
// subject to modification by other functions within this package.
var (
	enableDiscovery    bool
	discoveryFrequency time.Duration
)

// DiscoveryCommand creates a new 'discovery' subcommand for Oracle.
func DiscoveryCommand() *cobra.Command {
	discoveryCmd := &cobra.Command{
		Use:   "discovery",
		Short: "Configure Oracle discovery",
		Long: `Configure Oracle discovery settings.

This command allows you to enable or disable Oracle discovery and set the update frequency.`,
		Run: func(cmd *cobra.Command, args []string) {
			if enableDiscovery {
				fmt.Println("Oracle Discovery is Enabled.")
				fmt.Printf("  Update Frequency: %s\n", discoveryFrequency)
				fmt.Println("  Performing Oracle discovery...")
			} else {
				fmt.Println("Oracle Discovery is Disabled.")
			}
		},
	}

	discoveryCmd.Flags().BoolVarP(&enableDiscovery, "enabled", "e", false, "Enable Oracle discovery")
	discoveryCmd.Flags().DurationVar(&discoveryFrequency, "frequency", 3*time.Hour, "Discovery update frequency (e.g., 5m, 1h)")

	return discoveryCmd
}
