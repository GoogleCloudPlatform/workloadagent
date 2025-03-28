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

// Package configure implements the configure subcommand.
// TODO: Setup Logging for configure OTE.
package configure

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime/configure/oracle"

	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

// NewCommand creates a new 'configure' command.
func NewCommand(cloudProps *cpb.CloudProperties) *cobra.Command {
	configureCmd := &cobra.Command{
		Use:   "configure",
		Short: "Configure the Google Cloud Agent for Compute Workloads",
		// We don't set a custom Run/RunE here.  We only want to customize the usage.
	}

	// Custom Usage Function
	configureCmd.SetUsageFunc(func(cmd *cobra.Command) error {
		fmt.Printf("Usage for %s:\n\n", cmd.Name())
		fmt.Printf("  %s [subcommand] [flags]\n\n", cmd.CommandPath()) // Dynamic command path

		// Manually print the rest, mimicking Cobra's default behavior
		if cmd.HasAvailableSubCommands() {
			fmt.Println("Available Subcommands:")
			for _, subCmd := range cmd.Commands() {
				if !subCmd.Hidden && subCmd.Name() != "help" { // Avoid showing the default 'help'
					fmt.Printf("  %-15s %s\n", subCmd.Use, subCmd.Short)
				}
			}
			fmt.Println()
		}

		if cmd.HasAvailableLocalFlags() {
			fmt.Println("Flags:")
			fmt.Println(cmd.LocalFlags().FlagUsages()) // Use Cobra's built-in flag formatting
		}

		if cmd.HasAvailableInheritedFlags() {
			fmt.Println("Global Flags:")
			fmt.Println(cmd.InheritedFlags().FlagUsages())
		}
		return nil
	})

	configureCmd.AddCommand(oracle.NewCommand(cloudProps))

	return configureCmd
}
