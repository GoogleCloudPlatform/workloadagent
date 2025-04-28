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
	"os"
	"runtime"

	"github.com/spf13/cobra"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/configuration"
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime/configure/cliconfig"
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime/configure/mysql"
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime/configure/oracle"
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime/configure/redis"
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime/configure/sqlserver"
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"

	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

// configFromFileFunc abstracts the configuration.ConfigFromFile function signature for testability.
type configFromFileFunc func(path string, readFile configuration.ReadConfigFile) (*cpb.Configuration, error)

// NewCommand creates a new 'configure' command.
func NewCommand(lp log.Parameters) *cobra.Command {
	cfg := cliconfig.NewConfigure(configPath(runtime.GOOS), lp, nil, nil)

	configureCmd := &cobra.Command{
		Use:   "configure",
		Short: "Configure the Google Cloud Agent for Compute Workloads",
		// PersistentPreRunE is called before each cli command is run.
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			onetime.SetValues("google-cloud-workload-agent", &cfg.Lp, cmd, "configure")
			if cfg.Lp.CloudLoggingClient != nil {
				defer cfg.Lp.CloudLoggingClient.Close()
			}
			log.SetupLoggingForOTE("google-cloud-workload-agent", "configure", cfg.Lp)

			var err error
			cfg.Configuration, err = loadWAConfiguration(os.ReadFile, configuration.ConfigFromFile)
			if err != nil {
				return fmt.Errorf("failed to load configuration: %w", err)
			}
			return nil
		},
		// PersistentPostRunE is called after each cli command is run.
		PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
			if !cfg.IsConfigModified() {
				cfg.LogToBoth(cmd.Context(), "No configuration changes to save.")
				return nil
			}
			// TODO: Display Modified Configuration on Console.
			return cfg.WriteFile(cmd.Context())
		},
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

	configureCmd.AddCommand(oracle.NewCommand(cfg))
	configureCmd.AddCommand(sqlserver.NewCommand(cfg))
	configureCmd.AddCommand(mysql.NewCommand(cfg))
	configureCmd.AddCommand(redis.NewCommand(cfg))

	return configureCmd
}

// loadWAConfiguration creates a new Configuration.
// TODO: Remove this function
func loadWAConfiguration(rf configuration.ReadConfigFile, cf configFromFileFunc) (*cpb.Configuration, error) {
	config, err := cf(configPath(runtime.GOOS), rf)
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration: %w", err)
	}
	return config, nil
}

// configPath determines the configuration path based on the OS.
func configPath(goos string) string {
	if goos == "windows" {
		return configuration.WindowsConfigPath
	}
	return configuration.LinuxConfigPath
}
