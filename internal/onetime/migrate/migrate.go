/*
Copyright 2024 Google LLC

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

// Package migrate implements the migrate subcommand.
package migrate

import (
	"github.com/spf13/cobra"
	"github.com/GoogleCloudPlatform/workloadagent/internal/sqlservermetrics/migration"
)

// NewCommand creates a new migrate command.
func NewCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "migrate",
		Short: "Migrate Google Cloud SQL Server Agent configurations",
		RunE: func(cmd *cobra.Command, args []string) error {
			return migration.Migrate()
		},
	}
}
