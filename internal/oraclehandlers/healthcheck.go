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

// Package oraclehandlers implements guest action handlers for Oracle.
package oraclehandlers

import (
	"context"

	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce/metadataserver"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
	gpb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/guestactions"
)

// HealthCheck implements the oracle_health_check guest action.
func HealthCheck(ctx context.Context, command *gpb.Command, cloudProperties *metadataserver.CloudProperties) *gpb.CommandResult {
	log.CtxLogger(ctx).Info("oracle_health_check handler called")
	// TODO: Implement oracle_health_check handler.
	return &gpb.CommandResult{
		Command:  command,
		ExitCode: 1,
		Stdout:   "oracle_health_check not implemented.",
	}
}
