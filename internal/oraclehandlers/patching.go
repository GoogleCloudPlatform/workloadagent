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

package oraclehandlers

import (
	"context"

	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce/metadataserver"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
	gpb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/guestactions"
)

// DisableAutostart implements the oracle_disable_autostart guest action.
func (h *OracleHandler) DisableAutostart(ctx context.Context, command *gpb.Command, cloudProperties *metadataserver.CloudProperties) *gpb.CommandResult {
	log.CtxLogger(ctx).Info("oracle_disable_autostart handler called")
	// TODO: Implement oracle_disable_autostart handler.
	return &gpb.CommandResult{
		Command:  command,
		ExitCode: 1,
		Stdout:   "oracle_disable_autostart not implemented.",
	}
}

// RunDatapatch implements the oracle_run_datapatch guest action.
func (h *OracleHandler) RunDatapatch(ctx context.Context, command *gpb.Command, cloudProperties *metadataserver.CloudProperties) *gpb.CommandResult {
	log.CtxLogger(ctx).Info("oracle_run_datapatch handler called")
	// TODO: Implement oracle_run_datapatch handler.
	return &gpb.CommandResult{
		Command:  command,
		ExitCode: 1,
		Stdout:   "oracle_run_datapatch not implemented.",
	}
}

// DisableRestrictedMode implements the oracle_disable_restricted_mode guest action.
func (h *OracleHandler) DisableRestrictedMode(ctx context.Context, command *gpb.Command, cloudProperties *metadataserver.CloudProperties) *gpb.CommandResult {
	log.CtxLogger(ctx).Info("oracle_disable_restricted_mode handler called")
	// TODO: Implement oracle_disable_restricted_mode handler.
	return &gpb.CommandResult{
		Command:  command,
		ExitCode: 1,
		Stdout:   "oracle_disable_restricted_mode not implemented.",
	}
}

// StartListener implements the oracle_start_listener guest action.
func (h *OracleHandler) StartListener(ctx context.Context, command *gpb.Command, cloudProperties *metadataserver.CloudProperties) *gpb.CommandResult {
	log.CtxLogger(ctx).Info("oracle_start_listener handler called")
	// TODO: Implement oracle_start_listener handler.
	return &gpb.CommandResult{
		Command:  command,
		ExitCode: 1,
		Stdout:   "oracle_start_listener not implemented.",
	}
}

// EnableAutostart implements the oracle_enable_autostart guest action.
func (h *OracleHandler) EnableAutostart(ctx context.Context, command *gpb.Command, cloudProperties *metadataserver.CloudProperties) *gpb.CommandResult {
	log.CtxLogger(ctx).Info("oracle_enable_autostart handler called")
	// TODO: Implement oracle_enable_autostart handler.
	return &gpb.CommandResult{
		Command:  command,
		ExitCode: 1,
		Stdout:   "oracle_enable_autostart not implemented.",
	}
}
