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
	"fmt"

	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce/metadataserver"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"

	codepb "google.golang.org/genproto/googleapis/rpc/code"
	gpb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/guestactions"
)

// DisableAutostart implements the oracle_disable_autostart guest action.
func DisableAutostart(ctx context.Context, command *gpb.Command, cloudProperties *metadataserver.CloudProperties) *gpb.CommandResult {
	log.CtxLogger(ctx).Info("oracle_disable_autostart handler called")
	// TODO: Implement oracle_disable_autostart handler.
	return &gpb.CommandResult{
		Command:  command,
		ExitCode: 1,
		Stdout:   "oracle_disable_autostart not implemented.",
	}
}

// RunDatapatch implements the oracle_run_datapatch guest action.
func RunDatapatch(ctx context.Context, command *gpb.Command, cloudProperties *metadataserver.CloudProperties) *gpb.CommandResult {
	log.CtxLogger(ctx).Info("oracle_run_datapatch handler called")
	// TODO: Implement oracle_run_datapatch handler.
	return &gpb.CommandResult{
		Command:  command,
		ExitCode: 1,
		Stdout:   "oracle_run_datapatch not implemented.",
	}
}

// DisableRestrictedSession implements the oracle_disable_restricted_mode guest action.
// It executes "ALTER SYSTEM DISABLE RESTRICTED SESSION" to allow normal users to log in.
func DisableRestrictedSession(ctx context.Context, command *gpb.Command, cloudProperties *metadataserver.CloudProperties) *gpb.CommandResult {
	params := command.GetAgentCommand().GetParameters()
	logger := log.CtxLogger(ctx)
	if result := validateParams(ctx, logger, command, params); result != nil {
		return result
	}
	logger = logger.With("oracle_sid", params["oracle_sid"], "oracle_home", params["oracle_home"], "oracle_user", params["oracle_user"])
	logger.Infow("oracle_disable_restricted_mode handler called")

	sql := "ALTER SYSTEM DISABLE RESTRICTED SESSION;"
	stdout, stderr, err := runSQL(ctx, params, sql, 120, true)
	if err != nil {
		errMsg := fmt.Errorf("failed to disable restricted mode: %w", err)
		logger.Warnw("failed to disable restricted session", "stdout", stdout, "stderr", stderr, "error", errMsg)
		return commandResult(ctx, logger, command, stdout, stderr, codepb.Code_FAILED_PRECONDITION, errMsg.Error(), errMsg)
	}
	logger.Infow("Restricted session disabled successfully", "stdout", stdout)
	return commandResult(ctx, logger, command, stdout, stderr, codepb.Code_OK, "Restricted session disabled successfully", nil)
}

// StartListener implements the oracle_start_listener guest action.
func StartListener(ctx context.Context, command *gpb.Command, cloudProperties *metadataserver.CloudProperties) *gpb.CommandResult {
	log.CtxLogger(ctx).Info("oracle_start_listener handler called")
	// TODO: Implement oracle_start_listener handler.
	return &gpb.CommandResult{
		Command:  command,
		ExitCode: 1,
		Stdout:   "oracle_start_listener not implemented.",
	}
}

// EnableAutostart implements the oracle_enable_autostart guest action.
func EnableAutostart(ctx context.Context, command *gpb.Command, cloudProperties *metadataserver.CloudProperties) *gpb.CommandResult {
	log.CtxLogger(ctx).Info("oracle_enable_autostart handler called")
	// TODO: Implement oracle_enable_autostart handler.
	return &gpb.CommandResult{
		Command:  command,
		ExitCode: 1,
		Stdout:   "oracle_enable_autostart not implemented.",
	}
}
