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
	"strings"

	codepb "google.golang.org/genproto/googleapis/rpc/code"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce/metadataserver"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
	gpb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/guestactions"
)

const (
	// This message indicates a successful shutdown.
	shutdownSuccess = "ORACLE instance shut down."
	// This message indicates that shutdown is not available because the database is already shut down.
	// ORA-01034: ORACLE not available
	alreadyDown = "ORA-01034"
	// This message indicates a successful startup or open.
	startupSuccess = "Database opened."
	// This message indicates that the database is already running (either OPEN or MOUNTED).
	// ORA-01081: cannot start already-running ORACLE - shut it down first
	alreadyRunning = "ORA-01081"
)

func validateParams(ctx context.Context, command *gpb.Command, params map[string]string) *gpb.CommandResult {
	for _, requiredParam := range []string{"oracle_sid", "oracle_home", "oracle_user"} {
		if params[requiredParam] == "" {
			errMsg := fmt.Sprintf("parameter %s is missing", requiredParam)
			log.CtxLogger(ctx).Warnw(errMsg, "oracle_sid", params["oracle_sid"], "oracle_home", params["oracle_home"], "oracle_user", params["oracle_user"])
			return commandResult(ctx, command, nil, codepb.Code_INVALID_ARGUMENT, errMsg)
		}
	}
	return nil
}

// StopDatabase implements the oracle_stop_database guest action.
// It attempts to shut down the database gracefully using "SHUTDOWN IMMEDIATE".
// If the command fails, it returns an error in the payload.
func (h *OracleHandler) StopDatabase(ctx context.Context, command *gpb.Command, cloudProperties *metadataserver.CloudProperties) *gpb.CommandResult {
	params := command.GetAgentCommand().GetParameters()
	logger := log.CtxLogger(ctx).With("oracle_sid", params["oracle_sid"], "oracle_home", params["oracle_home"], "oracle_user", params["oracle_user"])
	logger.Infow("oracle_stop_database handler called")
	if result := validateParams(ctx, command, params); result != nil {
		return result
	}

	unlock, result := h.lockDatabase(ctx, command)
	if result != nil {
		return result
	}
	defer unlock()

	// TODO: Handle Data Guard standby databases.
	sqlResult := runSQL(ctx, params, "SHUTDOWN IMMEDIATE")
	if sqlResult.Error != nil || sqlResult.ExitCode != 0 {
		logger.Warnw("Shutdown failed", "exit_code", sqlResult.ExitCode, "stdout", sqlResult.StdOut, "stderr", sqlResult.StdErr, "error", sqlResult.Error)
		return commandResult(ctx, command, sqlResult, codepb.Code_FAILED_PRECONDITION, "shutdown failed")
	}
	if strings.Contains(strings.TrimSpace(sqlResult.StdOut), shutdownSuccess) {
		logger.Infow("Database stopped successfully")
		return commandResult(ctx, command, sqlResult, codepb.Code_OK, "database stopped successfully")
	}
	if strings.Contains(strings.TrimSpace(sqlResult.StdOut), alreadyDown) {
		logger.Infow("Database is already shut down")
		return commandResult(ctx, command, sqlResult, codepb.Code_OK, "database is already shut down")
	}
	logger.Warnw("Shutdown failed", "exit_code", sqlResult.ExitCode, "stdout", sqlResult.StdOut, "stderr", sqlResult.StdErr, "error", sqlResult.Error)
	return commandResult(ctx, command, sqlResult, codepb.Code_FAILED_PRECONDITION, "shutdown failed")
}

// StartDatabase implements the oracle_start_database guest action.
// It checks the current status of the database and starts it if it is not already running.
// If the database is already mounted, it attempts to open it.
func (h *OracleHandler) StartDatabase(ctx context.Context, command *gpb.Command, cloudProperties *metadataserver.CloudProperties) *gpb.CommandResult {
	params := command.GetAgentCommand().GetParameters()
	logger := log.CtxLogger(ctx).With("oracle_sid", params["oracle_sid"], "oracle_home", params["oracle_home"], "oracle_user", params["oracle_user"])
	logger.Infow("oracle_start_database handler called")
	if result := validateParams(ctx, command, params); result != nil {
		return result
	}

	unlock, result := h.lockDatabase(ctx, command)
	if result != nil {
		return result
	}
	defer unlock()

	// TODO: Handle Data Guard standby databases.
	// Cases to consider:
	// 1. Active Data Guard: Open read-only.
	// 2. Manual standby (no broker): Initiate managed recovery.
	startupCmd := "STARTUP"
	if params["startup_mode"] == "RESTRICTED" {
		startupCmd += " RESTRICT"
	}
	logger.Infow("Starting database", "startup_command", startupCmd)
	// TODO: Handle ORA-16005: database requires recovery.
	// See https://docs.oracle.com/en/error-help/db/ora-16005/?r=19c for more details.
	sqlResult := runSQL(ctx, params, startupCmd)
	if sqlResult.Error != nil || sqlResult.ExitCode != 0 {
		logger.Warnw("Startup failed", "exit_code", sqlResult.ExitCode, "stdout", sqlResult.StdOut, "stderr", sqlResult.StdErr, "error", sqlResult.Error)
		return commandResult(ctx, command, sqlResult, codepb.Code_FAILED_PRECONDITION, "startup failed")
	}
	if strings.Contains(strings.TrimSpace(sqlResult.StdOut), startupSuccess) {
		logger.Infow("Database started successfully")
		return commandResult(ctx, command, sqlResult, codepb.Code_OK, "database started successfully")
	}
	// ORA-01081: cannot start already-running ORACLE - shut it down first
	// If we get ORA-01081, instance is running. It could be MOUNTED or OPEN.
	// We check v$instance.status to see if we need to open it.
	if strings.Contains(strings.TrimSpace(sqlResult.StdOut), alreadyRunning) {
		logger.Infow("Database is mounted or open, checking status")
		statusResult := runSQL(ctx, params, "SELECT status FROM v$instance;")
		if statusResult.Error != nil || statusResult.ExitCode != 0 {
			logger.Warnw("Failed to get instance status", "exit_code", statusResult.ExitCode, "stdout", statusResult.StdOut, "stderr", statusResult.StdErr, "error", statusResult.Error)
			return commandResult(ctx, command, statusResult, codepb.Code_FAILED_PRECONDITION, "failed to get instance status")
		}
		if strings.TrimSpace(statusResult.StdOut) == "OPEN" {
			logger.Infow("Database is already open", "stdout", strings.TrimSpace(statusResult.StdOut))
			return commandResult(ctx, command, statusResult, codepb.Code_OK, "database is already open")
		}

		logger.Infow("Database is not open, attempting to open", "stdout", strings.TrimSpace(statusResult.StdOut))
		alterResult := runSQL(ctx, params, "WHENEVER SQLERROR EXIT FAILURE\nALTER DATABASE OPEN")
		if alterResult.Error != nil || alterResult.ExitCode != 0 {
			logger.Warnw("Alter database open failed", "exit_code", alterResult.ExitCode, "stdout", alterResult.StdOut, "stderr", alterResult.StdErr, "error", alterResult.Error)
			return commandResult(ctx, command, alterResult, codepb.Code_FAILED_PRECONDITION, "alter database open failed")
		}
		logger.Infow("Database opened successfully")
		return commandResult(ctx, command, alterResult, codepb.Code_OK, "database opened successfully")
	}
	logger.Warnw("Startup failed", "exit_code", sqlResult.ExitCode, "stdout", sqlResult.StdOut, "stderr", sqlResult.StdErr, "error", sqlResult.Error)
	return commandResult(ctx, command, sqlResult, codepb.Code_FAILED_PRECONDITION, "startup failed")
}
