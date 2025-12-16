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
			return commandResult(ctx, command, errMsg, "", codepb.Code_INVALID_ARGUMENT, errMsg, fmt.Errorf("%s", errMsg))
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
	stdout, stderr, err := stopDatabase(ctx, params)
	if err != nil {
		logger.Warnw("stopDatabase failed", "stdout", stdout, "stderr", stderr, "error", err)
		return commandResult(ctx, command, stdout, stderr, codepb.Code_FAILED_PRECONDITION, err.Error(), err)
	}
	return commandResult(ctx, command, stdout, stderr, codepb.Code_OK, "database stopped successfully", nil)
}

// stopDatabase contains the core logic for stopping the database.
func stopDatabase(ctx context.Context, params map[string]string) (stdout, stderr string, err error) {
	logger := log.CtxLogger(ctx).With("oracle_sid", params["oracle_sid"], "oracle_home", params["oracle_home"], "oracle_user", params["oracle_user"])
	// TODO: Handle Data Guard standby databases.
	stdout, stderr, err = runSQL(ctx, params, "SHUTDOWN IMMEDIATE")
	if err != nil {
		return stdout, stderr, fmt.Errorf("shutdown command failed: %w", err)
	}
	if strings.Contains(stdout, alreadyDown) {
		logger.Infow("Database is already shut down")
		return stdout, stderr, nil
	}
	if !strings.Contains(stdout, shutdownSuccess) {
		return stdout, stderr, fmt.Errorf("shutdown failed, unexpected output: %s", stdout)
	}
	logger.Infow("Database stopped successfully")
	return stdout, stderr, nil
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
	stdout, stderr, err := startDatabase(ctx, params)
	if err != nil {
		logger.Warnw("startDatabase failed", "stdout", stdout, "stderr", stderr, "error", err)
		return commandResult(ctx, command, stdout, stderr, codepb.Code_FAILED_PRECONDITION, err.Error(), err)
	}
	return commandResult(ctx, command, stdout, stderr, codepb.Code_OK, "database started successfully", nil)
}

// startDatabase contains the core logic for starting the database.
func startDatabase(ctx context.Context, params map[string]string) (stdout, stderr string, err error) {
	logger := log.CtxLogger(ctx).With("oracle_sid", params["oracle_sid"], "oracle_home", params["oracle_home"], "oracle_user", params["oracle_user"], "startup_mode", params["startup_mode"])
	startupCmd := "STARTUP"
	if params["startup_mode"] == "restricted" {
		startupCmd += " RESTRICT"
	}
	logger.Infow("Attempting database startup")

	// TODO: Handle ORA-16005: database requires recovery.
	// See https://docs.oracle.com/en/error-help/db/ora-16005/?r=19c for more details.
	stdout, stderr, err = runSQL(ctx, params, startupCmd)
	if err != nil {
		return stdout, stderr, fmt.Errorf("startup command failed: %w", err)
	}

	if strings.Contains(stdout, startupSuccess) {
		logger.Infow("Database started and opened successfully")
		return stdout, stderr, nil
	}

	if strings.Contains(stdout, alreadyRunning) {
		logger.Infow("Database is already running, checking status (MOUNTED or OPEN)")
		statusStdOut, statusStdErr, statusErr := runSQL(ctx, params, "SELECT status FROM v$instance;")
		if statusErr != nil {
			return statusStdOut, statusStdErr, fmt.Errorf("failed to get instance status: %w", statusErr)
		}

		if statusStdOut == "OPEN" {
			logger.Infow("Database is already OPEN")
			return statusStdOut, statusStdErr, nil
		}

		logger.Infow("Database is not OPEN, attempting to open", "current_status", statusStdOut)
		alterStdOut, alterStdErr, alterErr := runSQL(ctx, params, "WHENEVER SQLERROR EXIT FAILURE\nALTER DATABASE OPEN")

		if alterErr != nil {
			return alterStdOut, alterStdErr, fmt.Errorf("alter database open failed: %w", alterErr)
		}
		logger.Infow("Database opened successfully")
		return alterStdOut, alterStdErr, nil
	}

	return stdout, stderr, fmt.Errorf("startup command returned unexpected output: %s", stdout)
}
