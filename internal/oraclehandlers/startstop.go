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
	"errors"
	"fmt"
	"strings"

	"go.uber.org/zap"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce/metadataserver"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"

	codepb "google.golang.org/genproto/googleapis/rpc/code"
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
	// This message indicates that media recovery is already active.
	// ORA-01153: an incompatible media recovery is active
	recoveryActive = "ORA-01153"

	// This command is used to start managed recovery on a standby database.
	recoverCmd = "ALTER DATABASE RECOVER MANAGED STANDBY DATABASE DISCONNECT FROM SESSION USING CURRENT LOGFILE;"
)

// StopDatabase implements the oracle_stop_database guest action.
// It attempts to shut down the database gracefully using "SHUTDOWN IMMEDIATE".
// If the command fails, it returns an error in the payload.
func StopDatabase(ctx context.Context, command *gpb.Command, cloudProperties *metadataserver.CloudProperties) *gpb.CommandResult {
	params := command.GetAgentCommand().GetParameters()
	logger := log.CtxLogger(ctx)
	if result := validateParams(ctx, logger, command, params, []string{"oracle_sid", "oracle_home", "oracle_user"}); result != nil {
		return result
	}
	logger = logger.With("oracle_sid", params["oracle_sid"], "oracle_home", params["oracle_home"], "oracle_user", params["oracle_user"])
	logger.Infow("oracle_stop_database handler called")

	stdout, stderr, err := stopDatabase(ctx, logger, params)
	if err != nil {
		logger.Warnw("stopDatabase failed", "stdout", stdout, "stderr", stderr, "error", err)
		return commandResult(ctx, logger, command, stdout, stderr, codepb.Code_FAILED_PRECONDITION, err.Error(), err)
	}
	return commandResult(ctx, logger, command, stdout, stderr, codepb.Code_OK, "Database stopped successfully", nil)
}

// stopDatabase contains the core logic for stopping the database.
func stopDatabase(ctx context.Context, logger *zap.SugaredLogger, params map[string]string) (stdout, stderr string, err error) {
	shutdownCmd := "SHUTDOWN IMMEDIATE"

	// Check for Standby Role to attempt canceling managed recovery if it's running.
	if roleOut, _, err := runSQL(ctx, params, "SELECT database_role FROM v$database;", 120, true); err != nil {
		logger.Infow("Could not determine database role (it might not be mounted/open), proceeding with standard shutdown", "error", err)
	} else if strings.Contains(roleOut, "PHYSICAL STANDBY") {
		logger.Infow("Database is a Physical Standby. Attempting to cancel managed recovery before shutdown", "role", roleOut)
		shutdownCmd = "ALTER DATABASE RECOVER MANAGED STANDBY DATABASE CANCEL;\nSHUTDOWN IMMEDIATE"
	}

	// failOnSqlError is set to false because we want to handle ORA-01034 (already down) gracefully.
	stdout, stderr, err = runSQL(ctx, params, shutdownCmd, 120, false)
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
func StartDatabase(ctx context.Context, command *gpb.Command, cloudProperties *metadataserver.CloudProperties) *gpb.CommandResult {
	params := command.GetAgentCommand().GetParameters()
	logger := log.CtxLogger(ctx)
	if result := validateParams(ctx, logger, command, params, []string{"oracle_sid", "oracle_home", "oracle_user"}); result != nil {
		return result
	}
	logger = logger.With("oracle_sid", params["oracle_sid"], "oracle_home", params["oracle_home"], "oracle_user", params["oracle_user"])
	logger.Infow("oracle_start_database handler called")

	// TODO: Handle Data Guard standby databases.
	// Cases to consider:
	// 1. Active Data Guard: Open read-only.
	// 2. Manual standby (no broker): Initiate managed recovery.
	stdout, stderr, err := startDatabase(ctx, logger, params)
	if err != nil {
		logger.Warnw("startDatabase failed", "stdout", stdout, "stderr", stderr, "error", err)
		return commandResult(ctx, logger, command, stdout, stderr, codepb.Code_FAILED_PRECONDITION, err.Error(), err)
	}
	return commandResult(ctx, logger, command, stdout, stderr, codepb.Code_OK, "Database started successfully", nil)
}

// startDatabase contains the core logic for starting the database.
func startDatabase(ctx context.Context, logger *zap.SugaredLogger, params map[string]string) (stdout, stderr string, err error) {
	startupCmd := "STARTUP MOUNT"
	if val, ok := params["restrict"]; ok && strings.EqualFold(val, "true") {
		startupCmd += " RESTRICT"
	}
	logger.Debugw("Attempting database startup", "startup_cmd", startupCmd)

	// failOnSqlError is set to false because we want to handle ORA-01081 (already running) gracefully.
	startupStdOut, startupStderr, startupErr := runSQL(ctx, params, startupCmd, 120, false)
	if startupErr != nil {
		return startupStdOut, startupStderr, fmt.Errorf("startup command failed: %w", startupErr)
	}
	if strings.Contains(startupStdOut, "ORA-") && !strings.Contains(startupStdOut, alreadyRunning) {
		return startupStdOut, startupStderr, fmt.Errorf("startup failed with unexpected Oracle error: %s", startupStdOut)
	}

	// At this point, database should be at least MOUNTED, or OPEN.
	// Check the database role and open_mode to decide next steps.
	roleStdOut, roleStderr, roleErr := runSQL(ctx, params, "SELECT database_role FROM v$database;", 60, true)
	if roleErr != nil {
		return roleStdOut, roleStderr, fmt.Errorf("failed to detect database role: %w", roleErr)
	}
	currentRole := strings.TrimSpace(roleStdOut)
	openModeStdOut, openModeStderr, openModeErr := runSQL(ctx, params, "SELECT open_mode FROM v$database;", 60, true)
	if openModeErr != nil {
		return openModeStdOut, openModeStderr, fmt.Errorf("failed to detect open mode: %w", openModeErr)
	}
	currentOpenMode := strings.TrimSpace(openModeStdOut)

	switch strings.ToUpper(currentRole) {
	case "PRIMARY":
		logger.Infow("Handling primary database", "role", currentRole, "open_mode", currentOpenMode)
		return handlePrimary(ctx, logger, params, currentOpenMode)
	case "PHYSICAL STANDBY":
		logger.Infow("Handling physical standby database", "role", currentRole, "open_mode", currentOpenMode)
		return handleStandby(ctx, logger, params, currentOpenMode)
	default:
		logger.Warnw("Unsupported database role", "role", currentRole, "open_mode", currentOpenMode)
		return startupStdOut, startupStderr, fmt.Errorf("unsupported database role: %s", currentRole)
	}
}

// handlePrimary handles startup logic for a primary database.
func handlePrimary(ctx context.Context, logger *zap.SugaredLogger, params map[string]string, openMode string) (stdout, stderr string, err error) {
	if openMode == "MOUNTED" {
		logger.Infow("Primary database is mounted; attempting to open it")
		return runSQL(ctx, params, "ALTER DATABASE OPEN;", 120, true)
	}
	logger.Infow("Primary database has been started successfully", "open_mode", openMode)
	return "Primary database has been started successfully", "", nil
}

// handleStandby handles startup logic for a standby database.
// For Active Data Guard, we must open the database READ ONLY before starting recovery to avoid ORA-10456.
// For Standard Standby, we skip opening and start recovery while the database remains MOUNTED.
func handleStandby(ctx context.Context, logger *zap.SugaredLogger, params map[string]string, openMode string) (stdout, stderr string, err error) {
	// If open_standby_for_adg is true, we treat it as an Active Data Guard request.
	// Default to false if the parameter is not set.
	openADG := false
	if val, ok := params["open_standby_for_adg"]; ok {
		openADG = strings.EqualFold(val, "true")
	}

	if openADG {
		switch strings.ToUpper(strings.TrimSpace(openMode)) {
		case "MOUNTED":
			logger.Infow("Opening standby database in READ ONLY mode for Active Data Guard", "open_mode", openMode, "open_standby_for_adg", openADG)
			if stdout, stderr, err := runSQL(ctx, params, "ALTER DATABASE OPEN READ ONLY;", 120, true); err != nil {
				return stdout, stderr, fmt.Errorf("failed to open standby database in READ ONLY mode: %w", err)
			}
		case "READ ONLY":
			logger.Infow("Standby database is already open in READ ONLY mode, proceeding to start recovery", "open_mode", openMode, "open_standby_for_adg", openADG)
		case "READ ONLY WITH APPLY":
			// This is a special case where the database is already open READ ONLY and the Managed Recovery Process is active.
			// This can happen if the database was opened READ ONLY with APPLY before the agent took over.
			// In this case, we do not need to start recovery.
			msg := "The database is already open in Active Data Guard mode, and the Managed Recovery Process is active, no action is required"
			logger.Infow(msg, "open_mode", openMode, "open_standby_for_adg", openADG)
			return msg, "", nil // Already in the desired state.
		default:
			msg := "Standby database is not in READ ONLY mode, cannot start recovery"
			logger.Infow(msg, "open_mode", openMode, "open_standby_for_adg", openADG)
			return "", "", errors.New(msg)
		}
	}

	logger.Infow("Starting managed recovery for standby database", "open_mode", openMode, "open_standby_for_adg", openADG)
	return startRecovery(ctx, logger, params)
}

func startRecovery(ctx context.Context, logger *zap.SugaredLogger, params map[string]string) (string, string, error) {
	stdout, stderr, err := runSQL(ctx, params, recoverCmd, 120, true)
	if err != nil {
		if strings.Contains(stdout, recoveryActive) {
			logger.Infow("Recovery is already active, no action is required", "stdout", stdout, "stderr", stderr)
			return stdout, stderr, nil
		}
		return stdout, stderr, fmt.Errorf("failed to start recovery on standby: %w", err)
	}
	logger.Infow("Managed recovery started successfully")
	return stdout, stderr, nil
}
