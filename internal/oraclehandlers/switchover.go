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
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/commandlineexecutor"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce/metadataserver"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"

	codepb "google.golang.org/genproto/googleapis/rpc/code"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	gpb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/guestactions"
)

const (
	switchoverDefaultTimeout = 2 * time.Minute
	validateDatabaseTimeout  = 60 * time.Second
	// ORA-16558: database specified for switchover is not a standby database.
	// This indicates that the database is likely already the primary.
	errorNotStandby = "ORA-16558"
)

// runDGMGRL executes the given DGMGRL commands and returns the result.
var runDGMGRL = func(ctx context.Context, params map[string]string, dbUser, password, connectString, command string, timeout time.Duration) (string, string, error) {
	oracleHome := params["oracle_home"]
	oracleUser := params["oracle_user"]

	args := []string{"-silent"}

	result := executeCommand(ctx, commandlineexecutor.Params{
		Executable: filepath.Join(oracleHome, "bin", "dgmgrl"),
		Args:       args,
		Env:        []string{"ORACLE_HOME=" + oracleHome, "LD_LIBRARY_PATH=" + filepath.Join(oracleHome, "lib")},
		User:       oracleUser,
		Stdin:      fmt.Sprintf("CONNECT %s/\"%s\"@\"%s\";\n%s", dbUser, password, connectString, command),
		Timeout:    int(timeout.Seconds()),
	})

	stdout := strings.TrimSpace(result.StdOut)
	stderr := result.StdErr

	if result.Error != nil {
		if result.ExitCode != 0 {
			return stdout, stderr, fmt.Errorf("dgmgrl exited with code %d: %s", result.ExitCode, stderr)
		}
		return stdout, stderr, result.Error
	}
	return stdout, stderr, nil
}

// DataGuardSwitchover returns a handler for the oracle_data_guard_switchover guest action.
func DataGuardSwitchover(config *cpb.Configuration) func(context.Context, *gpb.Command, *metadataserver.CloudProperties) *gpb.CommandResult {
	return func(ctx context.Context, command *gpb.Command, cloudProperties *metadataserver.CloudProperties) *gpb.CommandResult {
		params := command.GetAgentCommand().GetParameters()
		logger := log.CtxLogger(ctx)

		if result := validateParams(ctx, logger, command, params, []string{"oracle_sid", "oracle_home", "oracle_user", "standby_db_unique_name"}); result != nil {
			return result
		}

		logger = logger.With("oracle_sid", params["oracle_sid"], "oracle_home", params["oracle_home"], "oracle_user", params["oracle_user"], "standby_db_unique_name", params["standby_db_unique_name"])
		logger.Infow("oracle_data_guard_switchover handler called")

		connectionParams := config.GetOracleConfiguration().GetOracleHandlers().GetConnectionParameters()
		if connectionParams == nil {
			errMsg := "oracle connection parameters are missing"
			logger.Warnw(errMsg)
			return commandResult(ctx, logger, command, errMsg, "", codepb.Code_INVALID_ARGUMENT, errMsg, errors.New(errMsg))
		}

		standbyDB := params["standby_db_unique_name"]

		validationCmd := fmt.Sprintf(`VALIDATE DATABASE '%s';`, standbyDB)

		gceClient, err := newGCEClient(ctx)
		if err != nil {
			logger.Warnw("Failed to create GCE client", "error", err)
			return commandResult(ctx, logger, command, "", "", codepb.Code_INTERNAL, "Failed to create GCE client", err)
		}
		password, err := retrievePassword(ctx, gceClient, connectionParams)
		if err != nil {
			logger.Warnw("Failed to retrieve password from Secret Manager", "error", err)
			return commandResult(ctx, logger, command, "", "", codepb.Code_INTERNAL, "Failed to retrieve password from Secret Manager", err)
		}

		connectionString := easyConnectString(connectionParams)
		dbUser := connectionParams.GetUsername()
		logger.Debugw("Connecting to DGMGRL", "connect_string", connectionString)

		stdout, stderr, err := runDGMGRL(ctx, params, dbUser, password, connectionString, validationCmd, validateDatabaseTimeout)
		if err != nil {
			logger.Warnw("DGMGRL validation execution failed", "error", err, "stdout", stdout, "stderr", stderr)
			return commandResult(ctx, logger, command, stdout, stderr, codepb.Code_INTERNAL, "DGMGRL validation execution failed", err)
		}

		readyRegex := regexp.MustCompile(`(?i)Ready\s+for\s+Switchover\s*:\s*Yes`)
		if !readyRegex.MatchString(stdout) {
			errMsg := fmt.Sprintf("validation failed for standby database %s", standbyDB)
			logger.Warnw(errMsg, "stdout", stdout, "stderr", stderr)
			return commandResult(ctx, logger, command, stdout, stderr, codepb.Code_FAILED_PRECONDITION, errMsg, errors.New(errMsg))
		}

		logger.Infow("Database validation successful, proceeding with switchover")

		switchoverCmd := fmt.Sprintf("SWITCHOVER TO '%s';", standbyDB)
		stdout, stderr, err = runDGMGRL(ctx, params, dbUser, password, connectionString, switchoverCmd, switchoverDefaultTimeout)

		// if the database is already primary, DGMGRL will return ORA-16558.
		if strings.Contains(stdout, errorNotStandby) {
			logger.Infow("Database is already a primary database (ORA-16558), switchover considered successful", "stdout", stdout)
			return commandResult(ctx, logger, command, stdout, stderr, codepb.Code_OK, "Switchover completed successfully", nil)
		}

		if err != nil {
			logger.Warnw("DGMGRL switchover execution failed", "error", err, "stdout", stdout, "stderr", stderr)
			return commandResult(ctx, logger, command, stdout, stderr, codepb.Code_INTERNAL, "DGMGRL switchover execution failed", err)
		}

		successRegex := regexp.MustCompile(`(?i)Switchover\s+succeeded`)
		if !successRegex.MatchString(stdout) {
			errMsg := "switchover failed or status unknown."
			logger.Warnw(errMsg, "stdout", stdout, "stderr", stderr)
			return commandResult(ctx, logger, command, stdout, stderr, codepb.Code_INTERNAL, errMsg, errors.New(errMsg))
		}

		logger.Infow("Switchover completed successfully")
		return commandResult(ctx, logger, command, stdout, stderr, codepb.Code_OK, "Switchover completed successfully", nil)
	}
}

// newGCEClient creates a new GCE client.
var newGCEClient = gce.NewGCEClient

// retrievePassword fetches the password from Secret Manager using the GCE client.
var retrievePassword = func(ctx context.Context, client *gce.GCE, params *cpb.ConnectionParameters) (string, error) {
	if params.GetSecret() == nil {
		return "", errors.New("secret is missing in connection parameters")
	}
	return client.GetSecret(ctx, params.GetSecret().GetProjectId(), params.GetSecret().GetSecretName())
}

// easyConnectString creates an easy connect string for the given connection parameters.
func easyConnectString(params *cpb.ConnectionParameters) string {
	// easyConnectString format: //hostname:port/service_name
	return fmt.Sprintf("//%s:%d/%s", params.GetHost(), params.GetPort(), params.GetServiceName())
}
