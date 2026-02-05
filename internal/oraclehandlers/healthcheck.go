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
	"regexp"

	"go.uber.org/zap"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce/metadataserver"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"

	codepb "google.golang.org/genproto/googleapis/rpc/code"
	gpb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/guestactions"
)

var errorRegex = regexp.MustCompile(`ORA-\d+`)

const healthCheckTimeoutSeconds = 15

// HealthCheck checks if the database is healthy by executing a simple SQL query.
func HealthCheck(ctx context.Context, command *gpb.Command, cloudProperties *metadataserver.CloudProperties) *gpb.CommandResult {
	params := command.GetAgentCommand().GetParameters()
	logger := log.CtxLogger(ctx)
	if result := validateParams(ctx, logger, command, params, []string{"oracle_sid", "oracle_home", "oracle_user"}); result != nil {
		return result
	}
	logger = logger.With("oracle_sid", params["oracle_sid"], "oracle_home", params["oracle_home"], "oracle_user", params["oracle_user"])
	logger.Debugw("oracle_health_check handler called")

	stdout, stderr, err := healthCheck(ctx, logger, params)
	if err != nil {
		logger.Warnw("healthCheck failed", "stdout", stdout, "stderr", stderr, "error", err)
		return commandResult(ctx, logger, command, stdout, stderr, codepb.Code_FAILED_PRECONDITION, err.Error(), err)
	}
	logger.Debugw("Database is healthy")
	return commandResult(ctx, logger, command, stdout, stderr, codepb.Code_OK, "Database is healthy", nil)
}

func healthCheck(ctx context.Context, logger *zap.SugaredLogger, params map[string]string) (stdout, stderr string, err error) {
	stdout, stderr, err = runSQL(ctx, params, "SELECT 1 FROM dual;", healthCheckTimeoutSeconds, true)
	if err != nil {
		return stdout, stderr, fmt.Errorf("health check failed: %w", err)
	}
	if errorRegex.MatchString(stdout) {
		return stdout, stderr, fmt.Errorf("health check failed: %s", stdout)
	}
	return stdout, stderr, nil
}
