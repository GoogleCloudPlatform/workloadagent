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
	"errors"
	"fmt"

	"go.uber.org/zap"

	anypb "google.golang.org/protobuf/types/known/anypb"
	codepb "google.golang.org/genproto/googleapis/rpc/code"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	gpb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/guestactions"
)

// commandResult creates a gpb.CommandResult with the given status code and message packed into the payload.
func commandResult(ctx context.Context, logger *zap.SugaredLogger, command *gpb.Command, stdout, stderr string, code codepb.Code, message string, execErr error) *gpb.CommandResult {
	anyPayload, err := anypb.New(&spb.Status{
		Code:    int32(code),
		Message: message,
	})
	if err != nil {
		logger.Warnw("Failed to pack payload", "error", err)
		// Fallback payload
		anyPayload, _ = anypb.New(&spb.Status{
			Code:    int32(codepb.Code_INTERNAL),
			Message: fmt.Sprintf("Failed to create original payload: %s", err.Error()),
		})
	}
	res := &gpb.CommandResult{
		Command:  command,
		Payload:  anyPayload,
		Stdout:   stdout,
		Stderr:   stderr,
		ExitCode: 0,
	}
	if execErr != nil {
		res.ExitCode = 1 // Generic failure code
	}
	return res
}

func validateParams(ctx context.Context, logger *zap.SugaredLogger, command *gpb.Command, params map[string]string) *gpb.CommandResult {
	if params == nil {
		errMsg := "Agent command parameters are missing"
		logger.Warnw(errMsg)
		return commandResult(ctx, logger, command, errMsg, "", codepb.Code_INVALID_ARGUMENT, errMsg, errors.New(errMsg))
	}
	for _, requiredParam := range []string{"oracle_sid", "oracle_home", "oracle_user"} {
		if val, ok := params[requiredParam]; !ok || val == "" {
			errMsg := fmt.Sprintf("Parameter %s is missing", requiredParam)
			logger.Warnw(errMsg)
			return commandResult(ctx, logger, command, errMsg, "", codepb.Code_INVALID_ARGUMENT, errMsg, errors.New(errMsg))
		}
	}
	return nil
}
