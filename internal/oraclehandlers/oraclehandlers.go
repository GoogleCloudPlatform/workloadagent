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
	"sync"

	"go.uber.org/zap"

	anypb "google.golang.org/protobuf/types/known/anypb"
	codepb "google.golang.org/genproto/googleapis/rpc/code"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	gpb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/guestactions"
)

// OracleHandler holds shared state for Oracle operations like in-process locking,
// in order to prevent concurrent operations on the same database instance (SID).
type OracleHandler struct {
	opMu sync.Mutex
	// activeOps tracks the oracle_sids that are currently being operated on (sid -> commandName).
	activeOps map[string]string
}

// New creates a new OracleHandler instance.
func New() *OracleHandler {
	return &OracleHandler{
		activeOps: make(map[string]string),
	}
}

// lockDatabase implements an in-memory lock to prevent concurrent agent operations
// (e.g., oracle_start_database, oracle_stop_database) on the same Oracle SID.
// It uses a per-SID locking mechanism to allow concurrent operations on different
// database SIDs, but preventing multiple operations on the same SID simultaneously.
// If an operation is already in progress for the SID specified in the command,
// lockDatabase returns an ABORTED result immediately.
// If no operation is in progress, it registers the current operation as active
// and returns:
// 1. A cleanup function to release the lock, which should be deferred by the caller.
// 2. A nil CommandResult.
func (h *OracleHandler) lockDatabase(ctx context.Context, logger *zap.SugaredLogger, command *gpb.Command) (func(), *gpb.CommandResult) {
	params := command.GetAgentCommand().GetParameters()
	commandName := command.GetAgentCommand().GetCommand()

	h.opMu.Lock()
	if op, ok := h.activeOps[params["oracle_sid"]]; ok {
		h.opMu.Unlock()
		logger.Errorw("Operation already in progress")
		anyPayload, err := anypb.New(&spb.Status{
			Code:    int32(codepb.Code_ABORTED),
			Message: fmt.Sprintf("operation %q already in progress for database %s", op, params["oracle_sid"]),
		})
		if err != nil {
			logger.Warnw("Failed to pack payload", "error", err)
		}
		return func() {}, &gpb.CommandResult{
			Command:  command,
			ExitCode: 1, // Signal guestactions framework that the command failed.
			Payload:  anyPayload,
		}
	}
	h.activeOps[params["oracle_sid"]] = commandName
	h.opMu.Unlock()

	return func() {
		h.opMu.Lock()
		delete(h.activeOps, params["oracle_sid"])
		h.opMu.Unlock()
	}, nil
}

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
