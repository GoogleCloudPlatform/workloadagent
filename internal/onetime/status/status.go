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

// Package status provides information on the agent, configuration, IAM and functional statuses.
package status

import (
	"context"
	"runtime"

	"github.com/spf13/cobra"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/commandlineexecutor"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/statushelper"

	spb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/status"
)

const agentPackageName = "google-cloud-workload-agent"

// Status stores the status command parameters.
type Status struct {
	exec commandlineexecutor.Execute
}

// NewCommand creates a new status command.
func NewCommand() *cobra.Command {
	s := &Status{}
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Print the status of the agent",
		Long:  "Print the status of the agent",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			s.exec = commandlineexecutor.ExecuteCommand
			statushelper.PrintStatus(ctx, s.agentStatus(ctx), false)
			return nil
		},
	}
	return cmd
}

// agentStatus returns the agent version, enabled/running, config path, and the
// configuration as parsed by the agent.
func (s *Status) agentStatus(ctx context.Context) *spb.AgentStatus {
	agentStatus := &spb.AgentStatus{}
	agentStatus.SystemdServiceEnabled = spb.State_FAILURE_STATE
	agentStatus.SystemdServiceRunning = spb.State_FAILURE_STATE
	enabled, running, err := statushelper.CheckAgentEnabledAndRunning(ctx, agentPackageName, runtime.GOOS, s.exec)
	if err != nil {
		log.CtxLogger(ctx).Errorw("Could not check agent enabled and running", "error", err)
		agentStatus.SystemdServiceEnabled = spb.State_ERROR_STATE
		agentStatus.SystemdServiceRunning = spb.State_ERROR_STATE
	} else {
		if enabled {
			agentStatus.SystemdServiceEnabled = spb.State_SUCCESS_STATE
		}
		if running {
			agentStatus.SystemdServiceRunning = spb.State_SUCCESS_STATE
		}
	}
	return agentStatus
}
