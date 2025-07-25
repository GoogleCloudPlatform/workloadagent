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
	"fmt"
	"runtime"

	"cloud.google.com/go/artifactregistry/apiv1"
	"github.com/spf13/cobra"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/commandlineexecutor"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/statushelper"

	spb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/status"
)

const (
	agentPackageName = "google-cloud-workload-agent"
)

// NewCommand creates a new status command.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Print the status of the agent",
		Long:  "Print the status of the agent",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			arClient, err := newARClient(ctx)
			if err != nil {
				return err
			}
			statushelper.PrintStatus(ctx, agentStatus(ctx, arClient, commandlineexecutor.ExecuteCommand), false)
			return nil
		},
	}
	return cmd
}

// newARClient creates a new artifact registry client.
func newARClient(ctx context.Context) (statushelper.ARClientInterface, error) {
	arClient, err := artifactregistry.NewClient(ctx)
	if err != nil {
		log.CtxLogger(ctx).Errorw("Could not create artifact registry client", "error", err)
		return nil, err
	}
	return &statushelper.ArtifactRegistryClient{Client: arClient}, nil
}

// agentStatus returns the agent version, enabled/running, config path, and the
// configuration as parsed by the agent.
func agentStatus(ctx context.Context, arClient statushelper.ARClientInterface, exec commandlineexecutor.Execute) *spb.AgentStatus {
	agentStatus := &spb.AgentStatus{
		AgentName:        agentPackageName,
		InstalledVersion: fmt.Sprintf("%s-%s", configuration.AgentVersion, configuration.AgentBuildChange),
	}

	var err error
	agentStatus.AvailableVersion, err = statushelper.LatestVersionArtifactRegistry(ctx, arClient, "workload-agent-products", "us", "google-cloud-workload-agent-x86-64", agentPackageName)
	if err != nil {
		log.CtxLogger(ctx).Errorw("Could not fetch latest version", "error", err)
		agentStatus.AvailableVersion = "Error: could not fetch latest version"
	}

	agentStatus.SystemdServiceEnabled = spb.State_FAILURE_STATE
	agentStatus.SystemdServiceRunning = spb.State_FAILURE_STATE
	enabled, running, err := statushelper.CheckAgentEnabledAndRunning(ctx, agentPackageName, runtime.GOOS, exec)
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
