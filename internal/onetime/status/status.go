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
	"os"
	"runtime"
	"slices"

	"cloud.google.com/go/artifactregistry/apiv1"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/configuration"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/commandlineexecutor"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/statushelper"

	spb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/status"
)

const (
	agentPackageName = "google-cloud-workload-agent"
	requiredScope    = "https://www.googleapis.com/auth/cloud-platform"
)

// NewCommand creates a new status command.
func NewCommand(cloudProps *cpb.CloudProperties) *cobra.Command {
	var config string
	var compact bool
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Print the status of the agent",
		Long:  "Print the status of the agent, including version, service status, and configuration validity.",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			arClient, err := newARClient(ctx)
			if err != nil {
				return err
			}
			if c, ok := arClient.(*statushelper.ArtifactRegistryClient); ok {
				defer c.Client.Close()
			}
			status := agentStatus(ctx, arClient, commandlineexecutor.ExecuteCommand, cloudProps, config, os.ReadFile)
			statushelper.PrintStatus(ctx, status, compact)
			return nil
		},
	}
	cmd.Flags().StringVar(&config, "config", "", "Configuration path override")
	cmd.Flags().BoolVar(&compact, "compact", false, "Compact output")
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
func agentStatus(ctx context.Context, arClient statushelper.ARClientInterface, exec commandlineexecutor.Execute, cloudProps *cpb.CloudProperties, config string, readFile func(string) ([]byte, error)) *spb.AgentStatus {
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

	if cloudProps == nil {
		log.CtxLogger(ctx).Errorw("Could not fetch cloud properties from metadata server. This may be because the agent is not running on a GCE VM, or the metadata server is not reachable.")
		agentStatus.CloudApiAccessFullScopesGranted = spb.State_ERROR_STATE
	} else {
		if slices.Contains(cloudProps.GetScopes(), requiredScope) {
			agentStatus.CloudApiAccessFullScopesGranted = spb.State_SUCCESS_STATE
		} else {
			agentStatus.CloudApiAccessFullScopesGranted = spb.State_FAILURE_STATE
		}
		agentStatus.InstanceUri = fmt.Sprintf("projects/%s/zones/%s/instances/%s", cloudProps.GetProjectId(), cloudProps.GetZone(), cloudProps.GetInstanceId())
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

	path := config
	if len(path) == 0 {
		switch runtime.GOOS {
		case "linux":
			path = configuration.LinuxConfigPath
		case "windows":
			path = configuration.WindowsConfigPath
		}
	}
	agentStatus.ConfigurationFilePath = path
	content, err := readFile(path)
	if err != nil {
		agentStatus.ConfigurationValid = spb.State_FAILURE_STATE
		agentStatus.ConfigurationErrorMessage = err.Error()
	} else {
		configProto := &cpb.Configuration{}
		if err := protojson.Unmarshal(content, configProto); err != nil {
			agentStatus.ConfigurationValid = spb.State_FAILURE_STATE
			agentStatus.ConfigurationErrorMessage = err.Error()
		} else {
			agentStatus.ConfigurationValid = spb.State_SUCCESS_STATE
		}
	}

	agentStatus.KernelVersion, err = statushelper.KernelVersion(ctx, runtime.GOOS, exec)
	if err != nil && runtime.GOOS == "linux" {
		log.CtxLogger(ctx).Errorw("Could not fetch kernel version", "error", err)
	}
	return agentStatus
}
