/*
Copyright 2024 Google LLC

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

// Package main serves as the Main entry point for the workload Agent.
package main

import (
	"github.com/GoogleCloudPlatform/workloadagent/internal/agentmain"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"

	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

func main() {
	opts := agentmain.Options{
		Name:      "google_cloud_workload_agent",
		ShortDesc: "Google Cloud Agent for Compute Workloads",
		LongDesc:  "Google Cloud Agent for Compute Workloads",
		DaemonCreator: func(lp log.Parameters, cp *cpb.CloudProperties) *daemon.Daemon {
			return daemon.NewDaemon(lp, cp)
		},
	}
	agentmain.Run(opts)
}
