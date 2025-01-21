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
	"context"
	"os"
	"runtime"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap/zapcore"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon"
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime/logusage"
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime"
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime/version"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/gce/metadataserver"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/log"

	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

func main() {
	ctx := context.Background()
	lp := log.Parameters{
		OSType:     runtime.GOOS,
		Level:      zapcore.InfoLevel,
		LogToCloud: true,
	}

	cloudProps := &cpb.CloudProperties{}
	if cp := metadataserver.FetchCloudProperties(); cp != nil {
		cloudProps = &cpb.CloudProperties{
			ProjectId:        cp.ProjectID,
			InstanceId:       cp.InstanceID,
			Zone:             cp.Zone,
			InstanceName:     cp.InstanceName,
			Image:            cp.Image,
			NumericProjectId: cp.NumericProjectID,
			MachineType:      cp.MachineType,
		}
	}
	lp.CloudLoggingClient = log.CloudLoggingClient(ctx, cloudProps.GetProjectId())

	rootCmd := &cobra.Command{
		Use:   "google_cloud_workload_agent",
		Short: "Google Cloud Workload Agent",
		Long:  "Google Cloud Workload Agent",
	}
	rootCmd.AddCommand(version.NewCommand())
	rootCmd.AddCommand(logusage.NewCommand(lp, cloudProps))
	d := daemon.NewDaemon(lp, cloudProps)
	p := daemon.NewPlugin(d)
	daemonCmd := daemon.NewDaemonSubCommand(d)
	pluginCmd := daemon.NewPluginSubcommand(p)
	daemon.PopulatePluginFlagValues(p, pluginCmd.Flags())
	rootCmd.AddCommand(pluginCmd)
	// When running on windows, the daemon is started using the winservice subcommand.
	// Having both the daemon command and the winservice command will cause an error when the
	// winservice tries to start the daemon, cobra will start the parent which is the winservice
	// causing a loop.
	if lp.OSType != "windows" {
		rootCmd.AddCommand(daemonCmd)
	}
	// Add any additional windows or linux specific subcommands.
	rootCmd.AddCommand(additionalSubcommands(ctx, daemonCmd)...)

	for _, cmd := range rootCmd.Commands() {
		if cmd.Name() != "startdaemon" {
			onetime.Register(lp.OSType, cmd)
		}
	}

	rootCmd.SetArgs(pflag.Args())
	rc := 0
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		log.Logger.Error(err)
		rc = 1
	}

	// Defer cloud log flushing to ensure execution on any exit from main.
	defer func() {
		if lp.CloudLoggingClient != nil {
			flushTimer := time.AfterFunc(5*time.Second, func() {
				log.Logger.Error("Cloud logging client failed to flush logs within the 5-second deadline, exiting.")
				os.Exit(rc)
			})
			log.FlushCloudLog()
			lp.CloudLoggingClient.Close()
			flushTimer.Stop()
		}
	}()
	os.Exit(rc)
}
