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

// Package agent provides a shared entry point for the workload Agent.
package agent

import (
	"context"
	"os"
	"runtime"

	"strings"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"github.com/spf13/pflag"
	"go.uber.org/zap/zapcore"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon"
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime/configure"
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime/logusage"
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime/migrate"
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime"
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime/status"
	"github.com/GoogleCloudPlatform/workloadagent/internal/onetime/version"
	"github.com/GoogleCloudPlatform/workloadagent/internal/openshiftmetrics/clients/openshift"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce/metadataserver"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

// Options defines the configuration for the agent.
type Options struct {
	Name          string
	ShortDesc     string
	LongDesc      string
	DaemonCreator func(log.Parameters, *cpb.CloudProperties) *daemon.Daemon
}

// Run executes the agent with the provided options.
func Run(opts Options) {
	// Trim space from arguments to avoid issues with parsing. We have seen cases where the arguments
	// have leading or trailing spaces (e.g. when running as an extension).
	for i := range os.Args {
		os.Args[i] = strings.TrimSpace(os.Args[i])
	}

	ctx := context.Background()
	lp := log.Parameters{
		OSType:     runtime.GOOS,
		Level:      zapcore.InfoLevel,
		LogToCloud: true,
	}

	cloudProps := &cpb.CloudProperties{}
	//TODO - Move OpenShift-specific Run logic to a separate main file.
	if os.Getenv("WLA_WORKLOAD") == "OPENSHIFT" {
		if cp := fetchCloudPropertiesFromOpenShift(ctx); cp != nil {
			cloudProps = &cpb.CloudProperties{
				ProjectId:    cp.ProjectID,
				Region:       cp.Region,
				InstanceName: cp.InstanceName,
				Zone:         cp.Zone,
			}

			log.Logger.Infow("Cloud Properties for OpenShift",
				"projectid", cloudProps.GetProjectId(),
				"region", cloudProps.GetRegion(),
				"instancename", cloudProps.GetInstanceName(),
				"zone", cloudProps.GetZone(),
			)
		}
	} else if cp := metadataserver.FetchCloudProperties(); cp != nil {
		cloudProps = &cpb.CloudProperties{
			ProjectId:           cp.ProjectID,
			InstanceId:          cp.InstanceID,
			Zone:                cp.Zone,
			Region:              cp.Region,
			InstanceName:        cp.InstanceName,
			Image:               cp.Image,
			NumericProjectId:    cp.NumericProjectID,
			MachineType:         cp.MachineType,
			Scopes:              cp.Scopes,
			ServiceAccountEmail: cp.ServiceAccountEmail,
		}
	}
	lp.CloudLoggingClient = log.CloudLoggingClient(ctx, cloudProps.GetProjectId())

	rootCmd := &cobra.Command{
		Use:   opts.Name,
		Short: opts.ShortDesc,
		Long:  opts.LongDesc,
	}
	rootCmd.AddCommand(version.NewCommand())
	rootCmd.AddCommand(logusage.NewCommand(lp, cloudProps))
	rootCmd.AddCommand(migrate.NewCommand())
	rootCmd.AddCommand(configure.NewCommand(lp))
	rootCmd.AddCommand(status.NewCommand(cloudProps))
	d := opts.DaemonCreator(lp, cloudProps)
	daemonCmd := daemon.NewDaemonSubCommand(d)

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

func fetchCloudPropertiesFromOpenShift(ctx context.Context) *metadataserver.CloudProperties {
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Logger.Errorw("Failed to get in-cluster config", "error", err)
		return nil
	}
	client, err := openshift.New(config)
	if err != nil {
		log.Logger.Errorw("Failed to create openshift client", "error", err)
		return nil
	}
	infra, err := client.GetInfrastructure()
	if err != nil {
		log.Logger.Errorw("Failed to get infrastructure from OpenShift API", "error", err)
		return nil
	}

	zone := "us-central1-a" // Placeholder value in case we fail to get the zone from a node.
	k8sClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Logger.Errorw("Failed to create kubernetes clientset", "error", err)
	} else {
		nodes, err := k8sClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: "node-role.kubernetes.io/worker"})
		if err != nil || len(nodes.Items) == 0 {
			// Fallback to any node if no worker nodes could be found.
			nodes, err = k8sClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		}
		if err != nil {
			log.Logger.Errorw("Failed to list nodes", "error", err)
		} else {
			for _, node := range nodes.Items {
				if z, ok := node.Labels["topology.kubernetes.io/zone"]; ok {
					zone = z
					break
				}
				if z, ok := node.Labels["failure-domain.beta.kubernetes.io/zone"]; ok {
					zone = z
					break
				}
			}
		}
	}

	return &metadataserver.CloudProperties{
		ProjectID:    infra.Status.PlatformStatus.GCP.ProjectID,
		Region:       infra.Status.PlatformStatus.GCP.Region,
		InstanceName: infra.Status.InfrastructureName,
		Zone:         zone,
	}
}
