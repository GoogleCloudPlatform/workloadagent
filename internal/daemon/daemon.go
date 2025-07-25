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

// Package daemon implements daemon mode execution subcommand in Workload Agent.
package daemon

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/configuration"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/mongodb"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/mysql"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/openshift"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/oracle"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/postgres"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/redis"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/sqlserver"
	"github.com/GoogleCloudPlatform/workloadagent/internal/databasecenter"
	"github.com/GoogleCloudPlatform/workloadagent/internal/servicecommunication/datawarehouseactivation"
	"github.com/GoogleCloudPlatform/workloadagent/internal/servicecommunication/discovery"
	"github.com/GoogleCloudPlatform/workloadagent/internal/servicecommunication"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	"github.com/GoogleCloudPlatform/workloadagent/internal/workloadmanager"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/osinfo"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/recovery"

	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

// Daemon has args for startdaemon subcommand.
type Daemon struct {
	configFilePath string
	lp             log.Parameters
	config         *cpb.Configuration
	cloudProps     *cpb.CloudProperties
	osData         osinfo.Data
	services       []Service
}

type (
	// Service defines the common interface for workload services.
	// Start methods are used to start the workload monitoring services.
	Service interface {
		Start(ctx context.Context, a any)
		String() string
		ErrorCode() int
		ExpectedMinDuration() time.Duration
	}
)

var (
	configFileReader = func(path string) (io.ReadCloser, error) {
		file, err := os.Open(path)
		var f io.ReadCloser = file
		return f, err
	}
)

// NewDaemon creates a new Daemon.
func NewDaemon(lp log.Parameters, cloudProps *cpb.CloudProperties) *Daemon {
	return &Daemon{
		lp:         lp,
		cloudProps: cloudProps,
	}
}

// NewDaemonSubCommand creates a new startdaemon subcommand.
func NewDaemonSubCommand(d *Daemon) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "startdaemon",
		Short: "Start daemon mode of the agent",
		Long:  "startdaemon [--config <path-to-config-file>]",
		RunE: func(cmd *cobra.Command, args []string) error {
			return d.Execute(cmd.Context())
		},
	}
	cmd.Flags().StringVar(&d.configFilePath, "config", "", "configuration path for startdaemon mode")
	cmd.Flags().StringVar(&d.configFilePath, "c", "", "configuration path for startdaemon mode")
	return cmd
}

// Execute runs the daemon command.
func (d *Daemon) Execute(ctx context.Context) error {
	// Configure daemon logging with default values until the config file is loaded.
	d.lp.CloudLogName = `google-cloud-workload-agent`
	d.lp.LogFileName = `/var/log/google-cloud-workload-agent.log`
	if d.lp.OSType == "windows" {
		logDir := fmt.Sprintf(`%s\Google\google-cloud-workload-agent\logs`, log.CreateWindowsLogBasePath())
		d.lp.LogFileName = fmt.Sprintf(`%s\google-cloud-workload-agent.log`, logDir)
		os.MkdirAll(logDir, 0755)
		os.Chmod(logDir, 0777)
	}
	log.SetupLogging(d.lp)

	osData, err := osinfo.ReadData(ctx, osinfo.FileReadCloser(configFileReader), osinfo.OSName, osinfo.OSReleaseFilePath)
	if err != nil {
		log.Logger.Errorw("Unable to read OS data. Agent services may be impacted.", "error", err)
	}
	d.osData = osData

	// Run the daemon handler that will start any services
	ctx, cancel := context.WithCancel(ctx)
	return d.startdaemonHandler(ctx, cancel)
}

func (d *Daemon) startdaemonHandler(ctx context.Context, cancel context.CancelFunc) error {
	var err error
	d.config, err = configuration.Load(d.configFilePath, os.ReadFile, d.cloudProps)
	if err != nil {
		if d.lp.OSType == "windows" {
			// Windows gobbles up the error message, so we need to log it separately and exit.
			log.Logger.Error("Invalid configuration file, please fix the configuration file and restart the service.")
			log.Logger.Error(err)
			usagemetrics.Misconfigured()
			os.Exit(1)
		}
		return fmt.Errorf("loading %s configuration file, please fix the configuration file and restart the service: %w", d.configFilePath, err)
	}

	d.lp.LogToCloud = d.config.GetLogToCloud()
	d.lp.Level = configuration.LogLevelToZapcore(d.config.GetLogLevel())
	if d.config.GetCloudProperties().GetProjectId() != "" {
		d.lp.CloudLoggingClient = log.CloudLoggingClient(ctx, d.config.GetCloudProperties().GetProjectId())
	}
	if d.lp.CloudLoggingClient != nil {
		defer d.lp.CloudLoggingClient.Close()
	}

	log.SetupLogging(d.lp)

	// Get vCPU count and memory size from GCE
	gceClient, err := gce.NewGCEClient(ctx)
	if err != nil {
		log.Logger.Errorw("Error creating GCE client", "error", err)
	}
	vCPU, memorySize, err := gceClient.GetInstanceCPUAndMemorySize(ctx, d.cloudProps.GetProjectId(), d.cloudProps.GetZone(), d.cloudProps.GetInstanceName())
	if err != nil {
		log.Logger.Errorw("Error getting vCPU and memory size from GCE", "error", err)
	}
	// add vCPU and memory size to cloudProps
	d.cloudProps.VcpuCount = vCPU
	d.cloudProps.MemorySizeMb = memorySize

	log.Logger.Infow("Starting daemon mode", "agent_name", configuration.AgentName, "agent_version", configuration.AgentVersion)
	log.Logger.Infow("Cloud Properties",
		"projectid", d.cloudProps.GetProjectId(),
		"projectnumber", d.cloudProps.GetNumericProjectId(),
		"instanceid", d.cloudProps.GetInstanceId(),
		"zone", d.cloudProps.GetZone(),
		"region", d.cloudProps.GetRegion(),
		"instancename", d.cloudProps.GetInstanceName(),
		"machinetype", d.cloudProps.GetMachineType(),
		"image", d.cloudProps.GetImage(),
		"vCPU", d.cloudProps.GetVcpuCount(),
		"memorysize", d.cloudProps.GetMemorySizeMb(),
	)
	log.Logger.Infow("OS Data",
		"name", d.osData.OSName,
		"vendor", d.osData.OSVendor,
		"version", d.osData.OSVersion,
	)

	configureUsageMetricsForDaemon(d.cloudProps)
	usagemetrics.Configured()
	usagemetrics.Started()

	shutdownch := make(chan os.Signal, 1)
	signal.Notify(shutdownch, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)

	wlmClient, err := workloadmanager.Client(ctx, d.config)
	if err != nil {
		log.Logger.Errorw("Error creating WLM Client", "error", err)
		usagemetrics.Error(usagemetrics.WorkloadManagerConnectionError)
		return err
	}

	// Check if the metric override file exists. If it does, operate in override mode.
	// Override mode will collect metrics from the override file and send them to Data Warehouse.
	// Override mode will not start any other services.
	if fileInfo, err := os.ReadFile(workloadmanager.MetricOverridePath); fileInfo != nil && err == nil {
		log.Logger.Info("Metric override file found. Operating in override mode.")
		metricCollectionService := workloadmanager.Service{Config: d.config, Client: wlmClient}
		metricCollectionCtx := log.SetCtx(ctx, "context", "WorkloadManagerMetrics")
		recoverableStart := &recovery.RecoverableRoutine{
			Routine:             metricCollectionService.CollectAndSendMetricsToDataWarehouse,
			RoutineArg:          d.config,
			ErrorCode:           0,
			ExpectedMinDuration: 20 * time.Second,
			UsageLogger:         *usagemetrics.UsageLogger,
		}
		recoverableStart.StartRoutine(metricCollectionCtx)

		// Log a RUNNING usage metric once a day.
		go usagemetrics.LogRunningDaily()
		d.waitForShutdown(shutdownch, cancel)
		return nil
	}

	log.Logger.Info("Starting common discovery")
	oracleCh := make(chan *servicecommunication.Message, 3)
	mySQLCh := make(chan *servicecommunication.Message, 3)
	redisCh := make(chan *servicecommunication.Message, 3)
	sqlserverCh := make(chan *servicecommunication.Message, 3)
	postgresCh := make(chan *servicecommunication.Message, 3)
	openshiftCh := make(chan *servicecommunication.Message, 3)
	mongoCh := make(chan *servicecommunication.Message, 3)
	scChs := map[string]chan<- *servicecommunication.Message{
		"mysql":     mySQLCh,
		"oracle":    oracleCh,
		"redis":     redisCh,
		"sqlserver": sqlserverCh,
		"postgres":  postgresCh,
		"openshift": openshiftCh,
		"mongodb":   mongoCh,
	}
	commondiscovery := discovery.Service{
		ProcessLister: discovery.DefaultProcessLister{},
		ReadFile:      os.ReadFile,
		Hostname:      os.Hostname,
		Config:        d.config,
	}
	recoverableStart := &recovery.RecoverableRoutine{
		Routine:             commondiscovery.CommonDiscovery,
		RoutineArg:          scChs,
		ErrorCode:           commondiscovery.ErrorCode(),
		ExpectedMinDuration: commondiscovery.ExpectedMinDuration(),
		UsageLogger:         *usagemetrics.UsageLogger,
	}
	recoverableStart.StartRoutine(ctx)

	dwActivation := datawarehouseactivation.Service{Config: d.config, Client: wlmClient}
	recoverableStart = &recovery.RecoverableRoutine{
		Routine:             dwActivation.DataWarehouseActivationCheck,
		RoutineArg:          scChs,
		ErrorCode:           dwActivation.ErrorCode(),
		ExpectedMinDuration: dwActivation.ExpectedMinDuration(),
		UsageLogger:         *usagemetrics.UsageLogger,
	}
	recoverableStart.StartRoutine(ctx)

	// Create a new databasecenter client
	dbcenterClient := databasecenter.NewClient(d.config, nil)

	// Add any additional services here.
	d.services = []Service{
		&oracle.Service{Config: d.config, CloudProps: d.cloudProps, CommonCh: oracleCh},
		&mysql.Service{Config: d.config, CloudProps: d.cloudProps, CommonCh: mySQLCh, WLMClient: wlmClient, DBcenterClient: dbcenterClient},
		&redis.Service{Config: d.config, CloudProps: d.cloudProps, CommonCh: redisCh, WLMClient: wlmClient, OSData: d.osData},
		&sqlserver.Service{Config: d.config, CloudProps: d.cloudProps, CommonCh: sqlserverCh, DBcenterClient: dbcenterClient},
		&postgres.Service{Config: d.config, CloudProps: d.cloudProps, CommonCh: postgresCh, WLMClient: wlmClient, DBcenterClient: dbcenterClient},
		&openshift.Service{Config: d.config, CloudProps: d.cloudProps, CommonCh: openshiftCh, WLMClient: wlmClient},
		&mongodb.Service{Config: d.config, CloudProps: d.cloudProps, CommonCh: mongoCh, WLMClient: wlmClient},
	}
	for _, service := range d.services {
		log.Logger.Infof("Starting %s", service.String())
		recoverableStart := &recovery.RecoverableRoutine{
			Routine:             service.Start,
			ErrorCode:           service.ErrorCode(),
			ExpectedMinDuration: service.ExpectedMinDuration(),
			UsageLogger:         *usagemetrics.UsageLogger,
		}
		recoverableStart.StartRoutine(ctx)
	}

	// Log a RUNNING usage metric once a day.
	go usagemetrics.LogRunningDaily()
	d.waitForShutdown(shutdownch, cancel)
	return nil
}

// configureUsageMetricsForDaemon sets up UsageMetrics for Daemon.
func configureUsageMetricsForDaemon(cp *cpb.CloudProperties) {
	usagemetrics.SetAgentProperties(&cpb.AgentProperties{
		Name:            configuration.AgentName,
		Version:         configuration.AgentVersion,
		LogUsageMetrics: true,
	})
	usagemetrics.SetCloudProperties(cp)
}

// waitForShutdown observes a channel for a shutdown signal, then proceeds to shut down the Agent.
func (d *Daemon) waitForShutdown(ch <-chan os.Signal, cancel context.CancelFunc) {
	// wait for the shutdown signal
	<-ch
	log.Logger.Info("Shutdown signal observed, the agent will begin shutting down")
	cancel()
	usagemetrics.Stopped()
	time.Sleep(3 * time.Second)
	log.Logger.Info("Shutting down...")
}
