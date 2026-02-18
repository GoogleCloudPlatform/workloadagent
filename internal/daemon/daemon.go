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
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/parametermanager"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/recovery"

	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

// Daemon has args for startdaemon subcommand.
type Daemon struct {
	cancel         context.CancelFunc
	configFilePath string
	lp             log.Parameters
	Config         *cpb.Configuration
	CloudProps     *cpb.CloudProperties
	osData         osinfo.Data
	services       []Service
	pmClient       *parametermanager.Client
	pmVersion      string
	pmPollInterval time.Duration
	ServiceFactory ServiceFactoryFunc
}

// ServiceSet groups services and their communication channels.
type ServiceSet struct {
	Services []Service
	Channels map[string]chan<- *servicecommunication.Message
}

// ServiceFactoryFunc is a function that creates a ServiceSet.
type ServiceFactoryFunc func(ctx context.Context, d *Daemon, wlmClient workloadmanager.WLMWriter, dbcenterClient databasecenter.Client) ServiceSet

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
	fetchParameter = parametermanager.FetchParameter
	sleep          = time.Sleep
	newShutdownCh  = func() <-chan os.Signal {
		shutdownch := make(chan os.Signal, 1)
		signal.Notify(shutdownch, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
		return shutdownch
	}
)

// NewDaemon creates a new Daemon.
func NewDaemon(lp log.Parameters, cloudProps *cpb.CloudProperties) *Daemon {
	return &Daemon{
		lp:             lp,
		CloudProps:     cloudProps,
		ServiceFactory: DefaultServiceFactory,
	}
}

// DefaultServiceFactory is the default service factory for the agent.
func DefaultServiceFactory(ctx context.Context, d *Daemon, wlmClient workloadmanager.WLMWriter, dbcenterClient databasecenter.Client) ServiceSet {
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
	services := []Service{
		&oracle.Service{Config: d.Config, CloudProps: d.CloudProps, CommonCh: oracleCh},
		&mysql.Service{Config: d.Config, CloudProps: d.CloudProps, CommonCh: mySQLCh, WLMClient: wlmClient, DBcenterClient: dbcenterClient},
		&redis.Service{Config: d.Config, CloudProps: d.CloudProps, CommonCh: redisCh, WLMClient: wlmClient, OSData: d.osData},
		&sqlserver.Service{Config: d.Config, CloudProps: d.CloudProps, CommonCh: sqlserverCh, DBcenterClient: dbcenterClient},
		&postgres.Service{Config: d.Config, CloudProps: d.CloudProps, CommonCh: postgresCh, WLMClient: wlmClient, DBcenterClient: dbcenterClient},
		&openshift.Service{Config: d.Config, CloudProps: d.CloudProps, CommonCh: openshiftCh, WLMClient: wlmClient},
		&mongodb.Service{Config: d.Config, CloudProps: d.CloudProps, CommonCh: mongoCh, WLMClient: wlmClient},
	}
	return ServiceSet{Services: services, Channels: scChs}
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
	cmd.Flags().StringVar(&d.configFilePath, "config", configuration.ConfigPath(), "configuration path for startdaemon mode")
	cmd.Flags().StringVar(&d.configFilePath, "c", configuration.ConfigPath(), "configuration path for startdaemon mode")
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

	// Run the config poller and daemon handler that will start any services.
	ctx, d.cancel = context.WithCancel(ctx)
	pmClient, err := parametermanager.NewClient(ctx)
	if err != nil {
		log.Logger.Errorw("Failed to create Parameter Manager client, continuing with local configuration", "error", err)
	} else {
		d.pmClient = pmClient
	}
	d.pmPollInterval = 5 * time.Minute
	d.startConfigPollerRoutine(ctx)
	return d.startdaemonHandler(ctx, false)
}

func (d *Daemon) startdaemonHandler(ctx context.Context, restarting bool) error {
	// Cloud properties are exclusively set from the metadata server.
	configureUsageMetricsForDaemon(d.CloudProps)

	// Load the agent configuration from the config file.
	var err error
	d.Config, d.pmVersion, err = configuration.Load(d.configFilePath, os.ReadFile, d.CloudProps, d.pmClient)
	if err != nil {
		log.Logger.Errorw("Invalid configuration file, please fix the configuration file and restart the service.", "error", err, "configFile", d.configFilePath)
		usagemetrics.Misconfigured()
		return err
	}
	usagemetrics.Configured()

	// Setup logging based on the agent configuration.
	d.lp.LogToCloud = d.Config.GetLogToCloud()
	d.lp.Level = configuration.LogLevelToZapcore(d.Config.GetLogLevel())
	if d.Config.GetCloudProperties().GetProjectId() != "" {
		d.lp.CloudLoggingClient = log.CloudLoggingClient(ctx, d.Config.GetCloudProperties().GetProjectId())
	}
	if d.lp.CloudLoggingClient != nil {
		defer log.FlushCloudLog()
	}
	log.SetupLogging(d.lp)

	// Get vCPU count and memory size from GCE and add to cloudProps.
	gceClient, err := gce.NewGCEClient(ctx)
	if err != nil {
		log.Logger.Errorw("creating GCE client", "error", err)
		usagemetrics.Error(usagemetrics.StartDaemonFailure)
		return err
	}
	vCPU, memorySize, err := gceClient.GetInstanceCPUAndMemorySize(ctx, d.CloudProps.GetProjectId(), d.CloudProps.GetZone(), d.CloudProps.GetInstanceName())
	if err != nil {
		log.Logger.Errorw("getting vCPU and memory size from GCE", "error", err)
		usagemetrics.Error(usagemetrics.StartDaemonFailure)
		return err
	}
	d.CloudProps.VcpuCount = vCPU
	d.CloudProps.MemorySizeMb = memorySize

	log.Logger.Infow("Starting daemon mode", "agent_name", configuration.AgentName, "agent_version", configuration.AgentVersion)
	log.Logger.Infow("Cloud Properties",
		"projectid", d.CloudProps.GetProjectId(),
		"projectnumber", d.CloudProps.GetNumericProjectId(),
		"instanceid", d.CloudProps.GetInstanceId(),
		"zone", d.CloudProps.GetZone(),
		"region", d.CloudProps.GetRegion(),
		"instancename", d.CloudProps.GetInstanceName(),
		"machinetype", d.CloudProps.GetMachineType(),
		"image", d.CloudProps.GetImage(),
		"vCPU", d.CloudProps.GetVcpuCount(),
		"memorysize", d.CloudProps.GetMemorySizeMb(),
		"scopes", d.CloudProps.GetScopes(),
		"defaultserviceaccountemail", d.CloudProps.GetServiceAccountEmail(),
	)
	logDefaultCredentials(ctx, d.CloudProps)
	log.Logger.Infow("OS Data",
		"name", d.osData.OSName,
		"vendor", d.osData.OSVendor,
		"version", d.osData.OSVersion,
	)

	wlmClient, err := workloadmanager.Client(ctx, d.Config)
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
		metricCollectionService := workloadmanager.Service{Config: d.Config, Client: wlmClient}
		metricCollectionCtx := log.SetCtx(ctx, "context", "WorkloadManagerMetrics")
		recoverableStart := &recovery.RecoverableRoutine{
			Routine:             metricCollectionService.CollectAndSendMetricsToDataWarehouse,
			RoutineArg:          d.Config,
			ErrorCode:           0,
			ExpectedMinDuration: 20 * time.Second,
			UsageLogger:         *usagemetrics.UsageLogger,
		}
		recoverableStart.StartRoutine(metricCollectionCtx)

		log.Logger.Info("Daemon override mode startup complete")
		if !restarting {
			usagemetrics.Started()
			go usagemetrics.LogRunningDaily()
		}
		d.waitForShutdown(ctx, restarting)
		return nil
	}

	// Create a new databasecenter client.
	dbcenterClient := databasecenter.NewClient(d.Config, nil)

	log.Logger.Info("Starting common discovery")
	serviceSet := d.ServiceFactory(ctx, d, wlmClient, dbcenterClient)
	scChs := serviceSet.Channels
	commondiscovery := discovery.Service{
		ProcessLister: discovery.DefaultProcessLister{},
		ReadFile:      os.ReadFile,
		Hostname:      os.Hostname,
		Config:        d.Config,
	}
	recoverableStart := &recovery.RecoverableRoutine{
		Routine:             commondiscovery.CommonDiscovery,
		RoutineArg:          scChs,
		ErrorCode:           commondiscovery.ErrorCode(),
		ExpectedMinDuration: commondiscovery.ExpectedMinDuration(),
		UsageLogger:         *usagemetrics.UsageLogger,
	}
	recoverableStart.StartRoutine(ctx)

	dwActivation := datawarehouseactivation.Service{Config: d.Config, Client: wlmClient}
	recoverableStart = &recovery.RecoverableRoutine{
		Routine:             dwActivation.DataWarehouseActivationCheck,
		RoutineArg:          scChs,
		ErrorCode:           dwActivation.ErrorCode(),
		ExpectedMinDuration: dwActivation.ExpectedMinDuration(),
		UsageLogger:         *usagemetrics.UsageLogger,
	}
	recoverableStart.StartRoutine(ctx)

	// Add any additional services here.
	d.services = serviceSet.Services
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

	log.Logger.Info("Daemon mode startup complete")
	if !restarting {
		usagemetrics.Started()
		go usagemetrics.LogRunningDaily()
	}
	d.waitForShutdown(ctx, restarting)
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
func (d *Daemon) waitForShutdown(ctx context.Context, restarting bool) {
	// If we're restarting, we only need to wait for context cancellation
	// instead of a shutdown signal. This is because the initial shutdown
	// channel is still active and we don't want to duplicate its logic.
	if restarting {
		<-ctx.Done()
		return
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
	// Wait for the shutdown signal.
	<-shutdown
	log.Logger.Info("Shutdown signal observed, the agent will begin shutting down")
	d.cancel()
	usagemetrics.Stopped()
	time.Sleep(3 * time.Second)
	log.Logger.Info("Shutting down...")
}

// restart cancels the current context and invokes the startdaemonHandler once more.
func (d *Daemon) restart() {
	log.Logger.Info("Restarting daemon services")
	d.cancel()
	// Context cancellation is asynchronous, give agent services some time to clean up.
	time.Sleep(5 * time.Second)
	var ctx context.Context
	ctx, d.cancel = context.WithCancel(context.Background())
	go d.startdaemonHandler(ctx, true)
}

func (d *Daemon) startConfigPollerRoutine(ctx context.Context) {
	pollConfigFileRoutine := &recovery.RecoverableRoutine{
		Routine: func(_ context.Context, _ any) {
			d.pollConfigFile()
		},
		UsageLogger:         *usagemetrics.UsageLogger,
		ExpectedMinDuration: 30 * time.Second,
	}
	pollConfigFileRoutine.StartRoutine(ctx)
}

// pollConfigFile checks the last modified time of the agent config file.
// If the file has been modified, the daemon is restarted to pick up the new config.
func (d *Daemon) pollConfigFile() {
	// fileTicker polls for changes in the local configuration file every 30 seconds.
	fileTicker := time.NewTicker(30 * time.Second)
	defer fileTicker.Stop()
	// pmTicker polls for changes in the Parameter Manager configuration every 5 minutes.
	pmTicker := time.NewTicker(d.pmPollInterval)
	defer pmTicker.Stop()

	prev, err := d.lastModifiedTime()
	if err != nil {
		log.Logger.Errorw("Failed to get last modified time for config file", "error", err)
		return
	}
	shutdownch := newShutdownCh()
	for {
		select {
		case <-shutdownch:
			log.Logger.Info("Shutdown signal observed, exiting the config poller")
			return
		case <-fileTicker.C:
			log.Logger.Debug("Polling config file")
			res, err := d.lastModifiedTime()
			if err != nil {
				log.Logger.Errorw("Failed to get last modified time for config file", "error", err)
				continue
			}
			if res.After(prev) {
				prev = res
				log.Logger.Infow("Detected config file change", "configFile", d.configFilePath)
				d.restart()
			}
		case <-pmTicker.C:
			if d.checkForPMUpdate(context.Background()) {
				d.restart()
			}
		}
	}
}

func (d *Daemon) checkForPMUpdate(ctx context.Context) bool {
	if d.Config == nil || d.Config.GetParameterManagerConfig() == nil {
		return false
	}
	resource, err := fetchParameter(ctx, d.pmClient, d.Config.GetParameterManagerConfig().GetProject(), d.Config.GetParameterManagerConfig().GetLocation(), d.Config.GetParameterManagerConfig().GetParameterName(), "")
	if err != nil {
		log.Logger.Errorw("Failed to fetch parameter from Parameter Manager", "error", err)
		return false
	}
	if resource.Version != d.pmVersion {
		log.Logger.Infow("Parameter Manager config changed, restarting daemon", "oldVersion", d.pmVersion, "newVersion", resource.Version)
		return true
	}
	return false
}

func (d *Daemon) lastModifiedTime() (time.Time, error) {
	path := d.configFilePath
	if len(path) == 0 {
		path = configuration.ConfigPath()
	}
	res, err := os.Stat(path)
	if err != nil {
		return time.Time{}, err
	}
	return res.ModTime(), nil
}

func logDefaultCredentials(ctx context.Context, cloudProps *cpb.CloudProperties) {
	credsPath := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if credsPath != "" {
		log.CtxLogger(ctx).Infow("GOOGLE_APPLICATION_CREDENTIALS is set, authentication to GCP will use the service account associated with the key stored in this file.", "path", credsPath)
	} else {
		log.CtxLogger(ctx).Infow("Credentials are likely from the default service account.", "email", cloudProps.GetServiceAccountEmail())
	}
}
