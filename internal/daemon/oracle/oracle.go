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

// Package oracle implements the Oracle workload agent service.
package oracle

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/workloadagent/internal/oraclediscovery"
	"github.com/GoogleCloudPlatform/workloadagent/internal/oraclehandlers"
	"github.com/GoogleCloudPlatform/workloadagent/internal/oraclemetrics"
	"github.com/GoogleCloudPlatform/workloadagent/internal/servicecommunication"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce/metadataserver"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/guestactions"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/recovery"

	mrpb "google.golang.org/genproto/googleapis/monitoring/v3"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	odpb "github.com/GoogleCloudPlatform/workloadagent/protos/oraclediscovery"
	gapb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/guestactions"
)

const (
	// This is an ephemeral channel, meaning ACLs/quota are managed within the instance's project
	// where the agent is running, unlike registered channels that use a producer project.
	defaultChannel     = "oracle-operations-ephemeral-channel"
	defaultLockTimeout = 24 * time.Hour
	// discoveryCheckInterval is the interval to check if discovery has found an Oracle process.
	discoveryCheckInterval = 5 * time.Second
)

// DiscoveryClient is the interface for Oracle discovery operations.
type DiscoveryClient interface {
	Discover(ctx context.Context, cloudProps *cpb.CloudProperties, processes []servicecommunication.ProcessWrapper) (*odpb.Discovery, error)
}

// MetricCollector is the interface for Oracle metrics collection.
type MetricCollector interface {
	SendHealthMetricsToCloudMonitoring(ctx context.Context) []*mrpb.TimeSeries
	SendDefaultMetricsToCloudMonitoring(ctx context.Context) []*mrpb.TimeSeries
}

// guestActionsManager is an interface satisfied by guestactions.GuestActions
// to allow for mocking in tests.
type guestActionsManager interface {
	Start(context.Context, any)
}

// newGuestActionsManager allows overriding the GuestActions implementation for testing.
var newGuestActionsManager = func() guestActionsManager {
	return &guestactions.GuestActions{}
}

func convertCloudProperties(cp *cpb.CloudProperties) *metadataserver.CloudProperties {
	if cp == nil {
		return nil
	}
	return &metadataserver.CloudProperties{
		ProjectID:           cp.GetProjectId(),
		NumericProjectID:    cp.GetNumericProjectId(),
		InstanceID:          cp.GetInstanceId(),
		Zone:                cp.GetZone(),
		InstanceName:        cp.GetInstanceName(),
		Image:               cp.GetImage(),
		MachineType:         cp.GetMachineType(),
		Region:              cp.GetRegion(),
		ServiceAccountEmail: cp.GetServiceAccountEmail(),
		Scopes:              cp.GetScopes(),
	}
}

// Service implements the interfaces for Oracle workload agent service.
type Service struct {
	Config                  *cpb.Configuration
	CloudProps              *cpb.CloudProperties
	metricCollectionRoutine *recovery.RecoverableRoutine
	discoveryRoutine        *recovery.RecoverableRoutine
	guestActionsRoutine     *recovery.RecoverableRoutine
	currentSIDs             []string
	CommonCh                <-chan *servicecommunication.Message
	isProcessPresent        bool
	processes               []servicecommunication.ProcessWrapper
	processesMutex          sync.Mutex

	// discovery performs Oracle discovery operations.
	discovery DiscoveryClient
	// newMetricCollector creates a new metrics collector.
	// This is a factory function because the metrics collector is stateful (holds DB connections)
	// and runs inside a recoverable routine. If the routine crashes and restarts, we need
	// to create a fresh collector with new connections rather than reusing a potentially broken one.
	newMetricCollector func(context.Context, *cpb.Configuration) (MetricCollector, error)
}

type runGuestActionsArgs struct {
	s        *Service
	handlers map[string]guestactions.GuestActionHandler
}

type runDiscoveryArgs struct {
	s *Service
}

type runMetricCollectionArgs struct {
	s *Service
}

var oraProcessPrefixes = []string{"ora_pmon_", "db_pmon_"}

// Start initiates the Oracle workload agent service
func (s *Service) Start(ctx context.Context, a any) {
	s.initializeDependencies()

	go (func() {
		for {
			s.checkServiceCommunication(ctx)
		}
	})()

	if !s.waitForWorkload(ctx) {
		return
	}

	if runtime.GOOS != "linux" {
		log.CtxLogger(ctx).Error("Oracle service is only supported on Linux")
		return
	}

	s.startDiscoveryRoutine(ctx)
	s.startMetricCollectionRoutine(ctx)
	s.startGuestActionsRoutine(ctx)

	select {
	case <-ctx.Done():
		log.CtxLogger(ctx).Info("Oracle workload agent service cancellation requested")
		return
	}
}

func (s *Service) initializeDependencies() {
	if s.discovery == nil {
		s.discovery = oraclediscovery.New()
	}
	if s.newMetricCollector == nil {
		s.newMetricCollector = func(ctx context.Context, cfg *cpb.Configuration) (MetricCollector, error) {
			return oraclemetrics.New(ctx, cfg)
		}
	}
}

// waitForWorkload checks if the service should be enabled.
// If enabled field is unset, it waits for the workload to be present.
// Returns true if the service should proceed, false otherwise.
func (s *Service) waitForWorkload(ctx context.Context) bool {
	// Check if the enabled field is unset. If it is, then the service is still enabled if the workload is present.
	if s.Config.GetOracleConfiguration().Enabled == nil {
		log.CtxLogger(ctx).Info("Oracle service enabled field is not set, will check for workload presence to determine if service should be enabled.")
		// If the workload is present, proceed with starting the service even if it is not enabled.
		for {
			s.processesMutex.Lock()
			present := s.isProcessPresent
			s.processesMutex.Unlock()
			if present {
				break
			}
			select {
			case <-ctx.Done():
				return false
			case <-time.After(discoveryCheckInterval):
				continue
			}
		}
		log.CtxLogger(ctx).Info("Oracle workload is present. Starting service.")
		return true
	}
	if !s.Config.GetOracleConfiguration().GetEnabled() {
		log.CtxLogger(ctx).Info("Oracle service is disabled")
		return false
	}
	return true
}

func (s *Service) startDiscoveryRoutine(ctx context.Context) {
	if s.Config.GetOracleConfiguration().GetOracleDiscovery().GetEnabled() {
		dCtx := log.SetCtx(ctx, "context", "OracleDiscovery")
		s.discoveryRoutine = &recovery.RecoverableRoutine{
			Routine:             runDiscovery,
			RoutineArg:          runDiscoveryArgs{s},
			ErrorCode:           usagemetrics.OracleDiscoverDatabaseFailure,
			UsageLogger:         *usagemetrics.UsageLogger,
			ExpectedMinDuration: 20 * time.Second,
		}
		s.discoveryRoutine.StartRoutine(dCtx)
	}
}

func (s *Service) startMetricCollectionRoutine(ctx context.Context) {
	if s.Config.GetOracleConfiguration().GetOracleMetrics().GetEnabled() {
		mcCtx := log.SetCtx(ctx, "context", "OracleMetricCollection")
		s.metricCollectionRoutine = &recovery.RecoverableRoutine{
			Routine:             runMetricCollection,
			RoutineArg:          runMetricCollectionArgs{s},
			ErrorCode:           usagemetrics.OracleMetricCollectionFailure,
			UsageLogger:         *usagemetrics.UsageLogger,
			ExpectedMinDuration: 20 * time.Second,
		}
		s.metricCollectionRoutine.StartRoutine(mcCtx)
	}
}

func (s *Service) startGuestActionsRoutine(ctx context.Context) {
	gaCtx := log.SetCtx(ctx, "context", "OracleGuestActions")
	s.guestActionsRoutine = &recovery.RecoverableRoutine{
		Routine:             runGuestActions,
		RoutineArg:          runGuestActionsArgs{s: s, handlers: guestActionHandlers()},
		ErrorCode:           usagemetrics.GuestActionsFailure,
		UsageLogger:         *usagemetrics.UsageLogger,
		ExpectedMinDuration: 10 * time.Second,
	}
	log.CtxLogger(ctx).Info("Starting guest actions routine")
	s.guestActionsRoutine.StartRoutine(gaCtx)
}

func guestActionHandlers() map[string]guestactions.GuestActionHandler {
	return map[string]guestactions.GuestActionHandler{
		// go/keep-sorted start
		"oracle_data_guard_switchover":      oraclehandlers.DataGuardSwitchover,
		"oracle_disable_autostart":          oraclehandlers.DisableAutostart,
		"oracle_disable_restricted_session": oraclehandlers.DisableRestrictedSession,
		"oracle_enable_autostart":           oraclehandlers.EnableAutostart,
		"oracle_health_check":               oraclehandlers.HealthCheck,
		"oracle_run_datapatch":              oraclehandlers.RunDatapatch,
		"oracle_run_discovery":              oraclehandlers.RunDiscovery,
		"oracle_start_database":             oraclehandlers.StartDatabase,
		"oracle_start_listener":             oraclehandlers.StartListener,
		"oracle_stop_database":              oraclehandlers.StopDatabase,
		// go/keep-sorted end
	}
}

func runGuestActions(ctx context.Context, a any) {
	log.CtxLogger(ctx).Infow("Starting guest actions listener", "channel_id", defaultChannel)
	args, ok := a.(runGuestActionsArgs)
	if !ok {
		log.CtxLogger(ctx).Error("args is not of type runGuestActionsArgs")
		return
	}
	ga := newGuestActionsManager()

	gaOpts := guestactions.Options{
		Channel:               defaultChannel,
		CloudProperties:       convertCloudProperties(args.s.CloudProps),
		LROHandlers:           args.handlers,
		CommandConcurrencyKey: oracleCommandKey,
	}
	ga.Start(ctx, gaOpts)
}

// oracleCommandKey returns the locking key and timeout for a given command and whether the command
// should be locked.
// The locking key is formed by instance ID, oracle home, and oracle sid.
func oracleCommandKey(ctx context.Context, cmd *gapb.Command, cp *metadataserver.CloudProperties) (key string, timeout time.Duration, lock bool) {
	params := cmd.GetAgentCommand().GetParameters()
	sid, sidOk := params["oracle_sid"]
	home, homeOk := params["oracle_home"]

	if !sidOk || !homeOk {
		log.CtxLogger(ctx).Warnw("Required parameters are missing, cannot form a unique key", "command", cmd.GetAgentCommand(), "cloud_properties", cp, "oracle_sid", sid, "oracle_home", home)
		return "", 0, false
	}
	if cp == nil {
		log.CtxLogger(ctx).Warnw("Cloud properties are nil, cannot form locking key", "command", cmd.GetAgentCommand(), "cloud_properties", cp, "oracle_sid", sid, "oracle_home", home)
		return "", 0, false
	}
	return fmt.Sprintf("%s:%s:%s", cp.InstanceID, home, sid), defaultLockTimeout, true
}

func runDiscovery(ctx context.Context, a any) {
	log.CtxLogger(ctx).Info("Running Oracle Discovery")
	args, ok := a.(runDiscoveryArgs)
	if !ok {
		log.CtxLogger(ctx).Error("args is not of type runDiscoveryArgs")
		return
	}

	s := args.s
	ticker := time.NewTicker(s.Config.GetOracleConfiguration().GetOracleDiscovery().GetUpdateFrequency().AsDuration())
	defer ticker.Stop()

	ds := s.discovery

	for {
		// Discovery data is not used yet.
		s.processesMutex.Lock()
		processes := s.processes
		s.processesMutex.Unlock()
		// Don't start discovery until processes are populated.
		for processes == nil {
			select {
			case <-ctx.Done():
				log.CtxLogger(ctx).Info("Oracle Discovery cancellation requested")
				return
			case <-time.After(discoveryCheckInterval):
			}
			s.processesMutex.Lock()
			processes = s.processes
			s.processesMutex.Unlock()
		}
		_, err := ds.Discover(ctx, s.CloudProps, processes)
		if err != nil {
			log.CtxLogger(ctx).Errorw("Failed to discover databases", "error", err)
			return
		}

		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("Oracle Discovery cancellation requested")
			return
		case <-ticker.C:
			continue
		}
	}
}

func runMetricCollection(ctx context.Context, a any) {
	log.CtxLogger(ctx).Info("Running Oracle metric collection")
	args, ok := a.(runMetricCollectionArgs)
	if !ok {
		log.CtxLogger(ctx).Errorw("Failed to parse metric collection args", "args", a)
		return
	}

	ticker := time.NewTicker(args.s.Config.GetOracleConfiguration().GetOracleMetrics().GetCollectionFrequency().AsDuration())
	defer ticker.Stop()

	metricCollector, err := args.s.newMetricCollector(ctx, args.s.Config)
	if err != nil {
		log.CtxLogger(ctx).Errorw("Failed to initialize metric collector", "error", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("Metric Collection cancellation requested")
			return
		case <-ticker.C:
			metricCollector.SendHealthMetricsToCloudMonitoring(ctx)
			metricCollector.SendDefaultMetricsToCloudMonitoring(ctx)
		}
	}
}

// checkServiceCommunication listens to the common channel for messages and processes them.
func (s *Service) checkServiceCommunication(ctx context.Context) {
	// Effectively give ctx.Done() priority over the channel.
	if ctx.Err() != nil {
		return
	}

	select {
	case <-ctx.Done():
		return
	case msg := <-s.CommonCh:
		log.CtxLogger(ctx).Debugw("Oracle workload agent service received a message on the common channel", "message", msg)
		switch msg.Origin {
		case servicecommunication.Discovery:
			log.CtxLogger(ctx).Debugw("Oracle workload agent service received a discovery message")
			found := false
			for _, p := range msg.DiscoveryResult.Processes {
				name, err := p.Name()
				if err == nil && servicecommunication.HasAnyPrefix(name, oraProcessPrefixes) {
					found = true
					break
				}
			}
			s.processesMutex.Lock()
			s.processes = msg.DiscoveryResult.Processes
			s.isProcessPresent = found
			s.processesMutex.Unlock()
		case servicecommunication.DWActivation:
			log.CtxLogger(ctx).Debugw("Oracle workload agent service received a DW activation message")
		default:
			log.CtxLogger(ctx).Debugw("Oracle workload agent service received a message with an unexpected origin", "origin", msg.Origin)
		}
	}
}

// String returns the name of the oracle service.
func (s *Service) String() string {
	return "Oracle Service"
}

// ErrorCode returns the error code for the oracle service.
func (s *Service) ErrorCode() int {
	return usagemetrics.OracleServiceError
}

// ExpectedMinDuration returns the expected minimum duration for the oracle service.
// Used by the recovery handler to determine if the service ran long enough to be considered
// successful.
func (s *Service) ExpectedMinDuration() time.Duration {
	return 20 * time.Second
}
