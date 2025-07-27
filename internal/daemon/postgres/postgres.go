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

// Package postgres implements the Postgres workload agent service.
package postgres

import (
	"context"
	"strings"
	"time"

	"go.uber.org/zap/zapcore"
	"github.com/GoogleCloudPlatform/workloadagent/internal/databasecenter"
	"github.com/GoogleCloudPlatform/workloadagent/internal/postgresdiscovery"
	"github.com/GoogleCloudPlatform/workloadagent/internal/postgresmetrics"
	"github.com/GoogleCloudPlatform/workloadagent/internal/servicecommunication"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	"github.com/GoogleCloudPlatform/workloadagent/internal/workloadmanager"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/recovery"

	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

const (
	discoveryFrequency                       = 10 * time.Minute
	wlmMetricCollectionFrequencyDefault      = 5 * time.Minute // Frequency for WLM metrics
	dbCenterMetricCollectionFrequencyMin     = 10 * time.Minute
	dbCenterMetricCollectionFrequencyMax     = 6 * time.Hour
	dbCenterMetricCollectionFrequencyDefault = 1 * time.Hour
)

var (
	newGCEClient       = gce.NewGCEClient
	newPostgresMetrics = func(ctx context.Context, config *configpb.Configuration, wlmClient workloadmanager.WLMWriter, dbcenterClient databasecenter.Client) MetricsInterface {
		return postgresmetrics.New(ctx, config, wlmClient, dbcenterClient)
	}
	newTicker = time.NewTicker
)

// MetricsInterface defines an interface for postgresmetrics.PostgresMetrics to allow faking
type MetricsInterface interface {
	InitDB(ctx context.Context, gceService postgresmetrics.GceInterface) error
	CollectWlmMetricsOnce(ctx context.Context, dwActivated bool) (*workloadmanager.WorkloadMetrics, error)
	CollectDBCenterMetricsOnce(ctx context.Context) error
}

// Service implements the interfaces for Postgres workload agent service.
type Service struct {
	Config            *configpb.Configuration
	CloudProps        *configpb.CloudProperties
	CommonCh          <-chan *servicecommunication.Message
	processes         servicecommunication.DiscoveryResult
	postgresProcesses []servicecommunication.ProcessWrapper
	dwActivated       bool
	WLMClient         workloadmanager.WLMWriter
	DBcenterClient    databasecenter.Client
}

type runDiscoveryArgs struct {
	s *Service
}

type runWlmMetricCollectionArgs struct {
	s *Service
}

type runDBCenterMetricCollectionArgs struct {
	s *Service
}

// Start initiates the Postgres workload agent service
func (s *Service) Start(ctx context.Context, a any) {
	if s.Config.GetPostgresConfiguration() != nil && !s.Config.GetPostgresConfiguration().GetEnabled() {
		// If Postgres workload agent service is explicitly disabled in the configuration, then return.
		log.CtxLogger(ctx).Info("Postgres workload agent service is disabled in the configuration")
		return
	}

	go (func() {
		for {
			if ctx.Err() != nil {
				return
			}
			s.checkServiceCommunication(ctx)
		}
	})()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	enabled := s.Config.GetPostgresConfiguration().GetEnabled()
EnableCheck:
	for {
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("Postgres workload agent service cancellation requested")
			return
		case <-ticker.C:
			// Once the workload is present/enabled, start discovery and metric collection.
			if s.isWorkloadPresent() || enabled {
				log.CtxLogger(ctx).Info("Postgres workload agent service is enabled. Starting discovery and metric collection")
				break EnableCheck
			}
		}
	}

	// Start Postgres Discovery
	dCtx := log.SetCtx(ctx, "context", "PostgresDiscovery")
	discoveryRoutine := &recovery.RecoverableRoutine{
		Routine:             runDiscovery,
		RoutineArg:          runDiscoveryArgs{s},
		ErrorCode:           usagemetrics.PostgresDiscoveryFailure,
		UsageLogger:         *usagemetrics.UsageLogger,
		ExpectedMinDuration: 20 * time.Second,
	}
	discoveryRoutine.StartRoutine(dCtx)

	// Start PostgreSQL WLM Metric Collection
	wlmMCCtx := log.SetCtx(ctx, "context", "PostgreSQLWlmMetricCollection")
	wlmMetricCollectionRoutine := &recovery.RecoverableRoutine{
		Routine:             runWlmMetricCollection,
		RoutineArg:          runWlmMetricCollectionArgs{s},
		ErrorCode:           usagemetrics.PostgresMetricCollectionFailure,
		UsageLogger:         *usagemetrics.UsageLogger,
		ExpectedMinDuration: 20 * time.Second,
	}
	wlmMetricCollectionRoutine.StartRoutine(wlmMCCtx)

	// Start PostgreSQL DB Center Metric Collection
	dbcenterMCCtx := log.SetCtx(ctx, "context", "PostgreSQLDBCenterMetricCollection")
	dbcenterMetricCollectionRoutine := &recovery.RecoverableRoutine{
		Routine:             runDBCenterMetricCollection,
		RoutineArg:          runDBCenterMetricCollectionArgs{s},
		ErrorCode:           usagemetrics.PostgresMetricCollectionFailure,
		UsageLogger:         *usagemetrics.UsageLogger,
		ExpectedMinDuration: 20 * time.Second,
	}
	dbcenterMetricCollectionRoutine.StartRoutine(dbcenterMCCtx)
	select {
	case <-ctx.Done():
		log.CtxLogger(ctx).Info("Postgres workload agent service cancellation requested")
		return
	}
}

func runDiscovery(ctx context.Context, a any) {
	log.CtxLogger(ctx).Info("Starting Postgres Discovery")
	var args runDiscoveryArgs
	var ok bool
	if args, ok = a.(runDiscoveryArgs); !ok {
		log.CtxLogger(ctx).Errorw("Failed to parse discovery args", "args", a)
		return
	}
	log.CtxLogger(ctx).Debugw("Postgres discovery args", "args", args)
	ticker := time.NewTicker(discoveryFrequency)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("Postgres discovery cancellation requested")
			return
		case <-ticker.C:
			postgresdiscovery.Discover(ctx)
		}
	}
}

func runWlmMetricCollection(ctx context.Context, a any) {
	log.CtxLogger(ctx).Info("Starting Postgres WLM Metric Collection")
	var args runWlmMetricCollectionArgs
	var ok bool
	if args, ok = a.(runWlmMetricCollectionArgs); !ok {
		log.CtxLogger(ctx).Errorw("Failed to parse WLM metric collection args", "args", a)
		return
	}
	log.CtxLogger(ctx).Debugw("Postgres WLM metric collection args", "args", args)

	ticker := newTicker(wlmMetricCollectionFrequencyDefault)
	defer ticker.Stop()
	gceService, err := newGCEClient(ctx)
	if err != nil {
		usagemetrics.Error(usagemetrics.GCEServiceCreationFailure)
		log.CtxLogger(ctx).Errorf("Error while initializing GCE services: %v", err)
		return
	}
	p := newPostgresMetrics(ctx, args.s.Config, args.s.WLMClient, args.s.DBcenterClient)
	err = p.InitDB(ctx, gceService)
	if err != nil {
		log.CtxLogger(ctx).Errorf("Failed to initialize Postgres DB for WLM metrics: %v", err)
		return
	}
	for {
		_, err := p.CollectWlmMetricsOnce(ctx, args.s.dwActivated)
		if err != nil {
			log.CtxLogger(ctx).Debugf("Failed to collect Postgres WLM metrics: %v", err)
		}
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("Postgres WLM metric collection cancellation requested")
			return
		case <-ticker.C:
			continue
		}
	}
}

func getDbCenterMetricCollectionFrequency(args runDBCenterMetricCollectionArgs) time.Duration {
	if args.s == nil || args.s.Config == nil {
		return dbCenterMetricCollectionFrequencyDefault
	}
	config := args.s.Config.GetPostgresConfiguration()
	if config == nil || config.DbcenterCollectionFrequency == nil {
		return dbCenterMetricCollectionFrequencyDefault
	}
	freq := config.GetDbcenterCollectionFrequency().AsDuration()
	if freq < dbCenterMetricCollectionFrequencyMin {
		return dbCenterMetricCollectionFrequencyMin
	}
	if freq > dbCenterMetricCollectionFrequencyMax {
		return dbCenterMetricCollectionFrequencyMax
	}
	return freq
}

func runDBCenterMetricCollection(ctx context.Context, a any) {
	log.CtxLogger(ctx).Info("Starting Postgres DB Center Metric Collection")
	var args runDBCenterMetricCollectionArgs
	var ok bool
	if args, ok = a.(runDBCenterMetricCollectionArgs); !ok {
		log.CtxLogger(ctx).Errorw("Failed to parse DB Center metric collection args", "args", a)
		return
	}
	log.CtxLogger(ctx).Debugw("Postgres DB Center metric collection args", "args", args)
	metricCollectionFrequency := getDbCenterMetricCollectionFrequency(args)
	ticker := newTicker(metricCollectionFrequency)
	defer ticker.Stop()
	gceService, err := newGCEClient(ctx)
	if err != nil {
		usagemetrics.Error(usagemetrics.GCEServiceCreationFailure)
		log.CtxLogger(ctx).Errorf("Error while initializing GCE services: %v", err)
		return
	}
	p := newPostgresMetrics(ctx, args.s.Config, args.s.WLMClient, args.s.DBcenterClient)
	err = p.InitDB(ctx, gceService)
	if err != nil {
		log.CtxLogger(ctx).Errorf("Failed to initialize Postgres DB for DB Center metrics: %v", err)
		return
	}
	for {
		err := p.CollectDBCenterMetricsOnce(ctx)
		if err != nil {
			log.CtxLogger(ctx).Debugf("Failed to collect Postgres DB Center metrics: %v", err)
		}
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("Postgres DB Center metric collection cancellation requested")
			return
		case <-ticker.C:
			continue
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
		log.CtxLogger(ctx).Debugw("Postgres workload agent service received a message on the common channel", "message", msg)
		switch msg.Origin {
		case servicecommunication.Discovery:
			s.processes = msg.DiscoveryResult
			s.identifyPostgresProcesses(ctx)
		case servicecommunication.DWActivation:
			s.dwActivated = msg.DWActivationResult.Activated
		default:
			log.CtxLogger(ctx).Debugw("Postgres workload agent service received a message with an unexpected origin", "origin", msg.Origin)
		}
	}
}

func (s *Service) identifyPostgresProcesses(ctx context.Context) {
	s.postgresProcesses = []servicecommunication.ProcessWrapper{}
	for _, process := range s.processes.Processes {
		name, err := process.Name()
		if err == nil && strings.Contains(name, "postgres") {
			s.postgresProcesses = append(s.postgresProcesses, process)
		}
	}
	s.logPostgresProcesses(ctx, zapcore.DebugLevel)
}

func (s *Service) isWorkloadPresent() bool {
	return len(s.postgresProcesses) > 0
}

func (s *Service) logPostgresProcesses(ctx context.Context, loglevel zapcore.Level) {
	log.CtxLogger(ctx).Logf(loglevel, "Number of processes found: %v", len(s.processes.Processes))
	log.CtxLogger(ctx).Logf(loglevel, "Number of Postgres processes found: %v", len(s.postgresProcesses))
	for _, process := range s.postgresProcesses {
		name, _ := process.Name()
		username, _ := process.Username()
		cmdline, _ := process.CmdlineSlice()
		env, _ := process.Environ()
		log.CtxLogger(ctx).Logw(loglevel, "Postgres process", "name", name, "username", username, "cmdline", cmdline, "env", env, "pid", process.Pid())
	}
}

// String returns the name of the Postgres service.
func (s *Service) String() string {
	return "Postgres Service"
}

// ErrorCode returns the error code for the Postgres service.
func (s *Service) ErrorCode() int {
	return usagemetrics.PostgresServiceError
}

// ExpectedMinDuration returns the expected minimum duration for the Postgres service.
// Used by the recovery handler to determine if the service ran long enough to be considered
// successful.
func (s *Service) ExpectedMinDuration() time.Duration {
	return 20 * time.Second
}
