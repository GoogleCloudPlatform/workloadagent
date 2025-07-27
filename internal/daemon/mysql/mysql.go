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

// Package mysql implements the MySQL workload agent service.
package mysql

import (
	"context"
	"time"

	"go.uber.org/zap/zapcore"
	"github.com/GoogleCloudPlatform/workloadagent/internal/databasecenter"
	"github.com/GoogleCloudPlatform/workloadagent/internal/mysqldiscovery"
	"github.com/GoogleCloudPlatform/workloadagent/internal/mysqlmetrics"
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
	wlmMetricCollectionFrequencyDefault      = 5 * time.Minute
	dbCenterMetricCollectionFrequencyMin     = 10 * time.Minute
	dbCenterMetricCollectionFrequencyMax     = 6 * time.Hour
	dbCenterMetricCollectionFrequencyDefault = 1 * time.Hour
)

var (
	newGCEClient    = gce.NewGCEClient
	newMySQLMetrics = func(ctx context.Context, config *configpb.Configuration, wlmClient workloadmanager.WLMWriter, dbcenterClient databasecenter.Client) MetricsInterface {
		return mysqlmetrics.New(ctx, config, wlmClient, dbcenterClient)
	}
	newTicker = time.NewTicker
)

// MetricsInterface defines an interface for mysqlmetrics.MySQLMetrics to allow faking
type MetricsInterface interface {
	InitDB(ctx context.Context, gceService mysqlmetrics.GceInterface) error
	CollectWlmMetricsOnce(ctx context.Context, dwActivated bool) (*workloadmanager.WorkloadMetrics, error)
	CollectDBCenterMetricsOnce(ctx context.Context) error
}

// Service implements the interfaces for MySQL workload agent service.
type Service struct {
	Config         *configpb.Configuration
	CloudProps     *configpb.CloudProperties
	CommonCh       <-chan *servicecommunication.Message
	processes      servicecommunication.DiscoveryResult
	mySQLProcesses []servicecommunication.ProcessWrapper
	dwActivated    bool
	WLMClient      workloadmanager.WLMWriter
	DBcenterClient databasecenter.Client
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

// Start initiates the MySQL workload agent service
func (s *Service) Start(ctx context.Context, a any) {
	if s.Config.GetMysqlConfiguration() != nil && !s.Config.GetMysqlConfiguration().GetEnabled() {
		// If MySQL workload agent service is explicitly disabled in the configuration, then return.
		log.CtxLogger(ctx).Info("MySQL workload agent service is disabled in the configuration")
		return
	}

	go (func() {
		for {
			s.checkServiceCommunication(ctx)
		}
	})()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	enabled := s.Config.GetMysqlConfiguration().GetEnabled()
EnableCheck:
	for {
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("MySQL workload agent service cancellation requested")
			return
		case <-ticker.C:
			// Once the workload is present/enabled, start discovery and metric collection.
			if s.isWorkloadPresent() || enabled {
				log.CtxLogger(ctx).Info("MySQL workload agent service is enabled. Starting discovery and metric collection")
				break EnableCheck
			}
		}
	}

	// Start MySQL Discovery
	dCtx := log.SetCtx(ctx, "context", "MySQLDiscovery")
	discoveryRoutine := &recovery.RecoverableRoutine{
		Routine:             runDiscovery,
		RoutineArg:          runDiscoveryArgs{s},
		ErrorCode:           usagemetrics.MySQLDiscoveryFailure,
		UsageLogger:         *usagemetrics.UsageLogger,
		ExpectedMinDuration: 20 * time.Second,
	}
	discoveryRoutine.StartRoutine(dCtx)

	// Start MySQL Wlm Metric Collection
	mcCtx := log.SetCtx(ctx, "context", "MySQLWlmMetricCollection")
	wlmMetricCollectionRoutine := &recovery.RecoverableRoutine{
		Routine:             runWlmMetricCollection,
		RoutineArg:          runWlmMetricCollectionArgs{s},
		ErrorCode:           usagemetrics.MySQLMetricCollectionFailure,
		UsageLogger:         *usagemetrics.UsageLogger,
		ExpectedMinDuration: 20 * time.Second,
	}
	wlmMetricCollectionRoutine.StartRoutine(mcCtx)

	// Start MySQL DB Center Metric Collection
	dbcenterMCCtx := log.SetCtx(ctx, "context", "MySQLDBCenterMetricCollection")
	dbcenterMetricCollectionRoutine := &recovery.RecoverableRoutine{
		Routine:             runDBCenterMetricCollection,
		RoutineArg:          runDBCenterMetricCollectionArgs{s},
		ErrorCode:           usagemetrics.MySQLMetricCollectionFailure,
		UsageLogger:         *usagemetrics.UsageLogger,
		ExpectedMinDuration: 20 * time.Second,
	}
	dbcenterMetricCollectionRoutine.StartRoutine(dbcenterMCCtx)
	select {
	case <-ctx.Done():
		log.CtxLogger(ctx).Info("MySQL workload agent service cancellation requested")
		return
	}
}

func runDiscovery(ctx context.Context, a any) {
	log.CtxLogger(ctx).Info("Starting MySQL Discovery")
	var args runDiscoveryArgs
	var ok bool
	if args, ok = a.(runDiscoveryArgs); !ok {
		log.CtxLogger(ctx).Errorw("failed to parse discovery args", "args", a)
		return
	}
	log.CtxLogger(ctx).Debugw("MySQL discovery args", "args", args)
	ticker := time.NewTicker(discoveryFrequency)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("MySQL discovery cancellation requested")
			return
		case <-ticker.C:
			mysqldiscovery.Discover(ctx)
		}
	}
}

func getDbCenterMetricCollectionFrequency(args runDBCenterMetricCollectionArgs) time.Duration {
	metricCollectionFrequencyDefault := dbCenterMetricCollectionFrequencyDefault
	if args.s == nil || args.s.Config == nil {
		return metricCollectionFrequencyDefault
	}
	config := args.s.Config.GetMysqlConfiguration()
	if config == nil || config.DbcenterCollectionFrequency == nil {
		return dbCenterMetricCollectionFrequencyDefault
	}
	freq := config.DbcenterCollectionFrequency.AsDuration()
	if freq < dbCenterMetricCollectionFrequencyMin {
		return dbCenterMetricCollectionFrequencyMin
	}
	if freq > dbCenterMetricCollectionFrequencyMax {
		return dbCenterMetricCollectionFrequencyMax
	}
	return freq
}

func runDBCenterMetricCollection(ctx context.Context, a any) {
	log.CtxLogger(ctx).Info("Starting MySQL DB Center Metric Collection")
	var args runDBCenterMetricCollectionArgs
	var ok bool
	if args, ok = a.(runDBCenterMetricCollectionArgs); !ok {
		log.CtxLogger(ctx).Errorw("failed to parse dbcenter metric collection args", "args", a)
		return
	}
	log.CtxLogger(ctx).Debugw("MySQL dbcenter metric collection args", "args", args)
	ticker := newTicker(getDbCenterMetricCollectionFrequency(args))
	defer ticker.Stop()
	gceService, err := newGCEClient(ctx)
	if err != nil {
		usagemetrics.Error(usagemetrics.GCEServiceCreationFailure)
		log.CtxLogger(ctx).Errorf("initializing GCE services: %w", err)
		return
	}
	m := newMySQLMetrics(ctx, args.s.Config, args.s.WLMClient, args.s.DBcenterClient)
	err = m.InitDB(ctx, gceService)
	if err != nil {
		log.CtxLogger(ctx).Errorf("failed to initialize MySQL DB: %v", err)
		return
	}
	for {
		err := m.CollectDBCenterMetricsOnce(ctx)
		if err != nil {
			log.CtxLogger(ctx).Debugf("failed to collect MySQL metrics: %v", err)
		}
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("MySQL metric collection cancellation requested")
			return
		case <-ticker.C:
			continue
		}
	}
}

func runWlmMetricCollection(ctx context.Context, a any) {
	log.CtxLogger(ctx).Info("Starting MySQL Metric Collection")
	var args runWlmMetricCollectionArgs
	var ok bool
	if args, ok = a.(runWlmMetricCollectionArgs); !ok {
		log.CtxLogger(ctx).Errorw("failed to parse metric collection args", "args", a)
		return
	}
	log.CtxLogger(ctx).Debugw("MySQL metric collection args", "args", args)

	ticker := newTicker(wlmMetricCollectionFrequencyDefault)
	defer ticker.Stop()
	gceService, err := newGCEClient(ctx)
	if err != nil {
		usagemetrics.Error(usagemetrics.GCEServiceCreationFailure)
		log.CtxLogger(ctx).Errorf("initializing GCE services: %w", err)
		return
	}
	m := newMySQLMetrics(ctx, args.s.Config, args.s.WLMClient, args.s.DBcenterClient)
	err = m.InitDB(ctx, gceService)
	if err != nil {
		log.CtxLogger(ctx).Errorf("failed to initialize MySQL DB: %v", err)
		return
	}
	for {
		_, err := m.CollectWlmMetricsOnce(ctx, args.s.dwActivated)
		if err != nil {
			log.CtxLogger(ctx).Debugf("failed to collect MySQL metrics: %v", err)
		}
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("MySQL metric collection cancellation requested")
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
		log.CtxLogger(ctx).Debugw("MySQL workload agent service received a message on the common channel", "message", msg)
		switch msg.Origin {
		case servicecommunication.Discovery:
			s.processes = msg.DiscoveryResult
			s.identifyMySQLProcesses(ctx)
		case servicecommunication.DWActivation:
			s.dwActivated = msg.DWActivationResult.Activated
		default:
			log.CtxLogger(ctx).Debugw("MySQL workload agent service received a message with an unexpected origin", "origin", msg.Origin)
		}
	}
}

func (s *Service) identifyMySQLProcesses(ctx context.Context) {
	s.mySQLProcesses = []servicecommunication.ProcessWrapper{}
	for _, process := range s.processes.Processes {
		name, err := process.Name()
		if err == nil && name == "mysqld" {
			s.mySQLProcesses = append(s.mySQLProcesses, process)
		}
	}
	s.logMySQLProcesses(ctx, zapcore.DebugLevel)
}

func (s *Service) isWorkloadPresent() bool {
	return len(s.mySQLProcesses) > 0
}

func (s *Service) logMySQLProcesses(ctx context.Context, loglevel zapcore.Level) {
	log.CtxLogger(ctx).Logf(loglevel, "Number of processes found: %v", len(s.processes.Processes))
	log.CtxLogger(ctx).Logf(loglevel, "Number of MySQL processes found: %v", len(s.mySQLProcesses))
	for _, process := range s.mySQLProcesses {
		name, _ := process.Name()
		username, _ := process.Username()
		cmdline, _ := process.CmdlineSlice()
		env, _ := process.Environ()
		log.CtxLogger(ctx).Logw(loglevel, "MySQL process", "name", name, "username", username, "cmdline", cmdline, "env", env, "pid", process.Pid())
	}
}

// String returns the name of the MySQL service.
func (s *Service) String() string {
	return "MySQL Service"
}

// ErrorCode returns the error code for the MySQL service.
func (s *Service) ErrorCode() int {
	return usagemetrics.MySQLServiceError
}

// ExpectedMinDuration returns the expected minimum duration for the MySQL service.
// Used by the recovery handler to determine if the service ran long enough to be considered
// successful.
func (s *Service) ExpectedMinDuration() time.Duration {
	return 20 * time.Second
}
