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
	"github.com/GoogleCloudPlatform/workloadagent/internal/commondiscovery"
	"github.com/GoogleCloudPlatform/workloadagent/internal/mysqldiscovery"
	"github.com/GoogleCloudPlatform/workloadagent/internal/mysqlmetrics"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/gce"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/log"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/recovery"
)

// Service implements the interfaces for MySQL workload agent service.
type Service struct {
	Config         *configpb.Configuration
	CloudProps     *configpb.CloudProperties
	CommonCh       chan commondiscovery.Result
	processes      commondiscovery.Result
	mySQLProcesses []commondiscovery.ProcessWrapper
	// ... MySQL-specific attributes ...
}

type runDiscoveryArgs struct {
	s *Service
}

type runMetricCollectionArgs struct {
	s *Service
}

// Start initiates the MySQL workload agent service
func (s *Service) Start(ctx context.Context, a any) {
	if s.Config.GetMysqlConfiguration() == nil || s.Config.GetMysqlConfiguration().Enabled == nil {
		// If MySQL workload agent service is not explicitly enabled in the configuration,
		// then check if the workload is present on the host.
		log.CtxLogger(ctx).Info("MySQL workload agent service is not explicitly enabled in the configuration")
		if !s.checkCommonDiscovery(ctx) {
			return
		}
	} else if !s.Config.GetMysqlConfiguration().GetEnabled() {
		// If MySQL workload agent service is explicitly disabled in the configuration, then return.
		log.CtxLogger(ctx).Info("MySQL workload agent service is disabled in the configuration")
		return
	}

	// Start MySQL Discovery
	dCtx := log.SetCtx(ctx, "context", "MySQLDiscovery")
	discoveryRoutine := &recovery.RecoverableRoutine{
		Routine:             runDiscovery,
		RoutineArg:          runDiscoveryArgs{s},
		ErrorCode:           usagemetrics.MySQLDiscoveryFailure,
		UsageLogger:         *usagemetrics.UsageLogger,
		ExpectedMinDuration: 0,
	}
	discoveryRoutine.StartRoutine(dCtx)

	// Start MySQL Metric Collection
	mcCtx := log.SetCtx(ctx, "context", "MySQLMetricCollection")
	metricCollectionRoutine := &recovery.RecoverableRoutine{
		Routine:             runMetricCollection,
		RoutineArg:          runMetricCollectionArgs{s},
		ErrorCode:           usagemetrics.MySQLMetricCollectionFailure,
		UsageLogger:         *usagemetrics.UsageLogger,
		ExpectedMinDuration: 0,
	}
	metricCollectionRoutine.StartRoutine(mcCtx)
	select {
	case <-ctx.Done():
		log.CtxLogger(ctx).Info("MySQL workload agent service cancellation requested")
		return
	case s.processes = <-s.CommonCh:
		log.CtxLogger(ctx).Debugw("MySQL workload agent service received common discovery result", "result", s.processes)
		s.identifyMySQLProcesses(ctx)
		return
	}
}

func runDiscovery(ctx context.Context, a any) {
	log.CtxLogger(ctx).Info("Starting MySQL Discovery")
	var args runDiscoveryArgs
	var ok bool
	if args, ok = a.(runDiscoveryArgs); !ok {
		log.CtxLogger(ctx).Warnw("failed to parse discovery args", "args", a)
		return
	}
	log.CtxLogger(ctx).Debugw("MySQL discovery args", "args", args)
	ticker := time.NewTicker(10 * time.Minute)
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

func runMetricCollection(ctx context.Context, a any) {
	log.CtxLogger(ctx).Info("Starting MySQL Metric Collection")
	var args runMetricCollectionArgs
	var ok bool
	if args, ok = a.(runMetricCollectionArgs); !ok {
		log.CtxLogger(ctx).Warnw("failed to parse metric collection args", "args", a)
		return
	}
	log.CtxLogger(ctx).Debugw("MySQL metric collection args", "args", args)
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	gceService, err := gce.NewGCEClient(ctx)
	if err != nil {
		usagemetrics.Error(usagemetrics.GCEServiceCreationFailure)
		log.CtxLogger(ctx).Errorf("initializing GCE services: %w", err)
		return
	}
	m := mysqlmetrics.New(ctx, args.s.Config)
	err = m.InitDB(ctx, gceService)
	if err != nil {
		log.CtxLogger(ctx).Errorf("failed to initialize MySQL DB: %v", err)
		return
	}
	for {
		m.CollectMetricsOnce(ctx)
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("MySQL metric collection cancellation requested")
			return
		case <-ticker.C:
			continue
		}
	}
}

// checkCommonDiscovery checks for common discovery results.
// Returns true if MySQL workload is present on the host.
// Otherwise, returns false to indicate that the context is done.
func (s *Service) checkCommonDiscovery(ctx context.Context) bool {
	for {
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("MySQL workload agent service cancellation requested")
			return false
		case s.processes = <-s.CommonCh:
			log.CtxLogger(ctx).Debugw("MySQL workload agent service received common discovery result", "NumProcesses", len(s.processes.Processes))
			s.identifyMySQLProcesses(ctx)
			if s.isWorkloadPresent() {
				log.CtxLogger(ctx).Info("MySQL workload is present, starting discovery and metric collection")
				return true
			}
			log.CtxLogger(ctx).Debug("MySQL workload is not present")
		}
	}
}

func (s *Service) identifyMySQLProcesses(ctx context.Context) {
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
	return 0
}
