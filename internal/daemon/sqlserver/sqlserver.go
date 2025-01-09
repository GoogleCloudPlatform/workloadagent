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

// Package sqlserver implements the SQL Server workload agent service.
package sqlserver

import (
	"context"
	"time"

	"github.com/GoogleCloudPlatform/workloadagent/internal/servicecommunication"
	"github.com/GoogleCloudPlatform/workloadagent/internal/sqlservermetrics"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/log"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/recovery"
)

// Service implements the interfaces for SQL Server workload agent service.
type Service struct {
	Config             *configpb.Configuration
	CloudProps         *configpb.CloudProperties
	CommonCh           <-chan *servicecommunication.Message
	processes          servicecommunication.DiscoveryResult
	sqlServerProcesses []servicecommunication.ProcessWrapper
}

type runMetricCollectionArgs struct {
	s *Service
}

// Start initiates the SQL Server workload agent service
func (s *Service) Start(ctx context.Context, a any) {
	if s.Config.GetSqlserverConfiguration() == nil || s.Config.GetSqlserverConfiguration().Enabled == nil {
		// If SQL Server workload agent service is not explicitly enabled in the configuration, then return.
		log.CtxLogger(ctx).Info("SQL Server workload agent service is not explicitly enabled in the configuration")
		return
	}

	if !s.Config.GetSqlserverConfiguration().GetEnabled() {
		// If SQL Server workload agent service is explicitly disabled in the configuration, then return.
		log.CtxLogger(ctx).Info("SQL Server workload agent service is disabled in the configuration")
		return
	}

	// Start SQL Server Metric Collection
	mcCtx := log.SetCtx(ctx, "context", "SQLServerMetricCollection")
	metricCollectionRoutine := &recovery.RecoverableRoutine{
		Routine:             runMetricCollection,
		RoutineArg:          runMetricCollectionArgs{s},
		ErrorCode:           usagemetrics.SQLServerMetricCollectionFailure,
		UsageLogger:         *usagemetrics.UsageLogger,
		ExpectedMinDuration: 0,
	}
	metricCollectionRoutine.StartRoutine(mcCtx)
	for {
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("SQL Server workload agent service cancellation requested")
			return
		}
	}
}

// String returns the name of the SQL Server service.
func (s *Service) String() string {
	return "SQL Server Service"
}

// ErrorCode returns the error code for the SQL Server service.
func (s *Service) ErrorCode() int {
	return usagemetrics.SQLServerServiceError
}

// ExpectedMinDuration returns the expected minimum duration for the SQL Server service.
// Used by the recovery handler to determine if the service ran long enough to be considered
// successful.
func (s *Service) ExpectedMinDuration() time.Duration {
	return 0
}

func runMetricCollection(ctx context.Context, a any) {
	log.CtxLogger(ctx).Info("Starting SQL Server Metric Collection")
	var args runMetricCollectionArgs
	var ok bool
	if args, ok = a.(runMetricCollectionArgs); !ok {
		log.CtxLogger(ctx).Errorf("failed to parse metric collection args", "args", a)
		return
	}
	log.CtxLogger(ctx).Debugw("SqlServer metric collection args", "args", args)
	r := &sqlservermetrics.SQLServerMetrics{
		Config: args.s.Config.GetSqlserverConfiguration(),
	}
	ticker := time.NewTicker(args.s.Config.GetSqlserverConfiguration().GetCollectionConfiguration().GetCollectionFrequency().AsDuration())
	defer ticker.Stop()
	for {
		r.CollectMetricsOnce(ctx)
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("SQL Server metric collection cancellation requested")
			return
		case <-ticker.C:
			continue
		}
	}
}
