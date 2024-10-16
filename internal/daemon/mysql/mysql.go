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

	"github.com/GoogleCloudPlatform/sapagent/shared/log"
	"github.com/GoogleCloudPlatform/sapagent/shared/recovery"
	"github.com/GoogleCloudPlatform/workloadagent/internal/mysqldiscovery"
	"github.com/GoogleCloudPlatform/workloadagent/internal/mysqlmetrics"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

// Service implements the interfaces for MySQL workload agent service.
type Service struct {
	Config     *cpb.Configuration
	CloudProps *cpb.CloudProperties
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
	if !s.Config.GetMysqlConfiguration().GetEnabled() {
		log.CtxLogger(ctx).Info("MySQL workload agent service is disabled")
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
	for {
		mysqlmetrics.CollectMetricsOnce(ctx)
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("MySQL metric collection cancellation requested")
			return
		case <-ticker.C:
			continue
		}
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
