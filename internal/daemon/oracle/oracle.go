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
	"slices"
	"time"

	"github.com/GoogleCloudPlatform/sapagent/shared/log"
	"github.com/GoogleCloudPlatform/sapagent/shared/recovery"
	"github.com/GoogleCloudPlatform/workloadagent/internal/oraclediscovery"
	"github.com/GoogleCloudPlatform/workloadagent/internal/oraclemetrics"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	odpb "github.com/GoogleCloudPlatform/workloadagent/protos/oraclediscovery"
)

// Service implements the interfaces for Oracle workload agent service.
type Service struct {
	Config                  *cpb.Configuration
	CloudProps              *cpb.CloudProperties
	metricCollectionRoutine *recovery.RecoverableRoutine
	discoveryRoutine        *recovery.RecoverableRoutine
	ch                      chan *odpb.Discovery
	currentSIDs             []string
}

type runDiscoveryArgs struct {
	s *Service
}

type runMetricCollectionArgs struct {
	s *Service
}

// Start initiates the Oracle workload agent service
func (s *Service) Start(ctx context.Context, a any) {
	if !s.Config.GetOracleConfiguration().GetEnabled() {
		log.CtxLogger(ctx).Info("Oracle service is disabled")
		return
	}
	if runtime.GOOS != "linux" {
		log.CtxLogger(ctx).Error("Oracle service is only supported on Linux")
		return
	}

	// Create a channel to pass discovery data to the metric collection routine.
	s.ch = make(chan *odpb.Discovery)

	// Start Oracle Discovery
	dCtx := log.SetCtx(ctx, "context", "OracleDiscovery")
	s.discoveryRoutine = &recovery.RecoverableRoutine{
		Routine:             runDiscovery,
		RoutineArg:          runDiscoveryArgs{s},
		ErrorCode:           usagemetrics.OracleDiscoverDatabaseFailure,
		UsageLogger:         *usagemetrics.UsageLogger,
		ExpectedMinDuration: 0,
	}
	s.discoveryRoutine.StartRoutine(dCtx)

	if !s.Config.GetOracleConfiguration().GetOracleMetrics().GetEnabled() {
		log.CtxLogger(ctx).Info("Oracle Metric Collection is disabled")
		return
	}

	// Start Metric Collection
	mcCtx := log.SetCtx(ctx, "context", "OracleMetricCollection")
	s.metricCollectionRoutine = &recovery.RecoverableRoutine{
		Routine:             runMetricCollection,
		RoutineArg:          runMetricCollectionArgs{s},
		ErrorCode:           usagemetrics.OracleMetricCollectionFailure,
		UsageLogger:         *usagemetrics.UsageLogger,
		ExpectedMinDuration: 0,
	}
	s.metricCollectionRoutine.StartRoutine(mcCtx)
}

func runDiscovery(ctx context.Context, a any) {
	log.CtxLogger(ctx).Info("Starting Oracle Discovery")
	var args runDiscoveryArgs
	var ok bool
	if args, ok = a.(runDiscoveryArgs); !ok {
		log.CtxLogger(ctx).Warn("args is not of type runDiscoveryArgs")
		return
	}

	ticker := time.NewTicker(args.s.Config.GetOracleConfiguration().GetOracleDiscovery().GetUpdateFrequency().AsDuration())
	defer ticker.Stop()

	ds := oraclediscovery.New()

	for {
		if err := args.s.sendDiscovery(ctx, ds); err != nil {
			log.CtxLogger(ctx).Errorw("Failed to send discovery data to the metric collection routine", "error", err)
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

// sendDiscovery sends the discovery proto to the metric collection routine if the SIDs have changed.
func (s *Service) sendDiscovery(ctx context.Context, ds *oraclediscovery.DiscoveryService) error {
	discovery, err := ds.Discover(ctx, s.CloudProps)
	if err != nil {
		return err
	}

	newSIDs, err := oraclemetrics.ExtractSIDs(discovery)
	if err != nil {
		return fmt.Errorf("extracting SIDs from the discovery data: %w", err)
	}
	if !slices.Equal(s.currentSIDs, newSIDs) {
		log.CtxLogger(ctx).Infow("New databases discovered or existing ones removed; refreshing database connections", "SIDs", newSIDs)
		s.currentSIDs = newSIDs
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("Oracle Discovery cancellation requested")
			return nil
		case s.ch <- discovery:
		}
	}
	return nil
}

func runMetricCollection(ctx context.Context, a any) {
	log.CtxLogger(ctx).Info("Starting Oracle Metric Collection")
	var args runMetricCollectionArgs
	var ok bool
	if args, ok = a.(runMetricCollectionArgs); !ok {
		log.CtxLogger(ctx).Warnw("Failed to parse metric collection args", "args", a)
		return
	}

	ticker := time.NewTicker(args.s.Config.GetOracleConfiguration().GetOracleMetrics().GetCollectionFrequency().AsDuration())
	defer ticker.Stop()

	metricCollector, err := oraclemetrics.New(ctx, args.s.Config)
	if err != nil {
		log.CtxLogger(ctx).Errorw("Failed to initialize metric collector", "error", err)
		return
	}

	metricCollector.RefreshDBConnections(ctx, <-args.s.ch)

	for {
		metricCollector.CollectDBMetricsOnce(ctx)

		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("Metric Collection cancellation requested")
			metricCollector.Stop(ctx)
			return
		case discovery := <-args.s.ch:
			metricCollector.RefreshDBConnections(ctx, discovery)
		case <-ticker.C:
			continue
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
	return 0
}
