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
	"strings"
	"time"

	"github.com/GoogleCloudPlatform/workloadagent/internal/databasecenter"
	"github.com/GoogleCloudPlatform/workloadagent/internal/servicecommunication"
	"github.com/GoogleCloudPlatform/workloadagent/internal/sqlservermetrics"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/recovery"

	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

const (
	dbCenterMetricCollectionFrequencyMin     = 10 * time.Minute
	dbCenterMetricCollectionFrequencyMax     = 6 * time.Hour
	dbCenterMetricCollectionFrequencyDefault = 1 * time.Hour
)

// Service implements the interfaces for SQL Server workload agent service.
type Service struct {
	Config           *configpb.Configuration
	CloudProps       *configpb.CloudProperties
	CommonCh         <-chan *servicecommunication.Message
	isProcessPresent bool
	DBcenterClient   databasecenter.Client
	dwActivated      bool
}

type runMetricCollectionArgs struct {
	s *Service
}

type runDBCenterMetricCollectionArgs struct {
	s *Service
}

var sqlserverProcessSubstring = "sqlserv"

// Start initiates the SQL Server workload agent service
func (s *Service) Start(ctx context.Context, a any) {
	// Check if the enabled field is unset. If it is, then the service is still enabled if the workload is present.
	if s.Config.GetSqlserverConfiguration() == nil || s.Config.GetSqlserverConfiguration().Enabled == nil {
		log.CtxLogger(ctx).Info("SQL Server service enabled field is not set, will check for workload presence to determine if service should be enabled.")
		go (func() {
			for {
				s.checkServiceCommunication(ctx)
			}
		})()
		// If the workload is present, proceed with starting the service even if it is not enabled.
		for !s.isProcessPresent {
			time.Sleep(5 * time.Second)
		}
		log.CtxLogger(ctx).Info("SQL Server workload is present. Starting service.")
	} else if !s.Config.GetSqlserverConfiguration().GetEnabled() {
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
		ExpectedMinDuration: 20 * time.Second,
	}
	metricCollectionRoutine.StartRoutine(mcCtx)

	// Start SQL Server DB Center Metric Collection
	dbcenterMCCtx := log.SetCtx(ctx, "context", "SQLServerDBCenterMetricCollection")
	dbcenterMetricCollectionRoutine := &recovery.RecoverableRoutine{
		Routine:             runDBCenterMetricCollection,
		RoutineArg:          runDBCenterMetricCollectionArgs{s},
		ErrorCode:           usagemetrics.SQLServerMetricCollectionFailure,
		UsageLogger:         *usagemetrics.UsageLogger,
		ExpectedMinDuration: 20 * time.Second,
	}
	dbcenterMetricCollectionRoutine.StartRoutine(dbcenterMCCtx)

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
	return 20 * time.Second
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
		Config:         args.s.Config.GetSqlserverConfiguration(),
		DBcenterClient: args.s.DBcenterClient,
	}
	ticker := time.NewTicker(args.s.Config.GetSqlserverConfiguration().GetCollectionConfiguration().GetCollectionFrequency().AsDuration())
	defer ticker.Stop()
	for {
		r.CollectMetricsOnce(ctx, args.s.dwActivated)
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("SQL Server metric collection cancellation requested")
			return
		case <-ticker.C:
			continue
		}
	}
}

func runDBCenterMetricCollection(ctx context.Context, a any) {
	log.CtxLogger(ctx).Info("Starting SQL Server DB Center Metric Collection")
	var args runDBCenterMetricCollectionArgs
	var ok bool
	if args, ok = a.(runDBCenterMetricCollectionArgs); !ok {
		log.CtxLogger(ctx).Errorf("failed to parse dbcenter metric collection args", "args", a)
		return
	}
	log.CtxLogger(ctx).Debugw("SqlServer dbcenter metric collection args", "args", args)
	r := &sqlservermetrics.SQLServerMetrics{
		Config:         args.s.Config.GetSqlserverConfiguration(),
		DBcenterClient: args.s.DBcenterClient,
	}
	ticker := time.NewTicker(dbcenterMetricCollectionFrequency(args))
	defer ticker.Stop()
	for {
		r.CollectDBCenterMetricsOnce(ctx)
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("SQL Server dbcenter metric collection cancellation requested")
			return
		case <-ticker.C:
			continue
		}
	}
}

func dbcenterMetricCollectionFrequency(args runDBCenterMetricCollectionArgs) time.Duration {
	if args.s == nil || args.s.Config == nil || args.s.Config.GetSqlserverConfiguration() == nil || args.s.Config.GetSqlserverConfiguration().GetCollectionConfiguration() == nil {
		return dbCenterMetricCollectionFrequencyDefault
	}
	freq := args.s.Config.GetSqlserverConfiguration().GetCollectionConfiguration().GetDbcenterMetricsCollectionFrequency().AsDuration()
	if freq < dbCenterMetricCollectionFrequencyMin {
		return dbCenterMetricCollectionFrequencyMin
	}
	if freq > dbCenterMetricCollectionFrequencyMax {
		return dbCenterMetricCollectionFrequencyMax
	}
	return freq
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
		log.CtxLogger(ctx).Debugw("SQL Server workload agent service received a message on the common channel", "message", msg)
		switch msg.Origin {
		case servicecommunication.Discovery:
			log.CtxLogger(ctx).Debug("SQL Server workload agent service received a discovery message")
			for _, p := range msg.DiscoveryResult.Processes {
				name, err := p.Name()
				if err == nil && strings.Contains(name, sqlserverProcessSubstring) {
					s.isProcessPresent = true
					break
				}
			}
		case servicecommunication.DWActivation:
			log.CtxLogger(ctx).Debug("SQL Server workload agent service received a DW activation message")
			s.dwActivated = msg.DWActivationResult.Activated
		default:
			log.CtxLogger(ctx).Debugw("SQL Server workload agent service received a message with an unexpected origin", "origin", msg.Origin)
		}
	}
}
