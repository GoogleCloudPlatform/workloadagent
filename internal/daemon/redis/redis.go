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

// Package redis implements the Redis workload agent service.
package redis

import (
	"context"
	"time"

	"go.uber.org/zap/zapcore"
	"github.com/GoogleCloudPlatform/workloadagent/internal/commondiscovery"
	"github.com/GoogleCloudPlatform/workloadagent/internal/redisdiscovery"
	"github.com/GoogleCloudPlatform/workloadagent/internal/redismetrics"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/log"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/recovery"
)

// Service implements the interfaces for Redis workload agent service.
type Service struct {
	Config         *configpb.Configuration
	CloudProps     *configpb.CloudProperties
	CommonCh       chan commondiscovery.Result
	processes      commondiscovery.Result
	redisProcesses []commondiscovery.ProcessWrapper
}

type runDiscoveryArgs struct {
	s *Service
}

type runMetricCollectionArgs struct {
	s *Service
}

// Start initiates the Redis workload agent service
func (s *Service) Start(ctx context.Context, a any) {
	if s.Config.GetRedisConfiguration() == nil || s.Config.GetRedisConfiguration().Enabled == nil {
		// If Redis workload agent service is not explicitly enabled in the configuration,
		// then check if the workload is present on the host.
		log.CtxLogger(ctx).Info("Redis workload agent service is not explicitly enabled in the configuration")
		if !s.checkCommonDiscovery(ctx) {
			return
		}
	} else if !s.Config.GetRedisConfiguration().GetEnabled() {
		// If Redis workload agent service is explicitly disabled in the configuration, then return.
		log.CtxLogger(ctx).Info("Redis workload agent service is disabled in the configuration")
		return
	}

	// Start Redis Discovery
	dCtx := log.SetCtx(ctx, "context", "RedisDiscovery")
	discoveryRoutine := &recovery.RecoverableRoutine{
		Routine:             runDiscovery,
		RoutineArg:          runDiscoveryArgs{s},
		ErrorCode:           usagemetrics.RedisDiscoveryFailure,
		UsageLogger:         *usagemetrics.UsageLogger,
		ExpectedMinDuration: 0,
	}
	discoveryRoutine.StartRoutine(dCtx)

	// Start Redis Metric Collection
	mcCtx := log.SetCtx(ctx, "context", "RedisMetricCollection")
	metricCollectionRoutine := &recovery.RecoverableRoutine{
		Routine:             runMetricCollection,
		RoutineArg:          runMetricCollectionArgs{s},
		ErrorCode:           usagemetrics.RedisMetricCollectionFailure,
		UsageLogger:         *usagemetrics.UsageLogger,
		ExpectedMinDuration: 0,
	}
	metricCollectionRoutine.StartRoutine(mcCtx)
	select {
	case <-ctx.Done():
		log.CtxLogger(ctx).Info("Redis workload agent service cancellation requested")
		return
	case s.processes = <-s.CommonCh:
		log.CtxLogger(ctx).Debugw("Redis workload agent service received common discovery result", "result", s.processes)
		s.identifyRedisProcesses(ctx)
		return
	}
}

func runDiscovery(ctx context.Context, a any) {
	log.CtxLogger(ctx).Info("Starting Redis Discovery")
	var args runDiscoveryArgs
	var ok bool
	if args, ok = a.(runDiscoveryArgs); !ok {
		log.CtxLogger(ctx).Warnw("failed to parse discovery args", "args", a)
		return
	}
	log.CtxLogger(ctx).Debugw("Redis discovery args", "args", args)
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("Redis discovery cancellation requested")
			return
		case <-ticker.C:
			redisdiscovery.Discover(ctx)
		}
	}
}

func runMetricCollection(ctx context.Context, a any) {
	log.CtxLogger(ctx).Info("Starting Redis Metric Collection")
	var args runMetricCollectionArgs
	var ok bool
	if args, ok = a.(runMetricCollectionArgs); !ok {
		log.CtxLogger(ctx).Warnw("failed to parse metric collection args", "args", a)
		return
	}
	log.CtxLogger(ctx).Debugw("Redis metric collection args", "args", args)
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for {
		redismetrics.CollectMetricsOnce(ctx)
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("Redis metric collection cancellation requested")
			return
		case <-ticker.C:
			continue
		}
	}
}

// checkCommonDiscovery checks for common discovery results.
// Returns true if Redis workload is present on the host.
// Otherwise, returns false to indicate that the context is done.
func (s *Service) checkCommonDiscovery(ctx context.Context) bool {
	for {
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("Redis workload agent service cancellation requested")
			return false
		case s.processes = <-s.CommonCh:
			log.CtxLogger(ctx).Debugw("Redis workload agent service received common discovery result", "NumProcesses", len(s.processes.Processes))
			s.identifyRedisProcesses(ctx)
			if s.isWorkloadPresent() {
				log.CtxLogger(ctx).Info("Redis workload is present, starting discovery and metric collection")
				return true
			}
			log.CtxLogger(ctx).Debug("Redis workload is not present")
		}
	}
}

func (s *Service) identifyRedisProcesses(ctx context.Context) {
	for _, process := range s.processes.Processes {
		name, err := process.Name()
		if err == nil && name == "redis-server" {
			s.redisProcesses = append(s.redisProcesses, process)
		}
	}
	s.logRedisProcesses(ctx, zapcore.DebugLevel)
}

func (s *Service) isWorkloadPresent() bool {
	return len(s.redisProcesses) > 0
}

func (s *Service) logRedisProcesses(ctx context.Context, loglevel zapcore.Level) {
	log.CtxLogger(ctx).Logf(loglevel, "Number of Redis processes found: %v", len(s.redisProcesses))
	for _, process := range s.redisProcesses {
		name, _ := process.Name()
		username, _ := process.Username()
		cmdline, _ := process.CmdlineSlice()
		env, _ := process.Environ()
		log.CtxLogger(ctx).Logw(loglevel, "Redis process", "name", name, "username", username, "cmdline", cmdline, "env", env, "pid", process.Pid())
	}
}

// String returns the name of the Redis service.
func (s *Service) String() string {
	return "Redis Service"
}

// ErrorCode returns the error code for the Redis service.
func (s *Service) ErrorCode() int {
	return usagemetrics.RedisServiceError
}

// ExpectedMinDuration returns the expected minimum duration for the Redis service.
// Used by the recovery handler to determine if the service ran long enough to be considered
// successful.
func (s *Service) ExpectedMinDuration() time.Duration {
	return 0
}
