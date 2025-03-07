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
	"github.com/GoogleCloudPlatform/workloadagent/internal/redisdiscovery"
	"github.com/GoogleCloudPlatform/workloadagent/internal/redismetrics"
	"github.com/GoogleCloudPlatform/workloadagent/internal/servicecommunication"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	"github.com/GoogleCloudPlatform/workloadagent/internal/workloadmanager"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/osinfo"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/recovery"
)

const (
	discoveryFrequency     = 10 * time.Minute
	wlmCollectionFrequency = 5 * time.Minute
)

// Service implements the interfaces for Redis workload agent service.
type Service struct {
	Config         *configpb.Configuration
	CloudProps     *configpb.CloudProperties
	CommonCh       <-chan *servicecommunication.Message
	processes      servicecommunication.DiscoveryResult
	redisProcesses []servicecommunication.ProcessWrapper
	dwActivated    bool
	WLMClient      workloadmanager.WLMWriter
	OSData         osinfo.Data
}

type runDiscoveryArgs struct {
	s *Service
}

type runMetricCollectionArgs struct {
	s *Service
}

// Start initiates the Redis workload agent service
func (s *Service) Start(ctx context.Context, a any) {
	if s.Config.GetRedisConfiguration() != nil && !s.Config.GetRedisConfiguration().GetEnabled() {
		// If Redis workload agent service is explicitly disabled in the configuration, then return.
		log.CtxLogger(ctx).Info("Redis workload agent service is disabled in the configuration")
		return
	}

	go (func() {
		for {
			s.checkServiceCommunication(ctx)
		}
	})()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	enabled := s.Config.GetRedisConfiguration().GetEnabled()
EnableCheck:
	for {
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("Redis workload agent service cancellation requested")
			return
		case <-ticker.C:
			// Once DW is enabled and the workload is present/enabled, start discovery and metric collection.
			if s.dwActivated && (s.isWorkloadPresent() || enabled) {
				log.CtxLogger(ctx).Info("Redis workload agent service is enabled and DW is activated. Starting discovery and metric collection")
				break EnableCheck
			}
		}
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
	}
}

func runDiscovery(ctx context.Context, a any) {
	log.CtxLogger(ctx).Info("Starting Redis Discovery")
	var args runDiscoveryArgs
	var ok bool
	if args, ok = a.(runDiscoveryArgs); !ok {
		log.CtxLogger(ctx).Errorw("failed to parse discovery args", "args", a)
		return
	}
	log.CtxLogger(ctx).Debugw("Redis discovery args", "args", args)
	ticker := time.NewTicker(discoveryFrequency)
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
		log.CtxLogger(ctx).Errorf("failed to parse metric collection args", "args", a)
		return
	}
	log.CtxLogger(ctx).Debugw("Redis metric collection args", "args", args)
	gceService, err := gce.NewGCEClient(ctx)
	if err != nil {
		usagemetrics.Error(usagemetrics.GCEServiceCreationFailure)
		log.CtxLogger(ctx).Errorf("initializing GCE services: %w", err)
		return
	}
	r := redismetrics.New(ctx, args.s.Config, args.s.WLMClient, args.s.OSData)
	err = r.InitDB(ctx, gceService)
	if err != nil {
		log.CtxLogger(ctx).Errorf("failed to initialize Redis DB client", "error", err)
		return
	}
	ticker := time.NewTicker(wlmCollectionFrequency)
	defer ticker.Stop()
	for {
		r.CollectMetricsOnce(ctx)
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("Redis metric collection cancellation requested")
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
		log.CtxLogger(ctx).Debugw("Redis workload agent service received a message on the common channel", "message", msg)
		switch msg.Origin {
		case servicecommunication.Discovery:
			s.processes = msg.DiscoveryResult
			s.identifyRedisProcesses(ctx)
		case servicecommunication.DWActivation:
			s.dwActivated = msg.DWActivationResult.Activated
		default:
			log.CtxLogger(ctx).Debugw("Redis workload agent service received a message with an unexpected origin", "origin", msg.Origin)
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
