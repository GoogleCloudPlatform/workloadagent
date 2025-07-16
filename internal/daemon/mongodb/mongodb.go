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

// Package mongodb implements the MongoDB workload agent service.
package mongodb

import (
	"context"
	"strings"
	"time"

	"go.uber.org/zap/zapcore"
	"github.com/GoogleCloudPlatform/workloadagent/internal/databasecenter"
	"github.com/GoogleCloudPlatform/workloadagent/internal/mongodbdiscovery"
	"github.com/GoogleCloudPlatform/workloadagent/internal/mongodbmetrics"
	"github.com/GoogleCloudPlatform/workloadagent/internal/servicecommunication"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	"github.com/GoogleCloudPlatform/workloadagent/internal/workloadmanager"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/recovery"

	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

const (
	discoveryFrequency               = 10 * time.Minute
	metricCollectionFrequencyMin     = 10 * time.Minute
	metricCollectionFrequencyMax     = 6 * time.Hour
	metricCollectionFrequencyDefault = 1 * time.Hour
)

// Service implements the interfaces for MongoDB workload agent service.
type Service struct {
	Config           *configpb.Configuration
	CloudProps       *configpb.CloudProperties
	CommonCh         <-chan *servicecommunication.Message
	processes        servicecommunication.DiscoveryResult
	mongodbProcesses []servicecommunication.ProcessWrapper
	dwActivated      bool
	WLMClient        workloadmanager.WLMWriter
	DBcenterClient   databasecenter.Client
}

type runDiscoveryArgs struct {
	s *Service
}

type runMetricCollectionArgs struct {
	s *Service
}

// Start initiates the MongoDB workload agent service
func (s *Service) Start(ctx context.Context, a any) {
	if s.Config.GetMongoDbConfiguration() != nil && !s.Config.GetMongoDbConfiguration().GetEnabled() {
		// If MongoDB workload agent service is explicitly disabled in the configuration, then return.
		log.CtxLogger(ctx).Info("MongoDB workload agent service is disabled in the configuration")
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
	enabled := s.Config.GetMongoDbConfiguration().GetEnabled()
EnableCheck:
	for {
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("MongoDB workload agent service cancellation requested")
			return
		case <-ticker.C:
			// Once the workload is present/enabled, start discovery and metric collection.
			if s.isWorkloadPresent() || enabled {
				log.CtxLogger(ctx).Info("MongoDB workload agent service is enabled. Starting discovery and metric collection")
				break EnableCheck
			}
		}
	}

	// Start MongoDB Discovery
	dCtx := log.SetCtx(ctx, "context", "MongoDBDiscovery")
	discoveryRoutine := &recovery.RecoverableRoutine{
		Routine:             runDiscovery,
		RoutineArg:          runDiscoveryArgs{s},
		ErrorCode:           usagemetrics.MongoDBDiscoveryFailure,
		UsageLogger:         *usagemetrics.UsageLogger,
		ExpectedMinDuration: 20 * time.Second,
	}
	discoveryRoutine.StartRoutine(dCtx)

	// Start MongoDB Metric Collection
	mcCtx := log.SetCtx(ctx, "context", "MongoDBMetricCollection")
	metricCollectionRoutine := &recovery.RecoverableRoutine{
		Routine:             runMetricCollection,
		RoutineArg:          runMetricCollectionArgs{s},
		ErrorCode:           usagemetrics.MongoDBMetricCollectionFailure,
		UsageLogger:         *usagemetrics.UsageLogger,
		ExpectedMinDuration: 20 * time.Second,
	}
	metricCollectionRoutine.StartRoutine(mcCtx)
	select {
	case <-ctx.Done():
		log.CtxLogger(ctx).Info("MongoDB workload agent service cancellation requested")
		return
	}
}

func runDiscovery(ctx context.Context, a any) {
	log.CtxLogger(ctx).Info("Starting MongoDB Discovery")
	var args runDiscoveryArgs
	var ok bool
	if args, ok = a.(runDiscoveryArgs); !ok {
		log.CtxLogger(ctx).Errorw("Failed to parse discovery args", "args", a)
		return
	}
	log.CtxLogger(ctx).Debugw("MongoDB discovery args", "args", args)
	ticker := time.NewTicker(discoveryFrequency)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("MongoDB discovery cancellation requested")
			return
		case <-ticker.C:
			mongodbdiscovery.Discover(ctx)
		}
	}
}

func metricCollectionFrequency(args runMetricCollectionArgs) time.Duration {
	if args.s == nil || args.s.Config == nil {
		return metricCollectionFrequencyDefault
	}
	config := args.s.Config.GetMongoDbConfiguration()
	if config == nil || config.CollectionFrequency == nil {
		return metricCollectionFrequencyDefault
	}
	freq := config.GetCollectionFrequency().AsDuration()
	if freq < metricCollectionFrequencyMin {
		return metricCollectionFrequencyMin
	}
	if freq > metricCollectionFrequencyMax {
		return metricCollectionFrequencyMax
	}
	return freq
}

func runMetricCollection(ctx context.Context, a any) {
	log.CtxLogger(ctx).Info("Starting MongoDB Metric Collection")
	var args runMetricCollectionArgs
	var ok bool
	if args, ok = a.(runMetricCollectionArgs); !ok {
		log.CtxLogger(ctx).Errorw("Failed to parse metric collection args", "args", a)
		return
	}
	log.CtxLogger(ctx).Debugw("MongoDB metric collection args", "args", args)
	// Get the metric collection frequency from the configuration.
	metricCollectionFrequency := metricCollectionFrequency(args)
	ticker := time.NewTicker(metricCollectionFrequency)
	defer ticker.Stop()
	gceService, err := gce.NewGCEClient(ctx)
	if err != nil {
		usagemetrics.Error(usagemetrics.GCEServiceCreationFailure)
		log.CtxLogger(ctx).Errorf("Error while initializing GCE services: %w", err)
		return
	}
	m := mongodbmetrics.New(ctx, args.s.Config, args.s.WLMClient, args.s.DBcenterClient, mongodbmetrics.DefaultRunCommand)
	// 30 seconds is the default server selection timeout for MongoDB. The parameter is used to allow unit tests to fail faster.
	err = m.InitDB(ctx, gceService, 30*time.Second)
	if err != nil {
		log.CtxLogger(ctx).Errorf("Failed to initialize MongoDB DB: %w", err)
		return
	}
	for {
		_, err := m.CollectMetricsOnce(ctx, args.s.dwActivated)
		if err != nil {
			log.CtxLogger(ctx).Debugf("failed to collect MongoDB metrics: %v", err)
		}
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("MongoDB metric collection cancellation requested")
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
		log.CtxLogger(ctx).Debugw("MongoDB workload agent service received a message on the common channel", "message", msg)
		switch msg.Origin {
		case servicecommunication.Discovery:
			s.processes = msg.DiscoveryResult
			s.identifyMongoDBProcesses(ctx)
		case servicecommunication.DWActivation:
			s.dwActivated = msg.DWActivationResult.Activated
		default:
			log.CtxLogger(ctx).Debugw("MongoDB workload agent service received a message with an unexpected origin", "origin", msg.Origin)
		}
	}
}

func (s *Service) identifyMongoDBProcesses(ctx context.Context) {
	s.mongodbProcesses = []servicecommunication.ProcessWrapper{}
	for _, process := range s.processes.Processes {
		name, err := process.Name()
		if err == nil && strings.Contains(name, "mongod") {
			s.mongodbProcesses = append(s.mongodbProcesses, process)
		}
	}
	s.logMongoDBProcesses(ctx, zapcore.DebugLevel)
}

func (s *Service) isWorkloadPresent() bool {
	return len(s.mongodbProcesses) > 0
}

func (s *Service) logMongoDBProcesses(ctx context.Context, loglevel zapcore.Level) {
	log.CtxLogger(ctx).Logf(loglevel, "Number of processes found: %v", len(s.processes.Processes))
	log.CtxLogger(ctx).Logf(loglevel, "Number of MongoDB processes found: %v", len(s.mongodbProcesses))
	for _, process := range s.mongodbProcesses {
		name, _ := process.Name()
		username, _ := process.Username()
		cmdline, _ := process.CmdlineSlice()
		env, _ := process.Environ()
		log.CtxLogger(ctx).Logw(loglevel, "MongoDB process", "name", name, "username", username, "cmdline", cmdline, "env", env, "pid", process.Pid())
	}
}

// String returns the name of the MongoDB service.
func (s *Service) String() string {
	return "MongoDB Service"
}

// ErrorCode returns the error code for the MongoDB service.
func (s *Service) ErrorCode() int {
	return usagemetrics.MongoDBServiceError
}

// ExpectedMinDuration returns the expected minimum duration for the MongoDB service.
// Used by the recovery handler to determine if the service ran long enough to be considered
// successful.
func (s *Service) ExpectedMinDuration() time.Duration {
	return 20 * time.Second
}
