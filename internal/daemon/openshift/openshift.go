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

// Package openshift implements the OpenShift workload agent service.
package openshift

import (
	"context"
	"time"

	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/configuration"
	"github.com/GoogleCloudPlatform/workloadagent/internal/openshiftmetrics"
	"github.com/GoogleCloudPlatform/workloadagent/internal/servicecommunication"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	"github.com/GoogleCloudPlatform/workloadagent/internal/workloadmanager"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/recovery"

	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

const (
	wlmCollectionFrequency = 30 * time.Second
	payloadVersion         = "v0.1.0-pre"
)

// Service implements the interfaces for OpenShift workload agent service.
type Service struct {
	Config     *configpb.Configuration
	CloudProps *configpb.CloudProperties
	CommonCh   <-chan *servicecommunication.Message
	WLMClient  workloadmanager.WLMWriter
}

type runMetricCollectionArgs struct {
	s *Service
}

// Start initiates the Openshift workload agent service
func (s *Service) Start(ctx context.Context, a any) {
	if !s.Config.GetOpenshiftConfiguration().GetEnabled() {
		// If Openshift workload agent service is not explicitly enabled in the configuration, then return.
		log.CtxLogger(ctx).Debug("Openshift workload agent service is not enabled in the configuration")
		return
	}
	log.CtxLogger(ctx).Debug("Starting OpenShift workload agent service")

	// Start Openshift Metric Collection
	mcCtx := log.SetCtx(ctx, "context", "OpenShiftMetricCollection")
	metricCollectionRoutine := &recovery.RecoverableRoutine{
		Routine:             runMetricCollection,
		RoutineArg:          runMetricCollectionArgs{s},
		ErrorCode:           usagemetrics.OpenShiftMetricCollectionFailure,
		UsageLogger:         *usagemetrics.UsageLogger,
		ExpectedMinDuration: wlmCollectionFrequency,
	}
	metricCollectionRoutine.StartRoutine(mcCtx)
	select {
	case <-ctx.Done():
		log.CtxLogger(ctx).Debug("Openshift workload agent service cancellation requested")
		return
	}
}

func runMetricCollection(ctx context.Context, a any) {
	log.CtxLogger(ctx).Debug("Starting OpenShift Metric Collection")
	var args runMetricCollectionArgs
	var ok bool
	if args, ok = a.(runMetricCollectionArgs); !ok {
		log.CtxLogger(ctx).Errorf("failed to parse metric collection args", "args", a)
		return
	}
	ticker := time.NewTicker(wlmCollectionFrequency)
	defer ticker.Stop()

	for {
		collectMetrics(ctx, args)
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Debug("OpenShift metric collection cancellation requested")
			return
		case <-ticker.C:
			continue
		}
	}
}

// collectMetrics collects metrics from the OpenShift cluster and sends to the datawarehouse API.
func collectMetrics(ctx context.Context, args runMetricCollectionArgs) {
	log.CtxLogger(ctx).Debug("Creating OpenShift metric client")
	metricClient := openshiftmetrics.New(ctx, args.s.Config, args.s.WLMClient)
	if err := metricClient.Init(ctx); err != nil {
		log.CtxLogger(ctx).Errorw("failed to initialize OpenShift metric client", "error", err)
		return
	}

	log.CtxLogger(ctx).Debug("OpenShift metric client created")
	versionData := openshiftmetrics.MetricVersioning{
		PayloadVersion: payloadVersion,
		AgentVersion:   configuration.AgentVersion,
	}

	log.CtxLogger(ctx).Debug("Collecting Openshift metrics")
	metrics, err := metricClient.CollectMetrics(ctx, versionData)
	if err != nil {
		log.CtxLogger(ctx).Errorw("failed to collect metrics", "error", err)
		return
	}
	log.CtxLogger(ctx).Debugw("Metrics collected\n", metrics, "\nSending metrics to WLM")
	if err := metricClient.SendMetricsToWLM(ctx, args.s.Config, metrics); err != nil {
		// This fails silently so that the loop keeps running.
		log.CtxLogger(ctx).Errorw("failed to write metrics to WLM", "error", err)
		return
	}
	log.CtxLogger(ctx).Debug("Metrics successfully sent to WLM")
}

// String returns the name of the OpenShift service.
func (s *Service) String() string {
	return "OpenShift Service"
}

// ErrorCode returns the error code for the OpenShift service.
func (s *Service) ErrorCode() int {
	return usagemetrics.OpenShiftServiceError
}

// ExpectedMinDuration returns the expected minimum duration for the OpenShift service.
// Used by the recovery handler to determine if the service ran long enough to be considered
// successful.
func (s *Service) ExpectedMinDuration() time.Duration {
	return 20 * time.Second
}
