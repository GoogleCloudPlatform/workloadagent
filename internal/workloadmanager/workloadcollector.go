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

// Package workloadmanager collects workload manager metrics and sends them to Data Warehouse.
package workloadmanager

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/configuration"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/log"
	dwpb "github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/protos/datawarehouse"
)

// ConfigFileReader is a function that reads a config file.
type ConfigFileReader func(string) (io.ReadCloser, error)

// WorkloadMetrics is a struct that collect data from override configuration file for testing purposes.
// Future enhancements will include the collection of actual WLM metrics.
type WorkloadMetrics struct {
	workloadType string
	metrics      map[string]string
}

type wlmWriter interface {
	WriteInsight(project, location string, writeInsightRequest *dwpb.WriteInsightRequest) error
}

// sendMetricsParams defines the set of parameters required to call sendMetrics
type sendMetricsParams struct {
	wm         []WorkloadMetrics
	cp         *cpb.CloudProperties
	wlmService wlmWriter
}

type data struct {
	wm         WorkloadMetrics
	cp         *cpb.CloudProperties
	wlmService wlmWriter
	l          string
}

// metricEmitter is a container for constructing metrics from an override configuration file
type metricEmitter struct {
	scanner      *bufio.Scanner
	workloadType string
	metrics      map[string]string // Add a field to store metrics for the current workload
}

const metricOverridePath = "/etc/google-cloud-workload-agent/wlmmetricoverride.yaml"

// CollectAndSendMetricsToDataWarehouse collects workload metrics and sends them to Data Warehouse.
func CollectAndSendMetricsToDataWarehouse(ctx context.Context, wlmService wlmWriter, cp *cpb.CloudProperties) {
	if !readAndLogMetricOverrideYAML(ctx, readFileWrapper) {
		return
	}
	wm := collectOverrideMetrics(ctx, readFileWrapper)
	sendMetricsToDataWarehouse(ctx, sendMetricsParams{
		wm:         wm,
		cp:         cp,
		wlmService: wlmService,
	})
}

func readAndLogMetricOverrideYAML(ctx context.Context, reader ConfigFileReader) bool {
	file, err := reader(metricOverridePath)
	if err != nil {
		log.CtxLogger(ctx).Debugw("Could not read the metric override file", "error", err)
		return false
	}
	defer file.Close()

	log.CtxLogger(ctx).Info("Using override metrics from yaml file")
	// Create a new scanner
	scanner := bufio.NewScanner(file)
	// Loop over each line in the file
	for scanner.Scan() {
		log.CtxLogger(ctx).Debug("Override metric line: %s", scanner.Text())
	}
	if err = scanner.Err(); err != nil {
		log.CtxLogger(ctx).Warnw("Could not read from the override metrics file", "error", err)
	}

	return true
}

// collectOverrideMetrics reads workload metrics from an override file.
func collectOverrideMetrics(ctx context.Context, reader ConfigFileReader) []WorkloadMetrics {
	file, err := reader(metricOverridePath)
	if err != nil {
		log.CtxLogger(ctx).Debugw("Could not read the metric override file", "error", err)
		return []WorkloadMetrics{}
	}
	defer file.Close()

	var wm []WorkloadMetrics
	scanner := bufio.NewScanner(file)
	metricEmitter := metricEmitter{scanner: scanner}
	for {
		wt, metrics, last := metricEmitter.getMetric(ctx)
		wm = append(wm, WorkloadMetrics{workloadType: wt, metrics: metrics})
		if last {
			break
		}
	}
	return wm
}

// getMetric reads the next metric from the underlying scanner.
//
// It returns the workload type, a map for validation metrics,
// and a boolean indicating whether this is the last metric in the current workload group.
func (e *metricEmitter) getMetric(ctx context.Context) (string, map[string]string, bool) {
	if e.metrics == nil {
		e.metrics = make(map[string]string) // Initialize the metrics map if it's nil
	}

	for e.scanner.Scan() {
		line := e.scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue // Skip empty lines and comments directly
		}
		key, value, found := strings.Cut(line, ":")
		if !found {
			log.CtxLogger(ctx).Warnw("Invalid format: " + line)
			continue
		}

		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)

		if key == "workload_type" {
			// Found the first workload type, continue scanning for its metrics.
			if e.workloadType == "" {
				e.workloadType = value
				continue
			}
			// Found a new workload type, return the current one with its metrics
			workloadType := e.workloadType
			metrics := e.metrics
			e.workloadType = value              // Update the workload type for the next record
			e.metrics = make(map[string]string) // Reset the metrics map for the new workload
			return workloadType, metrics, false
		}
		e.metrics[key] = value
	}

	if err := e.scanner.Err(); err != nil {
		log.CtxLogger(ctx).Warnw("Could not read from the override metrics file", "error", err)
	}

	// Reached end of file, return the last workload type and its metrics
	workloadType := e.workloadType
	metrics := e.metrics
	return workloadType, metrics, true
}

func sendMetricsToDataWarehouse(ctx context.Context, params sendMetricsParams) {
	log.CtxLogger(ctx).Info("Sending metrics to Data Warehouse")

	location := formatLocation(params.cp.GetZone())

	var wg sync.WaitGroup
	for _, wm := range params.wm {
		wg.Add(1)
		go sendDataInsight(ctx, data{
			wm:         wm,
			cp:         params.cp,
			wlmService: params.wlmService,
			l:          location,
		}, &wg)
	}
	wg.Wait()
}

func sendDataInsight(ctx context.Context, d data, wg *sync.WaitGroup) {
	defer wg.Done()
	log.CtxLogger(ctx).Debugw("Create WriteInsightRequest and call WriteInsight for workload type %s", d.wm.workloadType)

	req := createWriteInsightRequest(d.wm, d.cp)
	err := d.wlmService.WriteInsight(d.cp.GetProjectId(), d.l, req)
	if err != nil {
		log.CtxLogger(ctx).Errorw("Failed to send metrics to Data Warehouse", "error", err, "workload_type", d.wm.workloadType)
		usagemetrics.Error(usagemetrics.DataWarehouseActivationServiceFailure)
		return
	}
	log.CtxLogger(ctx).Debugw("Sent metrics to Data Warehouse for workload type %s", d.wm.workloadType)
}

func formatLocation(zone string) string {
	locationParts := strings.Split(zone, "-")
	return strings.Join([]string{locationParts[0], locationParts[1]}, "-")
}

func createWriteInsightRequest(wm WorkloadMetrics, cp *cpb.CloudProperties) *dwpb.WriteInsightRequest {
	workloadTypeMap := map[string]dwpb.TorsoValidation_WorkloadType{
		"MYSQL":  dwpb.TorsoValidation_MYSQL,
		"ORACLE": dwpb.TorsoValidation_ORACLE,
		"REDIS":  dwpb.TorsoValidation_REDIS,
	}

	workloadType, ok := workloadTypeMap[wm.workloadType]
	if !ok {
		workloadType = dwpb.TorsoValidation_WORKLOAD_TYPE_UNSPECIFIED
	}

	return &dwpb.WriteInsightRequest{
		Insight: &dwpb.Insight{
			InstanceId: cp.GetInstanceId(),
			TorsoValidation: &dwpb.TorsoValidation{
				WorkloadType:      workloadType,
				ValidationDetails: wm.metrics,
				ProjectId:         cp.GetProjectId(),
				InstanceName:      cp.GetInstanceId(), // TODO: b/392061770 - Modify instance ID to instance name to the metrics
			},
		},
		AgentVersion: configuration.AgentVersion,
	}
}

func readFileWrapper(path string) (io.ReadCloser, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(content)), nil
}
