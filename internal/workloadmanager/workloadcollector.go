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

	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/log"
	dwpb "github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/protos/datawarehouse"
)

// ConfigFileReader is a function that reads a config file.
type ConfigFileReader func(string) (io.ReadCloser, error)

// WorkloadMetrics is a struct that collect data from override configuration file for testing purposes.
// Future enhancements will include the collection of actual WLM metrics.
type WorkloadMetrics struct {
}

type wlmWriter interface {
	WriteInsight(project, location string, writeInsightRequest *dwpb.WriteInsightRequest) error
}

// sendMetricsParams defines the set of parameters required to call sendMetrics
type sendMetricsParams struct {
	wm         WorkloadMetrics
	cp         *cpb.CloudProperties
	wlmService wlmWriter
}

const metricOverridePath = "/etc/google-cloud-workload-agent/wlmmetricoverride.yaml"

// CollectAndSendMetricsToDataWarehouse collects workload metrics and sends them to Data Warehouse.
func CollectAndSendMetricsToDataWarehouse(ctx context.Context, wlmService wlmWriter, cp *cpb.CloudProperties) {
	if !readAndLogMetricOverrideYAML(ctx, readFileWrapper) {
		return
	}
	// TODO - Parse the metric override file and create the workload metrics.
	// TODO - Send the metrics to Data Warehouse.
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

func readFileWrapper(path string) (io.ReadCloser, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(content)), nil
}
