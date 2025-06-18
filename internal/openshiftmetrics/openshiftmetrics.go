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

// Package openshiftmetrics implements metric collection for the OpenShift workload agent service.
package openshiftmetrics

import (
	"context"

	"github.com/GoogleCloudPlatform/workloadagent/internal/workloadmanager"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

// OpenShiftMetrics contains variables and methods to collect metrics for OpenShift running on the current host.
type OpenShiftMetrics struct {
	WLMClient workloadmanager.WLMWriter
}

// New initializes and returns the MetricCollector struct.
func New(ctx context.Context, config *configpb.Configuration, wlmClient workloadmanager.WLMWriter) *OpenShiftMetrics {
	return &OpenShiftMetrics{WLMClient: wlmClient}
}
