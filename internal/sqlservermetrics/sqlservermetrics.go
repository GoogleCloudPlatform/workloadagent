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

// Package sqlservermetrics implements metric collection for the SQL Server workload agent service.
package sqlservermetrics

import (
	"context"

	"github.com/GoogleCloudPlatform/sapagent/shared/log"
)

// CollectMetricsOnce collects metrics for SQL Server databases running on the host.
func CollectMetricsOnce(ctx context.Context) {
	log.CtxLogger(ctx).Info("SQL Server metric collection not yet implemented.")
	return
}
