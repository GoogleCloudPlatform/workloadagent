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

// Package sqlserverdiscovery implements discovery for the SQL Server workload agent service.
package sqlserverdiscovery

import (
	"context"

	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
)

// Discover runs the SQL Server discovery routine.
func Discover(ctx context.Context) {
	log.CtxLogger(ctx).Info("SQL Server discovery not yet implemented.")
	return
}
