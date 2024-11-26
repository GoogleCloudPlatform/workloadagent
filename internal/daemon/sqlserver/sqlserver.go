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
	"time"

	"github.com/GoogleCloudPlatform/workloadagent/internal/commondiscovery"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

// Service implements the interfaces for SQL Server workload agent service.
type Service struct {
	Config         *configpb.Configuration
	CloudProps     *configpb.CloudProperties
	CommonCh       chan commondiscovery.Result
	processes      commondiscovery.Result
	redisProcesses []commondiscovery.ProcessWrapper
}

// Start initiates the SQL Server workload agent service
func (s *Service) Start(ctx context.Context, a any) {
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
	return 0
}
