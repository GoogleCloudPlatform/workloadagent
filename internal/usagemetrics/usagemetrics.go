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

// Package usagemetrics provides logging utility for the operational status of Google Cloud Agent for SAP.
package usagemetrics

import "github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/usagemetrics"

// The following status values are supported.
const (
	StatusRunning       usagemetrics.Status = "RUNNING"
	StatusStarted       usagemetrics.Status = "STARTED"
	StatusStopped       usagemetrics.Status = "STOPPED"
	StatusConfigured    usagemetrics.Status = "CONFIGURED"
	StatusMisconfigured usagemetrics.Status = "MISCONFIGURED"
	StatusError         usagemetrics.Status = "ERROR"
	StatusInstalled     usagemetrics.Status = "INSTALLED"
	StatusUpdated       usagemetrics.Status = "UPDATED"
	StatusUninstalled   usagemetrics.Status = "UNINSTALLED"
	StatusAction        usagemetrics.Status = "ACTION"
)

// Agent wide error code mappings.
const (
	UnknownError                          = 0
	OracleDiscoverDatabaseFailure         = 1
	OracleServiceError                    = 2
	OracleMetricCollectionFailure         = 3
	GCEServiceCreationFailure             = 4
	MetricClientCreationFailure           = 5
	ConnectionParametersReadFailure       = 6
	DatabaseConnectionFailure             = 7
	OracleMetricsCreateWorkerPoolFailure  = 8
	MalformedDefaultOracleQueriesFile     = 9
	MySQLServiceError                     = 10
	MySQLMetricCollectionFailure          = 11
	MySQLDiscoveryFailure                 = 12
	CommonDiscoveryFailure                = 13
	RedisServiceError                     = 14
	RedisMetricCollectionFailure          = 15
	RedisDiscoveryFailure                 = 16
	SQLServerServiceError                 = 17
	SQLServerMetricCollectionFailure      = 18
	SQLServerDiscoveryFailure             = 19
	OracleConnectionFailure               = 20
	OraclePingFailure                     = 21
	SQLServerInvalidConfigurationsError   = 22
	SecretManagerValueError               = 23
	WorkloadManagerConnectionError        = 24
	DataWarehouseActivationServiceFailure = 25
	DataWarehouseWriteInsightFailure      = 26
	PostgresServiceError                  = 27
	PostgresMetricCollectionFailure       = 28
	PostgresDiscoveryFailure              = 29
	OpenShiftServiceError                 = 30
	OpenShiftMetricCollectionFailure      = 31
)

// Agent wide action mappings.
const (
	UnknownAction                = 0
	SQLServerAgentConfigMigrated = 1
	// SQL Server deployment status codes.
	SQLServerDeploymentSuccess       = 2
	SQLServerDeploymentGlobalTimeout = 3
	SQLServerDeploymentTimeout       = 4
)

// projectNumbers contains known project numbers for test instances.
var projectExclusionList = []string{
	"161716815775",
	"950182482124",
}
