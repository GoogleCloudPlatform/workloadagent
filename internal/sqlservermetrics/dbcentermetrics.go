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

package sqlservermetrics

import (
	"context"
	"fmt"

	"github.com/GoogleCloudPlatform/workloadagent/internal/databasecenter"
	"github.com/GoogleCloudPlatform/workloadagent/internal/sqlservermetrics/sqlcollector"
	"github.com/GoogleCloudPlatform/workloadagent/internal/sqlservermetrics/sqlserverutils"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
)

func (s *SQLServerMetrics) dbCenterMetrics(ctx context.Context) databasecenter.DBCenterMetrics {
	metrics := map[string]string{}

	for _, credentialCfg := range s.Config.GetCredentialConfigurations() {
		for _, sqlCfg := range sqlConfigFromCredential(credentialCfg) {
			if err := validateCredCfgSQL(false, false, sqlCfg, &sqlserverutils.GuestConfig{}, credentialCfg.GetVmProperties().GetInstanceId(), credentialCfg.GetVmProperties().GetInstanceName()); err != nil {
				usagemetrics.Error(usagemetrics.SQLServerInvalidConfigurationsError)
				log.Logger.Errorw("Invalid credential configuration", "error", err)
				continue
			}
			projectID := sqlCfg.ProjectID
			if projectID == "" {
				projectID = sip.ProjectID
			}
			pswd, err := secretValue(ctx, projectID, sqlCfg.SecretName)
			if err != nil {
				usagemetrics.Error(usagemetrics.SecretManagerValueError)
				log.Logger.Errorw("Failed to get secret value", "error", err)
				continue
			}
			conn := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;", sqlCfg.Host, sqlCfg.Username, pswd, sqlCfg.PortNumber)
			c, err := sqlcollector.NewV1("sqlserver", conn, false)
			if err != nil {
				log.Logger.Errorw("Failed to run sql collection", "error", err)
				continue
			}
			defer c.Close()
			// Start collection for dbcenter metrics.
			log.Logger.Debug("Started collecting SQL Server rules for dbcenter metrics.")
			ctxWithTimeout, cancel := context.WithTimeout(ctx, s.Config.GetCollectionTimeout().AsDuration())
			defer cancel()
			majorVersion, minorVersion, err := sqlcollector.Version(ctxWithTimeout, c)
			if err != nil {
				log.Logger.Errorw("Failed to get version", "error", err)
				continue
			}
			metrics[databasecenter.MajorVersionKey] = majorVersion
			metrics[databasecenter.MinorVersionKey] = minorVersion
			// End collection for dbcenter metrics.
			log.Logger.Debug("Completed collecting SQL Server rules for dbcenter metrics.")
		}
	}

	return databasecenter.DBCenterMetrics{
		EngineType: databasecenter.SQLSERVER,
		Metrics:    metrics,
	}
}
