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

var (
	versionQueryKey = "version query key"
	sqlMetrics      map[string]sqlcollector.SQLMetricsStruct
)

func initSQLMetrics() {
	sqlMetrics = map[string]sqlcollector.SQLMetricsStruct{
		versionQueryKey: {
			// https://cloud.google.com/sql/docs/sqlserver/db-versions#database-version-support
			// Major Version: "SQL Server <Year> <Edition>"
			// Minor Version: "CUXX" or "RTM" etc.
			Query: `SELECT
    	N'SQL Server ' +
    	CASE
        WHEN CONVERT(NVARCHAR(128), SERVERPROPERTY('ProductMajorVersion')) = '16' THEN '2022'
        WHEN CONVERT(NVARCHAR(128), SERVERPROPERTY('ProductMajorVersion')) = '15' THEN '2019'
        WHEN CONVERT(NVARCHAR(128), SERVERPROPERTY('ProductMajorVersion')) = '14' THEN '2017'
        WHEN CONVERT(NVARCHAR(128), SERVERPROPERTY('ProductMajorVersion')) = '13' THEN '2016'
        WHEN CONVERT(NVARCHAR(128), SERVERPROPERTY('ProductMajorVersion')) = '12' THEN '2014'
        WHEN CONVERT(NVARCHAR(128), SERVERPROPERTY('ProductMajorVersion')) = '11' THEN '2012'
        WHEN CONVERT(NVARCHAR(128), SERVERPROPERTY('ProductMajorVersion')) = '10' THEN '2008/2008 R2'
        WHEN CONVERT(NVARCHAR(128), SERVERPROPERTY('ProductMajorVersion')) = '9'  THEN '2005'
        WHEN CONVERT(NVARCHAR(128), SERVERPROPERTY('ProductMajorVersion')) = '8'  THEN '2000'
        ELSE 'UNKNOWN_YEAR'
    END + ' ' +
    CASE
        WHEN CHARINDEX('Enterprise', CONVERT(NVARCHAR(128), SERVERPROPERTY('Edition'))) > 0 THEN 'Enterprise'
        WHEN CHARINDEX('Standard', CONVERT(NVARCHAR(128), SERVERPROPERTY('Edition'))) > 0 THEN 'Standard'
        WHEN CHARINDEX('Express', CONVERT(NVARCHAR(128), SERVERPROPERTY('Edition'))) > 0 THEN 'Express'
        WHEN CHARINDEX('Web', CONVERT(NVARCHAR(128), SERVERPROPERTY('Edition'))) > 0 THEN 'Web'
        WHEN CHARINDEX('Developer', CONVERT(NVARCHAR(128), SERVERPROPERTY('Edition'))) > 0 THEN 'Developer'
        WHEN CHARINDEX('Evaluation', CONVERT(NVARCHAR(128), SERVERPROPERTY('Edition'))) > 0 THEN 'Evaluation'
        ELSE CONVERT(NVARCHAR(128), SERVERPROPERTY('Edition'))
    END AS major_version,

    ISNULL(CONVERT(NVARCHAR(128), SERVERPROPERTY('ProductUpdateLevel')), 'RTM') AS minor_version;`,
			Fields: func(rows [][]any) []map[string]string {
				var res []map[string]string
				for _, row := range rows {
					res = append(res, map[string]string{
						majorVersionKey: row[0].(string),
						minorVersionKey: row[1].(string),
					})
				}
				return res
			},
		},
	}
}

func version(ctx context.Context, c *sqlcollector.V1) (string, string, error) {
	queryResult, err := c.ExecuteSQL(ctx, sqlMetrics[versionQueryKey].Query)
	if err != nil {
		return "", "", fmt.Errorf("failed to run sql query: %v", err)
	}
	fields := sqlMetrics[versionQueryKey].Fields(queryResult)
	log.Logger.Debugw("SQL Server version result", "majorVersion ", fields[0][majorVersionKey], " minorVersion ", fields[0][minorVersionKey])
	return fields[0][majorVersionKey], fields[0][minorVersionKey], nil
}

func (s *SQLServerMetrics) dbCenterMetrics(ctx context.Context) databasecenter.DBCenterMetrics {
	initSQLMetrics()
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
			majorVersion, minorVersion, err := version(ctxWithTimeout, c)
			if err != nil {
				log.Logger.Errorw("Failed to get version", "error", err)
				continue
			}
			metrics[majorVersionKey] = majorVersion
			metrics[minorVersionKey] = minorVersion
			// End collection for dbcenter metrics.
			log.Logger.Debug("Completed collecting SQL Server rules for dbcenter metrics.")
		}
	}

	return databasecenter.DBCenterMetrics{
		EngineType: databasecenter.SQLSERVER,
		Metrics:    metrics,
	}
}
