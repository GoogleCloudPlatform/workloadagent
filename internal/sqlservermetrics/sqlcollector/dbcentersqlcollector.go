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

package sqlcollector

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/GoogleCloudPlatform/workloadagent/internal/databasecenter"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
)

// dbCenterSQLMetricsStruct defines the data struct of sql server metrics definitions and results.
type dbCenterSQLMetricsStruct struct {
	// name defines the rule name.
	name string
	// query is the sql query statement for the rule.
	query string
	// fields returns the <key, value> of collected columns and values. Different rules query
	// different tables and columns.
	fields func([][]any) []map[string]string
	// resultProcessor is a function that processes the result of the query and updates the metrics map.
	resultProcessor func(result, metrics map[string]string)
}

var (
	versionQueryKey  = "version query key"
	rawVersionString = "raw_version_string"
	rawEditionString = "raw_edition_string"
	cuNumber         = "cu_number"

	auditingEnabledQueryKey = "auditing enabled query key"
	auditingEnabled         = "auditing_enabled"

	allowUnencryptedConnQueryKey = "allow unencrypted connections query key"
	allowUnencryptedConn         = "allow_unencrypted_connections"

	exposedToBroadIPAccessQueryKey = "exposed to broad ip access query key"
	exposedToBroadAccess           = "exposed_to_broad_ip_access"

	notProtectedByAutoFailoverQueryKey = "not protected by automatic failover query key"
	notProtectedByAutoFailover         = "not_protected_by_auto_failover"

	noAutomatedBackupPolicyQueryKey = "no automated backup policy query key"
	noAutomatedBackupPolicy         = "no_automated_backup_policy"

	lastBackupOldQueryKey = "last backup old query key"
	lastBackupOld         = "last_backup_old"

	signals = []string{auditingEnabledQueryKey, allowUnencryptedConnQueryKey, exposedToBroadIPAccessQueryKey, notProtectedByAutoFailoverQueryKey, noAutomatedBackupPolicyQueryKey, lastBackupOldQueryKey}

	sqlMetrics = map[string]dbCenterSQLMetricsStruct{
		versionQueryKey: {
			// https://cloud.google.com/sql/docs/sqlserver/db-versions#database-version-support
			// Major Version: "SQL Server <Year> <Edition>"
			// Minor Version: "CUXX" or "RTM" etc.
			query: `SELECT
	CONVERT(NVARCHAR(MAX), @@VERSION) AS raw_version_string,
	CONVERT(NVARCHAR(MAX), SERVERPROPERTY('Edition')) AS raw_edition_string,
	ISNULL(CONVERT(NVARCHAR(128), SERVERPROPERTY('ProductUpdateLevel')), 'RTM') AS cu_number;`,
			fields: func(rows [][]any) []map[string]string {
				var res []map[string]string
				for _, row := range rows {
					res = append(res, map[string]string{
						rawVersionString: handleNilString(row[0]),
						rawEditionString: handleNilString(row[1]),
						cuNumber:         handleNilString(row[2]),
					})
				}
				return res
			},
		},

		auditingEnabledQueryKey: {
			query: `SELECT
	CASE
		WHEN COUNT(*) > 0 THEN 1
		ELSE 0
		END AS auditing_enabled
	FROM	sys.server_audits sa
	WHERE	sa.is_state_enabled = 1
	AND (
		EXISTS (SELECT 1
       	  	FROM sys.server_audit_specifications sas
           	WHERE sas.audit_guid = sa.audit_guid
            AND sas.is_state_enabled = 1)
	OR
		EXISTS (SELECT 1
       	  	FROM sys.database_audit_specifications das
            WHERE das.audit_guid = sa.audit_guid
            AND das.is_state_enabled = 1)
    );`,
			fields: func(rows [][]any) []map[string]string {
				var res []map[string]string
				for _, row := range rows {
					res = append(res, map[string]string{
						auditingEnabled: handleNilInt(row[0]),
					})
				}
				return res
			},
			resultProcessor: auditEnabled,
		},

		allowUnencryptedConnQueryKey: {
			query: `SELECT
	CASE
    WHEN COUNT(*) > 0 THEN 1
    ELSE 0
    END AS UnencryptedConnectionsExist
	FROM sys.dm_exec_connections
	WHERE encrypt_option = 'FALSE'
  AND net_transport <> 'Shared memory';`,
			fields: func(rows [][]any) []map[string]string {
				var res []map[string]string
				for _, row := range rows {
					res = append(res, map[string]string{
						allowUnencryptedConn: handleNilInt(row[0]),
					})
				}
				return res
			},
			resultProcessor: allowUnencryptedConnections,
		},

		exposedToBroadIPAccessQueryKey: {
			query: `SELECT
	CASE
		WHEN COUNT(*) > 0 THEN 1
		ELSE 0
	END AS exposed_to_broad_ip_access
	FROM sys.dm_tcp_listener_states
	WHERE state_desc = 'ONLINE'
		AND (ip_address = '0.0.0.0'
		OR ip_address = '::');`,
			fields: func(rows [][]any) []map[string]string {
				var res []map[string]string
				for _, row := range rows {
					res = append(res, map[string]string{
						exposedToBroadAccess: handleNilInt(row[0]),
					})
				}
				return res
			},
			resultProcessor: exposedToBroadIPAccess,
		},

		notProtectedByAutoFailoverQueryKey: {
			query: `WITH Protected_AGs AS (
	SELECT group_id
	FROM sys.availability_replicas
	WHERE availability_mode = 1
	  AND failover_mode = 1
	GROUP BY group_id
	HAVING COUNT(replica_id) >= 2
),
DBStatuses AS (
	SELECT
		CASE
			WHEN SERVERPROPERTY('IsClustered') = 1 THEN 'PROTECTED_BY_FCI'
			WHEN SERVERPROPERTY('IsHadrEnabled') = 1
				 AND pag.group_id IS NOT NULL
				 AND ar_local.failover_mode = 1
				 AND ar_local.availability_mode = 1 THEN 'PROTECTED_BY_AG'
			WHEN dm.mirroring_guid IS NOT NULL
				 AND dm.mirroring_safety_level_desc = 'FULL'
				 AND dm.mirroring_witness_name IS NOT NULL
				 AND dm.mirroring_witness_name <> '' THEN 'PROTECTED_BY_MIRRORING'
			ELSE 'UNPROTECTED'
		END AS FailoverProtectionStatus
	FROM master.sys.databases d
	LEFT JOIN sys.dm_hadr_database_replica_states drs
		ON d.database_id = drs.database_id AND drs.is_local = 1
	LEFT JOIN Protected_AGs pag
		ON drs.group_id = pag.group_id
	LEFT JOIN sys.availability_replicas ar_local
		ON drs.replica_id = ar_local.replica_id
	LEFT JOIN sys.database_mirroring dm
		ON d.database_id = dm.database_id
	WHERE d.name NOT IN ('master', 'model', 'msdb', 'tempdb')
	  AND d.state_desc = 'ONLINE'
)
SELECT
	CASE
		WHEN SUM(CASE WHEN FailoverProtectionStatus = 'UNPROTECTED' THEN 1 ELSE 0 END) > 0 THEN 1
		ELSE 0
	END AS not_protected_by_auto_failover
FROM DBStatuses;`,
			fields: func(rows [][]any) []map[string]string {
				var res []map[string]string
				for _, row := range rows {
					res = append(res, map[string]string{
						notProtectedByAutoFailover: handleNilInt(row[0]),
					})
				}
				return res
			},
			resultProcessor: notProtectedByAutoFailoverProcessor,
		},

		noAutomatedBackupPolicyQueryKey: {
			query: `WITH AgentJobs AS (
	SELECT
		ISNULL(MAX(CASE WHEN LOWER(js.command) LIKE '%backup database%'
							 OR LOWER(js.command) LIKE '%@backuptype = ''full''%'
							 OR LOWER(js.command) LIKE '%dbo.databasebackup%'
							 OR LOWER(js.command) LIKE '%minion.backup%' THEN 1 ELSE 0 END), 0) AS HasAgentFull,
		ISNULL(MAX(CASE WHEN LOWER(js.command) LIKE '%backup log%'
							 OR LOWER(js.command) LIKE '%@backuptype = ''log''%' THEN 1 ELSE 0 END), 0) AS HasAgentLog
	FROM msdb.dbo.sysjobs j
	INNER JOIN msdb.dbo.sysjobsteps js ON j.job_id = js.job_id
	INNER JOIN msdb.dbo.sysjobschedules jsch ON j.job_id = jsch.job_id
	INNER JOIN msdb.dbo.sysschedules s ON jsch.schedule_id = s.schedule_id
	WHERE j.enabled = 1 AND s.enabled = 1
),
MaintPlanJobs AS (
	SELECT
		ISNULL(MAX(CASE WHEN LOWER(sld.command) LIKE '%backup database%' THEN 1 ELSE 0 END), 0) AS HasMaintFull,
		ISNULL(MAX(CASE WHEN LOWER(sld.command) LIKE '%backup log%' THEN 1 ELSE 0 END), 0) AS HasMaintLog
	FROM msdb.dbo.sysmaintplan_plans p
	INNER JOIN msdb.dbo.sysmaintplan_subplans sp ON p.id = sp.plan_id
	INNER JOIN msdb.dbo.sysjobs j ON sp.job_id = j.job_id
	INNER JOIN msdb.dbo.sysjobschedules jsch ON j.job_id = jsch.job_id
	INNER JOIN msdb.dbo.sysschedules s ON jsch.schedule_id = s.schedule_id
	LEFT JOIN msdb.dbo.sysmaintplan_log sl ON sp.subplan_id = sl.subplan_id
	LEFT JOIN msdb.dbo.sysmaintplan_logdetail sld ON sl.task_detail_id = sld.task_detail_id
	WHERE j.enabled = 1 AND s.enabled = 1
),
ScheduledJobs AS (
	SELECT
		(SELECT HasAgentFull FROM AgentJobs) | (SELECT HasMaintFull FROM MaintPlanJobs) AS HasScheduledFull,
		(SELECT HasAgentLog FROM AgentJobs) | (SELECT HasMaintLog FROM MaintPlanJobs) AS HasScheduledLog
),
DBCompliance AS (
	SELECT
		d.name,
		CASE
			WHEN d.recovery_model_desc = 'SIMPLE' AND j.HasScheduledFull = 1 THEN 1
			WHEN d.recovery_model_desc IN ('FULL', 'BULK_LOGGED') AND j.HasScheduledFull = 1 AND j.HasScheduledLog = 1 THEN 1
			ELSE 0
		END AS IsCompliant
	FROM master.sys.databases d
	CROSS JOIN ScheduledJobs j
	WHERE d.name NOT IN ('master', 'model', 'msdb', 'tempdb')
	  AND d.state_desc = 'ONLINE'
)
SELECT ISNULL(CASE WHEN SUM(CASE WHEN IsCompliant = 0 THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END, 0) AS no_automated_backup_policy
FROM DBCompliance;`,
			fields: func(rows [][]any) []map[string]string {
				var res []map[string]string
				for _, row := range rows {
					res = append(res, map[string]string{
						noAutomatedBackupPolicy: handleNilInt(row[0]),
					})
				}
				return res
			},
			resultProcessor: noAutomatedBackupPolicyProcessor,
		},

		lastBackupOldQueryKey: {
			query: `WITH LatestBackups AS (
	SELECT
		database_name,
		MAX(CASE WHEN type IN ('D', 'I') THEN backup_finish_date END) AS LastBaseBackup,
		MAX(CASE WHEN type = 'L' THEN backup_finish_date END) AS LastLogBackup
	FROM msdb.dbo.backupset
	WHERE type IN ('D', 'I', 'L')
	  AND backup_finish_date >= DATEADD(day, -14, GETDATE())
	GROUP BY database_name
),
DBStatuses AS (
	SELECT
		d.name,
		CASE
			WHEN d.recovery_model_desc = 'SIMPLE'
				 AND b.LastBaseBackup >= DATEADD(hour, -24, GETDATE())
				 THEN 'HEALTHY'
			WHEN d.recovery_model_desc IN ('FULL', 'BULK_LOGGED')
				 AND b.LastBaseBackup >= DATEADD(hour, -24, GETDATE())
				 AND b.LastLogBackup >= DATEADD(hour, -24, GETDATE())
				 THEN 'HEALTHY'
			ELSE 'STALE_OR_MISSING_BACKUP'
		END AS OperationalStatus
	FROM master.sys.databases d
	LEFT JOIN LatestBackups b ON d.name = b.database_name
	WHERE d.name NOT IN ('master', 'model', 'msdb', 'tempdb')
	  AND d.state_desc = 'ONLINE'
	  AND d.create_date < DATEADD(hour, -24, GETDATE())
)
SELECT ISNULL(CASE WHEN SUM(CASE WHEN OperationalStatus = 'STALE_OR_MISSING_BACKUP' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END, 0) AS last_backup_old
FROM DBStatuses;`,
			fields: func(rows [][]any) []map[string]string {
				var res []map[string]string
				for _, row := range rows {
					res = append(res, map[string]string{
						lastBackupOld: handleNilInt(row[0]),
					})
				}
				return res
			},
			resultProcessor: lastBackupOldProcessor,
		},
	}
)

// Version returns the major and minor version of the SQL Server.
func Version(ctx context.Context, c *V1) (string, string, error) {
	queryResult, err := c.ExecuteSQL(ctx, sqlMetrics[versionQueryKey].query)
	if err != nil {
		return "", "", fmt.Errorf("failed to run sql query: %v", err)
	}

	fields := sqlMetrics[versionQueryKey].fields(queryResult)
	if fields == nil {
		return "", "", fmt.Errorf("Empty query result")
	}
	rawVersionData := fields[0]
	if rawVersionData == nil {
		return "", "", fmt.Errorf("Empty query result")
	}
	rawVersionString := rawVersionData[rawVersionString]
	rawEditionString := rawVersionData[rawEditionString]
	minorVersion := rawVersionData[cuNumber]

	// Get Year (4th word from @@VERSION)
	// Version string looks like: Microsoft SQL Server 2022 (RTM-CU13) (KB5036432)
	versionFields := strings.Fields(rawVersionString)
	year := ""
	if len(versionFields) >= 4 { // Check if there are at least 4 words
		// The 4th word could be "2019" or "2019(" depending on exact @@VERSION format.
		// Trim non-numeric characters from the end to get just the year digits.
		cleanedYear := strings.TrimRightFunc(versionFields[3], func(r rune) bool {
			return r < '0' || r > '9'
		})
		if len(cleanedYear) == 4 { // Validation for a 4-digit year
			year = cleanedYear
		}
	}

	// Get Edition (1st word from SERVERPROPERTY('Edition'))
	// Edition string looks like: Express Edition (64-bit)
	editionFields := strings.Fields(rawEditionString)
	edition := ""
	if len(editionFields) > 0 {
		edition = editionFields[0]
	}

	// Format: "SQL Server <Year> <Edition>"
	majorVersion := fmt.Sprintf("SQL Server %s %s", year, edition)
	return majorVersion, minorVersion, nil
}

func auditEnabled(result, metrics map[string]string) {
	if result[auditingEnabled] != "1" {
		metrics[databasecenter.DatabaseAuditingDisabledKey] = strconv.FormatBool(true)
	} else {
		metrics[databasecenter.DatabaseAuditingDisabledKey] = strconv.FormatBool(false)
	}
}

func allowUnencryptedConnections(result, metrics map[string]string) {
	if result[allowUnencryptedConn] == "1" {
		metrics[databasecenter.UnencryptedConnectionsKey] = strconv.FormatBool(true)
	} else {
		metrics[databasecenter.UnencryptedConnectionsKey] = strconv.FormatBool(false)
	}
}

func exposedToBroadIPAccess(result, metrics map[string]string) {
	if result[exposedToBroadAccess] == "1" {
		metrics[databasecenter.ExposedToPublicAccessKey] = strconv.FormatBool(true)
	} else {
		metrics[databasecenter.ExposedToPublicAccessKey] = strconv.FormatBool(false)
	}
}

func notProtectedByAutoFailoverProcessor(result, metrics map[string]string) {
	count, err := strconv.Atoi(result[notProtectedByAutoFailover])
	hasNotProtectedByAutoFailover := (err == nil && count > 0)
	metrics[databasecenter.NotProtectedByAutomaticFailoverKey] = strconv.FormatBool(hasNotProtectedByAutoFailover)
}

func noAutomatedBackupPolicyProcessor(result, metrics map[string]string) {
	count, err := strconv.Atoi(result[noAutomatedBackupPolicy])
	hasAutomatedBackupPolicy := (err == nil && count > 0)
	metrics[databasecenter.NoAutomatedBackupPolicyKey] = strconv.FormatBool(hasAutomatedBackupPolicy)
}

func lastBackupOldProcessor(result, metrics map[string]string) {
	count, err := strconv.Atoi(result[lastBackupOld])
	hasLastBackupOld := (err == nil && count > 0)
	metrics[databasecenter.LastBackupOldKey] = strconv.FormatBool(hasLastBackupOld)
}

// PopulateSignals populates the signals for the SQL Server in the metrics map.
func PopulateSignals(ctx context.Context, c *V1, metrics map[string]string) {
	for _, signal := range signals {
		queryResult, err := c.ExecuteSQL(ctx, sqlMetrics[signal].query)
		if err != nil {
			log.Logger.Errorw("Failed to run sql query", "query", sqlMetrics[signal].query, "error", err)
			continue
		}
		fields := sqlMetrics[signal].fields(queryResult)
		if fields == nil {
			log.Logger.Errorw("Empty query result", "query", sqlMetrics[signal].query)
			continue
		}
		result := fields[0]
		if result == nil {
			log.Logger.Errorw("Empty query result", "query", sqlMetrics[signal].query)
			continue
		}
		sqlMetrics[signal].resultProcessor(result, metrics)
	}
}
