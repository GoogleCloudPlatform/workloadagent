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
			query: fmt.Sprintf(`WITH %s
SELECT auditing_enabled FROM AuditingStatus;`, auditingEnabledCTE),
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
			query: fmt.Sprintf(`WITH %s
SELECT allows_unencrypted_connections FROM UnencryptedConnectionsStatus;`, allowsUnencryptedConnectionsCTE),
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
			query: fmt.Sprintf(`WITH %s
SELECT exposed_to_broad_ip_access FROM BroadIPAccessStatus;`, exposedToBroadIPAccessCTE),
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
			query: fmt.Sprintf(`WITH %s
SELECT
	ISNULL(CASE WHEN SUM(CASE WHEN FailoverProtectionStatus = 'UNPROTECTED' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END, 0) AS not_protected_by_auto_failover
FROM DBStatuses;`, notProtectedByAutoFailoverCTE),
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
			query: fmt.Sprintf("WITH %s SELECT no_automated_backup_policy FROM NoAutomatedBackupPolicyStatus;", noAutomatedBackupPolicyCTE),
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
			query: fmt.Sprintf("WITH %s SELECT last_backup_old FROM LastBackupOldStatus;", lastBackupOldCTE),
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
