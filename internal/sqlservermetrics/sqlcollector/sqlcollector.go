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

// Package sqlcollector contains modules that collects rules from Sql server.
package sqlcollector

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/GoogleCloudPlatform/workloadagent/internal/sqlservermetrics/sqlserverutils"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
)

// GCEInterface provides an interface for GCE client operations.
type GCEInterface interface {
	GetSecret(ctx context.Context, projectID, secretName string) (string, error)
}

var newCollectorFunc = NewV1

// PingSQLServer performs a connectivity check for all configured SQL Server instances.
func PingSQLServer(ctx context.Context, cfg *cpb.SQLServerConfiguration, gceService GCEInterface, cloudProps *cpb.CloudProperties) error {
	credConfigs := cfg.GetCredentialConfigurations()
	if len(credConfigs) == 0 {
		return fmt.Errorf("no credential configurations for SQL Server")
	}
	hasConnections := false
	for _, credConfig := range credConfigs {
		conParams := credConfig.GetConnectionParameters()
		if len(conParams) == 0 {
			continue
		}
		hasConnections = true
		for _, conn := range conParams {
			password := ""
			if conn.GetSecret() != nil {
				secret := conn.GetSecret()
				projectID := secret.GetProjectId()
				if projectID == "" && cloudProps != nil {
					projectID = cloudProps.GetProjectId()
				}
				p, err := gceService.GetSecret(ctx, projectID, secret.GetSecretName())
				if err != nil {
					return fmt.Errorf("failed to get SQL Server password secret: %v", err)
				}
				password = strings.TrimSuffix(p, "\n")
			}

			connString := BuildConnectionString(conn, password)
			collector, err := newCollectorFunc("sqlserver", connString, false)
			if err != nil {
				return fmt.Errorf("failed to create SQL Server collector for host %s: %v", conn.GetHost(), err)
			}

			ctxPing, cancel := context.WithTimeout(ctx, 10*time.Second)
			err = collector.dbConn.PingContext(ctxPing)
			cancel()
			collector.Close()
			if err != nil {
				return fmt.Errorf("failed to ping SQL Server host %s: %v", conn.GetHost(), err)
			}
		}
	}
	if !hasConnections {
		return fmt.Errorf("no connections configured for SQL Server")
	}
	return nil
}

// BuildConnectionString creates a SQL Server connection string.
func BuildConnectionString(conn *cpb.ConnectionParameters, password string) string {
	query := url.Values{}
	query.Add("database", conn.GetServiceName())
	query.Add("encrypt", "false")

	connURL := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(conn.GetUsername(), password),
		Host:     fmt.Sprintf("%s:%d", conn.GetHost(), conn.GetPort()),
		RawQuery: query.Encode(),
	}
	return connURL.String()
}

// SQLCollector is the interface of Collector
// which declares all funcs that needs to be implemented.
type SQLCollector interface {
	CollectSQLMetrics(context.Context, time.Duration) []sqlserverutils.MetricDetails
}

// SQLMetricsStruct defines the data struct of sql server metrics definitions and results.
type SQLMetricsStruct struct {
	// Name defines the rule name.
	Name string
	// Query is the sql query statement for the rule.
	Query string
	// Fields returns the <key, value> of collected columns and values. Different rules query
	// different tables and columns.
	Fields func([][]any) []map[string]string
}

// SQLMetrics defines the rules the agent will collect from sql server.
var SQLMetrics = []SQLMetricsStruct{
	{
		Name: "DB_LOG_DISK_SEPARATION",
		Query: `SELECT type, d.name, physical_name, m.state, size, growth, is_percent_growth
						FROM sys.master_files m
						JOIN sys.databases d ON m.database_id = d.database_id`,
		Fields: func(fields [][]any) []map[string]string {
			res := []map[string]string{}
			for _, f := range fields {
				res = append(res, map[string]string{
					"db_name":           handleNilString(f[1]),
					"filetype":          handleNilInt(f[0]),
					"physical_name":     handleNilString(f[2]),
					"physical_drive":    "unknown",
					"state":             handleNilInt(f[3]),
					"size":              handleNilInt(f[4]),
					"growth":            handleNilInt(f[5]),
					"is_percent_growth": handleNilBool(f[6]),
				})
			}
			return res
		},
	},
	{
		Name: "DB_MAX_PARALLELISM",
		Query: `SELECT value_in_use as maxDegreeOfParallelism
						FROM sys.configurations
						WHERE name = 'max degree of parallelism'`,
		Fields: func(fields [][]any) []map[string]string {
			res := []map[string]string{}
			for _, f := range fields {
				res = append(res, map[string]string{
					"maxDegreeOfParallelism": handleNilInt(f[0]),
				})
			}
			return res
		},
	},
	{
		Name: "DB_TRANSACTION_LOG_HANDLING",
		Query: `WITH cte AS (
						SELECT d.name, MAX(b.backup_finish_date) AS backup_finish_date, MAX(m.growth) AS growth
						FROM master.sys.sysdatabases d
								LEFT JOIN msdb.dbo.backupset b ON b.database_name = d.name AND b.type = 'L'
								LEFT JOIN sys.master_files m ON d.dbid = m.database_id AND m.type = 1
						WHERE d.name NOT IN ('master', 'tempdb', 'model', 'msdb')
						GROUP BY d.name
						)
					SELECT cte.name,
					CASE
						WHEN b.backup_finish_date IS NULL THEN 100000
						ELSE DATEDIFF(HOUR, b.backup_finish_date, GETDATE())
					END AS [backup_age],
					b.backup_size, b.compressed_backup_size,
					CASE
						WHEN growth > 0 THEN 1
						ELSE 0
					END AS auto_growth
					FROM cte
					LEFT JOIN msdb.dbo.backupset b
					ON b.database_name = cte.name
					AND b.backup_finish_date = cte.backup_finish_date`,
		Fields: func(fields [][]any) []map[string]string {
			res := []map[string]string{}
			for _, f := range fields {
				res = append(res, map[string]string{
					"db_name":                handleNilString(f[0]),
					"backup_age_in_hours":    handleNilInt(f[1]),
					"backup_size":            handleNilInt(f[2]),
					"compressed_backup_size": handleNilInt(f[3]),
					"auto_growth":            handleNilInt(f[4]),
				})
			}
			return res
		},
	},
	{
		Name: "DB_VIRTUAL_LOG_FILE_COUNT",
		Query: `SELECT [name], COUNT(l.database_id) AS 'VLFCount', SUM(vlf_size_mb) AS 'VLFSizeInMB',
								SUM(CAST(vlf_active AS INT)) AS 'ActiveVLFCount',
								SUM(vlf_active*vlf_size_mb) AS 'ActiveVLFSizeInMB'
						FROM sys.databases s
						CROSS APPLY sys.dm_db_log_info(s.database_id) l
						WHERE [name] NOT IN ('master', 'tempdb', 'model', 'msdb')
						GROUP BY [name]`,
		Fields: func(fields [][]any) []map[string]string {
			res := []map[string]string{}
			for _, f := range fields {
				res = append(res, map[string]string{
					"db_name":               handleNilString(f[0]),
					"vlf_count":             handleNilInt(f[1]),
					"vlf_size_in_mb":        handleNilFloat64(f[2]),
					"active_vlf_count":      handleNilInt(f[3]),
					"active_vlf_size_in_mb": handleNilFloat64(f[4]),
				})
			}
			return res
		},
	},
	{
		Name: "DB_BUFFER_POOL_EXTENSION",
		Query: `SELECT path, state, current_size_in_kb
						FROM sys.dm_os_buffer_pool_extension_configuration`,
		Fields: func(fields [][]any) []map[string]string {
			res := []map[string]string{}
			for _, f := range fields {
				res = append(res, map[string]string{
					"path":       handleNilString(f[0]),
					"state":      handleNilInt(f[1]),
					"size_in_kb": handleNilInt(f[2]),
				})
			}
			return res
		},
	},
	{
		Name: "DB_MAX_SERVER_MEMORY",
		Query: `SELECT [name], [value], [value_in_use]
						FROM sys.configurations
						WHERE [name] = 'max server memory (MB)';`,
		Fields: func(fields [][]any) []map[string]string {
			res := []map[string]string{}
			for _, f := range fields {
				res = append(res, map[string]string{
					"name":         handleNilString(f[0]),
					"value":        handleNilInt(f[1]),
					"value_in_use": handleNilInt(f[2]),
				})
			}
			return res
		},
	},
	{
		Name: "DB_INDEX_FRAGMENTATION",
		Query: `SELECT top 1 1 AS found_index_fragmentation
						FROM sys.databases d
							CROSS APPLY sys.dm_db_index_physical_stats (d.database_id, NULL, NULL, NULL, NULL) AS DDIPS
						WHERE ddips.avg_fragmentation_in_percent > 95
							AND d.name NOT IN ('master', 'model', 'msdb', 'tempdb')
							And d.name NOT IN (
								SELECT DISTINCT dbcs.database_name AS [DatabaseName]
								FROM master.sys.availability_groups AS AG
									INNER JOIN master.sys.availability_replicas AS AR ON AG.group_id = AR.group_id
									INNER JOIN master.sys.dm_hadr_availability_replica_states AS arstates ON AR.replica_id = arstates.replica_id AND arstates.is_local = 1
									INNER JOIN master.sys.dm_hadr_database_replica_cluster_states AS dbcs ON arstates.replica_id = dbcs.replica_id
								WHERE ISNULL(arstates.role, 3) = 2 AND ISNULL(dbcs.is_database_joined, 0) = 1)`,
		Fields: func(fields [][]any) []map[string]string {
			res := []map[string]string{}
			for _, f := range fields {
				res = append(res, map[string]string{
					"found_index_fragmentation": handleNilInt(f[0]),
				})
			}
			return res
		},
	},
	{
		Name: "DB_TABLE_INDEX_COMPRESSION",
		Query: `SELECT COUNT(*) numOfPartitionsWithCompressionEnabled
						FROM sys.partitions p
						WHERE data_compression <> 0 and rows > 0`,
		Fields: func(fields [][]any) []map[string]string {
			res := []map[string]string{}
			for _, f := range fields {
				res = append(res, map[string]string{
					"numOfPartitionsWithCompressionEnabled": handleNilInt(f[0]),
				})
			}
			return res
		},
	},
	{
		Name: "INSTANCE_METRICS",
		Query: `SELECT
							SERVERPROPERTY('productversion') AS productversion,
							SERVERPROPERTY ('productlevel') AS productlevel,
							SERVERPROPERTY ('edition') AS edition,
							cpu_count AS cpuCount,
							hyperthread_ratio AS hyperthreadRatio,
							physical_memory_kb AS physicalMemoryKb,
							virtual_memory_kb AS virtualMemoryKb,
							socket_count AS socketCount,
							cores_per_socket AS coresPerSocket,
							numa_node_count AS numaNodeCount
						FROM sys.dm_os_sys_info`,
		Fields: func(fields [][]any) []map[string]string {
			res := []map[string]string{}
			for _, f := range fields {
				res = append(res, map[string]string{
					"os":                 handleNilString(f[10]),
					"product_version":    handleNilString(f[0]),
					"product_level":      handleNilString(f[1]),
					"edition":            handleNilString(f[2]),
					"cpu_count":          handleNilInt(f[3]),
					"hyperthread_ratio":  handleNilInt(f[4]),
					"physical_memory_kb": handleNilInt(f[5]),
					"virtual_memory_kb":  handleNilInt(f[6]),
					"socket_count":       handleNilInt(f[7]),
					"cores_per_socket":   handleNilInt(f[8]),
					"numa_node_count":    handleNilInt(f[9]),
				})
			}
			return res
		},
	},
	{
		Name: "DB_BACKUP_POLICY",
		Query: `WITH cte AS (
							SELECT master.sys.sysdatabases.NAME AS database_name,
								CASE
									WHEN MAX(msdb.dbo.backupset.backup_finish_date) IS NULL THEN 100000
									ELSE DATEDIFF(DAY, MAX(msdb.dbo.backupset.backup_finish_date), GETDATE())
								END AS [backup_age]
							FROM
									master.sys.sysdatabases
									LEFT JOIN msdb.dbo.backupset
									ON master.sys.sysdatabases.name = msdb.dbo.backupset.database_name
							WHERE
									master.sys.sysdatabases.name NOT IN ('master', 'model', 'msdb', 'tempdb' )
							GROUP BY
									master.sys.sysdatabases.name
							HAVING
									MAX(msdb.dbo.backupset.backup_finish_date) IS NULL
									OR (MAX(msdb.dbo.backupset.backup_finish_date) < DATEADD(hh, - 24, GETDATE()))
					)
					SELECT
							MAX(backup_age) as maxBackupAge
					FROM cte`,
		Fields: func(fields [][]any) []map[string]string {
			res := []map[string]string{}
			for _, f := range fields {
				res = append(res, map[string]string{
					"max_backup_age": handleNilInt(f[0]),
				})
			}
			return res
		},
	},
}

// handleNilString converts generic string to the desired string output,
// or returns 'unknown' if desired type if nil.
func handleNilString(data any) string {
	if data == nil {
		return "unknown"
	}
	return fmt.Sprintf("%v", data.(string))
}

// handleNilInt converts generic int64 to desired string output,
// or returns 'unknown' if desired type if nil.
func handleNilInt(data any) string {
	if data == nil {
		return "unknown"
	}
	// The passed in data might not be int64 so we need to handle the conversion from
	// all possible integer types to string.
	res, err := integerToString(data)
	if err != nil {
		log.Logger.Error(err)
		return "unknown"
	}

	return res
}

// handleNilFloat64 converts generic float64 to desired string output,
// or returns 'unknown' if desired type if nil.
func handleNilFloat64(data any) string {
	if data == nil {
		return "unknown"
	}
	return fmt.Sprintf("%f", data.(float64))
}

// handleNilBool converts generic bool to desired string output,
// or returns 'unknown' if desired type if nil.
func handleNilBool(data any) string {
	if data == nil {
		return "unknown"
	}
	return fmt.Sprintf("%v", data.(bool))
}

// integerToString converts any valid integer type to a string representation.
func integerToString(num any) (string, error) {
	switch v := num.(type) {
	case int:
		return strconv.Itoa(v), nil
	case int8:
		return strconv.FormatInt(int64(v), 10), nil
	case int16:
		return strconv.FormatInt(int64(v), 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case []uint8:
		return string([]uint8(v)), nil
	default:
		return "", fmt.Errorf("unsupported number type: %T", num)
	}
}
