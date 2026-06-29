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

// Package postgresmetrics implements metric collection for the Postgres workload agent service.
package postgresmetrics

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	// Register the pq driver for Postgres with the database/sql package.
	_ "github.com/lib/pq"
	"github.com/GoogleCloudPlatform/workloadagent/internal/databasecenter"
	"github.com/GoogleCloudPlatform/workloadagent/internal/workloadmanager"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/commandlineexecutor"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/filesystem"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/secret"
)

const (
	workMemKey = "work_mem"
	kilobyte   = 1024
	megabyte   = 1024 * 1024
	gigabyte   = 1024 * 1024 * 1024
)

// GceInterface defines an interface for gce.GCEClient to allow faking
type GceInterface interface {
	GetSecret(ctx context.Context, projectID, secretName string) (string, error)
}

type rowsInterface interface {
	Next() bool
	Close() error
	Scan(dest ...any) error
	Err() error
}

type rowInterface interface {
	Scan(dest ...any) error
}

type dbWrapper struct {
	db *sql.DB
	dbInterface
}

type dbInterface interface {
	QueryContext(ctx context.Context, query string, args ...any) (rowsInterface, error)
	QueryRowContext(ctx context.Context, query string, args ...any) rowInterface
	Ping() error
}

func (d dbWrapper) QueryContext(ctx context.Context, query string, args ...any) (rowsInterface, error) {
	return d.db.QueryContext(ctx, query, args...)
}

func (d dbWrapper) QueryRowContext(ctx context.Context, query string, args ...any) rowInterface {
	return d.db.QueryRowContext(ctx, query, args...)
}

func (d dbWrapper) Ping() error {
	return d.db.Ping()
}

// PostgresMetrics contains variables and methods to collect metrics for Postgres databases running on the current host.
type PostgresMetrics struct {
	execute        commandlineexecutor.Execute
	Config         *configpb.Configuration
	db             dbInterface
	connect        func(ctx context.Context, dataSource string) (dbInterface, error)
	WLMClient      workloadmanager.WLMWriter
	DBcenterClient databasecenter.Client
	fs             filesystem.FileSystem
}

// password gets the password for the Postgres database.
// If the password is set in the configuration, it is used directly (not recommended).
// Otherwise, if the secret configuration is set, the secret is fetched from GCE.
// Without either, the password is not set and requests to the Postgres database will fail.
func (m *PostgresMetrics) password(ctx context.Context, gceService GceInterface) (secret.String, error) {
	pw := ""
	if m.Config.GetPostgresConfiguration().GetConnectionParameters().GetPassword() != "" {
		return secret.String(m.Config.GetPostgresConfiguration().GetConnectionParameters().GetPassword()), nil
	}
	secretCfg := m.Config.GetPostgresConfiguration().GetConnectionParameters().GetSecret()
	if secretCfg.GetSecretName() != "" && secretCfg.GetProjectId() != "" {
		var err error
		pw, err = gceService.GetSecret(ctx, secretCfg.GetProjectId(), secretCfg.GetSecretName())
		if err != nil {
			return secret.String(""), fmt.Errorf("failed to get secret: %w", err)
		}
	}
	return secret.String(pw), nil
}

// defaultConnect connects to the Postgres database.
func defaultConnect(ctx context.Context, dataSource string) (dbInterface, error) {
	d, err := sql.Open("postgres", dataSource)
	if err != nil {
		return nil, fmt.Errorf("failed to open Postgres connection: %w", err)
	}
	return dbWrapper{db: d}, nil
}

func (m *PostgresMetrics) dbDSN(ctx context.Context, gceService GceInterface) (string, error) {
	pw, err := m.password(ctx, gceService)
	if err != nil {
		return "", fmt.Errorf("initializing password: %w", err)
	}
	user := m.Config.GetPostgresConfiguration().GetConnectionParameters().GetUsername()
	if user == "" {
		user = "postgres"
	}
	host := "localhost"
	port := "5432"
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=postgres", host, port, user, pw.SecretValue())
	return psqlInfo, nil
}

// New creates a new PostgresMetrics object initialized with default values.
func New(ctx context.Context, config *configpb.Configuration, wlmClient workloadmanager.WLMWriter, dbcenterClient databasecenter.Client) *PostgresMetrics {
	return &PostgresMetrics{
		execute:        commandlineexecutor.ExecuteCommand,
		Config:         config,
		connect:        defaultConnect,
		WLMClient:      wlmClient,
		DBcenterClient: dbcenterClient,
		fs:             filesystem.Helper{},
	}
}

// InitDB initializes the Postgres database connection.
func (m *PostgresMetrics) InitDB(ctx context.Context, gceService GceInterface) error {
	dbDSN, err := m.dbDSN(ctx, gceService)
	if err != nil {
		return fmt.Errorf("getting dbDSN: %w", err)
	}
	db, err := m.connect(ctx, dbDSN)
	if err != nil {
		return fmt.Errorf("connecting to Postgres: %w", err)
	}
	m.db = db
	err = m.db.Ping()
	if err != nil {
		log.CtxLogger(ctx).Debugw("Failed to ping Postgres connection, trying to connect without SSL")
		db, err = m.connect(ctx, fmt.Sprintf("%s sslmode=disable", dbDSN))
		if err != nil {
			return fmt.Errorf("connecting to Postgres without SSL: %w", err)
		}
		m.db = db
		err = m.db.Ping()
		if err != nil {
			return fmt.Errorf("failed to ping Postgres connection: %w", err)
		}
	}
	log.CtxLogger(ctx).Debugw("Postgres connection ping success")

	return nil
}

func executeQuery(ctx context.Context, db dbInterface, query string) (rowsInterface, error) {
	return db.QueryContext(ctx, query)
}

func (m *PostgresMetrics) getWorkMem(ctx context.Context) (int, error) {
	// Default value is "4MB". Minimum value is "64KB".
	rows, err := executeQuery(ctx, m.db, "SHOW work_mem")
	if err != nil {
		return 0, fmt.Errorf("issue trying to show work_mem: %w", err)
	}
	log.CtxLogger(ctx).Debugw("Postgres show work_mem result", "rows", rows)
	defer rows.Close()
	var workMem string
	if !rows.Next() {
		return 0, errors.New("no rows returned from work_mem query")
	}
	if err := rows.Scan(&workMem); err != nil {
		return 0, err
	}

	var unit string
	var multiplier int
	workMemLower := strings.ToLower(workMem)
	if strings.Contains(workMemLower, "kb") {
		unit = "kb"
		multiplier = kilobyte
	} else if strings.Contains(workMemLower, "mb") {
		unit = "mb"
		multiplier = megabyte
	} else if strings.Contains(workMemLower, "gb") {
		unit = "gb"
		multiplier = gigabyte
	} else {
		return 0, fmt.Errorf("unknown units in work_mem: %s", workMem)
	}
	workMemMagnitude, err := strconv.Atoi(strings.ReplaceAll(workMemLower, unit, ""))
	if err != nil {
		return 0, err
	}
	workMemBytes := workMemMagnitude * multiplier

	log.CtxLogger(ctx).Debugw("Postgres getWorkMem", "workMem", workMem, "workMemMagnitude", workMemMagnitude, "unit", unit, "workMemBytes", workMemBytes)
	return workMemBytes, nil
}

// Get Version of Postgres
func (m *PostgresMetrics) version(ctx context.Context) (string, string, error) {
	rows, err := executeQuery(ctx, m.db, "SHOW server_version")
	if err != nil {
		log.CtxLogger(ctx).Debugw("Postgres version error", "err", err)
		return "", "", fmt.Errorf("can't get version in test Postgres connection: %v", err)
	}
	if rows == nil {
		return "", "", fmt.Errorf("no rows returned from version query")
	}
	defer rows.Close()
	if !rows.Next() {
		return "", "", errors.New("no rows returned from version query")
	}
	var fullVersion string
	if err := rows.Scan(&fullVersion); err != nil {
		return "", "", err
	}
	// full version output example: "16.4 (Debian 16.4-1.pgdg110+1)"
	log.CtxLogger(ctx).Debugf("Postgres fullversion: %s", fullVersion)
	// Step 1: Extract the primary version string (e.g., "16.4")
	// We split by space and take the first field.
	parts := strings.Fields(fullVersion)
	var primaryVersion string
	if len(parts) > 0 {
		log.CtxLogger(ctx).Debugf("Postgres parts: %s", parts)
		primaryVersion = parts[0]
	}
	log.CtxLogger(ctx).Debugf("Postgres primaryVersion: %s", primaryVersion)
	// Step 2: Extract the major version from the primary version string
	// Split "16.4" by '.' and take the first part.
	versionComponents := strings.Split(primaryVersion, ".")
	log.CtxLogger(ctx).Debugf("Postgres versionComponents: %s", versionComponents)
	majorVersion := versionComponents[0]

	// The "minor version" is the full primary version string (e.g., "17.4")
	minorVersion := primaryVersion

	log.CtxLogger(ctx).Debugf("Postgres majorVersion: %s, minorVersion: %s", majorVersion, minorVersion)
	return majorVersion, minorVersion, nil
}

// auditingEnabled checks if pgAudit auditing is enabled.
// It returns true if the 'pgaudit.log' setting is not 'none'.
func (m *PostgresMetrics) auditingEnabled(ctx context.Context) (bool, error) {
	query := "SHOW pgaudit.log"
	rows, err := executeQuery(ctx, m.db, query)
	if err != nil {
		return false, fmt.Errorf("issue trying to show pgaudit.log: %w", err)
	}
	defer rows.Close()

	var pgauditLog string
	found := false
	if rows.Next() {
		if err := rows.Scan(&pgauditLog); err != nil {
			return false, fmt.Errorf("failed to scan pgaudit.log value: %w", err)
		}
		found = true
	}
	if !found {
		// This should not happen if executeQuery did not return an error,
		// as SHOW for a non-existent parameter errors out.
		// However, to be safe, treat as disabled.
		return false, fmt.Errorf("pgaudit.log not found")
	}

	if strings.TrimSpace(strings.ToLower(pgauditLog)) == "none" {
		log.CtxLogger(ctx).Debugw("pgaudit.log is set to 'none', auditing is disabled.")
		return false, nil // Auditing is disabled
	}

	log.CtxLogger(ctx).Debugw("pgaudit.log is set to", "pgauditLog", pgauditLog)
	return true, nil // Auditing is enabled
}

// unencryptedConnectionsAllowed checks if PostgreSQL allows unencrypted connections
// by checking if the 'ssl' setting is OFF.
// Returns true if ssl is off, false otherwise.
func (m *PostgresMetrics) unencryptedConnectionsAllowed(ctx context.Context) (bool, error) {
	querySSL := "SHOW ssl"
	rowsSSL, err := executeQuery(ctx, m.db, querySSL)
	if err != nil {
		return false, fmt.Errorf("failed to execute SHOW ssl: %w", err)
	}
	defer rowsSSL.Close()

	var sslValue string
	if !rowsSSL.Next() {
		return false, errors.New("SHOW ssl returned no rows")
	}
	if err := rowsSSL.Scan(&sslValue); err != nil {
		return false, fmt.Errorf("failed to scan ssl value: %w", err)
	}

	isOff := strings.ToLower(sslValue) == "off"
	log.CtxLogger(ctx).Debugw("ssl value", "sslValue", sslValue, "isOff", isOff)
	return isOff, nil
}

// exposedToPublicAccess checks if PostgreSQL allows connections from any IP address.
// Returns true if any rule in pg_hba.conf allows '0.0.0.0' or '::'.
func (m *PostgresMetrics) exposedToPublicAccess(ctx context.Context) (bool, error) {
	query := `
		SELECT COUNT(*)
		FROM pg_hba_file_rules()
		WHERE
		  error IS NULL AND
		  type IN ('host', 'hostssl', 'hostnossl') AND
		  (address = '0.0.0.0' OR address = '::')
	`
	rows, err := executeQuery(ctx, m.db, query)
	if err != nil {
		// Permissions errors on pg_hba_file_rules() are common if not superuser.
		log.CtxLogger(ctx).Debugw("Failed to query pg_hba_file_rules, cannot determine public access", "err", err)
		return false, fmt.Errorf("failed to query pg_hba_file_rules: %w", err)
	}
	defer rows.Close()

	var count int
	if !rows.Next() {
		return false, errors.New("pg_hba_file_rules count query returned no rows")
	}
	if err := rows.Scan(&count); err != nil {
		return false, fmt.Errorf("failed to scan count from pg_hba_file_rules: %w", err)
	}

	isExposed := count > 0
	log.CtxLogger(ctx).Debugw("Exposure to 0.0.0.0 or ::", "count", count, "isExposed", isExposed)
	return isExposed, nil
}

// notFailoverProtected returns true if the instance is NOT protected by automatic failover.
// It returns false if it is protected, or if it is a standby node.
func (m *PostgresMetrics) notFailoverProtected(ctx context.Context) (bool, error) {
	isStandby, err := m.isStandbyNode(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to check if standby node: %w", err)
	}
	if isStandby {
		log.CtxLogger(ctx).Debugw("Instance is a standby node, skipping failover protection check")
		return false, nil // Return false (no issue) on standby nodes
	}

	haActive, err := m.isHAActive(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to check if HA daemons are active: %w", err)
	}
	if !haActive {
		log.CtxLogger(ctx).Debugw("Failover protection status", "haActive", false, "replicationActive", false, "protected", false)
		return true, nil
	}

	replicationActive, err := m.hasActiveReplication(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to check active replication standbys: %w", err)
	}

	log.CtxLogger(ctx).Debugw("Failover protection status", "haActive", haActive, "replicationActive", replicationActive, "protected", replicationActive)

	return !replicationActive, nil
}

func (m *PostgresMetrics) isStandbyNode(ctx context.Context) (bool, error) {
	rows, err := executeQuery(ctx, m.db, "SELECT pg_is_in_recovery()")
	if err != nil {
		return false, err
	}
	defer rows.Close()

	var isRecovery bool
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return false, fmt.Errorf("error iterating rows: %w", err)
		}
		return false, errors.New("no rows returned from pg_is_in_recovery query")
	}
	if err := rows.Scan(&isRecovery); err != nil {
		return false, err
	}
	return isRecovery, nil
}

func (m *PostgresMetrics) isHAActive(ctx context.Context) (bool, error) {
	patroniActive, err := m.isPatroniActive(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to check Patroni: %w", err)
	}
	if patroniActive {
		return true, nil
	}

	pgAutoctlActive, err := m.isPgAutoFailoverActive(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to check pg_auto_failover: %w", err)
	}
	if pgAutoctlActive {
		return true, nil
	}

	return false, nil
}

func (m *PostgresMetrics) isPatroniActive(ctx context.Context) (bool, error) {
	patroniRunning, err := m.isProcessRunning(ctx, "patroni")
	if err != nil {
		return false, err
	}
	if patroniRunning {
		log.CtxLogger(ctx).Debugw("Patroni process is running")
		return true, nil
	}
	return false, nil
}

func (m *PostgresMetrics) isPgAutoFailoverActive(ctx context.Context) (bool, error) {
	pgAutoctlRunning, err := m.isProcessRunning(ctx, "pg_autoctl")
	if err != nil {
		return false, err
	}
	if !pgAutoctlRunning {
		return false, nil
	}
	log.CtxLogger(ctx).Debugw("pg_autoctl process is running, checking state")
	healthy, err := m.checkPgAutoctlState(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to check pg_autoctl state: %w", err)
	}
	return healthy, nil
}

type pgAutoctlNode struct {
	Name         string `json:"nodename"`
	Host         string `json:"nodehost"`
	Port         int    `json:"nodeport"`
	CurrentState string `json:"current_group_state"`
	Health       int    `json:"health"`
}

func (m *PostgresMetrics) checkPgAutoctlState(ctx context.Context) (bool, error) {
	pgdata, err := m.getDataDirectory(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get pgdata directory: %w", err)
	}

	stdout, err := m.runCommandAsPostgres(ctx, "pg_autoctl", []string{"show", "state", "--pgdata", pgdata, "--json"})
	if err != nil {
		return false, fmt.Errorf("failed to execute pg_autoctl: %w", err)
	}
	if stdout == "" {
		return false, nil
	}

	var nodes []pgAutoctlNode
	if err := json.Unmarshal([]byte(stdout), &nodes); err != nil {
		return false, fmt.Errorf("failed to unmarshal pg_autoctl output: %w, stdout: %s", err, stdout)
	}

	// Look for a reachable secondary node
	for _, node := range nodes {
		if (node.CurrentState == "secondary" || node.CurrentState == "catchingup") && node.Health == 1 {
			log.CtxLogger(ctx).Debugw("Found healthy pg_auto_failover standby node", "node", node.Name, "state", node.CurrentState)
			return true, nil
		}
	}

	log.CtxLogger(ctx).Debugw("No healthy pg_auto_failover standby node found")
	return false, nil
}

func (m *PostgresMetrics) getDataDirectory(ctx context.Context) (string, error) {
	rows, err := executeQuery(ctx, m.db, "SHOW data_directory")
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var pgdata string
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", fmt.Errorf("error iterating rows: %w", err)
		}
		return "", errors.New("no rows returned from SHOW data_directory query")
	}
	if err := rows.Scan(&pgdata); err != nil {
		return "", err
	}
	return pgdata, nil
}

func (m *PostgresMetrics) isProcessRunning(ctx context.Context, processName string) (bool, error) {
	if m.execute == nil {
		log.CtxLogger(ctx).Debugw("m.execute is nil, assuming process is not running", "processName", processName)
		return false, nil
	}
	result := m.execute(ctx, commandlineexecutor.Params{
		Executable: "pgrep",
		Args:       []string{"-x", processName},
	})
	if result.Error != nil {
		// If exit code is 1, it means process not found, which is a valid result (not running).
		if result.ExitCode == 1 {
			return false, nil
		}
		return false, fmt.Errorf("failed to run pgrep for %s: %w", processName, result.Error)
	}
	return true, nil
}

func (m *PostgresMetrics) hasActiveReplication(ctx context.Context) (bool, error) {
	rows, err := executeQuery(ctx, m.db, "SELECT COUNT(*) FROM pg_stat_replication")
	if err != nil {
		return false, err
	}
	defer rows.Close()

	var count int
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return false, fmt.Errorf("error iterating rows: %w", err)
		}
		return false, errors.New("no rows returned from pg_stat_replication count query")
	}
	if err := rows.Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

// noAutomatedBackupPolicy checks if PostgreSQL is missing an automated backup policy.
// Returns true if no automated backup policy is detected (e.g., WAL archiving is disabled or no base backup schedule is found), false otherwise.
func (m *PostgresMetrics) noAutomatedBackupPolicy(ctx context.Context) (bool, error) {
	// 1. WAL Archiving Check
	walEnabled, archiveCommand, err := m.isWALArchivingEnabled(ctx)
	if err != nil {
		return false, err
	}
	if !walEnabled {
		return true, nil
	}

	// 2. Base Backups Check
	hasBase, err := m.hasBaseBackup(ctx, archiveCommand)
	if err != nil {
		log.CtxLogger(ctx).Warnw("Failed to verify base backups, defaulting to policy present (false) to avoid false alerts", "err", err)
		return false, nil
	}

	return !hasBase, nil
}

// isWALArchivingEnabled checks if PostgreSQL has WAL archiving enabled and configured with a valid command.
// It returns a boolean indicating if it's enabled, the archive_command string, and any database error.
func (m *PostgresMetrics) isWALArchivingEnabled(ctx context.Context) (bool, string, error) {
	query := "SELECT name, setting FROM pg_settings WHERE name IN ('archive_mode', 'archive_command')"
	rows, err := executeQuery(ctx, m.db, query)
	if err != nil {
		return false, "", fmt.Errorf("failed to query pg_settings: %w", err)
	}
	defer rows.Close()

	var archiveMode, archiveCommand string
	for rows.Next() {
		var name, setting string
		if err := rows.Scan(&name, &setting); err != nil {
			return false, "", fmt.Errorf("failed to scan pg_settings row: %w", err)
		}
		switch name {
		case "archive_mode":
			archiveMode = setting
		case "archive_command":
			archiveCommand = setting
		}
	}

	log.CtxLogger(ctx).Debugw("Postgres archiving configuration", "archive_mode", archiveMode, "archive_command", archiveCommand)

	if archiveMode != "on" && archiveMode != "always" {
		log.CtxLogger(ctx).Debugw("Postgres archiving is disabled", "archive_mode", archiveMode)
		return false, "", nil
	}

	if !isArchiveCommandValid(archiveCommand) {
		log.CtxLogger(ctx).Debugw("Postgres archive command is invalid", "archive_command", archiveCommand)
		return false, "", nil
	}

	return true, archiveCommand, nil
}

// isArchiveCommandValid evaluates if the given PostgreSQL archive command is valid.
// It verifies that the command contains at least one placeholder:
// '%p' (replaced by the path of the file to archive) or '%f' (replaced by
// just the file name).
func isArchiveCommandValid(command string) bool {
	return strings.Contains(command, "%p") || strings.Contains(command, "%f")
}

type backupToolConfig struct {
	name           string
	configFiles    []string
	envDirs        []string
	archivePattern string
}

// hasBaseBackup verifies if any base backup schedules or tool configurations exist on the host.
// It checks specialised tools first, then falls back to inspecting cron jobs and systemd timers.
func (m *PostgresMetrics) hasBaseBackup(ctx context.Context, archiveCommand string) (bool, error) {
	tools := []backupToolConfig{
		{
			name:           "pgbackrest",
			configFiles:    []string{"/etc/pgbackrest/pgbackrest.conf", "/etc/pgbackrest.conf"},
			archivePattern: "pgbackrest",
		},
		{
			name:           "barman",
			configFiles:    []string{"/etc/barman.conf"},
			envDirs:        []string{"/etc/barman.d/"},
			archivePattern: "barman",
		},
		{
			name:           "wal-g",
			configFiles:    []string{"/etc/wal-g.d/config.json"},
			envDirs:        []string{"/etc/wal-e.d/env/", "/etc/wal-g.d/env/"},
			archivePattern: "wal-g",
		},
	}

	// 1. Loop through specialised tools
	for _, tool := range tools {
		if m.checkToolActive(ctx, tool, archiveCommand) {
			log.CtxLogger(ctx).Infow("Active backup tool detected via configuration", "tool", tool.name)
			return true, nil
		}
	}

	// 2. Fallback to Cron Jobs (with error bubbling)
	cronFound, err := m.checkCronJobs(ctx)
	if err != nil {
		return false, err
	}
	if cronFound {
		log.CtxLogger(ctx).Info("Active backup detected via Cron job")
		return true, nil
	}

	// 3. Fallback to Systemd Timers (with error bubbling)
	systemdFound, err := m.checkSystemdTimers(ctx)
	if err != nil {
		return false, err
	}
	if systemdFound {
		log.CtxLogger(ctx).Info("Active backup detected via Systemd timer")
		return true, nil
	}

	return false, nil
}

func (m *PostgresMetrics) checkToolActive(ctx context.Context, tool backupToolConfig, archiveCommand string) bool {
	// 1. Check archive_command
	if tool.archivePattern != "" && strings.Contains(archiveCommand, tool.archivePattern) {
		log.CtxLogger(ctx).Debugw("Backup tool active via archive_command", "tool", tool.name)
		return true
	}

	// 2. Check config files
	for _, file := range tool.configFiles {
		if m.pathExists(file) {
			log.CtxLogger(ctx).Debugw("Backup tool active via config file", "tool", tool.name, "path", file)
			return true
		}
	}

	// 3. Check env directories
	for _, dir := range tool.envDirs {
		if m.dirExistsAndNotEmpty(dir) {
			log.CtxLogger(ctx).Debugw("Backup tool active via env directory", "tool", tool.name, "path", dir)
			return true
		}
	}

	return false
}

// checkCronJobs inspects postgres/root user crontabs and system-wide cron files (/etc/crontab, /etc/cron.*)
// for any active PostgreSQL backup tool executions or custom backup scripts.
func (m *PostgresMetrics) checkCronJobs(ctx context.Context) (bool, error) {
	pipeline := "(crontab -u postgres -l 2>/dev/null; crontab -u root -l 2>/dev/null; grep -rE 'pg_basebackup|pg_dump|pgbackrest|barman|wal-g|pg_start_backup|pg_backup_start' /etc/crontab /etc/cron.* 2>/dev/null) | grep -qE 'pg_basebackup|pg_dump|pgbackrest|barman|wal-g|pg_start_backup|pg_backup_start'"

	result := m.execute(ctx, commandlineexecutor.Params{
		Executable: "bash",
		Args:       []string{"-c", pipeline},
	})

	if result.Error != nil {
		// No match found for active cron backup
		if result.ExitCode == 1 {
			return false, nil
		}
		// Scheduler not installed
		if result.ExitCode == 127 {
			return false, nil
		}
		return false, fmt.Errorf("failed to execute cron check pipeline: %w", result.Error)
	}
	return true, nil
}

// checkSystemdTimers inspects all active systemd timers and performs deep payload inspection
// of their target services' ExecStart command to identify active PostgreSQL backup schedules.
func (m *PostgresMetrics) checkSystemdTimers(ctx context.Context) (bool, error) {
	pipeline := "systemctl list-timers --no-legend --full | awk '$NF != \"-\" {print $NF}' | xargs -r systemctl cat 2>/dev/null | grep -qE 'pg_basebackup|pg_dump|pgbackrest|barman|wal-g|pg_start_backup|pg_backup_start'"

	result := m.execute(ctx, commandlineexecutor.Params{
		Executable: "bash",
		Args:       []string{"-c", pipeline},
	})

	if result.Error != nil {
		// No match found for active systemd backup
		if result.ExitCode == 1 {
			return false, nil
		}
		// Scheduler not installed
		if result.ExitCode == 127 {
			return false, nil
		}
		return false, fmt.Errorf("failed to execute systemd check pipeline: %w", result.Error)
	}
	return true, nil
}

// pathExists checks if a file or directory exists.
func (m *PostgresMetrics) pathExists(path string) bool {
	_, err := m.fs.Stat(path)
	return err == nil
}

// dirExistsAndNotEmpty checks if a directory exists and contains at least one file.
func (m *PostgresMetrics) dirExistsAndNotEmpty(path string) bool {
	files, err := m.fs.ReadDir(path)
	if err != nil {
		return false
	}
	return len(files) > 0
}

// isLastBackupOld evaluates backup age across WAL Archiver, pgBackRest, and WAL-G.
// It returns true if the most recent backup is older than 24 hours (degraded recovery posture).
func (m *PostgresMetrics) isLastBackupOld(ctx context.Context) (bool, error) {
	const maxAge = 24 * time.Hour

	// 1. Check if WAL archiving is active but stale.
	// If WAL archiving is broken, the entire recovery policy is considered unhealthy.
	walStale, err := m.isWALArchivingStale(ctx, maxAge)
	if err != nil {
		log.CtxLogger(ctx).Warnw("Failed to check WAL archiver status", "err", err)
	} else if walStale {
		return true, nil
	}

	// 2. Check if any base backup tool has a fresh backup (<= 24h).
	hasFresh, err := m.hasFreshBaseBackup(ctx, maxAge)
	if err != nil {
		// Fail-open strategy: if all backup checks errored out and we couldn't verify anything,
		// we return false (healthy) to prevent triggering false-positive alerts on transient failures.
		log.CtxLogger(ctx).Warnw("Errors encountered while checking base backups, failing open to avoid false alerts", "err", err)
		return false, err
	}

	if hasFresh {
		return false, nil
	}

	log.CtxLogger(ctx).Debug("No healthy base backups found across configured tools")
	return true, nil
}

// isWALArchivingStale checks if WAL archiving is active but has not successfully archived a segment in over maxAge.
func (m *PostgresMetrics) isWALArchivingStale(ctx context.Context, maxAge time.Duration) (bool, error) {
	t, err := m.checkWalArchiverAge(ctx)
	if err != nil {
		return false, err
	}
	if !t.IsZero() && time.Since(t) > maxAge {
		log.CtxLogger(ctx).Debugw("WAL Archiver is active but stale (> 24h)", "time", t)
		return true, nil
	}
	return false, nil
}

// hasFreshBaseBackup returns true if any configured backup tool has a backup newer than maxAge.
func (m *PostgresMetrics) hasFreshBaseBackup(ctx context.Context, maxAge time.Duration) (bool, error) {
	// Query industry-standard PostgreSQL backup and recovery tools.
	providers := []struct {
		name  string
		check func(context.Context) (time.Time, error)
	}{
		{"pgBackRest", m.checkPgBackRestAge},
		{"WAL-G", m.checkWalGAge},
	}

	var errs []error
	var foundOldBackup bool
	for _, provider := range providers {
		t, err := provider.check(ctx)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to check %s: %w", provider.name, err))
			continue
		}
		if !t.IsZero() {
			isOld := time.Since(t) > maxAge
			log.CtxLogger(ctx).Debugw("Evaluated base backup tool", "tool", provider.name, "latestTime", t, "isOld", isOld)
			// Early exit: if ANY tool successfully reports a fresh backup, the system is protected.
			if !isOld {
				return true, nil
			}
			foundOldBackup = true
		}
	}

	if foundOldBackup {
		if len(errs) > 0 {
			log.CtxLogger(ctx).Warnw("Some backup tool checks failed, but a stale backup was definitively identified", "errs", errors.Join(errs...))
		}
		return false, nil
	}

	if len(errs) > 0 {
		return false, errors.Join(errs...)
	}
	return false, nil
}

// checkWalArchiverAge queries the db to check the latest WAL archiving time from pg_stat_archiver.
func (m *PostgresMetrics) checkWalArchiverAge(ctx context.Context) (time.Time, error) {
	if m.db == nil {
		log.CtxLogger(ctx).Debug("Postgres db client is not initialized, skipping WAL archiver check")
		return time.Time{}, nil
	}
	query := "SELECT last_archived_time FROM pg_stat_archiver"
	var lastArchivedTime sql.NullTime
	err := m.db.QueryRowContext(ctx, query).Scan(&lastArchivedTime)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return time.Time{}, nil
		}
		return time.Time{}, fmt.Errorf("failed to query pg_stat_archiver: %w", err)
	}

	if !lastArchivedTime.Valid {
		// Valid can be false if no WAL segments have been archived yet.
		log.CtxLogger(ctx).Debug("last_archived_time is NULL")
		return time.Time{}, nil
	}

	return lastArchivedTime.Time, nil
}

type pgBackrestInfo []struct {
	Name   string `json:"name"`
	Backup []struct {
		Timestamp struct {
			Stop int64 `json:"stop"`
		} `json:"timestamp"`
	} `json:"backup"`
}

type walGBackup []struct {
	Time time.Time `json:"time"`
}

// checkPgBackRestAge runs the pgbackrest command to retrieve and parse the latest backup time.
func (m *PostgresMetrics) checkPgBackRestAge(ctx context.Context) (time.Time, error) {
	stdout, err := m.runCommandAsPostgres(ctx, "pgbackrest", []string{"info", "--output=json"})
	if err != nil {
		return time.Time{}, err
	}
	if stdout == "" {
		return time.Time{}, nil
	}

	var info pgBackrestInfo
	if err := json.Unmarshal([]byte(stdout), &info); err != nil {
		return time.Time{}, fmt.Errorf("failed to unmarshal pgbackrest output: %w", err)
	}

	// Find the most recent (maximum) backup completion time across all configured stanzas.
	// pgBackRest lists backups chronologically, so the last element in the slice is the latest.
	var maxOfLatest time.Time
	for _, stanza := range info {
		var stanzaLatest time.Time
		if stanza.Backup != nil && len(stanza.Backup) > 0 {
			stanzaLatest = time.Unix(stanza.Backup[len(stanza.Backup)-1].Timestamp.Stop, 0)
		}
		if !stanzaLatest.IsZero() {
			if maxOfLatest.IsZero() || stanzaLatest.After(maxOfLatest) {
				maxOfLatest = stanzaLatest
			}
		}
	}

	return maxOfLatest, nil
}

// checkWalGAge runs the wal-g command to retrieve and parse the latest backup time.
func (m *PostgresMetrics) checkWalGAge(ctx context.Context) (time.Time, error) {
	stdout, err := m.runCommandAsPostgres(ctx, "wal-g", []string{"backup-list", "--json", "--detail"})
	if err != nil {
		return time.Time{}, err
	}
	if stdout == "" {
		return time.Time{}, nil
	}

	var backups walGBackup
	if err := json.Unmarshal([]byte(stdout), &backups); err != nil {
		return time.Time{}, fmt.Errorf("failed to unmarshal wal-g output: %w", err)
	}

	if len(backups) == 0 {
		log.CtxLogger(ctx).Debug("wal-g returned empty backup list")
		return time.Time{}, nil
	}

	// WAL-G lists backups chronologically, so the last element represents the most recent backup.
	latestBackup := backups[len(backups)-1]
	return latestBackup.Time, nil
}

// runCommandAsPostgres runs the target executable as the 'postgres' user via a login shell.
// This is critical to ensure the postgres user's environment variables (PATH, PGDATA, config paths) are fully loaded.
func (m *PostgresMetrics) runCommandAsPostgres(ctx context.Context, executable string, args []string) (string, error) {
	if m.execute == nil {
		log.CtxLogger(ctx).Debugw("m.execute is nil, skipping check", "executable", executable)
		return "", nil
	}

	// Escape arguments to prevent shell injection and handle spaces in paths (e.g. var/lib/pg data).
	// Wrapping in single quotes and escaping existing single quotes is standard for POSIX shells.
	escapedArgs := make([]string, len(args))
	for i, arg := range args {
		escapedArgs[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(arg, "'", "'\\''"))
	}

	cmdStr := executable
	if len(escapedArgs) > 0 {
		cmdStr = fmt.Sprintf("%s %s", executable, strings.Join(escapedArgs, " "))
	}

	params := commandlineexecutor.Params{
		Executable: "su",
		Args:       []string{"-", "postgres", "-c", cmdStr},
	}
	res := m.execute(ctx, params)
	if !res.ExecutableFound {
		log.CtxLogger(ctx).Debugw("su executable not found on the system")
		return "", nil
	}
	if res.Error != nil {
		if res.ExitCode == 127 {
			log.CtxLogger(ctx).Debugw("Target executable not found in postgres user PATH", "executable", executable)
			return "", nil
		}
		return "", fmt.Errorf("%s command failed: %w (stderr: %s)", executable, res.Error, res.StdErr)
	}
	return res.StdOut, nil
}

// CollectWlmMetricsOnce collects metrics for Postgres databases running on the host.
func (m *PostgresMetrics) CollectWlmMetricsOnce(ctx context.Context, dwActivated bool) (*workloadmanager.WorkloadMetrics, error) {
	if !dwActivated {
		log.CtxLogger(ctx).Debugw("Data Warehouse is not activated, not sending metrics to Data Warehouse")
		return nil, nil
	}
	workMemBytes, err := m.getWorkMem(ctx)
	if err != nil {
		log.CtxLogger(ctx).Warnf("Failed to get work mem: %w", err)
		return nil, err
	}
	log.CtxLogger(ctx).Debugw("Finished collecting Postgres metrics once. Next step is to send to WLM (DW).", workMemKey, workMemBytes)

	metrics := workloadmanager.WorkloadMetrics{
		WorkloadType: workloadmanager.POSTGRES,
		Metrics: map[string]string{
			workMemKey: strconv.Itoa(workMemBytes),
		},
	}
	res, err := workloadmanager.SendDataInsight(ctx, workloadmanager.SendDataInsightParams{
		WLMetrics:  metrics,
		CloudProps: m.Config.GetCloudProperties(),
		WLMService: m.WLMClient,
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		log.CtxLogger(ctx).Warn("SendDataInsight did not return an error but the WriteInsight response is nil")
		return &metrics, nil
	}
	log.CtxLogger(ctx).Debugw("WriteInsight response", "StatusCode", res.HTTPStatusCode)
	return &metrics, nil
}

// CollectDBCenterMetricsOnce collects metrics for Postgres databases running on the host.
func (m *PostgresMetrics) CollectDBCenterMetricsOnce(ctx context.Context) error {
	majorVersion, minorVersion, err := m.version(ctx)
	if err != nil {
		// Don't return error here, we want to send metrics to DW even if version send fails.
		log.CtxLogger(ctx).Debugw("Failed to get version:", "err", err)
	}
	auditingEnabled, err := m.auditingEnabled(ctx)
	if err != nil {
		log.CtxLogger(ctx).Debugw("Failed to get auditing disabled", "err", err)
	}
	unencryptedConnectionsAllowed, err := m.unencryptedConnectionsAllowed(ctx)
	if err != nil {
		log.CtxLogger(ctx).Debugw("Failed to get unencrypted connections allowed", "err", err)
	}
	exposedToPublicAccess, err := m.exposedToPublicAccess(ctx)
	if err != nil {
		log.CtxLogger(ctx).Debugw("Failed to get exposed to public access", "err", err)
	}
	notFailoverProtected, err := m.notFailoverProtected(ctx)
	if err != nil {
		log.CtxLogger(ctx).Debugw("Failed to detect failover protection", "err", err)
	}
	noAutomatedBackupPolicy, err := m.noAutomatedBackupPolicy(ctx)
	if err != nil {
		log.CtxLogger(ctx).Debugw("Failed to check automated backup policy", "err", err)
		noAutomatedBackupPolicy = true
	}
	log.CtxLogger(ctx).Debugw("Postgres noAutomatedBackupPolicy result", "noAutomatedBackupPolicy", noAutomatedBackupPolicy)

	lastBackupOld, err := m.isLastBackupOld(ctx)
	if err != nil {
		log.CtxLogger(ctx).Debugw("Failed to check last backup old", "err", err)
	}

	// Send metadata details to database center
	err = m.DBcenterClient.SendMetadataToDatabaseCenter(ctx, databasecenter.DBCenterMetrics{EngineType: databasecenter.POSTGRES,
		Metrics: map[string]string{
			databasecenter.MajorVersionKey:                    majorVersion,
			databasecenter.MinorVersionKey:                    minorVersion,
			databasecenter.ExposedToPublicAccessKey:           strconv.FormatBool(exposedToPublicAccess),
			databasecenter.UnencryptedConnectionsKey:          strconv.FormatBool(unencryptedConnectionsAllowed),
			databasecenter.DatabaseAuditingDisabledKey:        strconv.FormatBool(!auditingEnabled),
			databasecenter.NotProtectedByAutomaticFailoverKey: strconv.FormatBool(notFailoverProtected),
			databasecenter.NoAutomatedBackupPolicyKey:         strconv.FormatBool(noAutomatedBackupPolicy),
			databasecenter.LastBackupOldKey:                   strconv.FormatBool(lastBackupOld),
		}})
	if err != nil {
		// Don't return error here, we want to send metrics to DW even if dbcenter metadata send fails.
		log.CtxLogger(ctx).Info("Unable to send information to Database Center, please refer to documentation to make sure that all prerequisites are met")
		log.CtxLogger(ctx).Debugf("Failed to send metadata to database center: %v", err)
	}
	return nil
}
