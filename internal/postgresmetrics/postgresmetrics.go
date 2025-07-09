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
	"errors"
	"fmt"
	"strconv"
	"strings"

	// Register the pq driver for Postgres with the database/sql package.
	_ "github.com/lib/pq"
	"github.com/GoogleCloudPlatform/workloadagent/internal/databasecenter"
	"github.com/GoogleCloudPlatform/workloadagent/internal/workloadmanager"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/commandlineexecutor"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/secret"
)

const (
	workMemKey = "work_mem"
	kilobyte   = 1024
	megabyte   = 1024 * 1024
	gigabyte   = 1024 * 1024 * 1024
)

type gceInterface interface {
	GetSecret(ctx context.Context, projectID, secretName string) (string, error)
}

type rowsInterface interface {
	Next() bool
	Close() error
	Scan(dest ...any) error
}

type dbWrapper struct {
	db *sql.DB
	dbInterface
}

type dbInterface interface {
	QueryContext(ctx context.Context, query string, args ...any) (rowsInterface, error)
	Ping() error
}

func (d dbWrapper) QueryContext(ctx context.Context, query string, args ...any) (rowsInterface, error) {
	return d.db.QueryContext(ctx, query, args...)
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
}

// password gets the password for the Postgres database.
// If the password is set in the configuration, it is used directly (not recommended).
// Otherwise, if the secret configuration is set, the secret is fetched from GCE.
// Without either, the password is not set and requests to the Postgres database will fail.
func (m *PostgresMetrics) password(ctx context.Context, gceService gceInterface) (secret.String, error) {
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

func (m *PostgresMetrics) dbDSN(ctx context.Context, gceService gceInterface) (string, error) {
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
	}
}

// InitDB initializes the Postgres database connection.
func (m *PostgresMetrics) InitDB(ctx context.Context, gceService gceInterface) error {
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
		return fmt.Errorf("failed to ping Postgres connection: %w", err)
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

// CollectMetricsOnce collects metrics for Postgres databases running on the host.
func (m *PostgresMetrics) CollectMetricsOnce(ctx context.Context, dwActivated bool) (*workloadmanager.WorkloadMetrics, error) {
	workMemBytes, err := m.getWorkMem(ctx)
	if err != nil {
		log.CtxLogger(ctx).Warnf("Failed to get work mem: %w", err)
		return nil, err
	}
	log.CtxLogger(ctx).Debugw("Finished collecting Postgres metrics once. Next step is to send to WLM (DW).", workMemKey, workMemBytes)
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
	// Send metadata details to database center
	err = m.DBcenterClient.SendMetadataToDatabaseCenter(ctx, databasecenter.DBCenterMetrics{EngineType: databasecenter.POSTGRES,
		Metrics: map[string]string{
			databasecenter.MajorVersionKey:             majorVersion,
			databasecenter.MinorVersionKey:             minorVersion,
			databasecenter.ExposedToPublicAccessKey:    strconv.FormatBool(exposedToPublicAccess),
			databasecenter.UnencryptedConnectionsKey:   strconv.FormatBool(unencryptedConnectionsAllowed),
			databasecenter.DatabaseAuditingDisabledKey: strconv.FormatBool(!auditingEnabled),
		}})
	if err != nil {
		// Don't return error here, we want to send metrics to DW even if dbcenter metadata send fails.
		log.CtxLogger(ctx).Info("Unable to send information to Database Center, please refer to documentation to make sure that all prerequisites are met")
		log.CtxLogger(ctx).Debugf("Failed to send metadata to database center: %v", err)
	}

	metrics := workloadmanager.WorkloadMetrics{
		WorkloadType: workloadmanager.POSTGRES,
		Metrics: map[string]string{
			workMemKey: strconv.Itoa(workMemBytes),
		},
	}
	if !dwActivated {
		log.CtxLogger(ctx).Debugw("Data Warehouse is not activated, not sending metrics to Data Warehouse")
		return &metrics, nil
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
