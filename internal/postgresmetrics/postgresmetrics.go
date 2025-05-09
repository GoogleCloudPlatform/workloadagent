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
	execute   commandlineexecutor.Execute
	Config    *configpb.Configuration
	db        dbInterface
	connect   func(ctx context.Context, dataSource string) (dbInterface, error)
	WLMClient workloadmanager.WLMWriter
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
func New(ctx context.Context, config *configpb.Configuration, wlmClient workloadmanager.WLMWriter) *PostgresMetrics {
	return &PostgresMetrics{
		execute:   commandlineexecutor.ExecuteCommand,
		Config:    config,
		connect:   defaultConnect,
		WLMClient: wlmClient,
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

// CollectMetricsOnce collects metrics for Postgres databases running on the host.
func (m *PostgresMetrics) CollectMetricsOnce(ctx context.Context) (*workloadmanager.WorkloadMetrics, error) {
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
