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

// Package mysqlmetrics implements metric collection for the MySQL workload agent service.
package mysqlmetrics

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/GoogleCloudPlatform/workloadagent/internal/secret"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/commandlineexecutor"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/log"
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

// MySQLMetrics contains variables and methods to collect metrics for MySQL databases running on the current host.
type MySQLMetrics struct {
	execute commandlineexecutor.Execute
	Config  *configpb.Configuration
	db      dbInterface
	connect func(ctx context.Context, dataSource string) (dbInterface, error)
}

type engineResult struct {
	// these are the fields in the table output from SHOW ENGINES
	engine       string
	support      string
	comment      string
	transactions string
	xa           string
	savepoints   string
}

// password gets the password for the MySQL database.
// If the password is set in the configuration, it is used directly (not recommended).
// Otherwise, if the secret configuration is set, the secret is fetched from GCE.
// Without either, the password is not set and requests to the MySQL database will fail.
func (m *MySQLMetrics) password(ctx context.Context, gceService gceInterface) (secret.String, error) {
	pw := ""
	if m.Config.GetMysqlConfiguration().GetPassword() != "" {
		return secret.String(m.Config.GetMysqlConfiguration().GetPassword()), nil
	}
	secretCfg := m.Config.GetMysqlConfiguration().GetSecret()
	if secretCfg.GetSecretName() != "" && secretCfg.GetProjectId() != "" {
		var err error
		pw, err = gceService.GetSecret(ctx, secretCfg.GetProjectId(), secretCfg.GetSecretName())
		if err != nil {
			return secret.String(""), fmt.Errorf("failed to get secret: %v", err)
		}
	}
	return secret.String(pw), nil
}

// defaultConnect connects to the MySQL database and pings it.
func defaultConnect(ctx context.Context, dataSource string) (dbInterface, error) {
	d, err := sql.Open("mysql", dataSource)
	if err != nil {
		return nil, fmt.Errorf("failed to open MySQL connection: %v", err)
	}
	return dbWrapper{db: d}, nil
}

func (m *MySQLMetrics) dbDSN(ctx context.Context, gceService gceInterface) (string, error) {
	pw, err := m.password(ctx, gceService)
	if err != nil {
		return "", fmt.Errorf("initializing password: %w", err)
	}
	cfg := mysql.Config{
		User:   m.Config.GetMysqlConfiguration().GetUser(),
		Passwd: pw.SecretValue(),
		Addr:   "localhost:3306", // using localhost because the agent is running on the same machine as the MySQL server
		DBName: "mysql",
	}
	return cfg.FormatDSN(), nil
}

// New creates a new MySQLMetrics object initialized with default values.
func New(ctx context.Context, config *configpb.Configuration) *MySQLMetrics {
	return &MySQLMetrics{
		execute: commandlineexecutor.ExecuteCommand,
		Config:  config,
		connect: defaultConnect,
	}
}

// InitDB initializes the MySQL database connection.
func (m *MySQLMetrics) InitDB(ctx context.Context, gceService gceInterface) error {
	dbDSN, err := m.dbDSN(ctx, gceService)
	if err != nil {
		return fmt.Errorf("getting dbDSN: %w", err)
	}
	db, err := m.connect(ctx, dbDSN)
	if err != nil {
		return fmt.Errorf("connecting to MySQL: %w", err)
	}
	m.db = db
	err = m.db.Ping()
	if err != nil {
		return fmt.Errorf("failed to ping MySQL connection: %v", err)
	}
	log.CtxLogger(ctx).Debugw("MySQL connection ping success")

	return nil
}

func executeQuery(ctx context.Context, db dbInterface, query string) (rowsInterface, error) {
	return db.QueryContext(ctx, query)
}

func readEngine(ctx context.Context, rows rowsInterface) (engineResult, error) {
	// These are the fields in the table output from SHOW ENGINES.
	var engine sql.NullString
	var support sql.NullString
	var comment sql.NullString
	var transactions sql.NullString
	var xa sql.NullString
	var savepoints sql.NullString

	if err := rows.Scan(&engine, &support, &comment, &transactions, &xa, &savepoints); err != nil {
		return engineResult{}, err
	}
	log.CtxLogger(ctx).Debugw("MySQL table name", "engine", engine, "support", support, "comment", comment, "transactions", transactions, "xa", xa, "savepoints", savepoints)
	return engineResult{
		engine:       engine.String,
		support:      support.String,
		comment:      comment.String,
		transactions: transactions.String,
		xa:           xa.String,
		savepoints:   savepoints.String,
	}, nil
}

func (m *MySQLMetrics) isInnoDBStorageEngine(ctx context.Context) (bool, error) {
	rows, err := executeQuery(ctx, m.db, "SHOW ENGINES")
	if err != nil {
		return false, fmt.Errorf("issue trying to show engines: %v", err)
	}
	log.CtxLogger(ctx).Debugw("MySQL show engines result", "rows", rows)
	engineResults := []engineResult{}
	defer rows.Close()
	for rows.Next() {
		engineResult, err := readEngine(ctx, rows)
		if err != nil {
			log.CtxLogger(ctx).Debugw("MySQL read engine error", "err", err)
			continue
		}
		engineResults = append(engineResults, engineResult)
	}
	isInnoDBDefault := false
	for _, engineResult := range engineResults {
		if engineResult.engine == "InnoDB" && engineResult.support == "DEFAULT" {
			isInnoDBDefault = true
		}
	}
	log.CtxLogger(ctx).Debugw("MySQL isInnoDBDefault", "isInnoDBDefault", isInnoDBDefault)
	return isInnoDBDefault, nil
}

func (m *MySQLMetrics) bufferPoolSize(ctx context.Context) (int64, error) {
	rows, err := executeQuery(ctx, m.db, "SELECT @@innodb_buffer_pool_size")
	if err != nil {
		log.CtxLogger(ctx).Debugw("MySQL buffer pool size error", "err", err)
		return 0, fmt.Errorf("can't get buffer pool size in test MySQL connection: %v", err)
	}
	log.CtxLogger(ctx).Debugw("MySQL buffer pool size result", "rows", rows)
	defer rows.Close()
	var bufferPoolSize int64
	if !rows.Next() {
		return 0, errors.New("no rows returned from buffer pool size query")
	}
	if err := rows.Scan(&bufferPoolSize); err != nil {
		return 0, err
	}
	log.CtxLogger(ctx).Debugw("MySQL buffer pool size", "bufferPoolSize", bufferPoolSize)
	return bufferPoolSize, nil
}

func (m *MySQLMetrics) totalRAM(ctx context.Context) (int, error) {
	cmd := commandlineexecutor.Params{
		Executable: "grep",
		Args:       []string{"MemTotal", "/proc/meminfo"},
	}
	log.CtxLogger(ctx).Debugw("getTotalRAM command", "command", cmd)
	res := m.execute(ctx, cmd)
	log.CtxLogger(ctx).Debugw("getTotalRAM result", "result", res)
	if res.Error != nil {
		return 0, fmt.Errorf("failed to execute command: %v", res.Error)
	}
	lines := strings.Split(res.StdOut, "\n")
	if len(lines) != 2 {
		return 0, fmt.Errorf("found wrong number of lines in total RAM: %d", len(lines))
	}
	fields := strings.Fields(lines[0])
	if len(fields) != 3 {
		return 0, fmt.Errorf("found wrong number of fields in total RAM: %d", len(fields))
	}
	ram, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0, fmt.Errorf("failed to convert total RAM to integer: %v", err)
	}
	units := fields[2]
	if strings.ToUpper(units) == "KB" {
		ram = ram * 1024
	}
	return ram, nil
}

// CollectMetricsOnce collects metrics for MySQL databases running on the host.
func (m *MySQLMetrics) CollectMetricsOnce(ctx context.Context) {
	bufferPoolSize, err := m.bufferPoolSize(ctx)
	if err != nil {
		log.CtxLogger(ctx).Warnf("Failed to get buffer pool size: %v", err)
	}
	totalRAM, err := m.totalRAM(ctx)
	if err != nil {
		log.CtxLogger(ctx).Warnf("Failed to get total RAM: %v", err)
	}

	isInnoDBDefault, err := m.isInnoDBStorageEngine(ctx)
	if err != nil {
		log.CtxLogger(ctx).Warnf("Failed to get InnoDB default status: %v", err)
	}
	// TODO: send these metrics to Data Warehouse.
	log.CtxLogger(ctx).Debugw("Finished collecting MySQL metrics once", "bufferPoolSize", bufferPoolSize, "totalRAM", totalRAM, "isInnoDBDefault", isInnoDBDefault)
}
