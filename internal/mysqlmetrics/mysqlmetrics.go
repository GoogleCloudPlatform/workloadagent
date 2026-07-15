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
	"net"
	"runtime"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/GoogleCloudPlatform/workloadagent/internal/databasecenter"
	"github.com/GoogleCloudPlatform/workloadagent/internal/ipinfo"
	"github.com/GoogleCloudPlatform/workloadagent/internal/workloadmanager"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/commandlineexecutor"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/secret"
)

const (
	innoDBKey             = "is_inno_db_default"
	bufferPoolKey         = "buffer_pool_size"
	totalRAMKey           = "total_ram"
	currentRoleKey        = "current_role"
	replicationZonesKey   = "replication_zones"
	sourceRole            = "source"
	replicaRole           = "replica"
	replicationZonesQuery = "SELECT HOST FROM information_schema.PROCESSLIST AS p WHERE p.COMMAND = 'Binlog Dump'"
)

type netInterface interface {
	LookupHost(host string) ([]string, error)
	ParseIP(ip string) net.IP
	LookupAddr(ip string) ([]string, error)
}

type netImpl struct{}

func (n netImpl) LookupHost(host string) ([]string, error) {
	return net.LookupHost(host)
}

func (n netImpl) ParseIP(ip string) net.IP {
	return net.ParseIP(ip)
}

func (n netImpl) LookupAddr(ip string) ([]string, error) {
	return net.LookupAddr(ip)
}

// GceInterface defines an interface for the GCE client to allow faking.
type GceInterface interface {
	GetSecret(ctx context.Context, projectID, secretName string) (string, error)
}

type rowsInterface interface {
	Next() bool
	Close() error
	Scan(dest ...any) error
	Columns() ([]string, error)
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
	execute        commandlineexecutor.Execute
	Config         *configpb.Configuration
	db             dbInterface
	connect        func(ctx context.Context, dataSource string) (dbInterface, error)
	WLMClient      workloadmanager.WLMWriter
	DBcenterClient databasecenter.Client
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
func (m *MySQLMetrics) password(ctx context.Context, gceService GceInterface) (secret.String, error) {
	pw := ""
	if m.Config.GetMysqlConfiguration().GetConnectionParameters().GetPassword() != "" {
		return secret.String(m.Config.GetMysqlConfiguration().GetConnectionParameters().GetPassword()), nil
	}
	secretCfg := m.Config.GetMysqlConfiguration().GetConnectionParameters().GetSecret()
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

func (m *MySQLMetrics) dbDSN(ctx context.Context, gceService GceInterface) (string, error) {
	pw, err := m.password(ctx, gceService)
	if err != nil {
		return "", fmt.Errorf("initializing password: %w", err)
	}
	cfg := mysql.Config{
		User:   m.Config.GetMysqlConfiguration().GetConnectionParameters().GetUsername(),
		Passwd: pw.SecretValue(),
		Addr:   "localhost:3306", // using localhost because the agent is running on the same machine as the MySQL server
		DBName: "mysql",
	}
	return cfg.FormatDSN(), nil
}

// New creates a new MySQLMetrics object initialized with default values.
func New(ctx context.Context, config *configpb.Configuration, wlmClient workloadmanager.WLMWriter, dbcenterClient databasecenter.Client) *MySQLMetrics {
	return &MySQLMetrics{
		execute:        commandlineexecutor.ExecuteCommand,
		Config:         config,
		connect:        defaultConnect,
		WLMClient:      wlmClient,
		DBcenterClient: dbcenterClient,
	}
}

// InitDB initializes the MySQL database connection.
func (m *MySQLMetrics) InitDB(ctx context.Context, gceService GceInterface) error {
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
	if rows == nil {
		return false, fmt.Errorf("no rows returned from show engines query")
	}
	log.CtxLogger(ctx).Debugw("MySQL show engines result", "rows", rows)
	var engineResults []engineResult
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
	if rows == nil {
		return 0, fmt.Errorf("no rows returned from buffer pool size query")
	}
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

func isReplica(ctx context.Context, db dbInterface) bool {
	isReplica := false
	// Only supported in versions 8.0.22 and later.
	rows, err := executeQuery(ctx, db, "SHOW REPLICA STATUS")
	if err != nil {
		if rows != nil {
			rows.Close()
		}
		log.CtxLogger(ctx).Debugw("MySQL error while running SHOW REPLICA STATUS", "err", err)
	} else if rows != nil {
		defer rows.Close()
		// If there is a row, then this is a replica. Otherwise, it is the source.
		isReplica = rows.Next()
	}
	// If we already know this is a replica, return early.
	if isReplica {
		return isReplica
	}

	// Work in versions prior to 8.0.22, but is deprecated in 8.0.22 and later in favor of SHOW REPLICA STATUS. May eventually stop working.
	rows, err = executeQuery(ctx, db, "SHOW SLAVE STATUS")
	if err != nil {
		log.CtxLogger(ctx).Debugw("MySQL current role error", "err", err)
	} else if rows != nil {
		defer rows.Close()
		// If there is a row, then this is a replica. Otherwise, it is the source.
		isReplica = rows.Next()
	}
	return isReplica
}

func (m *MySQLMetrics) currentRole(ctx context.Context) string {
	role := sourceRole
	if isReplica(ctx, m.db) {
		role = replicaRole
	}
	log.CtxLogger(ctx).Debugw("MySQL current role", "role", role)
	return role
}

func host(ctx context.Context, rows rowsInterface) string {
	var host sql.NullString
	if err := rows.Scan(&host); err != nil {
		log.CtxLogger(ctx).Debugw("MySQL error while running query", "query", replicationZonesQuery, "err", err)
	}
	return host.String
}

func (m *MySQLMetrics) replicationZones(ctx context.Context, currentRole string, netInterface netInterface) []string {
	// We only need to check the replication zones if the current role is the source.
	if currentRole != sourceRole {
		return nil
	}
	var zones []string
	rows, err := executeQuery(ctx, m.db, replicationZonesQuery)
	if err != nil {
		log.CtxLogger(ctx).Debugw("MySQL error while running query", "query", replicationZonesQuery, "err", err)
	}
	if rows == nil {
		log.CtxLogger(ctx).Debugw("MySQL no rows returned from replication zones query")
		return nil
	}
	defer rows.Close()
	for rows.Next() {
		host := host(ctx, rows)
		if host == "" {
			continue
		}
		ip := netInterface.ParseIP(host)
		isIP := ip != nil
		if isIP {
			zone := ipinfo.ZoneFromIP(ctx, host, netInterface.LookupAddr)
			if zone != "" {
				zones = append(zones, zone)
			}
		} else {
			_, err := netInterface.LookupHost(host)
			if err != nil {
				log.CtxLogger(ctx).Debugw("MySQL error while looking up host", "host", host, "err", err)
				continue
			}
			zone := ipinfo.ZoneFromHost(ctx, host)
			if zone != "" {
				zones = append(zones, zone)
			}
		}
	}
	return zones
}

func windowsTotalRAM(ctx context.Context, output string) (int, error) {
	// Expected to be something like "TotalPhysicalMemory\n134876032413"
	lines := strings.Split(output, "\n")
	if len(lines) < 2 {
		return 0, fmt.Errorf("not enough lines found in output for windows total RAM: %d", len(lines))
	}
	ramString := strings.TrimSpace(lines[1])
	ram, err := strconv.Atoi(ramString)
	if err != nil {
		return 0, fmt.Errorf("failed to convert total RAM to integer: %v", err)
	}
	return ram, nil
}

func (m *MySQLMetrics) totalRAM(ctx context.Context, isWindowsOS bool) (int, error) {
	cmd := commandlineexecutor.Params{
		Executable: "grep",
		Args:       []string{"MemTotal", "/proc/meminfo"},
	}
	if isWindowsOS {
		cmd = commandlineexecutor.Params{
			Executable: "cmd",
			Args:       []string{"/C", "wmic", "computersystem", "get", "totalphysicalmemory"},
		}
	}
	log.CtxLogger(ctx).Debugw("getTotalRAM command", "command", cmd)
	res := m.execute(ctx, cmd)
	log.CtxLogger(ctx).Debugw("getTotalRAM result", "result", res)
	if res.Error != nil {
		return 0, fmt.Errorf("failed to execute command: %v", res.Error)
	}
	if isWindowsOS {
		return windowsTotalRAM(ctx, res.StdOut)
	}
	// Expected to be something like "MemTotal:       1348760 kB"
	lines := strings.Split(res.StdOut, "\n")
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

// Get Version of MySQL
func (m *MySQLMetrics) version(ctx context.Context) (string, string, error) {
	rows, err := executeQuery(ctx, m.db, "SELECT @@version")
	if err != nil {
		log.CtxLogger(ctx).Debugw("MySQL version error", "err", err)
		return "", "", fmt.Errorf("can't get version in test MySQL connection: %v", err)
	}
	log.CtxLogger(ctx).Debugw("MySQL version result", "rows", rows)
	if rows == nil {
		return "", "", fmt.Errorf("no rows returned from version query")
	}
	defer rows.Close()
	var version string
	if !rows.Next() {
		return "", "", errors.New("no rows returned from version query")
	}
	if err := rows.Scan(&version); err != nil {
		return "", "", err
	}
	log.CtxLogger(ctx).Debugw("MySQL full version", "version", version)
	// extract the major version from the version string
	// example: "8.0.31" -> "8.0"
	// example: "8.0" -> "8.0"
	parts := strings.Split(version, ".")

	// Initialize a variable for the extracted major version
	var majorVersion string

	// We expect at least two parts (Major.Minor) for the version format "X.Y.Z"
	if len(parts) >= 2 {
		// Join the first two parts with a dot
		majorVersion = parts[0] + "." + parts[1]
	} else if len(parts) == 1 {
		// Handle cases like "8" where only the major component is present
		majorVersion = parts[0]
	} else {
		// Handle empty or unexpected input gracefully
		majorVersion = ""
		log.CtxLogger(ctx).Debugw("unexpected MySQL version", "majorVersion", majorVersion)
	}
	return majorVersion, version, nil
}

// mysqlUserRow represents a partial row from the mysql.user table.
type mysqlUserRow struct {
	User                 string
	Host                 string
	Plugin               string
	AuthenticationString sql.NullString // authentication_string can be NULL
}

// rootPasswordNotSet checks if any MySQL root user account using password-based
// authentication (like mysql_native_password or caching_sha2_password) has no password set.
// It returns true if at least one such root account has no password, false otherwise.
// An error is returned if the query fails or data scanning issues occur.
func (m *MySQLMetrics) rootPasswordNotSet(ctx context.Context) (bool, error) {
	query := `
		SELECT user, host, plugin, authentication_string
		FROM mysql.user
		WHERE user = 'root'
	`
	rows, err := executeQuery(ctx, m.db, query)
	if err != nil {
		return false, fmt.Errorf("failed to query mysql.user table with error: %v", err)
	}
	defer rows.Close()

	var users []mysqlUserRow
	for rows.Next() {
		var u mysqlUserRow
		if err := rows.Scan(&u.User, &u.Host, &u.Plugin, &u.AuthenticationString); err != nil {
			return false, fmt.Errorf("failed to scan row from mysql.user table with error: %v", err)
		}
		users = append(users, u)
	}

	if len(users) == 0 {
		// This state is unusual in a typical MySQL installation.
		return false, errors.New("no root users found in mysql.user table")
	}

	for _, u := range users {
		// Check for empty passwords only on password-based authentication plugins.
		if u.Plugin == "mysql_native_password" || u.Plugin == "caching_sha2_password" {
			if !u.AuthenticationString.Valid || u.AuthenticationString.String == "" {
				// Found a root user configured with a password-based auth plugin but has no password hash.
				log.CtxLogger(ctx).Debugw("Security Warning: root user has no password set.", "user", u.User, "host", u.Host, "plugin", u.Plugin)
				return true, nil
			}
		}
		// Note: Users with the 'auth_socket' plugin (common for root@localhost) authenticate
		// using system user credentials via the Unix socket, not the password hash stored
		// in mysql.user.authentication_string. Such cases are not treated as 'no password set'
		// in the context of password-based authentication.
	}

	// No root users found with empty passwords for standard password authentication methods.
	return false, nil
}

// simpleUserRow represents a User and Host from the mysql.user table.
type simpleUserRow struct {
	User string
	Host string
}

// exposedToPublicAccess checks if any MySQL user has host set to '%',
// allowing connections from any IP address.
// It returns true if at least one user has Host = '%', false otherwise.
// An error is returned if the query fails.
func (m *MySQLMetrics) exposedToPublicAccess(ctx context.Context) (bool, error) {
	query := `
		SELECT User, Host
		FROM mysql.user
		WHERE Host = '%'
	`
	rows, err := executeQuery(ctx, m.db, query)
	if err != nil {
		return false, fmt.Errorf("failed to query mysql.user table with error: %v", err)
	}
	defer rows.Close()

	exposedToPublicAccess := false
	for rows.Next() {
		var u simpleUserRow
		if err := rows.Scan(&u.User, &u.Host); err != nil {
			return false, fmt.Errorf("failed to scan row from mysql.user table with error: %v", err)
		}
		// Log the specific user and host
		log.CtxLogger(ctx).Debugw("Found user with broad access", "user", u.User, "host", u.Host)
		exposedToPublicAccess = true
	}

	return exposedToPublicAccess, nil
}

// unencryptedConnectionsAllowed checks if the MySQL server allows unencrypted connections.
// It returns true if unencrypted connections are possible, false if they are not.
// This is determined by the value of the 'require_secure_transport' system variable.
func (m *MySQLMetrics) unencryptedConnectionsAllowed(ctx context.Context) (bool, error) {
	query := `SHOW GLOBAL VARIABLES LIKE 'require_secure_transport'`
	rows, err := executeQuery(ctx, m.db, query)
	if err != nil {
		return false, fmt.Errorf("failed to query global variable require_secure_transport with error: %v", err)
	}
	defer rows.Close()

	var varName, varValue string
	found := false
	if rows.Next() {
		if err := rows.Scan(&varName, &varValue); err != nil {
			return false, fmt.Errorf("failed to scan row from global variable require_secure_transport with error: %v", err)
		}
		found = true
	}

	if !found {
		// This variable should always exist in recent MySQL versions.
		return false, fmt.Errorf("require_secure_transport variable not found")
	}

	// If require_secure_transport is ON, unencrypted connections are disabled.
	// If OFF, unencrypted connections are allowed.
	unencryptedConnAllowed := strings.ToUpper(varValue) == "OFF"
	log.CtxLogger(ctx).Debugw("require_secure_transport variable value", "unencryptedConnAllowed", unencryptedConnAllowed)
	return unencryptedConnAllowed, nil
}

// auditingEnabled checks if auditing is enabled.
// It returns true if the 'audit_log' plugin is ACTIVE, false otherwise.
func (m *MySQLMetrics) auditingEnabled(ctx context.Context) (bool, error) {
	query := `
		SELECT PLUGIN_STATUS
		FROM INFORMATION_SCHEMA.PLUGINS
		WHERE PLUGIN_NAME = 'audit_log'
	`

	rows, err := executeQuery(ctx, m.db, query)
	if err != nil {
		// If the query fails, we assume auditing is disabled.
		return false, fmt.Errorf("failed to query INFORMATION_SCHEMA.PLUGINS for audit_log with error: %v", err)
	}
	defer rows.Close()

	var pluginStatus string
	if rows.Next() {
		if err := rows.Scan(&pluginStatus); err != nil {
			return false, fmt.Errorf("failed to scan row for audit_log plugin status with error: %v", err)
		}
		isEnabled := strings.ToUpper(pluginStatus) == "ACTIVE"
		log.CtxLogger(ctx).Debugw("Audit plugin 'audit_log' status", "pluginStatus", pluginStatus, "Auditing Enabled", isEnabled)
		return isEnabled, nil
	}

	// Plugin not found, so auditing is disabled.
	log.CtxLogger(ctx).Debugw("Audit plugin 'audit_log' not found. Auditing is disabled.")
	return false, nil
}

// notProtectedByAutoFailover evaluates to true if no automatic failover mechanism is active for the MySQL instance.
// High-availability requires either a multi-node cluster (InnoDB Cluster, Galera/PXC, NDB)
// or a healthy primary-replica topology actively monitored by an external failover orchestrator (such as Orchestrator or MHA).
func (m *MySQLMetrics) notProtectedByAutoFailover(ctx context.Context) (bool, error) {
	hasCluster, err := m.isPartOfHACluster(ctx)
	if err != nil {
		log.CtxLogger(ctx).Debugw("Failed to check HA cluster", "err", err)
	}
	if hasCluster {
		return false, nil
	}

	if isReplica(ctx, m.db) {
		healthy, err := m.isReplicaHealthy(ctx)
		if err != nil || !healthy {
			log.CtxLogger(ctx).Debugw("Replica IO/SQL threads not healthy for auto failover", "err", err)
			return true, nil
		}
	} else {
		healthy, err := m.isPrimaryHealthy(ctx)
		if err != nil || !healthy {
			log.CtxLogger(ctx).Debugw("Primary has log_bin OFF or zero attached dump threads", "err", err)
			return true, nil
		}
	}

	hasAutoFailoverManager, err := m.hasAutoFailoverManager(ctx)
	if err != nil {
		log.CtxLogger(ctx).Debugw("Failed to check auto failover manager", "err", err)
	}
	if hasAutoFailoverManager {
		return false, nil
	}

	return true, nil
}

// isPartOfHACluster checks if the instance participates in a self-healing, multi-node HA cluster.
// Native clusters handle automatic failover and quorum internally without requiring an external failover manager.
func (m *MySQLMetrics) isPartOfHACluster(ctx context.Context) (bool, error) {
	if m.isInnoDBClusterActive(ctx) {
		log.CtxLogger(ctx).Debugw("InnoDB Cluster / Group Replication active")
		return true, nil
	}

	if m.isGaleraClusterActive(ctx) {
		log.CtxLogger(ctx).Debugw("Galera / Percona XtraDB Cluster active")
		return true, nil
	}

	if m.isStorageEngineActive(ctx, "ndbcluster") {
		log.CtxLogger(ctx).Debugw("NDB Cluster engine active")
		return true, nil
	}

	return false, nil
}

// isReplicaHealthy checks if both IO (receiver) and SQL (applier) replication threads are active on the replica.
// An unhealthy replica cannot safely take over during an automatic failover without risking data loss or split-brain.
func (m *MySQLMetrics) isReplicaHealthy(ctx context.Context) (bool, error) {
	// SHOW REPLICA STATUS is supported in MySQL 8.0.22+; fallback to SHOW SLAVE STATUS for older versions (e.g. MySQL 5.7 / < 8.0.22).
	rows, err := executeQuery(ctx, m.db, "SHOW REPLICA STATUS")
	if err != nil || rows == nil {
		if rows != nil {
			rows.Close()
		}
		rows, err = executeQuery(ctx, m.db, "SHOW SLAVE STATUS")
	}
	if err != nil {
		log.CtxLogger(ctx).Debugw("Failed to get replica/slave status", "err", err)
		return false, err
	}
	if rows == nil {
		log.CtxLogger(ctx).Debugw("No rows returned from replica/slave status query")
		return false, nil
	}
	defer rows.Close()

	if !rows.Next() {
		return false, nil
	}

	cols, err := rows.Columns()
	if err != nil {
		log.CtxLogger(ctx).Debugw("Failed to get columns from replica status", "err", err)
		return false, err
	}
	if len(cols) == 0 {
		return false, nil
	}

	// Dynamically find IO and SQL thread status column indices.
	// Column names differ between MySQL 8.0.22+ (Replica_IO_Running/Replica_SQL_Running)
	// and older versions (Slave_IO_Running/Slave_SQL_Running),
	ioIndex, sqlIndex := -1, -1
	for i, col := range cols {
		lowerCol := strings.ToLower(col)
		if lowerCol == "replica_io_running" || lowerCol == "slave_io_running" {
			ioIndex = i
		}
		if lowerCol == "replica_sql_running" || lowerCol == "slave_sql_running" {
			sqlIndex = i
		}
	}

	if ioIndex == -1 || sqlIndex == -1 {
		log.CtxLogger(ctx).Debugw("Replica IO or SQL running column not found in status output")
		return false, nil
	}

	dest := make([]any, len(cols))
	nullStrings := make([]sql.NullString, len(cols))
	for i := range dest {
		dest[i] = &nullStrings[i]
	}

	if err := rows.Scan(dest...); err != nil {
		log.CtxLogger(ctx).Debugw("Failed to scan replica status row", "err", err)
		return false, err
	}

	ioRunning := strings.ToUpper(nullStrings[ioIndex].String) == "YES"
	sqlRunning := strings.ToUpper(nullStrings[sqlIndex].String) == "YES"
	log.CtxLogger(ctx).Debugw("Replica thread status", "ioRunning", ioRunning, "sqlRunning", sqlRunning)

	return ioRunning && sqlRunning, nil
}

// isPrimaryHealthy checks if binary logging (log_bin) is enabled on the primary and at least one standby replica is actively connected.
// Automatic failover is impossible on a primary with disabled binlogs or zero attached replicas as there is no target to promote.
func (m *MySQLMetrics) isPrimaryHealthy(ctx context.Context) (bool, error) {
	binlogOn, err := m.isBinaryLoggingEnabled(ctx)
	if err != nil {
		return false, err
	}
	if !binlogOn {
		log.CtxLogger(ctx).Debugw("Primary binary logging is not ON")
		return false, nil
	}

	dumpThreads, err := m.hasActiveDumpThreads(ctx)
	if err != nil {
		return false, err
	}
	if !dumpThreads {
		log.CtxLogger(ctx).Debugw("Primary has zero attached dump threads")
		return false, nil
	}

	return true, nil
}

// hasAutoFailoverManager checks if an external failover orchestrator (Orchestrator or MHA) is configured to manage this instance.
// Detection looks for dedicated management DB accounts (remote manager) or local configuration files and active daemons (local manager).
func (m *MySQLMetrics) hasAutoFailoverManager(ctx context.Context) (bool, error) {
	// When auto-failover manager is running on a different VM, check for user accounts related to it in this MySQL instance (e.g. orchestrator, mha).
	query := `
		SELECT COUNT(*)
		FROM mysql.user
		WHERE user LIKE '%orchestrator%'
		   OR user LIKE '%mha%'
	`
	rows, err := executeQuery(ctx, m.db, query)
	if err == nil && rows != nil {
		defer rows.Close()
		var count int
		if rows.Next() && rows.Scan(&count) == nil && count > 0 {
			log.CtxLogger(ctx).Debugw("Found topology manager user account in mysql.user", "count", count)
			return true, nil
		}
	}

	// When auto-failover manager is running on the same VM, check for local config files and active daemons.
	if m.hasLocalAutoFailoverService(ctx) {
		return true, nil
	}

	return false, nil
}

// isInnoDBClusterActive checks if MySQL Group Replication / InnoDB Cluster has at least 2 ONLINE members.
// A single-node Group Replication instance lacks quorum and cannot perform automatic failover.
func (m *MySQLMetrics) isInnoDBClusterActive(ctx context.Context) bool {
	query := `
		SELECT COUNT(*)
		FROM performance_schema.replication_group_members
		WHERE MEMBER_STATE = 'ONLINE'
	`
	rows, err := executeQuery(ctx, m.db, query)
	if err != nil || rows == nil {
		return false
	}
	defer rows.Close()

	var count int
	return rows.Next() && rows.Scan(&count) == nil && count >= 2
}

// isGaleraClusterActive checks if Galera / Percona XtraDB Cluster (PXC) is in a ready state with at least 2 active nodes.
// Galera requires wsrep_ready=ON and a cluster size of >= 2 to maintain quorum for automatic failover.
func (m *MySQLMetrics) isGaleraClusterActive(ctx context.Context) bool {
	query := `
		SHOW GLOBAL STATUS
		WHERE Variable_name IN ('wsrep_cluster_size', 'wsrep_ready')
	`
	rows, err := executeQuery(ctx, m.db, query)
	if err != nil || rows == nil {
		return false
	}
	defer rows.Close()

	var wsrepReady bool
	var clusterSize int
	for rows.Next() {
		var name, val string
		if scanErr := rows.Scan(&name, &val); scanErr == nil {
			if name == "wsrep_ready" && strings.ToUpper(val) == "ON" {
				wsrepReady = true
			}
			if name == "wsrep_cluster_size" {
				clusterSize, _ = strconv.Atoi(val)
			}
		}
	}
	return wsrepReady && clusterSize >= 2
}

// isStorageEngineActive checks if a specified storage engine (e.g. 'ndbcluster') is supported and active in the database.
func (m *MySQLMetrics) isStorageEngineActive(ctx context.Context, engine string) bool {
	query := fmt.Sprintf(`
		SELECT Support
		FROM information_schema.engines
		WHERE Engine = '%s'
	`, engine)
	rows, err := executeQuery(ctx, m.db, query)
	if err != nil || rows == nil {
		return false
	}
	defer rows.Close()

	var support string
	if rows.Next() {
		if scanErr := rows.Scan(&support); scanErr == nil {
			s := strings.ToUpper(support)
			return s == "YES" || s == "DEFAULT"
		}
	}
	return false
}

// isBinaryLoggingEnabled checks if log_bin is ON, which is required for primary-replica replication and point-in-time recovery.
func (m *MySQLMetrics) isBinaryLoggingEnabled(ctx context.Context) (bool, error) {
	query := "SHOW GLOBAL VARIABLES LIKE 'log_bin'"
	rows, err := executeQuery(ctx, m.db, query)
	if err != nil {
		log.CtxLogger(ctx).Debugw("Failed to query log_bin variable", "err", err)
		return false, err
	}
	if rows == nil {
		log.CtxLogger(ctx).Debugw("No rows returned for log_bin variable")
		return false, errors.New("no rows returned for log_bin")
	}
	defer rows.Close()

	var varName, varVal string
	if !rows.Next() {
		return false, errors.New("log_bin variable row empty")
	}
	if err := rows.Scan(&varName, &varVal); err != nil {
		return false, err
	}
	return strings.ToUpper(varVal) == "ON", nil
}

// hasActiveDumpThreads checks if at least one replica is actively connected and streaming binary logs from this primary instance.
func (m *MySQLMetrics) hasActiveDumpThreads(ctx context.Context) (bool, error) {
	// Query performance_schema.threads first (MySQL 5.7+), fallback to information_schema.PROCESSLIST.
	query := "SELECT COUNT(*) FROM performance_schema.threads WHERE PROCESSLIST_COMMAND IN ('Binlog Dump', 'Binlog Dump GTID')"
	rows, err := executeQuery(ctx, m.db, query)
	if err != nil || rows == nil {
		if rows != nil {
			rows.Close()
		}
		query = "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE COMMAND IN ('Binlog Dump', 'Binlog Dump GTID')"
		rows, err = executeQuery(ctx, m.db, query)
	}
	if err != nil {
		log.CtxLogger(ctx).Debugw("Failed to query active dump threads", "err", err)
		return false, err
	}
	if rows == nil {
		return false, errors.New("no rows returned for dump threads")
	}
	defer rows.Close()

	var count int
	if !rows.Next() {
		return false, errors.New("dump threads row empty")
	}
	if err := rows.Scan(&count); err != nil {
		return false, err
	}
	return count >= 1, nil
}

// hasLocalAutoFailoverService checks if an auto-failover daemon (Orchestrator or MHA) is running locally on this VM host.
func (m *MySQLMetrics) hasLocalAutoFailoverService(ctx context.Context) bool {
	orchestratorConfigs := []string{
		"/etc/orchestrator.conf.json",
		"/opt/orchestrator/orchestrator.conf.json",
		"/usr/local/orchestrator/orchestrator.conf.json",
	}

	// Check Orchestrator local config for RecoverMasterClusterFilters auto-recovery setting
	for _, cfgPath := range orchestratorConfigs {
		cmd := commandlineexecutor.Params{
			Executable: "grep",
			Args:       []string{"RecoverMasterClusterFilters", cfgPath},
		}
		res := m.execute(ctx, cmd)
		if res.Error == nil && strings.TrimSpace(res.StdOut) != "" {
			// Check if Orchestrator service/process is active
			svcCmd := commandlineexecutor.Params{
				Executable: "systemctl",
				Args:       []string{"is-active", "orchestrator"},
			}
			svcRes := m.execute(ctx, svcCmd)
			if svcRes.Error == nil && strings.TrimSpace(svcRes.StdOut) == "active" {
				log.CtxLogger(ctx).Debugw("Found active local Orchestrator service", "config", cfgPath)
				return true
			}
		}
	}

	// Check MHA local service
	for _, mhaService := range []string{"mha-manager", "masterha_manager"} {
		svcCmd := commandlineexecutor.Params{
			Executable: "systemctl",
			Args:       []string{"is-active", mhaService},
		}
		svcRes := m.execute(ctx, svcCmd)
		if svcRes.Error == nil && strings.TrimSpace(svcRes.StdOut) == "active" {
			log.CtxLogger(ctx).Debugw("Found active local MHA manager service", "service", mhaService)
			return true
		}
	}

	return false
}

// CollectMetricsOnce collects metrics for MySQL databases running on the host.
func (m *MySQLMetrics) CollectWlmMetricsOnce(ctx context.Context, dwActivated bool) (*workloadmanager.WorkloadMetrics, error) {
	if !dwActivated {
		log.CtxLogger(ctx).Debugw("Data Warehouse is not activated, not sending metrics to Data Warehouse")
		return nil, nil
	}
	bufferPoolSize, err := m.bufferPoolSize(ctx)
	if err != nil {
		log.CtxLogger(ctx).Warnf("Failed to get buffer pool size: %v", err)
		return nil, err
	}
	isWindowsOS := runtime.GOOS == "windows"
	totalRAM, err := m.totalRAM(ctx, isWindowsOS)
	if err != nil {
		log.CtxLogger(ctx).Warnf("Failed to get total RAM: %v", err)
		return nil, err
	}
	isInnoDBDefault, err := m.isInnoDBStorageEngine(ctx)
	if err != nil {
		log.CtxLogger(ctx).Warnf("Failed to get InnoDB default status: %v", err)
		return nil, err
	}
	currentRole := m.currentRole(ctx)
	replicationZones := m.replicationZones(ctx, currentRole, &netImpl{})
	log.CtxLogger(ctx).Debugw("Finished collecting MySQL metrics once. Next step is to send to WLM (DW).",
		bufferPoolKey, bufferPoolSize,
		totalRAMKey, totalRAM,
		innoDBKey, isInnoDBDefault,
		currentRoleKey, currentRole,
		replicationZonesKey, strings.Join(replicationZones, ","),
	)
	metrics := workloadmanager.WorkloadMetrics{
		WorkloadType: workloadmanager.MYSQL,
		Metrics: map[string]string{
			bufferPoolKey:       strconv.FormatInt(bufferPoolSize, 10),
			totalRAMKey:         strconv.Itoa(totalRAM),
			innoDBKey:           strconv.FormatBool(isInnoDBDefault),
			currentRoleKey:      currentRole,
			replicationZonesKey: strings.Join(replicationZones, ","),
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

var (
	backupToolSignatures = []string{
		"mysqldump", "mysqlpump", "mydumper",
		"xtrabackup", "innobackupex", "mysqlbackup", "mariadb-backup",
		"gcloud compute disks snapshot", "lvcreate", "zfs snapshot",
	}
)

// noAutomatedBackupPolicy evaluates to true if no automated backup policy is detected for the MySQL instance.
// Detection evaluates OS-level schedulers (Cron, Systemd Timers) and DB tool history tables (MEB, XtraBackup).
func (m *MySQLMetrics) noAutomatedBackupPolicy(ctx context.Context) (bool, error) {
	if m.hasCronBackupPolicy(ctx) {
		log.CtxLogger(ctx).Debugw("Automated backup policy detected via OS cron schedule")
		return false, nil
	}

	if m.hasSystemdTimerBackupPolicy(ctx) {
		log.CtxLogger(ctx).Debugw("Automated backup policy detected via systemd timer")
		return false, nil
	}

	if m.hasBackupHistoryTablePolicy(ctx) {
		log.CtxLogger(ctx).Debugw("Automated backup policy detected via DB tool history tables")
		return false, nil
	}

	log.CtxLogger(ctx).Debugw("No automated backup policy detected for MySQL instance")
	return true, nil
}

// hasCronBackupPolicy checks if any active, uncommented cron job contains a known backup command.
func (m *MySQLMetrics) hasCronBackupPolicy(ctx context.Context) bool {
	return m.hasUserCronBackupPolicy(ctx) || m.hasSystemCronBackupPolicy(ctx)
}

// hasUserCronBackupPolicy checks user-specific crontabs (e.g. root, mysql) via `crontab -l`.
func (m *MySQLMetrics) hasUserCronBackupPolicy(ctx context.Context) bool {
	for _, user := range []string{"root", "mysql"} {
		res := m.execute(ctx, commandlineexecutor.Params{
			Executable: "crontab",
			Args:       []string{"-u", user, "-l"},
		})
		if res.Error == nil {
			for _, line := range strings.Split(res.StdOut, "\n") {
				if containsBackupSignature(line) {
					log.CtxLogger(ctx).Debugw("Found backup cron job in user crontab", "user", user)
					return true
				}
			}
		}
	}
	return false
}

// hasSystemCronBackupPolicy checks system-wide cron files and directories for backup commands.
// Uses `grep -rI` to pre-filter matching lines at the OS level, avoiding reading and parsing vast system cron files in Go.
func (m *MySQLMetrics) hasSystemCronBackupPolicy(ctx context.Context) bool {
	cronPaths := []string{
		"/etc/crontab",
		"/etc/cron.d",
		"/etc/cron.daily",
		"/etc/cron.hourly",
		"/etc/cron.weekly",
		"/var/spool/cron/crontabs",
	}

	grepPattern := buildGrepBackupPattern()

	for _, p := range cronPaths {
		res := m.execute(ctx, commandlineexecutor.Params{
			Executable: "grep",
			Args:       []string{"-rI", "-E", grepPattern, p},
		})
		if res.Error == nil && strings.TrimSpace(res.StdOut) != "" {
			for _, line := range strings.Split(res.StdOut, "\n") {
				if containsBackupSignature(line) {
					log.CtxLogger(ctx).Debugw("Found backup cron job in system cron path")
					return true
				}
			}
		}
	}

	return false
}

// buildGrepBackupPattern constructs a regex pattern from backupToolSignatures for use in grep.
func buildGrepBackupPattern() string {
	patterns := make([]string, len(backupToolSignatures))
	for i, sig := range backupToolSignatures {
		patterns[i] = strings.ReplaceAll(sig, " ", ".*")
	}
	return strings.Join(patterns, "|")
}

// hasSystemdTimerBackupPolicy checks if any active systemd timer triggers a service with a backup command.
func (m *MySQLMetrics) hasSystemdTimerBackupPolicy(ctx context.Context) bool {
	res := m.execute(ctx, commandlineexecutor.Params{
		Executable: "systemctl",
		Args:       []string{"list-timers", "--no-legend", "--no-pager"},
	})
	if res.Error != nil || strings.TrimSpace(res.StdOut) == "" {
		return false
	}

	for _, line := range strings.Split(res.StdOut, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 1 {
			continue
		}
		// In systemctl list-timers output, the last column contains the service name that is activated by the timer
		serviceName := fields[len(fields)-1]
		if !strings.HasSuffix(serviceName, ".service") {
			continue
		}

		catRes := m.execute(ctx, commandlineexecutor.Params{
			Executable: "systemctl",
			Args:       []string{"cat", serviceName},
		})
		if catRes.Error == nil {
			for _, serviceLine := range strings.Split(catRes.StdOut, "\n") {
				if containsBackupSignature(serviceLine) {
					log.CtxLogger(ctx).Debugw("Found backup systemd timer service", "service", serviceName)
					return true
				}
			}
		}
	}

	return false
}

// hasBackupHistoryTablePolicy checks if MEB or Percona XtraBackup history tables show >= 2 successful backups in the last 14 days.
func (m *MySQLMetrics) hasBackupHistoryTablePolicy(ctx context.Context) bool {
	return m.hasMEBBackupHistory(ctx) || m.hasXtraBackupHistory(ctx)
}

// hasMEBBackupHistory checks mysql.backup_history for recent successful MySQL Enterprise Backups.
func (m *MySQLMetrics) hasMEBBackupHistory(ctx context.Context) bool {
	query := `
		SELECT COUNT(*)
		FROM mysql.backup_history
		WHERE exit_state = 'SUCCESS'
		  AND start_time >= NOW() - INTERVAL 14 DAY
	`
	return m.checkBackupCountQuery(ctx, query, "Found recent successful MEB backups in mysql.backup_history")
}

// hasXtraBackupHistory checks PERCONA_SCHEMA.xtrabackup_history for recent Percona XtraBackups.
func (m *MySQLMetrics) hasXtraBackupHistory(ctx context.Context) bool {
	query := `
		SELECT COUNT(*)
		FROM PERCONA_SCHEMA.xtrabackup_history
		WHERE start_time >= NOW() - INTERVAL 14 DAY
	`
	return m.checkBackupCountQuery(ctx, query, "Found recent successful XtraBackup records in PERCONA_SCHEMA.xtrabackup_history")
}

// checkBackupCountQuery runs a COUNT(*) query and returns true if count >= 2.
func (m *MySQLMetrics) checkBackupCountQuery(ctx context.Context, query string, debugMsg string) bool {
	rows, err := executeQuery(ctx, m.db, query)
	if err != nil || rows == nil {
		return false
	}
	defer rows.Close()

	var count int
	if rows.Next() && rows.Scan(&count) == nil && count >= 2 {
		log.CtxLogger(ctx).Debugw(debugMsg, "count", count)
		return true
	}
	return false
}

// containsBackupSignature returns true if the given command line contains a known backup tool signature and is not commented out.
func containsBackupSignature(line string) bool {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, "#") {
		return false
	}
	lower := strings.ToLower(trimmed)
	for _, sig := range backupToolSignatures {
		if strings.Contains(lower, sig) {
			return true
		}
	}
	return false
}

// CollectDBCenterMetricsOnce collects metrics to send to Database Center for MySQL databases running on the host.
func (m *MySQLMetrics) CollectDBCenterMetricsOnce(ctx context.Context) error {
	// Get major and minor version of MySQL
	majorVersion, minorVersion, err := m.version(ctx)
	if err != nil {
		log.CtxLogger(ctx).Debugw("Failed to get MySQL version", "with error: ", err)
	}
	// Check if No Root Password is set
	rootPasswordNotSet, err := m.rootPasswordNotSet(ctx)
	if err != nil {
		log.CtxLogger(ctx).Debugw("Failed to check if root password is not set", "with error: ", err)
	}
	// Check if broad access is set
	exposedToPublicAccess, err := m.exposedToPublicAccess(ctx)
	if err != nil {
		log.CtxLogger(ctx).Debugw("Failed to check if broad access is set", "with error: ", err)
	}
	// Check if unencrypted connections are allowed
	unencryptedConnectionsAllowed, err := m.unencryptedConnectionsAllowed(ctx)
	if err != nil {
		log.CtxLogger(ctx).Debugw("Failed to check if unencrypted connections are allowed", "with error: ", err)
	}
	auditingEnabled, err := m.auditingEnabled(ctx)
	if err != nil {
		log.CtxLogger(ctx).Debugw("Failed to check if auditing is disabled", "with error: ", err)
	}
	notProtectedByAutoFailover, err := m.notProtectedByAutoFailover(ctx)
	if err != nil {
		log.CtxLogger(ctx).Debugw("Failed to check if not protected by auto failover", "with error: ", err)
	}
	noAutomatedBackupPolicy, err := m.noAutomatedBackupPolicy(ctx)
	if err != nil {
		log.CtxLogger(ctx).Debugw("Failed to check if no automated backup policy", "with error: ", err)
	}
	// send metadata details to database center
	err = m.DBcenterClient.SendMetadataToDatabaseCenter(ctx, databasecenter.DBCenterMetrics{EngineType: databasecenter.MYSQL,
		Metrics: map[string]string{
			databasecenter.MajorVersionKey:                    majorVersion,
			databasecenter.MinorVersionKey:                    minorVersion,
			databasecenter.NoRootPasswordKey:                  strconv.FormatBool(rootPasswordNotSet),
			databasecenter.ExposedToPublicAccessKey:           strconv.FormatBool(exposedToPublicAccess),
			databasecenter.UnencryptedConnectionsKey:          strconv.FormatBool(unencryptedConnectionsAllowed),
			databasecenter.DatabaseAuditingDisabledKey:        strconv.FormatBool(!auditingEnabled),
			databasecenter.NotProtectedByAutomaticFailoverKey: strconv.FormatBool(notProtectedByAutoFailover),
			databasecenter.NoAutomatedBackupPolicyKey:         strconv.FormatBool(noAutomatedBackupPolicy),
		}})
	if err != nil {
		// Don't return error here, we want to send metrics to DW even if dbcenter metadata send fails.
		log.CtxLogger(ctx).Info("Unable to send information to Database Center, please refer to documentation to make sure that all prerequisites are met")
		log.CtxLogger(ctx).Debugf("Failed to send metadata to database center: %v", err)
	}
	return nil
}
