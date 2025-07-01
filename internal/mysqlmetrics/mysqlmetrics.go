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
func (m *MySQLMetrics) password(ctx context.Context, gceService gceInterface) (secret.String, error) {
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

func (m *MySQLMetrics) dbDSN(ctx context.Context, gceService gceInterface) (string, error) {
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

// CollectMetricsOnce collects metrics for MySQL databases running on the host.
func (m *MySQLMetrics) CollectMetricsOnce(ctx context.Context, dwActivated bool) (*workloadmanager.WorkloadMetrics, error) {
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
	// send metadata details to database center
	err = m.DBcenterClient.SendMetadataToDatabaseCenter(ctx, databasecenter.DBCenterMetrics{EngineType: databasecenter.MYSQL,
		Metrics: map[string]string{
			databasecenter.MajorVersionKey:             majorVersion,
			databasecenter.MinorVersionKey:             minorVersion,
			databasecenter.NoRootPasswordKey:           strconv.FormatBool(rootPasswordNotSet),
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
		WorkloadType: workloadmanager.MYSQL,
		Metrics: map[string]string{
			bufferPoolKey:       strconv.FormatInt(bufferPoolSize, 10),
			totalRAMKey:         strconv.Itoa(totalRAM),
			innoDBKey:           strconv.FormatBool(isInnoDBDefault),
			currentRoleKey:      currentRole,
			replicationZonesKey: strings.Join(replicationZones, ","),
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
