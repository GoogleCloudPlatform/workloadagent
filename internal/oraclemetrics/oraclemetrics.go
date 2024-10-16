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

// Package oraclemetrics queries Oracle database and sends the results as metrics to Cloud Monitoring.
package oraclemetrics

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/monitoring/apiv3"
	"github.com/sijms/go-ora"

	"golang.org/x/exp/maps"
	"github.com/gammazero/workerpool"
	"github.com/GoogleCloudPlatform/sapagent/shared/cloudmonitoring"
	"github.com/GoogleCloudPlatform/sapagent/shared/gce"
	"github.com/GoogleCloudPlatform/sapagent/shared/log"
	"github.com/GoogleCloudPlatform/sapagent/shared/recovery"
	"github.com/GoogleCloudPlatform/sapagent/shared/timeseries"
	"github.com/GoogleCloudPlatform/workloadagent/internal/secret"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"

	mpb "google.golang.org/genproto/googleapis/api/metric"
	mrpb "google.golang.org/genproto/googleapis/monitoring/v3"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	odpb "github.com/GoogleCloudPlatform/workloadagent/protos/oraclediscovery"
)

const (
	metricURL        = "workload.googleapis.com/oracle"
	maxQueryFailures = 3
	maxLoginFailures = 9 // by default FAILED_LOGIN_ATTEMPTS for the DEFAULT profile is set to 10
	dbViewQuery      = `SELECT dbid, db_unique_name, database_role FROM v$database`
)

type (
	gceInterface interface {
		GetSecret(ctx context.Context, projectID, secretName string) (string, error)
	}

	dbPinger interface {
		PingContext(ctx context.Context) error
	}

	// MetricCollector holds the parameters required for metric collection.
	MetricCollector struct {
		Config                  *configpb.Configuration
		TimeSeriesCreator       cloudmonitoring.TimeSeriesCreator
		BackOffs                *cloudmonitoring.BackOffIntervals
		GCEService              gceInterface
		createWorkerPoolRoutine *recovery.RecoverableRoutine
		wp                      *workerpool.WorkerPool
		dbManager               *dbManager
		startTime               *tspb.Timestamp
	}

	// manages database connections, allowing for opening and closing connections dynamically.
	dbManager struct {
		connections          map[string]*sql.DB
		connectionParameters []connectionParameters
		mu                   sync.RWMutex
		failCounts           map[string]map[string]int
		skipMsgLogged        map[string]map[string]bool
		nextRetry            map[string]time.Time
		baseDelaySeconds     int64
		pingContext          func(context.Context, *sql.DB)
	}

	// connectionParameters holds connection parameters for the database.
	connectionParameters struct {
		Host        string `json:"host"`
		Port        int    `json:"port"`
		Username    string `json:"username"`
		Password    secret.String
		ServiceName string `json:"service_name"`
	}

	// queryOptions holds the parameters required to query the database.
	queryOptions struct {
		db            *sql.DB
		query         *configpb.Query
		timeout       int64
		runningSum    map[timeSeriesKey]prevVal
		serviceName   string
		collector     *MetricCollector
		defaultLabels map[string]string
	}

	// timeSeriesKey uniquely identifies each timeseries and is used as a key in the runningSum map.
	timeSeriesKey struct {
		MetricType   string
		MetricKind   string
		MetricLabels string
	}

	// prevVal struct stores the value of the last datapoint in the timeseries. It is needed to build
	// a cumulative timeseries which uses the previous data point value and timestamp since the process
	// started.
	prevVal struct {
		val       any
		startTime *tspb.Timestamp
	}

	// databaseInfo holds the result of the dbViewQuery query.
	databaseInfo struct {
		dbID         string
		dbUniqueName string
		databaseRole string
	}
)

// openConnections connects to all the databases specified in the connection parameters.
func (m *dbManager) openConnections(ctx context.Context) {
	for _, params := range m.connectionParameters {
		urlOptions := map[string]string{
			"dba privilege": "sysdg", // sysdg system privilege is required to connect to a closed standby.
		}
		connStr := go_ora.BuildUrl(params.Host, params.Port, params.ServiceName, params.Username, params.Password.SecretValue(), urlOptions)
		connection, err := sql.Open("oracle", connStr)
		if err != nil {
			log.CtxLogger(ctx).Errorw("Failed to connect to the database", "error", err, "connection_parameters", params)
			continue
		}
		m.connections[params.ServiceName] = connection
	}
}

// closeConnections closes all database connections.
func (m *dbManager) closeConnections(ctx context.Context) {
	for serviceName, db := range m.connections {
		if err := db.Close(); err != nil {
			log.CtxLogger(ctx).Errorw("Failed to close database connection", "error", err, "service_name", serviceName)
		}
		delete(m.connections, serviceName)
	}
}

// populateConnectionParameters populates connection parameters based on the discovery proto
// if they are not already set in the configuration.json file.
func (m *dbManager) populateConnectionParameters(ctx context.Context, discovery *odpb.Discovery) {
	// Map to easily find host and port by the service name
	discoveredEndpoints := make(map[string]*odpb.Discovery_Listener_Endpoint)
	for _, listener := range discovery.GetListeners() {
		for _, service := range listener.GetServices() {
			for _, endpoint := range listener.GetEndpoints() {
				// connect to TCP endpoints only
				if _, ok := endpoint.GetProtocol().(*odpb.Discovery_Listener_Endpoint_Tcp); ok {
					discoveredEndpoints[service.GetName()] = endpoint
				}
			}
		}
	}

	// Populate host and port from the discovery data if they are not already set in the configuration.json file.
	for i, params := range m.connectionParameters {
		if endpoint, ok := discoveredEndpoints[params.ServiceName]; ok {
			if m.connectionParameters[i].Host == "" {
				m.connectionParameters[i].Host = endpoint.GetTcp().GetHost()
			}
			if m.connectionParameters[i].Port == 0 {
				m.connectionParameters[i].Port = int(endpoint.GetTcp().GetPort())
			}
		} else {
			log.CtxLogger(ctx).Warnw("Service name not found in discovery", "service_name", params.ServiceName, "discovery", discovery)
		}
	}
}

func (m *dbManager) failCountFor(serviceName, queryName string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if queryNameMap, exists := m.failCounts[serviceName]; exists {
		if failCount, fcExists := queryNameMap[queryName]; fcExists {
			return failCount
		}
	}
	return 0
}

func (m *dbManager) incrementFailCount(serviceName, queryName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.failCounts[serviceName]; !exists {
		m.failCounts[serviceName] = make(map[string]int)
	}
	m.failCounts[serviceName][queryName]++
}

func (m *dbManager) resetFailCount(serviceName, queryName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if queryNameMap, exists := m.failCounts[serviceName]; exists {
		if _, fcExists := queryNameMap[queryName]; fcExists {
			m.failCounts[serviceName][queryName] = 0
		}
	}
}

// New initializes and returns the MetricCollector struct.
func New(ctx context.Context, config *configpb.Configuration) (*MetricCollector, error) {
	gceService, err := gce.NewGCEClient(ctx)
	if err != nil {
		usagemetrics.Error(usagemetrics.GCEServiceCreationFailure)
		return nil, fmt.Errorf("initializing GCE services: %w", err)
	}

	metricClient, err := monitoring.NewMetricClient(ctx)
	if err != nil {
		usagemetrics.Error(usagemetrics.MetricClientCreationFailure)
		return nil, fmt.Errorf("creating metric service client: %w", err)
	}

	conParams, err := readConnectionParameters(ctx, gceService, config)
	if err != nil {
		usagemetrics.Error(usagemetrics.ConnectionParametersReadFailure)
		return nil, fmt.Errorf("fetching secret data from Secret Manager: %w", err)
	}
	log.CtxLogger(ctx).Debugw("Successfully read connection parameters", "connection_parameters", conParams)

	dbManager := &dbManager{
		connectionParameters: conParams,
		connections:          make(map[string]*sql.DB),
		failCounts:           make(map[string]map[string]int),
		skipMsgLogged:        make(map[string]map[string]bool),
		nextRetry:            make(map[string]time.Time),
		baseDelaySeconds:     config.GetOracleConfiguration().GetOracleMetrics().GetCollectionFrequency().GetSeconds(),
	}

	wp := workerpool.New(int(config.GetOracleConfiguration().GetOracleMetrics().GetMaxExecutionThreads()))

	return &MetricCollector{
		Config:            config,
		TimeSeriesCreator: metricClient,
		BackOffs:          cloudmonitoring.NewDefaultBackOffIntervals(),
		GCEService:        gceService,
		dbManager:         dbManager,
		wp:                wp,
		startTime:         tspb.Now(),
	}, nil
}

// RefreshDBConnections refreshes the database connections based on the given discovery proto.
func (c *MetricCollector) RefreshDBConnections(ctx context.Context, discovery *odpb.Discovery) {
	discoveredSIDs, err := ExtractSIDs(discovery)
	if err != nil {
		log.CtxLogger(ctx).Errorw("Failed to extract SIDs from the discovery data", "error", err, "discovery", discovery)
		return
	}

	if len(discoveredSIDs) == 0 {
		log.CtxLogger(ctx).Warn("Cannot refresh database connections, no SIDs found")
		return
	}

	c.dbManager.populateConnectionParameters(ctx, discovery)
	c.dbManager.closeConnections(ctx)
	c.dbManager.openConnections(ctx)
}

// CollectDBMetricsOnce submits a task for each database connection to the worker pool.
// Each task executes a query from default_queries.json and sends the results as metrics to Cloud Monitoring.
func (c *MetricCollector) CollectDBMetricsOnce(ctx context.Context) {
	if len(c.dbManager.connections) == 0 {
		// No connections, return and wait for the next collection cycle.
		return
	}

	cfg := c.Config.GetOracleConfiguration().GetOracleMetrics()

	queryNamesMap := queryMap(cfg.GetQueries())
	var queryNames []string
	for qn := range queryNamesMap {
		queryNames = append(queryNames, qn)
	}

	for serviceName, db := range c.dbManager.connections {
		if c.dbManager.shouldSkip(ctx, serviceName, db, time.Now()) {
			continue
		}
		dbInfo, err := fetchDatabaseInfo(ctx, db)
		if err != nil {
			log.CtxLogger(ctx).Errorw("Failed to fetch database information from v$database view", "error", err, "service_name", serviceName)
			continue
		}

		// Submit a task for each query to the workerpool.
		for _, qn := range queryNames {
			query, ok := queryNamesMap[qn]
			if !ok {
				log.CtxLogger(ctx).Warnw("Query not found", "query_name", qn)
				continue
			}
			if c.dbManager.failCountFor(serviceName, qn) >= maxQueryFailures {
				if !c.dbManager.skipMsgLogged[serviceName][qn] {
					log.CtxLogger(ctx).Warnw("Skipping query due to 3 consecutive failures", "query_name", qn)
					c.dbManager.skipMsgLogged[serviceName] = map[string]bool{qn: true}
				}
				continue
			}
			if !shouldRunQuery(query, dbInfo.databaseRole) {
				continue
			}
			c.wp.Submit(func() {
				executeQueryAndSendMetrics(ctx, queryOptions{
					db:            db,
					query:         query,
					timeout:       cfg.GetQueryTimeout().GetSeconds(),
					collector:     c,
					runningSum:    make(map[timeSeriesKey]prevVal),
					serviceName:   serviceName,
					defaultLabels: map[string]string{"dbid": dbInfo.dbID, "db_unique_name": dbInfo.dbUniqueName},
				})
			})
		}
	}
}

// Stop gracefully stops the metric collection process.
func (c *MetricCollector) Stop(ctx context.Context) {
	c.wp.Stop() // Ensure all current tasks are completed
	c.dbManager.closeConnections(ctx)
}

// shouldSkip determines if query execution should be skipped for the given service name.
func (m *dbManager) shouldSkip(ctx context.Context, serviceName string, db dbPinger, now time.Time) bool {
	// Skip if we've reached the maximum allowed login failures in the default profile.
	loginFailures := m.failCountFor(serviceName, "login")
	if loginFailures >= maxLoginFailures {
		if !m.skipMsgLogged[serviceName]["max_login_failures"] {
			log.CtxLogger(ctx).Warnw("Reached maximum allowed login failures in the default profile. No further attempts to avoid lockout", "service_name", serviceName, "failed_login_attempts", loginFailures)
			m.skipMsgLogged[serviceName] = map[string]bool{"max_login_failures": true}
		}
		return true
	}
	// Skip if it's not time to retry yet
	if now.Before(m.nextRetry[serviceName]) {
		return true
	}
	if err := db.PingContext(ctx); err != nil {
		// for invalid username/password failures, retry with an exponential backoff
		// ORA-01017 is the error code for invalid username/password.
		if strings.Contains(err.Error(), "ORA-01017") {
			m.incrementFailCount(serviceName, "login")
			loginFailures := m.failCountFor(serviceName, "login")
			m.nextRetry[serviceName] = now.Add(m.delay(loginFailures))
			log.CtxLogger(ctx).Warnw("Invalid username/password detected; will retry to connect later", "service_name", serviceName, "failed_login_attempts", loginFailures, "next_retry_after", m.nextRetry[serviceName].Format("2006-01-02T15:04:05"))
		} else {
			// for all other errors, log them once per service name
			if !m.skipMsgLogged[serviceName]["other_failures"] {
				log.CtxLogger(ctx).Warnw("Failed to connect to the database", "error", err, "service_name", serviceName)
				m.skipMsgLogged[serviceName] = map[string]bool{"other_failures": true}
			}
		}
		return true
	}
	// Reset all failure counts if ping was successful
	m.resetFailCount(serviceName, "login")
	m.skipMsgLogged[serviceName] = map[string]bool{"other_failures": false, "max_login_failures": false}
	return false
}

// delay returns the delay duration based on the number of login failures.
// This code calculates a login delay that increases exponentially with the number of failed attempts.
// It starts with a baseDelaySeconds and multiplies it by the number of login failures raised to the power of 4.
func (m *dbManager) delay(loginFailures int) time.Duration {
	return time.Duration(float64(m.baseDelaySeconds)*math.Pow(float64(loginFailures), 4)) * time.Second
}

// executeQueryAndSendMetrics() executes the SQL query, packages the results into time series,
// and sends them as metrics to Cloud Monitoring.
func executeQueryAndSendMetrics(ctx context.Context, opts queryOptions) {
	queryName := opts.query.GetName()
	ctxTimeout, cancel := context.WithTimeout(ctx, time.Second*time.Duration(opts.timeout))
	defer cancel()

	cols := createColumns(opts.query.GetColumns())
	if cols == nil {
		log.CtxLogger(ctx).Errorw("No columns specified for query", "query_name", queryName)
		opts.collector.dbManager.incrementFailCount(opts.serviceName, queryName)
		return
	}

	// TODO:  Evaluate adding a backoff mechanism for retrying database queries.
	rows, err := opts.db.QueryContext(ctxTimeout, opts.query.GetSql())
	if err != nil {
		log.CtxLogger(ctx).Errorw("Failed to execute query", "query_name", queryName, "error", err)
		opts.collector.dbManager.incrementFailCount(opts.serviceName, queryName)
		return
	}

	var metrics []*mrpb.TimeSeries
	for rows.Next() {
		if err := rows.Scan(cols...); err != nil {
			log.CtxLogger(ctx).Errorw("Failed to scan row", "query_name", queryName, "error", err)
			opts.collector.dbManager.incrementFailCount(opts.serviceName, queryName)
			return
		}
		metrics = append(metrics, createMetricsForRow(ctx, opts, cols, opts.defaultLabels)...)
	}
	sent, batchCount, err := cloudmonitoring.SendTimeSeries(ctxTimeout, metrics, opts.collector.TimeSeriesCreator, opts.collector.BackOffs, opts.collector.Config.GetCloudProperties().GetProjectId())

	if err != nil {
		opts.collector.dbManager.incrementFailCount(opts.serviceName, queryName)
		log.CtxLogger(ctx).Errorw("Failed to query database and send metrics to Cloud Monitoring", "query_name", queryName, "service_name", opts.serviceName, "fail_count", opts.collector.dbManager.failCountFor(opts.serviceName, queryName), "error", err)
		usagemetrics.Error(usagemetrics.OracleMetricCollectionFailure)
		return
	}
	opts.collector.dbManager.resetFailCount(opts.serviceName, queryName)
	log.CtxLogger(ctx).Debugw("Successfully queried database and sent metrics to Cloud Monitoring", "query_name", queryName, "service_name", opts.serviceName, "sent", sent, "batches", batchCount)
}

// createMetricsForRow creates metrics for each row returned by the query.
func createMetricsForRow(ctx context.Context, opts queryOptions, cols []any, defaultLabels map[string]string) []*mrpb.TimeSeries {
	labels := createLabels(opts.query, cols)

	// Merge defaultLabels into labels, overwriting existing values.
	maps.Copy(labels, defaultLabels)

	var metrics []*mrpb.TimeSeries
	for i, c := range opts.query.GetColumns() {
		switch c.GetMetricType() {
		case configpb.MetricType_METRIC_GAUGE:
			if metric, ok := createGaugeMetric(c, cols[i], labels, opts, tspb.Now()); ok {
				metrics = append(metrics, metric)
			}
		case configpb.MetricType_METRIC_CUMULATIVE:
			if metric, ok := createCumulativeMetric(ctx, c, cols[i], labels, opts, tspb.Now()); ok {
				metrics = append(metrics, metric)
			}
		case configpb.MetricType_METRIC_LABEL:
			// Labels are handled by createLabels()
		default:
			log.CtxLogger(ctx).Warnw("Unsupported metric type", "metric_type", c.GetMetricType())
		}
	}
	return metrics
}

// fetchDatabaseInfo executes the dbViewQuery query to fetch database info.
func fetchDatabaseInfo(ctx context.Context, db *sql.DB) (*databaseInfo, error) {
	var dbid, dbUniqueName, databaseRole string
	err := db.QueryRowContext(ctx, dbViewQuery).Scan(&dbid, &dbUniqueName, &databaseRole)
	if err != nil {
		log.CtxLogger(ctx).Errorw("Failed to parse query results", "query", dbViewQuery, "error", err)
		return nil, fmt.Errorf("parsing query results: %w", err)
	}
	return &databaseInfo{
		dbID:         dbid,
		dbUniqueName: dbUniqueName,
		databaseRole: databaseRole,
	}, nil
}

// createLabels creates a map of metric labels for the given query.
func createLabels(query *configpb.Query, cols []any) map[string]string {
	labels := map[string]string{}
	for i, c := range query.GetColumns() {
		if c.GetMetricType() == configpb.MetricType_METRIC_LABEL {
			// String type is enforced by the config validator for METRIC_LABEL.
			// Type asserting to a pointer due to the coupling with sql.Rows.Scan() populating the columns as such.
			if result, ok := cols[i].(*string); ok {
				labels[c.GetName()] = *result
			}
		}
	}
	return labels
}

// createColumns creates pointers to the types defined in the configuration for each column in a query.
func createColumns(queryColumns []*configpb.Column) []any {
	if len(queryColumns) == 0 {
		return nil
	}
	cols := make([]any, len(queryColumns))
	for i, c := range queryColumns {
		var col any
		switch c.GetValueType() {
		case configpb.ValueType_VALUE_INT64:
			col = new(int64)
		case configpb.ValueType_VALUE_DOUBLE:
			col = new(float64)
		case configpb.ValueType_VALUE_BOOL:
			col = new(bool)
		case configpb.ValueType_VALUE_STRING:
			col = new(string)
		default:
			// Rows.Scan() is able to populate any cell as *interface{} by
			// "copying the value provided by the underlying driver without conversion".
			// Reference: https://pkg.go.dev/database/sql#Rows.Scan
			col = new(any)
		}
		cols[i] = col
	}
	return cols
}

// createGaugeMetric builds a cloud monitoring time series with a boolean, int, or float point value for the specified column.
func createGaugeMetric(c *configpb.Column, val any, labels map[string]string, opts queryOptions, timestamp *tspb.Timestamp) (*mrpb.TimeSeries, bool) {
	metricPath := metricURL + "/" + opts.query.GetName() + "/" + c.GetName()
	if c.GetNameOverride() != "" {
		metricPath = metricURL + "/" + c.GetNameOverride()
	}

	ts := timeseries.Params{
		CloudProp:    convertCloudProperties(opts.collector.Config.GetCloudProperties()),
		MetricType:   metricPath,
		MetricLabels: labels,
		Timestamp:    timestamp,
	}

	// Type asserting to pointers due to the coupling with sql.Rows.Scan() populating the columns as such.
	switch c.GetValueType() {
	case configpb.ValueType_VALUE_INT64:
		if result, ok := val.(*int64); ok {
			ts.Int64Value = *result
		}
		return timeseries.BuildInt(ts), true
	case configpb.ValueType_VALUE_DOUBLE:
		if result, ok := val.(*float64); ok {
			ts.Float64Value = *result
		}
		return timeseries.BuildFloat64(ts), true
	case configpb.ValueType_VALUE_BOOL:
		if result, ok := val.(*bool); ok {
			ts.BoolValue = *result
		}
		return timeseries.BuildBool(ts), true
	default:
		return nil, false
	}
}

// createCumulativeMetric builds a cloudmonitoring timeseries with an int or float point value for
// the specified column. It returns (nil, false) when it is unable to build the timeseries.
func createCumulativeMetric(ctx context.Context, c *configpb.Column, val any, labels map[string]string, opts queryOptions, endTime *tspb.Timestamp) (*mrpb.TimeSeries, bool) {
	metricPath := metricURL + "/" + opts.query.GetName() + "/" + c.GetName()
	if c.GetNameOverride() != "" {
		metricPath = metricURL + "/" + c.GetNameOverride()
	}

	ts := timeseries.Params{
		CloudProp:    convertCloudProperties(opts.collector.Config.GetCloudProperties()),
		MetricType:   metricPath,
		MetricLabels: labels,
		Timestamp:    endTime,
		StartTime:    opts.collector.startTime,
		MetricKind:   mpb.MetricDescriptor_CUMULATIVE,
	}
	tsKey := prepareKey(metricPath, ts.MetricKind.String(), labels)

	// Type asserting to pointers due to the coupling with sql.Rows.Scan() populating the columns as such.
	switch c.GetValueType() {
	case configpb.ValueType_VALUE_INT64:
		if result, ok := val.(*int64); ok {
			ts.Int64Value = *result
		}
		if lastVal, ok := opts.runningSum[tsKey]; ok {
			log.CtxLogger(ctx).Debugw("Found already existing key.", "Key", tsKey, "prevVal", lastVal)
			ts.Int64Value = ts.Int64Value + lastVal.val.(int64)
			ts.StartTime = lastVal.startTime
		}
		opts.runningSum[tsKey] = prevVal{val: ts.Int64Value, startTime: ts.StartTime}
		return timeseries.BuildInt(ts), true
	case configpb.ValueType_VALUE_DOUBLE:
		if result, ok := val.(*float64); ok {
			ts.Float64Value = *result
		}
		if lastVal, ok := opts.runningSum[tsKey]; ok {
			log.CtxLogger(ctx).Debugw("Found already existing key.", "Key", tsKey, "prevVal", lastVal)
			ts.Float64Value = ts.Float64Value + lastVal.val.(float64)
			ts.StartTime = lastVal.startTime
		}
		opts.runningSum[tsKey] = prevVal{val: ts.Float64Value, startTime: ts.StartTime}
		return timeseries.BuildFloat64(ts), true
	default:
		return nil, false
	}
}

func readConnectionParameters(ctx context.Context, gceService gceInterface, config *configpb.Configuration) ([]connectionParameters, error) {
	params := config.GetOracleConfiguration().GetOracleMetrics().GetConnectionParameters()
	var result []connectionParameters

	for _, p := range params {
		secretData, err := gceService.GetSecret(ctx, p.GetSecret().GetProjectId(), p.GetSecret().GetSecretName())
		if err != nil {
			return nil, fmt.Errorf("retrieving secret from Secret Manager for connection parameters %v: %w", p, err)
		}
		result = append(result, connectionParameters{
			Username:    p.GetUsername(),
			Password:    secret.String(strings.TrimSuffix(secretData, "\n")),
			ServiceName: p.GetServiceName(),
			Host:        p.GetHost(),
			Port:        int(p.GetPort()),
		})
	}

	return result, nil
}

// prepareKey creates the key which can be used to group a timeseries
// based on MetricType, MetricKind and MetricLabels.
func prepareKey(mtype, mkind string, labels map[string]string) timeSeriesKey {
	tsk := timeSeriesKey{
		MetricType: mtype,
		MetricKind: mkind,
	}
	var metricLabels []string
	for k, v := range labels {
		metricLabels = append(metricLabels, k+":"+v)
	}
	sort.Strings(metricLabels)
	tsk.MetricLabels = strings.Join(metricLabels, ",")
	return tsk
}

// queryMap returns a map of query names to query objects.
func queryMap(queries []*configpb.Query) map[string]*configpb.Query {
	res := make(map[string]*configpb.Query)
	for _, q := range queries {
		res[q.GetName()] = q
	}
	return res
}

// convertCloudProperties converts Cloud Properties proto to CloudProperties struct.
func convertCloudProperties(cp *configpb.CloudProperties) *timeseries.CloudProperties {
	return &timeseries.CloudProperties{
		ProjectID:        cp.GetProjectId(),
		InstanceID:       cp.GetInstanceId(),
		Zone:             cp.GetZone(),
		InstanceName:     cp.GetInstanceName(),
		Image:            cp.GetImage(),
		NumericProjectID: cp.GetNumericProjectId(),
		Region:           cp.GetRegion(),
	}
}

// ExtractSIDs extracts the SIDs from the given discovery proto.
func ExtractSIDs(discovery *odpb.Discovery) ([]string, error) {
	var sids []string
	for _, db := range discovery.GetDatabases() {
		switch v := db.TenancyType.(type) {
		case *odpb.Discovery_DatabaseRoot_Db:
			for _, instance := range v.Db.GetInstances() {
				sids = append(sids, instance.GetOracleSid())
			}
		case *odpb.Discovery_DatabaseRoot_Cdb:
			for _, instance := range v.Cdb.Root.GetInstances() {
				// In a multitenant environment, all PDBs share the SID of the CDB$ROOT.
				// PDBs are identified by their unique service names rather than SIDs, so we don't need to
				// extract their SIDs.
				sids = append(sids, instance.GetOracleSid())
			}
		default:
			return nil, fmt.Errorf("unknown database tenancy type: %T for database %v", v, db)
		}
	}
	return sids, nil
}

// shouldRunQuery determines whether the query should run on a given database role.
func shouldRunQuery(query *configpb.Query, role string) bool {
	if query.GetDisabled() {
		return false
	}
	switch query.GetDatabaseRole() {
	case configpb.Query_PRIMARY:
		return role == "PRIMARY"
	case configpb.Query_STANDBY:
		return strings.Contains(role, "STANDBY")
	case configpb.Query_BOTH:
		return true
	default:
		return false
	}
}
