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
	"sort"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/monitoring/apiv3"
	"github.com/sijms/go-ora"

	"golang.org/x/exp/maps"
	"github.com/gammazero/workerpool"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/cloudmonitoring"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce/metadataserver"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/recovery"
	"github.com/GoogleCloudPlatform/workloadagent/internal/secret"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/timeseries"

	mpb "google.golang.org/genproto/googleapis/api/metric"
	mrespb "google.golang.org/genproto/googleapis/api/monitoredres"
	cpb "google.golang.org/genproto/googleapis/monitoring/v3"
	mrpb "google.golang.org/genproto/googleapis/monitoring/v3"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

const (
	metricURL        = "workload.googleapis.com/oracle"
	maxQueryFailures = 3
	dbViewQuery      = `SELECT d.dbid, d.db_unique_name, d.database_role, p.name AS pdb_name FROM v$database d LEFT JOIN v$pdbs p ON 1=1`
	healthQuery      = `SELECT 1 FROM dual`
)

const (
	// Healthy is the healthy status of the database.
	Healthy HealthStatus = "Healthy"
	// Unhealthy is the unhealthy status of the database.
	Unhealthy HealthStatus = "Unhealthy"
	// Unknown is the unknown status of the database.
	Unknown HealthStatus = "Unknown"
)

type (
	gceInterface interface {
		GetSecret(ctx context.Context, projectID, secretName string) (string, error)
	}

	dbPinger interface {
		PingContext(ctx context.Context) error
	}

	// HealthStatus is the health status of a database.
	HealthStatus string

	// ServiceHealth holds the health information for a specific service.
	ServiceHealth struct {
		Status      HealthStatus
		LastChecked time.Time
		Message     string // Optional message (e.g., reason for unhealthy status)
	}

	// MetricCollector holds the parameters required for metric collection.
	MetricCollector struct {
		Config                  *configpb.Configuration
		TimeSeriesCreator       cloudmonitoring.TimeSeriesCreator
		BackOffs                *cloudmonitoring.BackOffIntervals
		GCEService              gceInterface
		createWorkerPoolRoutine *recovery.RecoverableRoutine
		startTime               *tspb.Timestamp
		mu                      sync.Mutex
		connections             map[string]*sql.DB
		failCount               map[string]int
		skipMsgLogged           map[string]bool
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
		DBID         string
		DBUniqueName string
		DatabaseRole string
		PdbName      string
	}
)

// openConnections connects to all the databases specified in the connection parameters.
func openConnections(ctx context.Context, conParams []connectionParameters) map[string]*sql.DB {
	connections := make(map[string]*sql.DB)
	for _, params := range conParams {
		urlOptions := map[string]string{
			"dba privilege": "sysdg", // sysdg system privilege is required to connect to a closed standby.
		}
		connStr := go_ora.BuildUrl(params.Host, params.Port, params.ServiceName, params.Username, params.Password.SecretValue(), urlOptions)
		conn, err := sql.Open("oracle", connStr)
		if err != nil {
			log.CtxLogger(ctx).Errorw("Failed to open database connection", "error", err, "connection_parameters", params)
			usagemetrics.Error(usagemetrics.OracleConnectionFailure)
			continue
		}
		if err := conn.PingContext(ctx); err != nil {
			log.CtxLogger(ctx).Errorw("Failed to ping the database", "error", err, "connection_parameters", params)
			usagemetrics.Error(usagemetrics.OraclePingFailure)
			continue
		}
		connections[params.ServiceName] = conn
	}
	return connections
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

	return &MetricCollector{
		connections:       openConnections(ctx, conParams),
		Config:            config,
		TimeSeriesCreator: metricClient,
		BackOffs:          cloudmonitoring.NewDefaultBackOffIntervals(),
		GCEService:        gceService,
		startTime:         tspb.Now(),
		failCount:         make(map[string]int),
		skipMsgLogged:     make(map[string]bool),
	}, nil
}

// createHealthMetrics creates health metrics for all the databases.
func createHealthMetrics(ctx context.Context, statusData map[string]*ServiceHealth, cfg *configpb.Configuration) []*mrpb.TimeSeries {
	timeSeries := []*mrpb.TimeSeries{}
	for serviceName, serviceHealth := range statusData {
		ts := &mrpb.TimeSeries{
			Metric: &mpb.Metric{
				Type:   metricURL + "/health",
				Labels: map[string]string{"service_name": serviceName},
			},
			Resource: &mrespb.MonitoredResource{
				Type: "gce_instance",
				Labels: map[string]string{
					"project_id":  cfg.GetCloudProperties().GetProjectId(),
					"instance_id": cfg.GetCloudProperties().GetInstanceId(),
					"zone":        cfg.GetCloudProperties().GetZone(),
				},
			},
			Points: []*mrpb.Point{
				{
					Interval: &cpb.TimeInterval{
						EndTime: tspb.New(serviceHealth.LastChecked),
					},
					Value: &cpb.TypedValue{
						Value: &cpb.TypedValue_BoolValue{
							BoolValue: serviceHealth.Status == Healthy,
						},
					},
				},
			},
		}
		timeSeries = append(timeSeries, ts)
	}
	return timeSeries
}

func checkDatabaseHealth(ctx context.Context, connections map[string]*sql.DB) map[string]*ServiceHealth {
	statusData := make(map[string]*ServiceHealth)

	for serviceName, db := range connections {
		rows, err := db.QueryContext(ctx, healthQuery)
		if err != nil {
			statusData[serviceName] = &ServiceHealth{
				Status:      Unhealthy,
				LastChecked: time.Now(),
				Message:     fmt.Sprintf("query execution failed: %v", err.Error()),
			}
			continue
		}
		defer rows.Close()

		var result int
		for rows.Next() {
			if err := rows.Scan(&result); err != nil {
				statusData[serviceName] = &ServiceHealth{
					Status:      Unhealthy,
					LastChecked: time.Now(),
					Message:     fmt.Sprintf("query returned no rows: %v", err.Error()),
				}
			} else {
				statusData[serviceName] = &ServiceHealth{
					Status:      Healthy,
					LastChecked: time.Now(),
				}
			}
		}
	}
	return statusData
}

// SendHealthMetricsToCloudMonitoring sends health metrics to Cloud Monitoring.
func (c *MetricCollector) SendHealthMetricsToCloudMonitoring(ctx context.Context) []*mrpb.TimeSeries {
	statusData := checkDatabaseHealth(ctx, c.connections)
	ts := createHealthMetrics(ctx, statusData, c.Config)

	sent, batchCount, err := cloudmonitoring.SendTimeSeries(ctx, ts, c.TimeSeriesCreator, c.BackOffs, c.Config.GetCloudProperties().GetProjectId())
	if err != nil {
		log.CtxLogger(ctx).Errorw("Failed to send health metrics to Cloud Monitoring", "error", err)
		usagemetrics.Error(usagemetrics.OracleMetricCollectionFailure)
		return nil
	}
	log.CtxLogger(ctx).Debugw("Successfully sent health metrics to Cloud Monitoring", "sent", sent, "batches", batchCount)
	return ts
}

func (c *MetricCollector) shouldSkipQuery(ctx context.Context, serviceName, qn string) bool {
	key := fmt.Sprintf("%s:%s", serviceName, qn)
	if c.failCount[key] >= maxQueryFailures {
		if !c.skipMsgLogged[key] {
			c.skipMsgLogged[key] = true
			log.CtxLogger(ctx).Warnw("Skipping query due to 3 consecutive failures", "service_name", serviceName, "query_name", qn)
		}
		return true
	}
	return false
}

// SendDefaultMetricsToCloudMonitoring submits a task for each database connection to the worker pool.
// Each task executes a query from default_queries.json and sends the results as metrics to Cloud Monitoring.
func (c *MetricCollector) SendDefaultMetricsToCloudMonitoring(ctx context.Context) []*mrpb.TimeSeries {
	queryNamesMap := queryMap(c.Config.GetOracleConfiguration().GetOracleMetrics().GetQueries())
	var queryNames []string
	for qn := range queryNamesMap {
		queryNames = append(queryNames, qn)
	}

	timeseries := []*mrpb.TimeSeries{}

	maxExecutionThreads := int(c.Config.GetOracleConfiguration().GetOracleMetrics().GetMaxExecutionThreads())
	wp := workerpool.New(maxExecutionThreads)

	for serviceName, db := range c.connections {
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
			if c.shouldSkipQuery(ctx, serviceName, qn) {
				continue
			}
			if !isQueryAllowedForRole(query, dbInfo.DatabaseRole) {
				continue
			}
			wp.Submit(func() {
				ts := executeQueryAndSendMetrics(ctx, queryOptions{
					db:            db,
					query:         query,
					timeout:       c.Config.GetOracleConfiguration().GetOracleMetrics().GetQueryTimeout().GetSeconds(),
					collector:     c,
					runningSum:    make(map[timeSeriesKey]prevVal),
					serviceName:   serviceName,
					defaultLabels: map[string]string{"dbid": dbInfo.DBID, "db_unique_name": dbInfo.DBUniqueName, "pdb_name": dbInfo.PdbName},
				})
				c.mu.Lock()
				timeseries = append(timeseries, ts...)
				c.mu.Unlock()
			})
		}
	}
	wp.StopWait()
	return timeseries
}

// executeQueryAndSendMetrics() executes the SQL query, packages the results into time series,
// and sends them as metrics to Cloud Monitoring.
func executeQueryAndSendMetrics(ctx context.Context, opts queryOptions) []*mrpb.TimeSeries {
	queryName := opts.query.GetName()
	ctxTimeout, cancel := context.WithTimeout(ctx, time.Second*time.Duration(opts.timeout))
	defer cancel()

	cols := createColumns(opts.query.GetColumns())
	if cols == nil {
		log.CtxLogger(ctx).Errorw("No columns specified for query", "query_name", queryName)
		opts.collector.failCount[fmt.Sprintf("%s:%s", opts.serviceName, queryName)]++
		return nil
	}

	// TODO:  Evaluate adding a backoff mechanism for retrying database queries.
	rows, err := opts.db.QueryContext(ctxTimeout, opts.query.GetSql())
	if err != nil {
		log.CtxLogger(ctx).Errorw("Failed to execute query", "query_name", queryName, "error", err)
		opts.collector.failCount[fmt.Sprintf("%s:%s", opts.serviceName, queryName)]++
		return nil
	}

	var ts []*mrpb.TimeSeries
	for rows.Next() {
		if err := rows.Scan(cols...); err != nil {
			log.CtxLogger(ctx).Errorw("Failed to scan row", "query_name", queryName, "error", err)
			opts.collector.failCount[fmt.Sprintf("%s:%s", opts.serviceName, queryName)]++
			return nil
		}
		ts = append(ts, createMetricsForRow(ctx, opts, cols, opts.defaultLabels)...)
	}
	sent, batchCount, err := cloudmonitoring.SendTimeSeries(ctxTimeout, ts, opts.collector.TimeSeriesCreator, opts.collector.BackOffs, opts.collector.Config.GetCloudProperties().GetProjectId())

	if err != nil {
		opts.collector.failCount[fmt.Sprintf("%s:%s", opts.serviceName, queryName)]++
		failCount := opts.collector.failCount[fmt.Sprintf("%s:%s", opts.serviceName, queryName)]
		log.CtxLogger(ctx).Errorw("Failed to query database and send metrics to Cloud Monitoring", "query_name", queryName, "service_name", opts.serviceName, "fail_count", failCount, "error", err)
		usagemetrics.Error(usagemetrics.OracleMetricCollectionFailure)
		return nil
	}
	delete(opts.collector.failCount, fmt.Sprintf("%s:%s", opts.serviceName, queryName))
	log.CtxLogger(ctx).Debugw("Successfully queried database and sent metrics to Cloud Monitoring", "query_name", queryName, "service_name", opts.serviceName, "sent", sent, "batches", batchCount)
	return ts
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
	rows, err := db.QueryContext(ctx, dbViewQuery)
	if err != nil {
		return nil, fmt.Errorf("executing %s query: %w", dbViewQuery, err)
	}
	defer rows.Close()

	var dbid, dbUniqueName, databaseRole string
	var pdbName sql.NullString

	for rows.Next() {
		if err := rows.Scan(&dbid, &dbUniqueName, &databaseRole, &pdbName); err != nil {
			return nil, fmt.Errorf("scanning row from v$database and v$pdb views: %w", err)
		}
	}
	return &databaseInfo{
		DBID:         dbid,
		DBUniqueName: dbUniqueName,
		DatabaseRole: databaseRole,
		PdbName:      pdbName.String,
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
func convertCloudProperties(cp *configpb.CloudProperties) *metadataserver.CloudProperties {
	return &metadataserver.CloudProperties{
		ProjectID:        cp.GetProjectId(),
		InstanceID:       cp.GetInstanceId(),
		Zone:             cp.GetZone(),
		InstanceName:     cp.GetInstanceName(),
		Image:            cp.GetImage(),
		NumericProjectID: cp.GetNumericProjectId(),
		Region:           cp.GetRegion(),
	}
}

// isQueryAllowedForRole determines if the query is allowed to run for a given database role.
func isQueryAllowedForRole(query *configpb.Query, role string) bool {
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
