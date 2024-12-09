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

package oraclemetrics

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	// database/sql driver for sqlite
	_ "github.com/mattn/go-sqlite3"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"github.com/GoogleCloudPlatform/workloadagent/internal/secret"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/cloudmonitoring"
	cmfake "github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/cloudmonitoring/fake"
	gcefake "github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/gce/fake"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/gce/metadataserver"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/log"

	mpb "google.golang.org/genproto/googleapis/api/metric"
	mrespb "google.golang.org/genproto/googleapis/api/monitoredres"
	cpb "google.golang.org/genproto/googleapis/monitoring/v3"
	mrpb "google.golang.org/genproto/googleapis/monitoring/v3"
	dpb "google.golang.org/protobuf/types/known/durationpb"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

var (
	defaultCollector = MetricCollector{
		Config: &configpb.Configuration{
			CloudProperties: &configpb.CloudProperties{
				ProjectId:  "test-project",
				Zone:       "test-zone",
				InstanceId: "123456",
			},
			OracleConfiguration: &configpb.OracleConfiguration{
				Enabled: proto.Bool(true),
				OracleMetrics: &configpb.OracleMetrics{
					Enabled: proto.Bool(true),
				},
			},
		},
		GCEService: &gcefake.TestGCE{
			GetSecretResp: []string{"fakePassword"},
			GetSecretErr:  []error{nil},
		},
		BackOffs:  cloudmonitoring.NewBackOffIntervals(time.Millisecond, time.Millisecond),
		startTime: &tspb.Timestamp{Seconds: 0},
	}
	defaultTimestamp = &tspb.Timestamp{Seconds: 123}
	defaultQuery     = &configpb.Query{
		Columns: []*configpb.Column{
			{},
		},
	}
)

func TestMain(t *testing.M) {
	log.SetupLoggingForTest()
	os.Exit(t.Run())
}

func newTimeSeriesKey(metricType, metricLabels string) timeSeriesKey {
	tsk := timeSeriesKey{
		MetricKind:   mpb.MetricDescriptor_CUMULATIVE.String(),
		MetricType:   metricType,
		MetricLabels: metricLabels,
	}
	return tsk
}

func newDefaultMetrics() *mrpb.TimeSeries {
	return &mrpb.TimeSeries{
		MetricKind: mpb.MetricDescriptor_GAUGE,
		Resource: &mrespb.MonitoredResource{
			Type: "gce_instance",
			Labels: map[string]string{
				"project_id":  "test-project",
				"zone":        "test-zone",
				"instance_id": "123456",
			},
		},
		Points: []*mrpb.Point{
			{
				Interval: &cpb.TimeInterval{
					StartTime: tspb.New(time.Unix(123, 0)),
					EndTime:   tspb.New(time.Unix(123, 0)),
				},
				Value: &cpb.TypedValue{},
			},
		},
	}
}

func newDefaultCumulativeMetric(st, et int64) *mrpb.TimeSeries {
	return &mrpb.TimeSeries{
		MetricKind: mpb.MetricDescriptor_CUMULATIVE,
		Resource: &mrespb.MonitoredResource{
			Type: "gce_instance",
			Labels: map[string]string{
				"project_id":  "test-project",
				"zone":        "test-zone",
				"instance_id": "123456",
			},
		},
		Points: []*mrpb.Point{
			{
				Interval: &cpb.TimeInterval{
					StartTime: tspb.New(time.Unix(st, 0)),
					EndTime:   tspb.New(time.Unix(et, 0)),
				},
				Value: &cpb.TypedValue{},
			},
		},
	}
}

func TestReadConnectionParameters(t *testing.T) {
	tests := []struct {
		name       string
		want       []connectionParameters
		wantErr    bool
		config     *configpb.Configuration
		gceService gceInterface
	}{
		{
			name: "Success",
			gceService: &gcefake.TestGCE{
				GetSecretResp: []string{"password1", "password2"},
				GetSecretErr:  []error{nil, nil},
			},
			want: []connectionParameters{
				{Username: "user1", Password: secret.String("password1"), ServiceName: "orcl1"},
				{Username: "user2", Password: secret.String("password2"), ServiceName: "orcl2"},
			},
			config: &configpb.Configuration{
				OracleConfiguration: &configpb.OracleConfiguration{
					Enabled: proto.Bool(true),
					OracleMetrics: &configpb.OracleMetrics{
						Enabled: proto.Bool(true),
						ConnectionParameters: []*configpb.ConnectionParameters{
							{
								Username: "user1",
								Secret: &configpb.SecretRef{
									ProjectId:  "my-project1",
									SecretName: "my-secret1",
								},
								ServiceName: "orcl1",
							},
							{
								Username: "user2",
								Secret: &configpb.SecretRef{
									ProjectId:  "my-project2",
									SecretName: "my-secret2",
								},
								ServiceName: "orcl2",
							},
						},
					},
				},
			},
		},
		{
			name: "Strip trailing newline character",
			gceService: &gcefake.TestGCE{
				GetSecretResp: []string{"password1\n"},
				GetSecretErr:  []error{nil, nil},
			},
			want: []connectionParameters{
				{Username: "user1", Password: secret.String("password1"), ServiceName: "orcl1"},
			},
			config: &configpb.Configuration{
				OracleConfiguration: &configpb.OracleConfiguration{
					Enabled: proto.Bool(true),
					OracleMetrics: &configpb.OracleMetrics{
						Enabled: proto.Bool(true),
						ConnectionParameters: []*configpb.ConnectionParameters{
							{
								Username: "user1",
								Secret: &configpb.SecretRef{
									ProjectId:  "my-project1",
									SecretName: "my-secret1",
								},
								ServiceName: "orcl1",
							},
						},
					},
				},
			},
		},
		{
			name: "SecretNotFound",
			gceService: &gcefake.TestGCE{
				GetSecretResp: []string{"password"},
				GetSecretErr:  []error{errors.New("secret not found")},
			},
			want:    nil,
			wantErr: true,
			config: &configpb.Configuration{
				OracleConfiguration: &configpb.OracleConfiguration{
					Enabled: proto.Bool(true),
					OracleMetrics: &configpb.OracleMetrics{
						Enabled: proto.Bool(true),
						ConnectionParameters: []*configpb.ConnectionParameters{
							{
								Username: "user",
								Secret: &configpb.SecretRef{
									ProjectId:  "my-project",
									SecretName: "my-secret",
								},
								ServiceName: "orcl",
							},
						},
					},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := readConnectionParameters(context.Background(), tc.gceService, tc.config)
			if err != nil && !tc.wantErr {
				t.Errorf("readConnectionParameters() returned unexpected error: %v", err)
			}
			if err == nil && tc.wantErr {
				t.Errorf("readConnectionParameters() did not return expected error")
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("readConnectionParameters() returned unexpected diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCreateMetricsForRow(t *testing.T) {
	// This test simulates a row with several GAUGE metrics (3), a couple LABELs (2).
	// The labels will be appended to each of the gauge metrics, making the number of gauge metrics (3) be the desired want value.
	query := &configpb.Query{
		Name: "testQuery",
		Columns: []*configpb.Column{
			{ValueType: configpb.ValueType_VALUE_INT64, Name: "testColInt", MetricType: configpb.MetricType_METRIC_GAUGE},
			{ValueType: configpb.ValueType_VALUE_DOUBLE, Name: "testColDouble", MetricType: configpb.MetricType_METRIC_GAUGE},
			{ValueType: configpb.ValueType_VALUE_BOOL, Name: "testColBool", MetricType: configpb.MetricType_METRIC_GAUGE},
			{ValueType: configpb.ValueType_VALUE_STRING, Name: "stringLabel", MetricType: configpb.MetricType_METRIC_LABEL},
			{ValueType: configpb.ValueType_VALUE_STRING, Name: "stringLabel2", MetricType: configpb.MetricType_METRIC_LABEL},
			{ValueType: configpb.ValueType_VALUE_DOUBLE, Name: "testColDouble2", MetricType: configpb.MetricType_METRIC_CUMULATIVE},
			// Add a misconfigured column (STRING cannot be GAUGE. This would be caught in the config validator) to kill mutants.
			{ValueType: configpb.ValueType_VALUE_STRING, Name: "misconfiguredCol", MetricType: configpb.MetricType_METRIC_GAUGE},
		},
	}
	cols := make([]any, len(query.Columns))
	cols[0], cols[1], cols[2], cols[3], cols[4], cols[5], cols[6] = new(int64), new(float64), new(bool), new(string), new(string), new(float64), new(string)

	runningSum := make(map[timeSeriesKey]prevVal)
	tsKey := newTimeSeriesKey("workload.googleapis.com/oracle/testQuery/testColDouble2", "sid:testSID,stringLabel2:,stringLabel:")
	runningSum[tsKey] = prevVal{val: float64(123.456), startTime: &tspb.Timestamp{Seconds: 0}}

	opts := queryOptions{
		query:       query,
		serviceName: "testSID",
		collector:   &defaultCollector,
		runningSum:  runningSum,
	}
	defaultLabels := map[string]string{"defaultLabel1": "test1", "defaultLabel2": "test2"}

	wantMetrics := 4
	got := createMetricsForRow(context.Background(), opts, cols, defaultLabels)
	gotMetrics := len(got)
	if gotMetrics != wantMetrics {
		t.Errorf("createMetricsForRow(%#v) = %d, want metrics length: %d", query, gotMetrics, wantMetrics)
	}

	// 2 correctly configured labels in the column plus 2 default labels.
	wantLabels := 4
	gotLabels := 0
	if len(got) > 0 {
		gotLabels = len(got[0].Metric.Labels)
	}
	if gotLabels != wantLabels {
		t.Errorf("createMetricsForRow(%#v) = %d, want labels length: %d", query, gotLabels, wantLabels)
	}
}

func TestCreateColumns(t *testing.T) {
	tests := []struct {
		name string
		cols []*configpb.Column
		want []any
	}{
		{
			name: "EmptyColumns",
			cols: nil,
			want: nil,
		},
		{
			name: "ColumnsWithMultipleTypes",
			cols: []*configpb.Column{
				{
					ValueType: configpb.ValueType_VALUE_BOOL,
				},
				{
					ValueType: configpb.ValueType_VALUE_STRING,
				},
				{
					ValueType: configpb.ValueType_VALUE_INT64,
				},
				{
					ValueType: configpb.ValueType_VALUE_DOUBLE,
				},
				{
					ValueType: configpb.ValueType_VALUE_UNSPECIFIED,
				},
			},
			want: []any{
				new(bool),
				new(string),
				new(int64),
				new(float64),
				new(any),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := createColumns(test.cols)

			if diff := cmp.Diff(test.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("createColumns(%#v) unexpected diff: (-want +got):\n%s", test.cols, diff)
			}
		})
	}
}

// For the following test, QueryResults.ReadRow() requires pointers in order to populate the column values.
// These values will eventually be passed to createGaugeMetric(). Simulate this behavior by creating pointers and populating them with a value.
func TestCreateGaugeMetric(t *testing.T) {
	tests := []struct {
		name       string
		column     *configpb.Column
		val        any
		want       *mrpb.TimeSeries
		wantMetric *mpb.Metric
		wantValue  *cpb.TypedValue
	}{
		{
			name:       "Int",
			column:     &configpb.Column{ValueType: configpb.ValueType_VALUE_INT64, Name: "testCol"},
			val:        proto.Int64(123),
			want:       newDefaultMetrics(),
			wantMetric: &mpb.Metric{Type: "workload.googleapis.com/oracle/testQuery/testCol", Labels: map[string]string{"abc": "def"}},
			wantValue:  &cpb.TypedValue{Value: &cpb.TypedValue_Int64Value{Int64Value: 123}},
		},
		{
			name:       "Double",
			column:     &configpb.Column{ValueType: configpb.ValueType_VALUE_DOUBLE, Name: "testCol"},
			val:        proto.Float64(123.456),
			want:       newDefaultMetrics(),
			wantMetric: &mpb.Metric{Type: "workload.googleapis.com/oracle/testQuery/testCol", Labels: map[string]string{"abc": "def"}},
			wantValue:  &cpb.TypedValue{Value: &cpb.TypedValue_DoubleValue{DoubleValue: 123.456}},
		},
		{
			name:       "BoolWithNameOverride",
			column:     &configpb.Column{ValueType: configpb.ValueType_VALUE_BOOL, Name: "testCol", NameOverride: "override/metric/path"},
			val:        proto.Bool(true),
			want:       newDefaultMetrics(),
			wantMetric: &mpb.Metric{Type: "workload.googleapis.com/oracle/override/metric/path", Labels: map[string]string{"abc": "def"}},
			wantValue:  &cpb.TypedValue{Value: &cpb.TypedValue_BoolValue{BoolValue: true}},
		},
		{
			name:   "Fails",
			column: &configpb.Column{ValueType: configpb.ValueType_VALUE_STRING, Name: "testCol"},
			val:    proto.String("test"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.want != nil {
				test.want.Metric = test.wantMetric
				test.want.Points[0].Value = test.wantValue
			}
			opts := queryOptions{
				query:       &configpb.Query{Name: "testQuery"},
				serviceName: "testSID",
				collector:   &defaultCollector,
				runningSum:  map[timeSeriesKey]prevVal{},
			}
			got, _ := createGaugeMetric(test.column, test.val, map[string]string{"abc": "def"}, opts, defaultTimestamp)
			if diff := cmp.Diff(test.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("createGaugeMetric(%#v) unexpected diff: (-want +got):\n%s", test.column, diff)
			}
		})
	}
}

func TestCreateCumulativeMetric(t *testing.T) {
	tests := []struct {
		name       string
		column     *configpb.Column
		val        any
		want       *mrpb.TimeSeries
		runningSum map[timeSeriesKey]prevVal
		wantMetric *mpb.Metric
		wantValue  *cpb.TypedValue
	}{
		{
			name:       "KeyDoesNotExistInCumulativeTimeSeriesInt",
			column:     &configpb.Column{ValueType: configpb.ValueType_VALUE_INT64, Name: "testCol", MetricType: configpb.MetricType_METRIC_CUMULATIVE},
			val:        proto.Int64(123),
			runningSum: map[timeSeriesKey]prevVal{},
			want:       newDefaultCumulativeMetric(0, 123),
			wantMetric: &mpb.Metric{Type: "workload.googleapis.com/oracle/testQuery/testCol", Labels: map[string]string{"abc": "def"}},
			wantValue:  &cpb.TypedValue{Value: &cpb.TypedValue_Int64Value{Int64Value: 123}},
		},
		{
			name:       "KeyDoesNotExistInCumulativeTimeSeriesDouble",
			column:     &configpb.Column{ValueType: configpb.ValueType_VALUE_DOUBLE, Name: "testCol", MetricType: configpb.MetricType_METRIC_CUMULATIVE},
			val:        proto.Float64(123.23),
			runningSum: map[timeSeriesKey]prevVal{},
			want:       newDefaultCumulativeMetric(0, 123),
			wantMetric: &mpb.Metric{Type: "workload.googleapis.com/oracle/testQuery/testCol", Labels: map[string]string{"abc": "def"}},
			wantValue:  &cpb.TypedValue{Value: &cpb.TypedValue_DoubleValue{DoubleValue: 123.23}},
		},
		{
			name:   "KeyAlreadyExistInCumulativeTimeSeries",
			column: &configpb.Column{ValueType: configpb.ValueType_VALUE_INT64, Name: "testCol", MetricType: configpb.MetricType_METRIC_CUMULATIVE},
			val:    proto.Int64(123),
			runningSum: map[timeSeriesKey]prevVal{
				newTimeSeriesKey("workload.googleapis.com/oracle/testQuery/testCol", "abc:def"): {val: int64(123), startTime: &tspb.Timestamp{Seconds: 0}},
			},
			want:       newDefaultCumulativeMetric(0, 123),
			wantMetric: &mpb.Metric{Type: "workload.googleapis.com/oracle/testQuery/testCol", Labels: map[string]string{"abc": "def"}},
			wantValue:  &cpb.TypedValue{Value: &cpb.TypedValue_Int64Value{Int64Value: 246}},
		},
		{
			name:   "CumulativeTimeSeriesDouble",
			column: &configpb.Column{ValueType: configpb.ValueType_VALUE_DOUBLE, Name: "testCol", MetricType: configpb.MetricType_METRIC_CUMULATIVE},
			val:    proto.Float64(123.23),
			runningSum: map[timeSeriesKey]prevVal{
				newTimeSeriesKey("workload.googleapis.com/oracle/testQuery/testCol", "abc:def"): {val: float64(123.23), startTime: &tspb.Timestamp{Seconds: 0}},
			},
			want:       newDefaultCumulativeMetric(0, 123),
			wantMetric: &mpb.Metric{Type: "workload.googleapis.com/oracle/testQuery/testCol", Labels: map[string]string{"abc": "def"}},
			wantValue:  &cpb.TypedValue{Value: &cpb.TypedValue_DoubleValue{DoubleValue: 246.46}},
		},
		{
			name:   "IntWithNameOverride",
			column: &configpb.Column{ValueType: configpb.ValueType_VALUE_INT64, Name: "testCol", MetricType: configpb.MetricType_METRIC_CUMULATIVE, NameOverride: "override/path"},
			val:    proto.Int64(123),
			runningSum: map[timeSeriesKey]prevVal{
				newTimeSeriesKey("workload.googleapis.com/oracle/override/path", "abc:def"): {val: int64(123), startTime: &tspb.Timestamp{Seconds: 0}},
			},
			want:       newDefaultCumulativeMetric(0, 123),
			wantMetric: &mpb.Metric{Type: "workload.googleapis.com/oracle/override/path", Labels: map[string]string{"abc": "def"}},
			wantValue:  &cpb.TypedValue{Value: &cpb.TypedValue_Int64Value{Int64Value: 246}},
		},
		{
			name:   "Fails",
			column: &configpb.Column{ValueType: configpb.ValueType_VALUE_STRING, Name: "testCol"},
			val:    proto.String("test"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.want != nil {
				test.want.Metric = test.wantMetric
				test.want.Points[0].Value = test.wantValue
			}
			opts := queryOptions{
				query:       &configpb.Query{Name: "testQuery"},
				serviceName: "testSID",
				collector:   &defaultCollector,
				runningSum:  test.runningSum,
			}
			got, _ := createCumulativeMetric(context.Background(), test.column, test.val, map[string]string{"abc": "def"}, opts, defaultTimestamp)
			if diff := cmp.Diff(test.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("createCumulativeMetric(%#v) unexpected diff: (-want +got):\n%s", test.column, diff)
			}
		})
	}
}

func TestPrepareTimeSeriesKey(t *testing.T) {
	tests := []struct {
		name         string
		metricType   string
		metricKind   string
		metricLabels map[string]string
		want         timeSeriesKey
	}{
		{
			name:         "PrepareKey",
			metricType:   "workload.googleapis.com/oracle/testQuery/testCol",
			metricKind:   mpb.MetricDescriptor_CUMULATIVE.String(),
			metricLabels: map[string]string{"sample": "labels", "abc": "def"},
			want: timeSeriesKey{
				MetricKind:   mpb.MetricDescriptor_CUMULATIVE.String(),
				MetricType:   "workload.googleapis.com/oracle/testQuery/testCol",
				MetricLabels: "abc:def,sample:labels",
			},
		},
		{
			name:         "PrepareKeyWithDifferentOrderLabels",
			metricType:   "workload.googleapis.com/oracle/testQuery/testCol",
			metricKind:   mpb.MetricDescriptor_CUMULATIVE.String(),
			metricLabels: map[string]string{"abc": "def", "sample": "labels"},
			want: timeSeriesKey{
				MetricKind:   mpb.MetricDescriptor_CUMULATIVE.String(),
				MetricType:   "workload.googleapis.com/oracle/testQuery/testCol",
				MetricLabels: "abc:def,sample:labels",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := prepareKey(test.metricType, test.metricKind, test.metricLabels)
			if got != test.want {
				t.Errorf("prepareKey() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestConvertCloudProperties(t *testing.T) {
	cp := &configpb.CloudProperties{
		ProjectId:        "project-id",
		InstanceId:       "instance-id",
		Zone:             "zone",
		InstanceName:     "instance-name",
		Image:            "image",
		NumericProjectId: "1234567890",
		Region:           "region",
	}

	want := &metadataserver.CloudProperties{
		ProjectID:        "project-id",
		InstanceID:       "instance-id",
		Zone:             "zone",
		InstanceName:     "instance-name",
		Image:            "image",
		NumericProjectID: "1234567890",
		Region:           "region",
	}

	got := convertCloudProperties(cp)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("convertCloudProperties(%v) returned diff (-want +got):\n%s", cp, diff)
	}
}

func TestIsQueryAllowedForRole(t *testing.T) {
	testCases := []struct {
		name      string
		queryRole configpb.Query_DatabaseRole
		role      string
		want      bool
	}{
		{
			name:      "Primary",
			queryRole: configpb.Query_PRIMARY,
			role:      "PRIMARY",
			want:      true,
		},
		{
			name:      "Physical Standby",
			queryRole: configpb.Query_STANDBY,
			role:      "PHYSICAL STANDBY",
			want:      true,
		},
		{
			name:      "Logical Standby",
			queryRole: configpb.Query_STANDBY,
			role:      "LOGICAL STANDBY",
			want:      true,
		},
		{
			name:      "Snapshot Standby",
			queryRole: configpb.Query_STANDBY,
			role:      "SNAPSHOT STANDBY",
			want:      true,
		},
		{
			name:      "Unspecified",
			queryRole: configpb.Query_UNSPECIFIED,
			role:      "PRIMARY",
			want:      false,
		},
		{
			name:      "Primary query on standby database",
			queryRole: configpb.Query_PRIMARY,
			role:      "PHYSICAL STANDBY",
			want:      false,
		},
		{
			name:      "Standby query on primary database",
			queryRole: configpb.Query_STANDBY,
			role:      "PRIMARY",
			want:      false,
		},
		{
			name:      "Both on primary database",
			queryRole: configpb.Query_BOTH,
			role:      "PRIMARY",
			want:      true,
		},
		{
			name:      "Both on standby database",
			queryRole: configpb.Query_BOTH,
			role:      "PHYSICAL STANDBY",
			want:      true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			query := &configpb.Query{DatabaseRole: tc.queryRole}
			if got := isQueryAllowedForRole(query, tc.role); got != tc.want {
				t.Errorf("isQueryAllowedForRole(%v, %v) = %v, want %v", query, tc.role, got, tc.want)
			}
		})
	}
}

func TestDisabledQuery(t *testing.T) {
	query := &configpb.Query{
		DatabaseRole: configpb.Query_BOTH,
		Disabled:     proto.Bool(true),
	}
	role := "PHYSICAL STANDBY"
	want := false
	if got := isQueryAllowedForRole(query, role); got != want {
		t.Errorf("isQueryAllowedForRole(%v, %v) = %v, want %v", query, role, got, want)
	}
}

// setupSQLiteDB creates an in-memory database suitable for testing oraclemetrics functionality
func setupSQLiteDB(t *testing.T) (*sql.DB, error) {
	t.Helper()
	seedSQLStatements := []string{
		// v$database table
		`CREATE TABLE "v$database" (
				dbid TEXT PRIMARY KEY,
				db_unique_name TEXT,
				database_role TEXT
			)`,
		`INSERT INTO "v$database" (dbid, db_unique_name, database_role) VALUES ('1', 'test_db_unique_name', 'PRIMARY')`,

		// v$pdbs table
		`CREATE TABLE "v$pdbs" (
				id TEXT PRIMARY KEY,
				name TEXT
			)`,
		`INSERT INTO "v$pdbs" (id, name) VALUES ('1', 'test_pdb_name')`,

		// DUAL table
		`CREATE TABLE DUAL (DUMMY TEXT PRIMARY KEY)`,
		`INSERT INTO DUAL (DUMMY) VALUES ('X')`,
	}
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("cannot create a sqlite testing database")
	}
	for _, s := range seedSQLStatements {
		if _, err := db.Exec(s); err != nil {
			return nil, fmt.Errorf("error executing sql statement: %v\nSQL: %s", err, s)
		}
	}
	return db, nil
}

func TestFetchDatabaseInfo(t *testing.T) {
	want := &databaseInfo{
		DBID:         "1",
		DBUniqueName: "test_db_unique_name",
		DatabaseRole: "PRIMARY",
		PdbName:      "test_pdb_name",
	}
	db, err := setupSQLiteDB(t)
	if err != nil {
		t.Fatalf("Failed to setup testing SQLite DB: %v", err)
	}
	defer db.Close()

	got, err := fetchDatabaseInfo(context.Background(), db)
	if err != nil {
		t.Errorf("fetchDatabaseInfo() returned an unexpected error: %v", err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("fetchDatabaseInfo() returned an unexpected diff (-want +got): %v", diff)
	}
}

func TestSendDefaultMetricsToCloudMonitoring(t *testing.T) {
	db, err := setupSQLiteDB(t)
	if err != nil {
		t.Fatalf("Failed to setup testing SQLite DB: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name                string
		OracleConfiguration *configpb.OracleConfiguration
		connections         map[string]*sql.DB
		failCount           map[string]int
		want                []*mrpb.TimeSeries
	}{
		{
			name: "Success",
			connections: map[string]*sql.DB{
				"test_service_name": db,
			},
			failCount: map[string]int{},
			OracleConfiguration: &configpb.OracleConfiguration{
				OracleMetrics: &configpb.OracleMetrics{
					Queries: []*configpb.Query{
						&configpb.Query{
							Name: "testQuery",
							Columns: []*configpb.Column{
								&configpb.Column{
									ValueType:    configpb.ValueType_VALUE_INT64,
									Name:         "testCol",
									MetricType:   configpb.MetricType_METRIC_GAUGE,
									NameOverride: "test_col",
								},
							},
							Sql:          "SELECT 1",
							DatabaseRole: configpb.Query_PRIMARY,
							Disabled:     proto.Bool(false),
						},
					},
					Enabled:             proto.Bool(true),
					MaxExecutionThreads: 1,
					QueryTimeout:        &dpb.Duration{Seconds: 10},
				},
			},
			want: []*mrpb.TimeSeries{
				{
					Metric: &mpb.Metric{
						Type:   "workload.googleapis.com/oracle/test_col",
						Labels: map[string]string{"dbid": "1", "db_unique_name": "test_db_unique_name", "pdb_name": "test_pdb_name"},
					},
					Resource: &mrespb.MonitoredResource{
						Type: "gce_instance",
						Labels: map[string]string{
							"project_id":  "test_project_id",
							"instance_id": "test_instance_id",
							"zone":        "test_zone",
						},
					},
					Points: []*mrpb.Point{
						{
							Interval: &cpb.TimeInterval{
								StartTime: &tspb.Timestamp{Seconds: 1724194800},
								EndTime:   &tspb.Timestamp{Seconds: 1724194800},
							},
							Value: &cpb.TypedValue{Value: &cpb.TypedValue_Int64Value{Int64Value: 1}},
						},
					},
					MetricKind: mpb.MetricDescriptor_GAUGE,
				},
			},
		},
		{
			name: "Non-primary database",
			connections: map[string]*sql.DB{
				"test_service_name": db,
			},
			failCount: map[string]int{},
			OracleConfiguration: &configpb.OracleConfiguration{
				OracleMetrics: &configpb.OracleMetrics{
					Queries: []*configpb.Query{
						&configpb.Query{
							Name:         "testQuery",
							DatabaseRole: configpb.Query_STANDBY,
							Disabled:     proto.Bool(false),
						},
					},
					Enabled: proto.Bool(true),
				},
			},
			want: []*mrpb.TimeSeries{},
		},
		{
			name: "Query disabled",
			connections: map[string]*sql.DB{
				"test_service_name": db,
			},
			OracleConfiguration: &configpb.OracleConfiguration{
				OracleMetrics: &configpb.OracleMetrics{
					Queries: []*configpb.Query{
						&configpb.Query{
							Name:     "testQuery",
							Disabled: proto.Bool(true),
						},
					},
					Enabled: proto.Bool(true),
				},
			},
			want: []*mrpb.TimeSeries{},
		},
		{
			name: "Invalid query",
			connections: map[string]*sql.DB{
				"test_service_name": db,
			},
			failCount: map[string]int{},
			OracleConfiguration: &configpb.OracleConfiguration{
				OracleMetrics: &configpb.OracleMetrics{
					Queries: []*configpb.Query{
						&configpb.Query{
							Name:     "testQuery",
							Sql:      "invalid query",
							Disabled: proto.Bool(false),
						},
					},
					Enabled:             proto.Bool(true),
					MaxExecutionThreads: 1,
					QueryTimeout:        &dpb.Duration{Seconds: 10},
				},
			},
			want: []*mrpb.TimeSeries{},
		},
		{
			name: "3 consecutive failures",
			connections: map[string]*sql.DB{
				"test_service_name": db,
			},
			failCount: map[string]int{"test_service_name:testQuery": 3},
			OracleConfiguration: &configpb.OracleConfiguration{
				OracleMetrics: &configpb.OracleMetrics{
					Queries: []*configpb.Query{
						&configpb.Query{
							Name:     "testQuery",
							Disabled: proto.Bool(false),
						},
					},
					Enabled: proto.Bool(true),
				},
			},
			want: []*mrpb.TimeSeries{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			collector := &MetricCollector{
				Config: &configpb.Configuration{
					CloudProperties: &configpb.CloudProperties{
						ProjectId:        "test_project_id",
						InstanceId:       "test_instance_id",
						Zone:             "test_zone",
						InstanceName:     "test_instance_name",
						Image:            "test_image",
						NumericProjectId: "1234567890",
						Region:           "test_region",
					},
					OracleConfiguration: tc.OracleConfiguration,
				},
				TimeSeriesCreator: &cmfake.TimeSeriesCreator{},
				failCount:         tc.failCount,
				skipMsgLogged:     map[string]bool{},
				connections:       tc.connections,
			}

			got := collector.SendDefaultMetricsToCloudMonitoring(context.Background())

			if diff := cmp.Diff(tc.want, got, protocmp.Transform(), protocmp.IgnoreFields(&mrpb.Point{}, "interval")); diff != "" {
				t.Errorf("SendDefaultMetricsToCloudMonitoring() returned an unexpected diff (-want +got): %v", diff)
			}
		})
	}
}

func TestShouldSkipQuery(t *testing.T) {
	tests := []struct {
		name          string
		serviceName   string
		queryName     string
		failCount     int
		skipMsgLogged map[string]bool
		want          bool
	}{
		{
			name:          "Skip query",
			serviceName:   "test",
			queryName:     "query1",
			failCount:     maxQueryFailures,
			skipMsgLogged: map[string]bool{},
			want:          true,
		},
		{
			name:          "Do not skip query",
			serviceName:   "test",
			queryName:     "query1",
			failCount:     maxQueryFailures - 1,
			skipMsgLogged: map[string]bool{},
			want:          false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			c := &MetricCollector{
				failCount:     map[string]int{"test:query1": tc.failCount},
				skipMsgLogged: tc.skipMsgLogged,
			}

			got := c.shouldSkipQuery(ctx, tc.serviceName, tc.queryName)

			if got != tc.want {
				t.Errorf("skipQuery(%q, %q) = %v, want %v", tc.serviceName, tc.queryName, got, tc.want)
			}

			if got && !tc.skipMsgLogged["test:query1"] {
				t.Errorf("skipQuery(%q, %q) did not log skip message", tc.serviceName, tc.queryName)
			}
		})
	}
}

func TestExecuteQueryAndSendMetrics(t *testing.T) {
	tests := []struct {
		name  string
		want  []*mrpb.TimeSeries
		query *configpb.Query
	}{
		{
			name: "Success",
			query: &configpb.Query{
				Name: "testQuery",
				Columns: []*configpb.Column{
					&configpb.Column{
						ValueType:    configpb.ValueType_VALUE_INT64,
						Name:         "testCol",
						MetricType:   configpb.MetricType_METRIC_GAUGE,
						NameOverride: "test_col",
					},
				},
				Sql:          "SELECT 1",
				DatabaseRole: configpb.Query_PRIMARY,
				Disabled:     proto.Bool(false),
			},
			want: []*mrpb.TimeSeries{
				{
					Metric: &mpb.Metric{
						Type:   "workload.googleapis.com/oracle/test_col",
						Labels: map[string]string{"dbid": "1", "db_unique_name": "test_db_unique_name", "pdb_name": "test_pdb_name"},
					},
					Resource: &mrespb.MonitoredResource{
						Type: "gce_instance",
						Labels: map[string]string{
							"project_id":  "test_project_id",
							"instance_id": "test_instance_id",
							"zone":        "test_zone",
						},
					},
					Points: []*mrpb.Point{
						{
							Interval: &cpb.TimeInterval{
								StartTime: &tspb.Timestamp{Seconds: 1724194800},
								EndTime:   &tspb.Timestamp{Seconds: 1724194800},
							},
							Value: &cpb.TypedValue{Value: &cpb.TypedValue_Int64Value{Int64Value: 1}},
						},
					},
					MetricKind: mpb.MetricDescriptor_GAUGE,
				},
			},
		},
		{
			name: "Invalid query",
			query: &configpb.Query{
				Name: "testQuery",
				Columns: []*configpb.Column{
					&configpb.Column{
						ValueType:    configpb.ValueType_VALUE_INT64,
						Name:         "testCol",
						MetricType:   configpb.MetricType_METRIC_GAUGE,
						NameOverride: "test_col",
					},
				},
				Sql:          "invalid query",
				DatabaseRole: configpb.Query_PRIMARY,
				Disabled:     proto.Bool(false),
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, err := setupSQLiteDB(t)
			if err != nil {
				t.Fatalf("Failed to setup testing SQLite DB: %v", err)
			}
			defer db.Close()

			collector := &MetricCollector{
				failCount:     map[string]int{},
				skipMsgLogged: map[string]bool{},
				Config: &configpb.Configuration{
					CloudProperties: &configpb.CloudProperties{
						ProjectId:        "test_project_id",
						InstanceId:       "test_instance_id",
						Zone:             "test_zone",
						InstanceName:     "test_instance_name",
						Image:            "test_image",
						NumericProjectId: "1234567890",
						Region:           "test_region",
					},
				},
				TimeSeriesCreator: &cmfake.TimeSeriesCreator{},
			}

			opts := queryOptions{
				db:            db,
				query:         tc.query,
				timeout:       10,
				collector:     collector,
				runningSum:    make(map[timeSeriesKey]prevVal),
				serviceName:   "test_service_name",
				defaultLabels: map[string]string{"dbid": "1", "db_unique_name": "test_db_unique_name", "pdb_name": "test_pdb_name"},
			}

			got := executeQueryAndSendMetrics(context.Background(), opts)

			if diff := cmp.Diff(tc.want, got, protocmp.Transform(), protocmp.IgnoreFields(&mrpb.Point{}, "interval")); diff != "" {
				t.Errorf("executeQueryAndSendMetrics() returned an unexpected diff (-want +got): %v", diff)
			}
		})
	}
}

func TestCreateHealthMetrics(t *testing.T) {
	statusData := map[string]*ServiceHealth{
		"service_name1": &ServiceHealth{Status: Healthy},
		"service_name2": &ServiceHealth{Status: Unhealthy},
	}
	cfg := &configpb.Configuration{
		CloudProperties: &configpb.CloudProperties{
			ProjectId:  "project-id",
			InstanceId: "instance-id",
			Zone:       "zone",
		},
	}

	want := []*mrpb.TimeSeries{
		{
			Metric: &mpb.Metric{
				Type:   metricURL + "/health",
				Labels: map[string]string{"service_name": "service_name1"},
			},
			Resource: &mrespb.MonitoredResource{
				Type: "gce_instance",
				Labels: map[string]string{
					"project_id":  "project-id",
					"instance_id": "instance-id",
					"zone":        "zone",
				},
			},
			Points: []*mrpb.Point{
				{
					Interval: &cpb.TimeInterval{
						EndTime: tspb.Now(),
					},
					Value: &cpb.TypedValue{
						Value: &cpb.TypedValue_BoolValue{
							BoolValue: true,
						},
					},
				},
			},
		},
		&mrpb.TimeSeries{
			Metric: &mpb.Metric{
				Type:   metricURL + "/health",
				Labels: map[string]string{"service_name": "service_name2"},
			},
			Resource: &mrespb.MonitoredResource{
				Type: "gce_instance",
				Labels: map[string]string{
					"project_id":  "project-id",
					"instance_id": "instance-id",
					"zone":        "zone",
				},
			},
			Points: []*mrpb.Point{
				{
					Interval: &cpb.TimeInterval{
						EndTime: tspb.Now(),
					},
					Value: &cpb.TypedValue{
						Value: &cpb.TypedValue_BoolValue{
							BoolValue: false,
						},
					},
				},
			},
		},
	}

	got := createHealthMetrics(context.Background(), statusData, cfg)

	if diff := cmp.Diff(want, got, protocmp.Transform(), protocmp.IgnoreFields(&mrpb.Point{}, "interval")); diff != "" {
		t.Errorf("createHealthMetrics() returned diff (-want +got):\n%s", diff)
	}
}

func TestDatabaseHealth(t *testing.T) {
	healthyDB, err := setupSQLiteDB(t)
	if err != nil {
		t.Fatalf("Failed to setup testing SQLite DB: %v", err)
	}

	unhealthyDB, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("cannot create a sqlite testing database")
	}

	tests := []struct {
		name        string
		connections map[string]*sql.DB
		want        map[string]*ServiceHealth
	}{
		{
			name: "Success",
			connections: map[string]*sql.DB{
				"service_name1": healthyDB,
			},
			want: map[string]*ServiceHealth{
				"service_name1": &ServiceHealth{
					Status: Healthy,
				},
			},
		},
		{
			name: "Failure",
			connections: map[string]*sql.DB{
				"service_name2": unhealthyDB,
			},
			want: map[string]*ServiceHealth{
				"service_name2": &ServiceHealth{
					Status:  Unhealthy,
					Message: "query execution failed: no such table: dual",
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := checkDatabaseHealth(context.Background(), tc.connections)

			opts := []cmp.Option{cmpopts.IgnoreFields(ServiceHealth{}, "LastChecked")}
			if diff := cmp.Diff(tc.want, got, opts...); diff != "" {
				t.Errorf("checkDatabaseHealth() returned diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSendHealthMetricsToCloudMonitoring(t *testing.T) {
	healthyDB, err := setupSQLiteDB(t)
	if err != nil {
		t.Fatalf("Failed to setup testing SQLite DB: %v", err)
	}

	unhealthyDB, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("cannot create a sqlite testing database")
	}

	tests := []struct {
		name        string
		connections map[string]*sql.DB
		want        []*mrpb.TimeSeries
	}{
		{
			name: "Healthy",
			connections: map[string]*sql.DB{
				"service_name1": healthyDB,
			},
			want: []*mrpb.TimeSeries{
				{
					Metric: &mpb.Metric{
						Type:   metricURL + "/health",
						Labels: map[string]string{"service_name": "service_name1"},
					},
					Resource: &mrespb.MonitoredResource{
						Type: "gce_instance",
						Labels: map[string]string{
							"project_id":  "project-id",
							"instance_id": "instance-id",
							"zone":        "zone",
						},
					},
					Points: []*mrpb.Point{
						{
							Interval: &cpb.TimeInterval{
								EndTime: tspb.Now(),
							},
							Value: &cpb.TypedValue{
								Value: &cpb.TypedValue_BoolValue{
									BoolValue: true,
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Unhealthy",
			connections: map[string]*sql.DB{
				"service_name2": unhealthyDB,
			},
			want: []*mrpb.TimeSeries{
				{
					Metric: &mpb.Metric{
						Type:   metricURL + "/health",
						Labels: map[string]string{"service_name": "service_name2"},
					},
					Resource: &mrespb.MonitoredResource{
						Type: "gce_instance",
						Labels: map[string]string{
							"project_id":  "project-id",
							"instance_id": "instance-id",
							"zone":        "zone",
						},
					},
					Points: []*mrpb.Point{
						{
							Interval: &cpb.TimeInterval{
								EndTime: tspb.Now(),
							},
							Value: &cpb.TypedValue{
								Value: &cpb.TypedValue_BoolValue{
									BoolValue: false,
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			collector := &MetricCollector{
				Config: &configpb.Configuration{
					CloudProperties: &configpb.CloudProperties{
						ProjectId:  "project-id",
						InstanceId: "instance-id",
						Zone:       "zone",
					},
				},
				TimeSeriesCreator: &cmfake.TimeSeriesCreator{},
				connections:       tc.connections,
			}

			got := collector.SendHealthMetricsToCloudMonitoring(context.Background())

			if diff := cmp.Diff(tc.want, got, protocmp.Transform(), protocmp.IgnoreFields(&mrpb.Point{}, "interval")); diff != "" {
				t.Errorf("SendHealthMetricsToCloudMonitoring() returned diff (-want +got):\n%s", diff)
			}
		})
	}
}
