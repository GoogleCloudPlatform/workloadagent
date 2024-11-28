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
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"github.com/GoogleCloudPlatform/workloadagent/internal/secret"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/cloudmonitoring"
	gcefake "github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/gce/fake"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/log"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/timeseries"

	mpb "google.golang.org/genproto/googleapis/api/metric"
	mrespb "google.golang.org/genproto/googleapis/api/monitoredres"
	cpb "google.golang.org/genproto/googleapis/monitoring/v3"
	mrpb "google.golang.org/genproto/googleapis/monitoring/v3"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	odpb "github.com/GoogleCloudPlatform/workloadagent/protos/oraclediscovery"
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

func TestExtractSIDs(t *testing.T) {
	tests := []struct {
		name      string
		discovery *odpb.Discovery
		want      []string
	}{
		{
			name: "One database, one instance",
			discovery: &odpb.Discovery{
				Databases: []*odpb.Discovery_DatabaseRoot{
					{
						TenancyType: &odpb.Discovery_DatabaseRoot_Db{
							Db: &odpb.Discovery_Database{
								Instances: []*odpb.Discovery_Database_Instance{
									{
										OracleSid: "ORCL",
									},
								},
							},
						},
					},
				},
			},
			want: []string{"ORCL"},
		},
		{
			name: "One database, two instances",
			discovery: &odpb.Discovery{
				Databases: []*odpb.Discovery_DatabaseRoot{
					{
						TenancyType: &odpb.Discovery_DatabaseRoot_Db{
							Db: &odpb.Discovery_Database{
								Instances: []*odpb.Discovery_Database_Instance{
									{
										OracleSid: "ORCL",
									},
									{
										OracleSid: "ORCL2",
									},
								},
							},
						},
					},
				},
			},
			want: []string{"ORCL", "ORCL2"},
		},
		{
			name: "Two databases, one instance each",
			discovery: &odpb.Discovery{
				Databases: []*odpb.Discovery_DatabaseRoot{
					{
						TenancyType: &odpb.Discovery_DatabaseRoot_Db{
							Db: &odpb.Discovery_Database{
								Instances: []*odpb.Discovery_Database_Instance{
									{
										OracleSid: "ORCL",
									},
								},
							},
						},
					},
					{
						TenancyType: &odpb.Discovery_DatabaseRoot_Db{
							Db: &odpb.Discovery_Database{
								Instances: []*odpb.Discovery_Database_Instance{
									{
										OracleSid: "ORCL2",
									},
								},
							},
						},
					},
				},
			},
			want: []string{"ORCL", "ORCL2"},
		},
		{
			name: "CDB and non-CDB",
			discovery: &odpb.Discovery{
				Databases: []*odpb.Discovery_DatabaseRoot{
					{
						TenancyType: &odpb.Discovery_DatabaseRoot_Cdb{
							Cdb: &odpb.Discovery_DatabaseRoot_ContainerDatabase{
								Root: &odpb.Discovery_Database{
									Instances: []*odpb.Discovery_Database_Instance{
										{
											OracleSid: "ORCL1",
										},
									},
								},
								Pdbs: []*odpb.Discovery_Database{
									{
										Instances: []*odpb.Discovery_Database_Instance{
											{
												OracleSid: "ORCL1",
											},
											{
												OracleSid: "ORCL1",
											},
										},
									},
									{
										Instances: []*odpb.Discovery_Database_Instance{
											{
												OracleSid: "ORCL1",
											},
											{
												OracleSid: "ORCL1",
											},
											{
												OracleSid: "ORCL1",
											},
										},
									},
								},
							},
						},
					},
					{
						TenancyType: &odpb.Discovery_DatabaseRoot_Db{
							Db: &odpb.Discovery_Database{
								Instances: []*odpb.Discovery_Database_Instance{
									{
										OracleSid: "ORCL2",
									},
									{
										OracleSid: "ORCL3",
									},
								},
							},
						},
					},
					{
						TenancyType: &odpb.Discovery_DatabaseRoot_Db{
							Db: &odpb.Discovery_Database{
								Instances: []*odpb.Discovery_Database_Instance{
									{
										OracleSid: "ORCL4",
									},
								},
							},
						},
					},
				},
			},
			want: []string{"ORCL1", "ORCL2", "ORCL3", "ORCL4"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ExtractSIDs(test.discovery)
			if err != nil {
				t.Errorf("ExtractSIDs(%v) returned an unexpected error: %v", test.discovery, err)
			}
			if diff := cmp.Diff(test.want, got); diff != "" {
				t.Errorf("extractSIDs(%v) returned diff (-want +got):\n%s", test.discovery, diff)
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

	want := &timeseries.CloudProperties{
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

func TestIncrementFailCount(t *testing.T) {
	d := &dbManager{
		failCounts: make(map[string]map[string]int),
	}

	d.incrementFailCount("SID1", "QUERY1")
	d.incrementFailCount("SID1", "QUERY2")
	d.incrementFailCount("SID2", "QUERY1")

	want := map[string]map[string]int{
		"SID1": {
			"QUERY1": 1,
			"QUERY2": 1,
		},
		"SID2": {
			"QUERY1": 1,
		},
	}

	if diff := cmp.Diff(want, d.failCounts); diff != "" {
		t.Errorf("failCounts returned diff (-want +got):\n%s", diff)
	}
}

func TestResetFailCount(t *testing.T) {
	tests := []struct {
		desc       string
		sid        string
		queryName  string
		failCounts map[string]map[string]int
		want       map[string]map[string]int
	}{
		{
			desc:       "Reset fail count for existing query",
			sid:        "SID1",
			queryName:  "QUERY1",
			failCounts: map[string]map[string]int{"SID1": {"QUERY1": 1, "QUERY2": 2}},
			want:       map[string]map[string]int{"SID1": {"QUERY1": 0, "QUERY2": 2}},
		},
		{
			desc:       "Reset fail count for non-existing query",
			sid:        "SID1",
			queryName:  "QUERY3",
			failCounts: map[string]map[string]int{"SID1": {"QUERY1": 1, "QUERY2": 2}},
			want:       map[string]map[string]int{"SID1": {"QUERY1": 1, "QUERY2": 2}},
		},
		{
			desc:       "Reset fail count for non-existing SID",
			sid:        "SID2",
			queryName:  "QUERY1",
			failCounts: map[string]map[string]int{"SID1": {"QUERY1": 1, "QUERY2": 2}},
			want:       map[string]map[string]int{"SID1": {"QUERY1": 1, "QUERY2": 2}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			d := &dbManager{
				failCounts: tc.failCounts,
			}
			d.resetFailCount(tc.sid, tc.queryName)
			if diff := cmp.Diff(tc.want, d.failCounts); diff != "" {
				t.Errorf("resetFailCount(%s, %s) returned unexpected diff (-want +got):\n%s", tc.sid, tc.queryName, diff)
			}
		})
	}
}

func TestFailCountFor(t *testing.T) {
	d := &dbManager{
		failCounts: map[string]map[string]int{
			"sid1": {
				"query1": 1,
				"query2": 2,
			},
			"sid2": {
				"query1": 3,
				"query2": 4,
			},
		},
	}

	tests := []struct {
		sid       string
		queryName string
		want      int
	}{
		{
			sid:       "sid1",
			queryName: "query1",
			want:      1,
		},
		{
			sid:       "sid1",
			queryName: "query2",
			want:      2,
		},
		{
			sid:       "sid2",
			queryName: "query1",
			want:      3,
		},
		{
			sid:       "sid2",
			queryName: "query2",
			want:      4,
		},
		{
			sid:       "sid3",
			queryName: "query1",
			want:      0,
		},
	}

	for _, test := range tests {
		if got := d.failCountFor(test.sid, test.queryName); got != test.want {
			t.Errorf("failCountFor(%q, %q) = %d, want %d", test.sid, test.queryName, got, test.want)
		}
	}
}

func TestRefreshDBConnections(t *testing.T) {
	discovery := &odpb.Discovery{
		Databases: []*odpb.Discovery_DatabaseRoot{
			{
				TenancyType: &odpb.Discovery_DatabaseRoot_Db{
					Db: &odpb.Discovery_Database{
						Instances: []*odpb.Discovery_Database_Instance{
							{
								OracleSid: "SID1",
							},
						},
					},
				},
			},
			{
				TenancyType: &odpb.Discovery_DatabaseRoot_Db{
					Db: &odpb.Discovery_Database{
						Instances: []*odpb.Discovery_Database_Instance{
							{
								OracleSid: "SID2",
							},
						},
					},
				},
			},
		},
		Listeners: []*odpb.Discovery_Listener{
			{
				Id: &odpb.Discovery_Listener_ListenerId{
					Alias:      "listener1",
					OracleHome: "ORACLE_HOME1",
				},
				Endpoints: []*odpb.Discovery_Listener_Endpoint{
					{
						Protocol: &odpb.Discovery_Listener_Endpoint_Tcp{
							Tcp: &odpb.Discovery_Listener_TCPProtocol{
								Host: "host1",
								Port: 1234,
							},
						},
					},
				},
				Services: []*odpb.Discovery_Listener_Service{
					{
						Name: "service1",
						Instances: []*odpb.Discovery_Listener_Service_DatabaseInstance{
							{
								Name: "SID1",
							},
						},
					},
				},
			},
			{
				Id: &odpb.Discovery_Listener_ListenerId{
					Alias:      "listener2",
					OracleHome: "ORACLE_HOME2",
				},
				Endpoints: []*odpb.Discovery_Listener_Endpoint{
					{
						Protocol: &odpb.Discovery_Listener_Endpoint_Tcp{
							Tcp: &odpb.Discovery_Listener_TCPProtocol{
								Host: "host2",
								Port: 2345,
							},
						},
					},
				},
				Services: []*odpb.Discovery_Listener_Service{
					{
						Name: "service2",
						Instances: []*odpb.Discovery_Listener_Service_DatabaseInstance{
							{
								Name: "SID2",
							},
						},
					},
				},
			},
		},
	}

	ctx := context.Background()
	c := &MetricCollector{
		dbManager: &dbManager{
			connectionParameters: []connectionParameters{
				{ServiceName: "service1"},
				{ServiceName: "service2"},
			},
			connections: make(map[string]*sql.DB),
		},
	}

	c.RefreshDBConnections(ctx, discovery)
	want := []connectionParameters{
		{ServiceName: "service1", Host: "host1", Port: 1234},
		{ServiceName: "service2", Host: "host2", Port: 2345},
	}
	if diff := cmp.Diff(want, c.dbManager.connectionParameters); diff != "" {
		t.Errorf("RefreshDBConnections(%v) returned diff (-want +got):\n%s", discovery, diff)
	}
}

func TestPopulateConnectionParameters(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name                 string
		discovery            *odpb.Discovery
		connectionParameters []connectionParameters
		want                 []connectionParameters
	}{
		{
			name: "populate host and port",
			discovery: &odpb.Discovery{
				Listeners: []*odpb.Discovery_Listener{
					{
						Services: []*odpb.Discovery_Listener_Service{
							{
								Name: "oracle",
							},
						},
						Endpoints: []*odpb.Discovery_Listener_Endpoint{
							{
								Protocol: &odpb.Discovery_Listener_Endpoint_Tcp{
									Tcp: &odpb.Discovery_Listener_TCPProtocol{
										Host: "127.0.0.1",
										Port: 1521,
									},
								},
							},
						},
					},
				},
			},
			connectionParameters: []connectionParameters{
				{
					ServiceName: "oracle",
				},
			},
			want: []connectionParameters{
				{
					ServiceName: "oracle",
					Host:        "127.0.0.1",
					Port:        1521,
				},
			},
		},
		{
			name: "do not populate host and port",
			discovery: &odpb.Discovery{
				Listeners: []*odpb.Discovery_Listener{
					{
						Services: []*odpb.Discovery_Listener_Service{
							{
								Name: "oracle",
							},
						},
						Endpoints: []*odpb.Discovery_Listener_Endpoint{
							{
								Protocol: &odpb.Discovery_Listener_Endpoint_Tcp{
									Tcp: &odpb.Discovery_Listener_TCPProtocol{
										Host: "127.0.0.1",
										Port: 1521,
									},
								},
							},
						},
					},
				},
			},
			connectionParameters: []connectionParameters{
				{
					ServiceName: "oracle",
					Host:        "localhost",
					Port:        1522,
				},
			},
			want: []connectionParameters{
				{
					ServiceName: "oracle",
					Host:        "localhost",
					Port:        1522,
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := &dbManager{connectionParameters: tc.connectionParameters}
			m.populateConnectionParameters(ctx, tc.discovery)
			if diff := cmp.Diff(tc.want, m.connectionParameters); diff != "" {
				t.Errorf("populateConnectionParameters() returned unexpected diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestShouldRunQuery(t *testing.T) {
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
			if got := shouldRunQuery(query, tc.role); got != tc.want {
				t.Errorf("shouldRunQuery(%v, %v) = %v, want %v", query, tc.role, got, tc.want)
			}
		})
	}
}

func TestShouldRunQueryDisabled(t *testing.T) {
	query := &configpb.Query{
		DatabaseRole: configpb.Query_BOTH,
		Disabled:     proto.Bool(true),
	}
	role := "PHYSICAL STANDBY"
	want := false
	if got := shouldRunQuery(query, role); got != want {
		t.Errorf("shouldRunQuery(%v, %v) = %v, want %v", query, role, got, want)
	}
}

type fakeDB struct {
	err error
}

func (f *fakeDB) PingContext(ctx context.Context) error {
	if f.err != nil {
		return f.err
	}
	return nil
}

func TestShouldSkip(t *testing.T) {
	now := time.Now()
	ctx := context.Background()
	baseDelaySeconds := int64(1)

	tests := []struct {
		name              string
		serviceName       string
		db                dbPinger
		dbManager         *dbManager
		want              bool
		wantFailCount     map[string]map[string]int
		wantNextRetry     map[string]time.Time
		wantSkipMsgLogged map[string]map[string]bool
	}{
		{
			name:        "should not skip if no errors",
			serviceName: "service1",
			db:          &fakeDB{},
			dbManager: &dbManager{
				nextRetry:        map[string]time.Time{},
				skipMsgLogged:    map[string]map[string]bool{},
				baseDelaySeconds: baseDelaySeconds,
			},
			want: false,
			wantSkipMsgLogged: map[string]map[string]bool{
				"service1": map[string]bool{"max_login_failures": false, "other_failures": false},
			},
		},
		{
			name:        "should skip if login failed",
			serviceName: "service1",
			db:          &fakeDB{err: errors.New("ORA-01017: invalid username/password")},
			dbManager: &dbManager{
				failCounts:       map[string]map[string]int{},
				nextRetry:        map[string]time.Time{},
				skipMsgLogged:    map[string]map[string]bool{},
				baseDelaySeconds: baseDelaySeconds,
			},
			want: true,
			wantFailCount: map[string]map[string]int{
				"service1": map[string]int{"login": 1},
			},
			wantNextRetry: map[string]time.Time{
				"service1": now.Add(1 * time.Second),
			},
		},
		{
			name:        "should skip if it's not time to retry yet",
			serviceName: "service1",
			db:          &fakeDB{err: errors.New("ORA-01017: invalid username/password")},
			dbManager: &dbManager{
				failCounts: map[string]map[string]int{},
				nextRetry: map[string]time.Time{
					"service1": now.Add(2 * time.Second),
				},
				skipMsgLogged:    map[string]map[string]bool{},
				baseDelaySeconds: baseDelaySeconds,
			},
			want: true,
			wantNextRetry: map[string]time.Time{
				"service1": now.Add(2 * time.Second),
			},
		},
		{
			name:        "should skip if reached maximum allowed login failures",
			serviceName: "service1",
			db:          &fakeDB{},
			dbManager: &dbManager{
				failCounts: map[string]map[string]int{
					"service1": map[string]int{"login": 9},
				},
				nextRetry: map[string]time.Time{
					"service1": now,
				},
				skipMsgLogged:    map[string]map[string]bool{},
				baseDelaySeconds: baseDelaySeconds,
			},
			want: true,
			wantFailCount: map[string]map[string]int{
				"service1": map[string]int{"login": maxLoginFailures},
			},
			wantNextRetry: map[string]time.Time{
				"service1": now,
			},
			wantSkipMsgLogged: map[string]map[string]bool{
				"service1": map[string]bool{"max_login_failures": true},
			},
		},
		{
			name:        "skip for all other errors",
			serviceName: "service1",
			db:          &fakeDB{err: errors.New("unknown error")},
			dbManager: &dbManager{
				nextRetry:        map[string]time.Time{},
				skipMsgLogged:    map[string]map[string]bool{},
				baseDelaySeconds: baseDelaySeconds,
			},
			want: true,
			wantSkipMsgLogged: map[string]map[string]bool{
				"service1": map[string]bool{"other_failures": true},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.dbManager.shouldSkip(ctx, tc.serviceName, tc.db, now)
			if got != tc.want {
				t.Errorf("shouldSkip(%q, %v) = %t, want %t", tc.serviceName, tc.db, got, tc.want)
			}
			if diff := cmp.Diff(tc.wantFailCount, tc.dbManager.failCounts, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("shouldSkip(%q, %v) resulted in unexpected fail count diff (-want +got):\n%s", tc.serviceName, tc.db, diff)
			}
			if diff := cmp.Diff(tc.wantNextRetry, tc.dbManager.nextRetry, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("shouldSkip(%q, %v) resulted in unexpected next retry time diff (-want +got):\n%s", tc.serviceName, tc.db, diff)
			}
			if diff := cmp.Diff(tc.wantSkipMsgLogged, tc.dbManager.skipMsgLogged, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("shouldSkip(%q, %v) resulted in unexpected skip message logged diff (-want +got):\n%s", tc.serviceName, tc.db, diff)
			}
		})
	}
}

func TestDelay(t *testing.T) {
	tests := []struct {
		name          string
		loginFailures int
		want          time.Duration
	}{
		{
			name:          "No login failures",
			loginFailures: 0,
			want:          0,
		},
		{
			name:          "One login failure",
			loginFailures: 1,
			want:          30 * time.Second,
		},
		{
			name:          "Two login failures",
			loginFailures: 2,
			want:          8 * time.Minute,
		},
		{
			name:          "Three login failures",
			loginFailures: 3,
			want:          40*time.Minute + 30*time.Second,
		},
		{
			name:          "Four login failures",
			loginFailures: 4,
			want:          2*time.Hour + 8*time.Minute,
		},
		{
			name:          "Five login failures",
			loginFailures: 5,
			want:          5*time.Hour + 12*time.Minute + 30*time.Second,
		},
		{
			name:          "Six login failures",
			loginFailures: 6,
			want:          10*time.Hour + 48*time.Minute,
		},
		{
			name:          "Seven login failures",
			loginFailures: 7,
			want:          20*time.Hour + 30*time.Second,
		},
		{
			name:          "Eight login failures",
			loginFailures: 8,
			want:          34*time.Hour + 8*time.Minute,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dbManager := &dbManager{baseDelaySeconds: 30}
			got := dbManager.delay(tc.loginFailures)
			if got != tc.want {
				t.Errorf("delay(%d) = %s, want %s", tc.loginFailures, got, tc.want)
			}
		})
	}
}
