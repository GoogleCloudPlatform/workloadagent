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

package sqlcollector

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/GoogleCloudPlatform/workloadagent/internal/sqlservermetrics/sqlserverutils"
)

func TestCollectSQLMetrics(t *testing.T) {
	testcases := []struct {
		name         string
		timeout      int32
		delay        int
		mockQueryRes []*sqlmock.Rows
		rule         []SQLMetricsStruct
		want         []sqlserverutils.MetricDetails
		queryError   bool
	}{
		{
			name:    "success",
			timeout: 30,
			delay:   0,

			mockQueryRes: []*sqlmock.Rows{
				sqlmock.NewRows([]string{"col1", "col2"}).AddRow("row1", "val1"),
			},

			rule: []SQLMetricsStruct{
				{
					Name:  "testRule",
					Query: "testQuery",
					Fields: func(fields [][]any) []map[string]string {
						return []map[string]string{
							map[string]string{
								"col1": "row1",
								"col2": "val1",
							},
						}
					},
				},
			},
			want: []sqlserverutils.MetricDetails{
				{
					Name: "testRule",
					Fields: []map[string]string{
						map[string]string{
							"col1": "row1",
							"col2": "val1",
						},
					},
				},
			},
		},
		{
			name:    "empty result returned when timeout",
			timeout: 3,
			delay:   4,
			mockQueryRes: []*sqlmock.Rows{
				sqlmock.NewRows([]string{"col1", "col2"}).AddRow("row1", "val1"),
			},
			rule: []SQLMetricsStruct{
				{
					Name:  "testRule",
					Query: "testQuery",
				},
			},
			want: nil,
		},
		{
			name:    "empty result when rule.Fields() returned empty map",
			timeout: 3,
			mockQueryRes: []*sqlmock.Rows{
				sqlmock.NewRows([]string{"col1", "col2"}).AddRow("row1", "val1"),
			},
			rule: []SQLMetricsStruct{
				{
					Name:   "testRule",
					Query:  "testQuery",
					Fields: func(fields [][]any) []map[string]string { return []map[string]string{} },
				},
			},
			want: []sqlserverutils.MetricDetails{
				{
					Name:   "testRule",
					Fields: []map[string]string{},
				},
			},
		},
		{
			name:    "error caught when sql query returns error",
			timeout: 3,
			delay:   0,
			rule: []SQLMetricsStruct{
				{
					Name:  "testRule",
					Query: "testQuery",
				},
			},
			want:       nil,
			queryError: true,
		},
		{
			name:    "test rule instance_metrics returns proper os type",
			timeout: 30,
			delay:   0,

			mockQueryRes: []*sqlmock.Rows{
				sqlmock.NewRows([]string{"col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10"}).AddRow("val1", "val2", "val3", "val4", "val5", "val6", "val7", "val8", "val9", "val10"),
			},

			rule: []SQLMetricsStruct{
				{
					Name:   "INSTANCE_METRICS",
					Query:  "testQuery",
					Fields: SQLMetrics[8].Fields,
				},
			},
			want: []sqlserverutils.MetricDetails{
				{
					Name: "INSTANCE_METRICS",
					Fields: []map[string]string{
						map[string]string{
							"cores_per_socket":   "unknown",
							"cpu_count":          "unknown",
							"edition":            "val3",
							"hyperthread_ratio":  "unknown",
							"numa_node_count":    "unknown",
							"os":                 "linux",
							"physical_memory_kb": "unknown",
							"product_level":      "val2",
							"product_version":    "val1",
							"socket_count":       "unknown",
							"virtual_memory_kb":  "unknown",
						},
					},
				},
			},
		},
		{
			name:         "empty result returned when query result is nil",
			timeout:      30,
			delay:        0,
			mockQueryRes: []*sqlmock.Rows{sqlmock.NewRows(nil)},
			rule: []SQLMetricsStruct{
				{
					Name:  "INSTANCE_METRICS",
					Query: "testQuery",
				},
			},
			want: nil,
		},
	}

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	c := V1{
		dbConn: db,
	}

	for _, test := range testcases {
		t.Run(test.name, func(t *testing.T) {
			SQLMetrics = test.rule

			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(test.timeout)*time.Second)
			defer cancel()

			if test.queryError {
				mock.ExpectQuery(test.rule[0].Query).WillReturnError(errors.New("new error"))
			} else {
				for i := range test.mockQueryRes {
					mock.ExpectQuery(test.rule[0].Query).WillReturnRows(test.mockQueryRes[i]).WillDelayFor(time.Duration(test.delay) * time.Second)
				}
			}

			r := c.CollectSQLMetrics(ctx, time.Second)
			if diff := cmp.Diff(r, test.want); diff != "" {
				t.Errorf("CollectSQLMetrics() returned wrong result (-got +want):\n%s", diff)
			}
		})
	}
}

func TestNewV1(t *testing.T) {
	testcases := []struct {
		name    string
		driver  string
		wantErr bool
	}{
		{
			name:   "success",
			driver: "sqlserver",
		},
		{
			name:    "error",
			driver:  "any",
			wantErr: true,
		},
	}

	for _, tc := range testcases {
		_, err := NewV1(tc.driver, "", true)
		if gotErr := err != nil; gotErr != tc.wantErr {
			t.Errorf("NewV1() = %v, want error presence = %v", err, tc.wantErr)
		}
	}
}

func TestClose(t *testing.T) {
	c, err := NewV1("sqlserver", "", true)
	if err != nil {
		t.Errorf("NewV1() = %v, want nil", err)
	}
	if err := c.Close(); err != nil {
		t.Errorf("Close() = %v, want nil", err)
	}
}
