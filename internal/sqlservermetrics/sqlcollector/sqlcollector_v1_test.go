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

func TestPing(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	c := &V1{
		dbConn: db,
	}

	mock.ExpectPing()

	if err := c.Ping(context.Background()); err != nil {
		t.Errorf("Ping() error = %v, want nil", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestExecuteSQL(t *testing.T) {
	ctx := context.Background()
	cols := []string{"id", "name", "value"}
	baseQuery := "SELECT id, name, value FROM test_table"

	testcases := []struct {
		name           string
		mockQueryError error
		expectedRows   *sqlmock.Rows
		expectedResult [][]any
		wantErr        bool
	}{
		{
			name:           "Success_MultipleRows",
			mockQueryError: nil,
			expectedRows: sqlmock.NewRows(cols).
				AddRow(int64(1), "Alice", "data1").
				AddRow(int64(2), "Bob", "data2").
				AddRow(nil, "Charlie", nil), // Testing nil values
			expectedResult: [][]any{
				{int64(1), "Alice", "data1"},
				{int64(2), "Bob", "data2"},
				{nil, "Charlie", nil},
			},
			wantErr: false,
		},
		{
			name:           "Error_QueryFails",
			mockQueryError: errors.New("invalid SQL syntax"),
			expectedRows:   nil,
			expectedResult: nil,
			wantErr:        true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// 1. Setup Mock DB
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
			}
			defer db.Close()

			c := &V1{
				dbConn:  db,
				windows: false,
			}
			// 2. Setup Mock Expectations
			queryExpect := mock.ExpectQuery(baseQuery)
			if tc.mockQueryError != nil {
				queryExpect.WillReturnError(tc.mockQueryError)
			} else if tc.expectedRows != nil {
				queryExpect.WillReturnRows(tc.expectedRows)
			}
			// 3. Execute the function
			result, err := c.ExecuteSQL(ctx, baseQuery)

			// 4. Assertions
			if tc.wantErr {
				if err == nil {
					t.Errorf("ExecuteSQL() expected error, but got none")
				}
				if result != nil {
					t.Errorf("ExecuteSQL() got result %v on error, want nil", result)
				}
			} else {
				if err != nil {
					t.Fatalf("ExecuteSQL() unexpected error: %v", err)
				}
				// Compare successful results
				if len(result) != len(tc.expectedResult) {
					t.Errorf("ExecuteSQL() got %d rows, want %d", len(result), len(tc.expectedResult))
				} else {
					// Check content row by row, cell by cell
					for i, row := range result {
						for j, cell := range row {
							expectedCell := tc.expectedResult[i][j]
							if cell != expectedCell {
								// Note: %v works well for comparing any types, including nil/NULL values
								t.Errorf("ExecuteSQL() row %d, col %d got %v (%T), want %v (%T)",
									i, j, cell, cell, expectedCell, expectedCell)
							}
						}
					}
				}
			}
			// 5. Ensure all expectations were met
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("There were unfulfilled expectations: %s", err)
			}
		})
	}
}
