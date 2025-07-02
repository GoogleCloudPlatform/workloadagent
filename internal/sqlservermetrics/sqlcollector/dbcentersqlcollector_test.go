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

package sqlcollector

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/GoogleCloudPlatform/workloadagent/internal/databasecenter"
)

func TestVersion(t *testing.T) {
	tests := []struct {
		name             string
		timeout          int32
		mockQueryRes     *sqlmock.Rows
		delay            int
		queryErr         bool
		wantMajorVersion string
		wantMinorVersion string
		wantErr          bool
	}{
		{
			name:             "success",
			timeout:          30,
			mockQueryRes:     sqlmock.NewRows([]string{"col1", "col2", "col3"}).AddRow("Microsoft SQL Server 2022 (RTM-CU13) (KB5036432)", "Express Edition (64-bit)", "CU13"),
			delay:            0,
			wantMajorVersion: "SQL Server 2022 Express",
			wantMinorVersion: "CU13",
		},
		{
			name:             "success with non numeric chars in year",
			timeout:          30,
			mockQueryRes:     sqlmock.NewRows([]string{"col1", "col2", "col3"}).AddRow("Microsoft SQL Server 2022() (RTM-CU13) (KB5036432)", "Express Edition (64-bit)", "CU13"),
			delay:            0,
			wantMajorVersion: "SQL Server 2022 Express",
			wantMinorVersion: "CU13",
		},
		{
			name:             "empty result returned when timeout",
			timeout:          3,
			delay:            4,
			mockQueryRes:     sqlmock.NewRows([]string{"col1", "col2", "col3"}).AddRow("Microsoft SQL Server 2022 (RTM-CU13) (KB5036432)", "Express Edition (64-bit)", "CU13"),
			wantMajorVersion: "",
			wantMinorVersion: "",
			wantErr:          true,
		},
		{
			name:             "error caught when sql query returns error",
			timeout:          3,
			delay:            0,
			queryErr:         true,
			wantMajorVersion: "",
			wantMinorVersion: "",
			wantErr:          true,
		},
		{
			name:             "error caught when sql query returns nil",
			timeout:          3,
			delay:            0,
			mockQueryRes:     sqlmock.NewRows(nil),
			wantMajorVersion: "",
			wantMinorVersion: "",
			wantErr:          true,
		},
	}

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	c := &V1{dbConn: db}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Duration(tc.timeout)*time.Second)
			defer cancel()

			if tc.queryErr {
				mock.ExpectQuery(sqlMetrics[versionQueryKey].query).WillReturnError(errors.New("new error"))
			} else {
				mock.ExpectQuery(sqlMetrics[versionQueryKey].query).WillReturnRows(tc.mockQueryRes).WillDelayFor(time.Duration(tc.delay) * time.Second)
			}

			gotMajorVersion, gotMinorVersion, err := Version(ctx, c)
			if (err != nil) != tc.wantErr {
				t.Errorf("version() returned error: %v, want error: %v", err, tc.wantErr)
			}
			if gotMajorVersion != tc.wantMajorVersion {
				t.Errorf("version() returned major version: %v, want major version: %v", gotMajorVersion, tc.wantMajorVersion)
			}
			if gotMinorVersion != tc.wantMinorVersion {
				t.Errorf("version() returned minor version: %v, want minor version: %v", gotMinorVersion, tc.wantMinorVersion)
			}
		})
	}
}

func TestPopulateSignals(t *testing.T) {
	tests := []struct {
		name         string
		timeout      int32
		mockQueryRes []*sqlmock.Rows
		delay        int
		queryErr     bool
		metrics      map[string]string
		wantMetrics  map[string]string
	}{
		{
			name:    "no signals are populated",
			timeout: 30,
			mockQueryRes: []*sqlmock.Rows{
				sqlmock.NewRows([]string{auditingEnabled}).AddRow(1),
				sqlmock.NewRows([]string{allowUnencryptedConn}).AddRow(0),
				sqlmock.NewRows([]string{exposedToBroadAccess}).AddRow(0),
			},
			delay:       0,
			metrics:     map[string]string{},
			wantMetrics: map[string]string{databasecenter.DatabaseAuditingDisabledKey: "false", databasecenter.UnencryptedConnectionsKey: "false", databasecenter.ExposedToPublicAccessKey: "false"},
		},
		{
			name:    "all signals are populated",
			timeout: 30,
			mockQueryRes: []*sqlmock.Rows{
				sqlmock.NewRows([]string{auditingEnabled}).AddRow(0),
				sqlmock.NewRows([]string{allowUnencryptedConn}).AddRow(1),
				sqlmock.NewRows([]string{exposedToBroadAccess}).AddRow(1),
			},
			delay:       0,
			metrics:     map[string]string{},
			wantMetrics: map[string]string{databasecenter.DatabaseAuditingDisabledKey: "true", databasecenter.UnencryptedConnectionsKey: "true", databasecenter.ExposedToPublicAccessKey: "true"},
		},
		{
			name:        "error caught when sql query returns error",
			timeout:     3,
			delay:       0,
			queryErr:    true,
			metrics:     map[string]string{},
			wantMetrics: map[string]string{},
		},
		{
			name:    "error caught when sql query returns nil",
			timeout: 3,
			delay:   0,
			mockQueryRes: []*sqlmock.Rows{
				sqlmock.NewRows(nil),
				sqlmock.NewRows(nil),
				sqlmock.NewRows(nil),
			},
			metrics:     map[string]string{},
			wantMetrics: map[string]string{},
		},
		{
			name:    "empty result returned when timeout",
			timeout: 3,
			delay:   4,
			mockQueryRes: []*sqlmock.Rows{
				sqlmock.NewRows([]string{auditingEnabled}).AddRow(1),
				sqlmock.NewRows([]string{allowUnencryptedConn}).AddRow(1),
				sqlmock.NewRows([]string{exposedToBroadAccess}).AddRow(1),
			},
			metrics:     map[string]string{},
			wantMetrics: map[string]string{},
		},
	}

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	c := &V1{dbConn: db}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Duration(tc.timeout)*time.Second)
			defer cancel()

			if tc.queryErr {
				for i := range tc.mockQueryRes {
					mock.ExpectQuery(sqlMetrics[signals[i]].query).WillReturnError(errors.New("new error"))
				}
			} else {
				for i := range tc.mockQueryRes {
					mock.ExpectQuery(sqlMetrics[signals[i]].query).WillReturnRows(tc.mockQueryRes[i]).WillDelayFor(time.Duration(tc.delay) * time.Second)
				}
			}

			PopulateSignals(ctx, c, tc.metrics)
			if diff := cmp.Diff(tc.wantMetrics, tc.metrics); diff != "" {
				t.Errorf("PopulateSignals() returned diff (-want +got):\n%s", diff)
			}
		})
	}
}
