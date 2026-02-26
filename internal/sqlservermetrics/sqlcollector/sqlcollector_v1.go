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
	"database/sql"
	"time"

	// Required for loading sqlserver driver.
	_ "github.com/microsoft/go-mssqldb"
	"github.com/GoogleCloudPlatform/workloadagent/internal/sqlservermetrics/sqlserverutils"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
)

// V1 that execute cmd and connect to SQL server.
type V1 struct {
	dbConn  *sql.DB
	windows bool
}

// NewV1 initializes a V1 instance.
func NewV1(driver, conn string, windows bool) (*V1, error) {
	dbConn, err := sql.Open(driver, conn)
	if err != nil {
		return nil, err
	}
	return &V1{dbConn: dbConn, windows: windows}, nil
}

// CollectSQLMetrics collects SQL metrics from target sql server.
func (c *V1) CollectSQLMetrics(ctx context.Context, timeout time.Duration) []sqlserverutils.MetricDetails {
	var details []sqlserverutils.MetricDetails
	for _, rule := range SQLMetrics {
		func() {
			ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			queryResult, err := c.ExecuteSQL(ctxWithTimeout, rule.Query)
			if err != nil {
				log.Logger.Errorw("Failed to run sql query", "query", rule.Query, "error", err)
				return
			}
			// queryResult is a 2d array and for most rules there is only one row in the query result.
			// For InstanceMetrics, the query result is in one row and we need to append the os type to the row in queryResult.
			if rule.Name == "INSTANCE_METRICS" {
				if queryResult == nil || len(queryResult) == 0 {
					log.Logger.Errorw("Empty query result", "query", rule.Query)
					return
				}
				os := "windows"
				if !c.windows {
					os = "linux"
				}
				queryResult[0] = append(queryResult[0], os)
			}
			details = append(details, sqlserverutils.MetricDetails{
				Name:   rule.Name,
				Fields: rule.Fields(queryResult),
			})
		}()
	}
	return details
}

// Close closes the database collection.
func (c *V1) Close() error {
	return c.dbConn.Close()
}

// Ping checks the connection to the database.
func (c *V1) Ping(ctx context.Context) error {
	return c.dbConn.PingContext(ctx)
}

func (c *V1) ExecuteSQL(ctx context.Context, query string) ([][]any, error) {
	// Execute query
	rows, err := c.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	width := len(cols)
	var res [][]any
	// Iterate through the result set.
	for rows.Next() {
		row := make([]any, width)
		ptrs := make([]any, width)
		for i := range row {
			ptrs[i] = &row[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		res = append(res, row)

	}
	return res, nil
}

func (c *V1) QueryContext(ctx context.Context, query string) (*sql.Rows, error) {
	err := c.dbConn.PingContext(ctx)
	if err != nil {
		return nil, err
	}
	// Execute query
	return c.dbConn.QueryContext(ctx, query)
}
