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
	"fmt"
	"strings"
)

var (
	versionQueryKey  = "version query key"
	rawVersionString = "raw_version_string"
	rawEditionString = "raw_edition_string"
	cuNumber         = "cu_number"

	sqlMetrics = map[string]SQLMetricsStruct{
		versionQueryKey: {
			// https://cloud.google.com/sql/docs/sqlserver/db-versions#database-version-support
			// Major Version: "SQL Server <Year> <Edition>"
			// Minor Version: "CUXX" or "RTM" etc.
			Query: `SELECT
	CONVERT(NVARCHAR(MAX), @@VERSION) AS raw_version_string,
	CONVERT(NVARCHAR(MAX), SERVERPROPERTY('Edition')) AS raw_edition_string,
	ISNULL(CONVERT(NVARCHAR(128), SERVERPROPERTY('ProductUpdateLevel')), 'RTM') AS cu_number;`,
			Fields: func(rows [][]any) []map[string]string {
				var res []map[string]string
				for _, row := range rows {
					res = append(res, map[string]string{
						rawVersionString: handleNilString(row[0]),
						rawEditionString: handleNilString(row[1]),
						cuNumber:         handleNilString(row[2]),
					})
				}
				return res
			},
		},
	}
)

// Version returns the major and minor version of the SQL Server.
func Version(ctx context.Context, c *V1) (string, string, error) {
	queryResult, err := c.executeSQL(ctx, sqlMetrics[versionQueryKey].Query)
	if err != nil {
		return "", "", fmt.Errorf("failed to run sql query: %v", err)
	}

	fields := sqlMetrics[versionQueryKey].Fields(queryResult)
	if fields == nil {
		return "", "", fmt.Errorf("Empty query result")
	}
	rawVersionData := fields[0]
	if rawVersionData == nil {
		return "", "", fmt.Errorf("Empty query result")
	}
	rawVersionString := rawVersionData[rawVersionString]
	rawEditionString := rawVersionData[rawEditionString]
	minorVersion := rawVersionData[cuNumber]

	// Get Year (4th word from @@VERSION)
	// Version string looks like: Microsoft SQL Server 2022 (RTM-CU13) (KB5036432)
	versionFields := strings.Fields(rawVersionString)
	year := ""
	if len(versionFields) >= 4 { // Check if there are at least 4 words
		// The 4th word could be "2019" or "2019(" depending on exact @@VERSION format.
		// Trim non-numeric characters from the end to get just the year digits.
		cleanedYear := strings.TrimRightFunc(versionFields[3], func(r rune) bool {
			return r < '0' || r > '9'
		})
		if len(cleanedYear) == 4 { // Validation for a 4-digit year
			year = cleanedYear
		}
	}

	// Get Edition (1st word from SERVERPROPERTY('Edition'))
	// Edition string looks like: Express Edition (64-bit)
	editionFields := strings.Fields(rawEditionString)
	edition := ""
	if len(editionFields) > 0 {
		edition = editionFields[0]
	}

	// Format: "SQL Server <Year> <Edition>"
	majorVersion := fmt.Sprintf("SQL Server %s %s", year, edition)
	return majorVersion, minorVersion, nil
}
