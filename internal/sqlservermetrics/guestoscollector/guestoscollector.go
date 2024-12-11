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

package guestoscollector

import (
	"context"
	"fmt"
	"time"

	"github.com/GoogleCloudPlatform/workloadagent/internal/sqlservermetrics/sqlserverutils"
)

// Guest OS Metrics that are in SQL Server validations.
const (
	// PowerProfileSetting used for power profile of machine.
	PowerProfileSetting = "power_profile_setting"
	// LocalSSD used to connect physical drive to disk type.
	UseLocalSSD = "local_ssd"
	// DataDiskAllocationUnits used to see blocksize of a physical drive.
	DataDiskAllocationUnits = "data_disk_allocation_units"
	// GCBDRAgentRunning used for checking if GCBDRAgentRunning is running on the target.
	GCBDRAgentRunning = "gcbdr_agent_running"
)

// DiskTypeEnum enum used for disktypes to keep linux and windows collection consistent .
type DiskTypeEnum int

// DiskType enum values.
const (
	// LocalSSD - local disk
	LocalSSD DiskTypeEnum = iota
	// PersistentSSD - persistent disk
	PersistentSSD
	// Other - not local or persistent disk but still a valid disk type
	Other
)

func (disk DiskTypeEnum) String() string {
	return []string{"LOCAL-SSD", "PERSISTENT-SSD", "OTHER"}[disk]
}

// GuestCollector interface.
type GuestCollector interface {
	CollectGuestRules(context.Context, time.Duration) sqlserverutils.MetricDetails
}

// CollectionOSFields returns all expected fields in OS collection
func CollectionOSFields() []string {
	allOSFields := []string{
		PowerProfileSetting,
		UseLocalSSD,
		DataDiskAllocationUnits,
		GCBDRAgentRunning,
	}
	return append([]string(nil), allOSFields...)
}

// UnknownOsFields checks the collected os fields; if nil or missing, then the data is marked as unknown
func UnknownOsFields(details *[]sqlserverutils.MetricDetails) error {
	if len(*details) != 1 {
		return fmt.Errorf("CheckOSCollectedMetrics details should have only 1 field for OS collection, got %d", len(*details))
	}
	detail := (*details)[0]
	if detail.Name != "OS" {
		return fmt.Errorf("CheckOSCollectedMetrics details.name should be collecting for OS, got %s", detail.Name)
	}
	if len(detail.Fields) > 1 {
		return fmt.Errorf("CheckOSCollectedMetrics details.fields should have 1 field in OS collection, got %d", len(detail.Fields))
	}

	if len(detail.Fields) == 0 {
		fields := map[string]string{
			PowerProfileSetting:     "unknown",
			UseLocalSSD:             "unknown",
			DataDiskAllocationUnits: "unknown",
			GCBDRAgentRunning:       "unknown",
		}
		(*details)[0].Fields = append((*details)[0].Fields, fields)
		return nil
	}

	// for os collection, details only has one element and details.Fields only has one element
	// sql collections is different as there can be multiple details and multiple details.Fields
	for _, field := range CollectionOSFields() {
		_, ok := detail.Fields[0][field]
		if !ok {
			(*details)[0].Fields[0][field] = "unknown"
		}
	}
	return nil
}
