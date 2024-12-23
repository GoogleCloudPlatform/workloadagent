/*
Copyright 2022 Google LLC

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

package sqlservermetrics

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/GoogleCloudPlatform/workloadagent/internal/sqlservermetrics/guestoscollector"
	"github.com/GoogleCloudPlatform/workloadagent/internal/sqlservermetrics/sqlcollector"
	"github.com/GoogleCloudPlatform/workloadagent/internal/sqlservermetrics/sqlserverutils"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/gce"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/log"
)

// osCollection is the linux implementation of OSCollection.
func (s *SQLServerMetrics) osCollection(ctx context.Context) error {
	if !s.Config.GetCollectionConfiguration().GetCollectGuestOsMetrics() {
		return nil
	}

	wlm, err := s.initCollection(ctx, false)
	if err != nil {
		usagemetrics.Error(usagemetrics.WorkloadManagerConnectionError)
		return err
	}

	log.Logger.Info("Guest os rules collection starts.")
	// only local collection is supported for linux binary.
	// therefore we only get the first credential from cred list and ignore the followings.
	credentialCfg := s.Config.GetCredentialConfigurations()[0]
	guestCfg := guestConfigFromCredential(credentialCfg)
	if err := validateCredCfgGuest(false, !guestCfg.LinuxRemote, guestCfg, credentialCfg.GetVmProperties().GetInstanceId(), credentialCfg.GetVmProperties().GetInstanceName()); err != nil {
		usagemetrics.Error(usagemetrics.SQLServerInvalidConfigurationsError)
		return err
	}

	targetInstanceProps := sip
	disks, err := allDisks(ctx, targetInstanceProps)
	if err != nil {
		return fmt.Errorf("failed to collect disk info: %w", err)
	}

	c := guestoscollector.NewLinuxCollector(disks, "", "", "", false, 22)
	details := []sqlserverutils.MetricDetails{}
	log.Logger.Debug("Collecting guest rules")
	details = append(details, c.CollectGuestRules(ctx, s.Config.GetCollectionTimeout().AsDuration()))
	if err := guestoscollector.UnknownOsFields(&details); err != nil {
		log.Logger.Warnf("RunOSCollection: Failed to mark unknown collected fields. error: %v", err)
	}
	log.Logger.Debug("Collecting guest rules completes")
	updateCollectedData(wlm, sip, targetInstanceProps, details)

	log.Logger.Debugf("Source vm %s is sending os collected data on target machine, %s, to workload manager.", sip.Instance, targetInstanceProps.Instance)
	sendRequestToWLM(ctx, wlm, sip.Name, s.Config.GetMaxRetries(), s.Config.GetRetryFrequency().AsDuration())

	log.Logger.Info("Guest os rules collection ends.")
	return nil
}

// sqlCollection is the linux implementation of SQLCollection.
func (s *SQLServerMetrics) sqlCollection(ctx context.Context) error {
	if !s.Config.GetCollectionConfiguration().GetCollectSqlMetrics() {
		return nil
	}

	wlm, err := s.initCollection(ctx, false)
	if err != nil {
		usagemetrics.Error(usagemetrics.WorkloadManagerConnectionError)
		return err
	}

	log.Logger.Info("Sql rules collection starts.")
	for _, credentialCfg := range s.Config.GetCredentialConfigurations() {
		validationDetails := []sqlserverutils.MetricDetails{}
		guestCfg := guestConfigFromCredential(credentialCfg)
		for _, sqlCfg := range sqlConfigFromCredential(credentialCfg) {
			if err := validateCredCfgSQL(false, !guestCfg.LinuxRemote, sqlCfg, guestCfg, credentialCfg.GetVmProperties().GetInstanceId(), credentialCfg.GetVmProperties().GetInstanceName()); err != nil {
				usagemetrics.Error(usagemetrics.SQLServerInvalidConfigurationsError)
				log.Logger.Errorw("Invalid credential configuration", "error", err)
				continue
			}
			pswd, err := secretValue(ctx, sip.ProjectID, sqlCfg.SecretName)
			if err != nil {
				usagemetrics.Error(usagemetrics.SecretManagerValueError)
				log.Logger.Errorw("Failed to get secret value", "error", err)
				continue
			}
			details, err := func(ctx context.Context, conn string, timeout time.Duration, windows bool) ([]sqlserverutils.MetricDetails, error) {
				c, err := sqlcollector.NewV1("sqlserver", conn, windows)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				// Start db collection.
				log.Logger.Debug("Collecting SQL Server rules.")
				details := c.CollectSQLMetrics(ctx, timeout)
				log.Logger.Debug("Collecting SQL Server rules completes.")
				return details, nil
			}(ctx, fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;", sqlCfg.Host, sqlCfg.Username, pswd, sqlCfg.PortNumber), s.Config.GetCollectionTimeout().AsDuration(), false)

			if err != nil {
				log.Logger.Errorw("Failed to run sql collection", "error", err)
				continue
			}
			for _, detail := range details {
				for _, field := range detail.Fields {
					field["host_name"] = sqlCfg.Host
					field["port_number"] = fmt.Sprintf("%d", sqlCfg.PortNumber)
				}
			}
			addPhysicalDriveLocal(ctx, details, false)

			for i, detail := range details {
				for _, vd := range validationDetails {
					if detail.Name == vd.Name {
						detail.Fields = append(vd.Fields, detail.Fields...)
						details[i] = detail
						break
					}
				}
			}
			validationDetails = details
		}
		targetInstanceProps := sip
		updateCollectedData(wlm, sip, targetInstanceProps, validationDetails)
		log.Logger.Debugf("Source vm %s is sending collected sql data on target machine, %s, to workload manager.", sip.Instance, targetInstanceProps.Instance)
		sendRequestToWLM(ctx, wlm, sip.Name, s.Config.GetMaxRetries(), s.Config.GetRetryFrequency().AsDuration())

	}
	log.Logger.Info("Sql rules collection ends.")
	return nil
}

// allDisks attempts to call compute api to return all possible disks.
func allDisks(ctx context.Context, ip instanceProperties) ([]*sqlserverutils.Disks, error) {
	tempGCE, err := gce.NewGCEClient(ctx)
	if err != nil {
		return nil, err
	}
	instance, err := tempGCE.GetInstance(ip.ProjectID, ip.Zone, ip.InstanceID)
	if err != nil {
		return nil, fmt.Errorf("missing Compute Viewer IAM role for the Service Account. project %v, zone %v, instanceId %v", ip.ProjectID, ip.Zone, ip.InstanceID)
	}
	allDisks := make([]*sqlserverutils.Disks, 0)
	for _, disks := range instance.Disks {
		deviceName, diskType := disks.DeviceName, deviceType(disks.Type)
		allDisks = append(allDisks, &sqlserverutils.Disks{
			DeviceName: deviceName,
			DiskType:   diskType,
			Mapping:    ""})
	}

	return allDisks, nil
}

// DeviceType returns a formatted device type for a given disk type and name.
// The returned device type will be formatted as: "LOCAL-SSD" or "PERSISTENT-SSD". "OTHER" if another disk type
func deviceType(diskType string) string {
	if diskType == "SCRATCH" {
		return guestoscollector.LocalSSD.String()
	} else if strings.Contains(diskType, "PERSISTENT") {
		return guestoscollector.PersistentSSD.String()
	} else {
		return guestoscollector.Other.String()
	}
}
