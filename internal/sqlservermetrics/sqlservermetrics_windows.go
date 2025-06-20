/*
Copyright 2023 Google LLC

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
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/GoogleCloudPlatform/workloadagent/internal/sqlservermetrics/guestoscollector"
	"github.com/GoogleCloudPlatform/workloadagent/internal/sqlservermetrics/guestoscollector/remote"
	"github.com/GoogleCloudPlatform/workloadagent/internal/sqlservermetrics/sqlcollector"
	"github.com/GoogleCloudPlatform/workloadagent/internal/sqlservermetrics/sqlserverutils"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
)

const (
	commandFind  = `sudo find %s -type f -iname "%s" -print`
	commandDf    = "sudo df --output=target %s | tail -n 1"
	commandMount = "mount | grep sd"
)

// osCollection is the windows implementation of OSCollection.
func (s *SQLServerMetrics) osCollection(ctx context.Context, dwActivated bool) error {
	if !s.Config.GetCollectionConfiguration().GetCollectGuestOsMetrics() {
		return nil
	}
	wlm, err := s.initCollection(ctx, true)
	if err != nil {
		usagemetrics.Error(usagemetrics.WorkloadManagerConnectionError)
		return err
	}

	for _, credentialCfg := range s.Config.GetCredentialConfigurations() {
		guestCfg := guestConfigFromCredential(credentialCfg)
		if err := validateCredCfgGuest(s.Config.GetRemoteCollection(), !guestCfg.LinuxRemote, guestCfg, credentialCfg.GetVmProperties().GetInstanceId(), credentialCfg.GetVmProperties().GetInstanceName()); err != nil {
			usagemetrics.Error(usagemetrics.SQLServerInvalidConfigurationsError)
			log.Logger.Errorw("Invalid credential configuration", "error", err)
			if !s.Config.GetRemoteCollection() {
				break
			}
			continue
		}

		targetInstanceProps := sip
		var c guestoscollector.GuestCollector
		if s.Config.GetRemoteCollection() {
			// remote collection
			targetInstanceProps = instanceProperties{
				InstanceID: credentialCfg.GetVmProperties().GetInstanceId(),
				Instance:   credentialCfg.GetVmProperties().GetInstanceName(),
			}
			host := guestCfg.ServerName
			username := guestCfg.GuestUserName
			if !guestCfg.LinuxRemote {
				log.Logger.Debug("Starting remote win guest collection for ip " + host)
				projectID := guestCfg.ProjectID
				if projectID == "" {
					projectID = sip.ProjectID
				}
				pswd, err := secretValue(ctx, projectID, guestCfg.GuestSecretName)
				if err != nil {
					usagemetrics.Error(usagemetrics.SecretManagerValueError)
					log.Logger.Errorw("Collection failed", "target", guestCfg.ServerName, "error", fmt.Errorf("failed to get secret value: %v", err))
					if !s.Config.GetRemoteCollection() {
						break
					}
					continue
				}
				c = guestoscollector.NewWindowsCollector(host, username, pswd)
			} else {
				// on local windows vm collecting on remote linux vm's, we use ssh, otherwise we use wmi
				log.Logger.Debug("Starting remote linux guest collection for ip " + host)
				// disks only used for local linux collection
				c = guestoscollector.NewLinuxCollector(nil, host, username, guestCfg.LinuxSSHPrivateKeyPath, true, guestCfg.GuestPortNumber)
			}
		} else {
			// local win collection
			log.Logger.Debug("Starting local win guest collection")
			c = guestoscollector.NewWindowsCollector(nil, nil, nil)
		}

		details := []sqlserverutils.MetricDetails{}
		log.Logger.Debug("Collecting guest rules")
		details = append(details, c.CollectGuestRules(ctx, s.Config.GetCollectionTimeout().AsDuration()))
		if err := guestoscollector.UnknownOsFields(&details); err != nil {
			log.Logger.Warnf("RunOSCollection: Failed to mark unknown collected fields. error: %v", err)
		}
		log.Logger.Debug("Collecting guest rules completes")

		log.Logger.Debugf("Source vm %s is sending os collected data on target machine, %s, to workload manager.", sip.Instance, targetInstanceProps.Instance)
		updateCollectedData(wlm, sip, targetInstanceProps, details)
		if !dwActivated {
			log.Logger.Debug("Data Warehouse is not activated, not sending metrics to Data Warehouse")
			continue
		}
		sendRequestToWLM(ctx, wlm, sip.Name, s.Config.GetMaxRetries(), s.Config.GetRetryFrequency().AsDuration())

		// Local collection.
		// Exit the loop. Only take the first credential in the credentialconfiguration array.
		if !s.Config.GetRemoteCollection() {
			break
		}
	}

	return nil
}

// sqlCollection is the windows implementation of SQLCollection.
func (s *SQLServerMetrics) sqlCollection(ctx context.Context, dwActivated bool) error {
	if !s.Config.GetCollectionConfiguration().GetCollectSqlMetrics() {
		return nil
	}

	wlm, err := s.initCollection(ctx, true)
	if err != nil {
		usagemetrics.Error(usagemetrics.WorkloadManagerConnectionError)
		return err
	}

	for _, credentialCfg := range s.Config.GetCredentialConfigurations() {
		validationDetails := []sqlserverutils.MetricDetails{}
		guestCfg := guestConfigFromCredential(credentialCfg)
		for _, sqlCfg := range sqlConfigFromCredential(credentialCfg) {
			if err := validateCredCfgSQL(s.Config.GetRemoteCollection(), !guestCfg.LinuxRemote, sqlCfg, guestCfg, credentialCfg.GetVmProperties().GetInstanceId(), credentialCfg.GetVmProperties().GetInstanceName()); err != nil {
				usagemetrics.Error(usagemetrics.SQLServerInvalidConfigurationsError)
				log.Logger.Errorw("Invalid credential configuration", "error", err)
				continue
			}
			projectID := sqlCfg.ProjectID
			if projectID == "" {
				projectID = sip.ProjectID
			}
			pswd, err := secretValue(ctx, projectID, sqlCfg.SecretName)
			if err != nil {
				usagemetrics.Error(usagemetrics.SecretManagerValueError)
				log.Logger.Errorw("Failed to get secret value", "error", err)
				continue
			}
			conn := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;", sqlCfg.Host, sqlCfg.Username, pswd, sqlCfg.PortNumber)
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
			}(ctx, conn, s.Config.GetCollectionTimeout().AsDuration(), !guestCfg.LinuxRemote)
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

			// getting physical drive if on local windows collecting sql on linux remote
			if s.Config.GetRemoteCollection() && guestCfg.LinuxRemote {
				addPhysicalDriveRemoteLinux(details, guestCfg)
			} else {
				addPhysicalDriveLocal(ctx, details, true)
			}

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
		// update targetInstanceProps value for remote collections.
		if s.Config.GetRemoteCollection() {
			// remote collection
			targetInstanceProps = instanceProperties{
				InstanceID: credentialCfg.GetVmProperties().GetInstanceId(),
				Instance:   credentialCfg.GetVmProperties().GetInstanceName(),
			}
		}
		updateCollectedData(wlm, sip, targetInstanceProps, validationDetails)
		log.Logger.Debugf("Source vm %s is sending collected sql data on target machine, %s, to workload manager.", sip.Instance, targetInstanceProps.Instance)
		if !dwActivated {
			log.Logger.Debug("Data Warehouse is not activated, not sending metrics to Data Warehouse")
			continue
		}
		sendRequestToWLM(ctx, wlm, sip.Name, s.Config.GetMaxRetries(), s.Config.GetRetryFrequency().AsDuration())
	}
	return nil
}

// addPhysicalDriveRemoteLinux adds physical drive to sql collection based off details for windows to remote linux instances
func addPhysicalDriveRemoteLinux(details []sqlserverutils.MetricDetails, cred *sqlserverutils.GuestConfig) {
	user := cred.GuestUserName
	port := cred.GuestPortNumber
	ip := cred.ServerName
	// We need to call NewRemote, SetupKeys and CreateClient respectively to set up the remote correctly.
	r := remote.NewRemote(ip, user, port)
	if err := r.SetupKeys(cred.LinuxSSHPrivateKeyPath); err != nil {
		log.Logger.Errorw("Failed to setup keys.", "error", err)
		return
	}
	if err := r.CreateClient(); err != nil {
		log.Logger.Errorw("Failed to create client.", "error", err)
		return
	}
	defer r.Close()
	for _, detail := range details {
		if detail.Name != "DB_LOG_DISK_SEPARATION" {
			continue
		}
		for _, field := range detail.Fields {
			physicalPath, pathExists := field["physical_name"]
			if !pathExists {
				log.Logger.Warn("physical_name field for DB_LOG_DISK_SEPERATION does not exist")
				continue
			}
			dir, filePath := filepath.Split(physicalPath)
			findCommand := fmt.Sprintf(commandFind, dir, filePath)

			filePath, filePathErr := remote.RunCommandWithPipes(findCommand, r)
			if filePathErr != nil {
				log.Logger.Warnf("Failed to run cmd %v. error: %v", findCommand, filePathErr)
				continue
			}
			filePath = strings.TrimSuffix(filePath, "\n")

			command := fmt.Sprintf(commandDf, filePath)
			physicalPathMount, physicalPathErr := remote.RunCommandWithPipes(command, r)
			if physicalPathErr != nil {
				log.Logger.Warnf("Failed to run cmd %v. error: %v", command, physicalPathErr)
				continue
			}
			physicalPathMount = strings.TrimSuffix(physicalPathMount, "\n")

			resultMount, mountErr := remote.RunCommandWithPipes(commandMount, r)
			if mountErr != nil {
				log.Logger.Warnf("Failed to run cmd %v. error: %v", commandMount, mountErr)
				continue
			}

			allMounts := strings.TrimSuffix(resultMount, "\n")
			physicalDriveHelper := regexp.MustCompile(` `+physicalPathMount+` `).Split(allMounts, -1)

			physicalDrives := []string{}
			for i := 0; i < len(physicalDriveHelper)-1; i++ {
				splitStr := regexp.MustCompile("\n| |/").Split(physicalDriveHelper[i], -1)
				if len(splitStr) < 2 {
					log.Logger.Warn("regex for linux error. Unable to find physical drive associated with mount.")
					continue
				}
				physicalDrives = append(physicalDrives, splitStr[len(splitStr)-2])
			}
			physicalDrive := strings.Join(physicalDrives, ", ")
			field["physical_drive"] = physicalDrive
		}
	}
}
