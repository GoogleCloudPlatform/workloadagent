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

// Package sqlservermetrics run SQL and OS collections and sends metrics to workload manager.
package sqlservermetrics

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/shirou/gopsutil/v3/disk"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce/metadataserver"

	bo "github.com/cenkalti/backoff/v4"
	retry "github.com/sethvargo/go-retry"
	"github.com/GoogleCloudPlatform/workloadagent/internal/sqlservermetrics/wlm"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce"

	"github.com/GoogleCloudPlatform/workloadagent/internal/sqlservermetrics/sqlserverutils"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
)

// instanceProperties represents properties of instance.
type instanceProperties struct {
	Name          string
	Instance      string
	InstanceID    string
	ProjectID     string
	ProjectNumber string
	Zone          string
	Image         string
}

// SQLServerMetrics contains variables and methods to collect metrics for SQL Server databases.
type SQLServerMetrics struct {
	Config *configpb.SQLServerConfiguration
}

// CollectMetricsOnce collects metrics for SQL Server databases running on the host.
func (s *SQLServerMetrics) CollectMetricsOnce(ctx context.Context) {
	log.Logger.Info("SQLServerMetrics SQL Collection starts.")
	if err := s.sqlCollection(ctx); err != nil {
		log.Logger.Errorw("SQLServerMetrics SQL Collection failed", "error", err)
	}
	log.Logger.Info("SQLServerMetrics SQL Collection ends.")
	log.Logger.Info("SQLServerMetrics OS Collection starts.")
	if err := s.osCollection(ctx); err != nil {
		log.Logger.Errorw("SQLServerMetrics OS Collection failed", "error", err)
	}
	log.Logger.Info("SQLServerMetrics OS Collection ends.")
}

// sip is the source instance properties.
var sip instanceProperties = func() instanceProperties {
	properties := metadataserver.ReadCloudPropertiesWithRetry(bo.NewConstantBackOff(30 * time.Second))
	location := string(properties.Zone[0:strings.LastIndex(properties.Zone, "-")])
	name := fmt.Sprintf("projects/%s/locations/%s", properties.ProjectID, location)
	return instanceProperties{
		Name:          name,
		ProjectID:     properties.ProjectID,
		ProjectNumber: properties.NumericProjectID,
		InstanceID:    properties.InstanceID,
		Instance:      properties.InstanceName,
		Zone:          properties.Zone,
		Image:         properties.Image,
	}
}()

// // initCollection executes steps for initializing a collection.
// // The func is called at the beginning of every guest and sql collection.
func (s *SQLServerMetrics) initCollection(ctx context.Context, windows bool) (*wlm.WLM, error) {
	if s.Config.GetRemoteCollection() && !windows {
		return nil, fmt.Errorf("remote collection from a linux vm is not supported; please use a windows vm to collect on other remote machines or turn off the remote collection flag")
	}

	if len(s.Config.GetCredentialConfigurations()) == 0 {
		return nil, fmt.Errorf("empty credentials")
	}

	wlm, err := wlm.NewWorkloadManager(ctx)
	if err != nil {
		return nil, err
	}

	return wlm, nil
}

// ValidateCredCfgSQL validates if the configuration file is valid for SQL collection.
// Each CredentialConfiguration must provide valid "user_name", "secret_name" and "port_number".
// If remote collection is enabled, the following fields must be provided:
//
//	"host", "instance_id", "instance_name"
func validateCredCfgSQL(remote, windows bool, sqlCfg *sqlserverutils.SQLConfig, guestCfg *sqlserverutils.GuestConfig, instanceID, instanceName string) error {
	errMsg := "invalid value for"
	hasError := false

	if sqlCfg.Username == "" {
		errMsg = errMsg + ` "user_name"`
		hasError = true
	}
	if sqlCfg.SecretName == "" {
		errMsg = errMsg + ` "secret_name"`
		hasError = true
	}
	if sqlCfg.PortNumber == 0 {
		errMsg = errMsg + ` "port_number"`
		hasError = true
	}

	if remote {
		if sqlCfg.Host == "" {
			errMsg = errMsg + ` "host"`
			hasError = true
		}
		if guestCfg.ServerName == "" {
			errMsg = errMsg + ` "server_name"`
			hasError = true
		}
		if guestCfg.GuestUserName == "" {
			errMsg = errMsg + ` "guest_user_name"`
			hasError = true
		}
		if windows && guestCfg.GuestSecretName == "" {
			errMsg = errMsg + ` "guest_secret_name"`
			hasError = true
		}
		if instanceID == "" {
			errMsg = errMsg + ` "instance_id"`
			hasError = true
		}
		if instanceName == "" {
			errMsg = errMsg + ` "instance_name"`
			hasError = true
		}
		if !windows {
			if guestCfg.LinuxSSHPrivateKeyPath == "" {
				errMsg = errMsg + ` "linux_ssh_private_key_path"`
				hasError = true
			}
			if guestCfg.GuestPortNumber == 0 {
				errMsg = errMsg + ` "guest_port_number"`
				hasError = true
			}
		}
	}

	if hasError {
		return errors.New(errMsg)
	}
	return nil
}

// ValidateCredCfgGuest validates if the configuration file is valid for guest collection.
// If remote collection is enabled, the following fields must be provided:
// "server_name", "guest_user_name", "guest_secret_name", "instance_id", "instance_name"
func validateCredCfgGuest(remote, windows bool, guestCfg *sqlserverutils.GuestConfig, instanceID, instanceName string) error {
	errMsg := "invalid value for"
	hasError := false

	if remote {
		if guestCfg.ServerName == "" {
			errMsg = errMsg + ` "server_name"`
			hasError = true
		}
		if guestCfg.GuestUserName == "" {
			errMsg = errMsg + ` "guest_user_name"`
			hasError = true
		}
		if windows && guestCfg.GuestSecretName == "" {
			errMsg = errMsg + ` "guest_secret_name"`
			hasError = true
		}
		if instanceID == "" {
			errMsg = errMsg + ` "instance_id"`
			hasError = true
		}
		if instanceName == "" {
			errMsg = errMsg + ` "instance_name"`
			hasError = true
		}
		if !windows {
			if guestCfg.LinuxSSHPrivateKeyPath == "" {
				errMsg = errMsg + ` "linux_ssh_private_key_path"`
				hasError = true
			}
			if guestCfg.GuestPortNumber == 0 {
				errMsg = errMsg + ` "guest_port_number"`
				hasError = true
			}
		}
	}

	if hasError {
		return errors.New(errMsg)
	}
	return nil
}

// secretValue gets secret value from Secret Manager.
func secretValue(ctx context.Context, projectID string, secretName string) (string, error) {
	gceClient, err := gce.NewGCEClient(ctx)
	if err != nil {
		return "", err
	}
	return gceClient.GetSecret(ctx, projectID, secretName)
}

// updateCollectedData constructs writeinsightrequest from given collected details.
// The func will be called by both guest and sql collections.
func updateCollectedData(wlmService wlm.WorkloadManagerService, sourceProps, targetProps instanceProperties, details []sqlserverutils.MetricDetails) {
	sqlservervalidation := wlm.InitializeSQLServerValidation(sourceProps.ProjectID, targetProps.Instance)
	sqlservervalidation = wlm.UpdateValidationDetails(sqlservervalidation, details)
	writeInsightRequest := wlm.InitializeWriteInsightRequest(sqlservervalidation, targetProps.InstanceID)
	writeInsightRequest.Insight.SentTime = time.Now().Format(time.RFC3339)
	// update wlmService.Request to writeInsightRequest
	wlmService.UpdateRequest(writeInsightRequest)
}

// sendRequestToWLM sends request to workloadmanager.
func sendRequestToWLM(ctx context.Context, wlmService wlm.WorkloadManagerService, location string, retries int32, retryFrequency time.Duration) {
	sendRequest := func(ctx context.Context) error {
		if _, err := wlmService.SendRequest(location); err != nil {
			return err
		}
		return nil
	}
	if err := retry.Do(ctx, retry.WithMaxRetries(uint64(retries), retry.NewConstant(retryFrequency)), sendRequest); err != nil {
		log.Logger.Errorw("Failed to retry sending request to workload manager", "error", err)
	}
}

// addPhysicalDriveLocal starts physical drive to physical path mapping
func addPhysicalDriveLocal(ctx context.Context, details []sqlserverutils.MetricDetails, windows bool) {
	for _, detail := range details {
		if detail.Name != "DB_LOG_DISK_SEPARATION" {
			continue
		}
		for _, field := range detail.Fields {
			physicalPath, pathExists := field["physical_name"]
			if !pathExists {
				log.Logger.Warn("physical_name field for DB_LOG_DISK_SEPARATION does not exist")
				continue
			}
			field["physical_drive"] = getPhysicalDriveFromPath(physicalPath, windows)
		}
	}
}

// getPhysicalDriveFromPath gets the physical drive associated with a file path for linux and windows env
func getPhysicalDriveFromPath(path string, windows bool) string {
	if path == "" {
		return "unknown"
	}
	// disk label is not implemented for windows, so we use the drive letter instead
	if windows {
		mapping := strings.Split(path, `:`)
		if len(mapping) <= 1 {
			log.Logger.Warn("Couldn't find windows drive associated with the physical path name.")
			return "unknown"
		}
		return mapping[0]
	}

	label, err := disk.Label(path)
	if err != nil {
		log.Logger.Warnf("Failed to get disk label for path %s. error: %v", path, err)
		return "unknown"
	}
	return label
}

// sqlConfigFromCredential.
func sqlConfigFromCredential(cred *configpb.SQLServerConfiguration_CredentialConfiguration) []*sqlserverutils.SQLConfig {
	var sqlConfigs []*sqlserverutils.SQLConfig
	for _, sqlCfg := range cred.GetConnectionParameters() {
		sqlConfigs = append(sqlConfigs, &sqlserverutils.SQLConfig{
			Host:       sqlCfg.GetHost(),
			Username:   sqlCfg.GetUsername(),
			SecretName: sqlCfg.GetSecret().GetSecretName(),
			PortNumber: sqlCfg.GetPort(),
		})
	}
	return sqlConfigs
}

// guestConfigFromCredential.
func guestConfigFromCredential(cred *configpb.SQLServerConfiguration_CredentialConfiguration) *sqlserverutils.GuestConfig {
	switch cred.GetGuestConfigurations().(type) {
	case *configpb.SQLServerConfiguration_CredentialConfiguration_RemoteWin:
		return &sqlserverutils.GuestConfig{
			ServerName:      cred.GetRemoteWin().GetConnectionParameters().GetHost(),
			GuestUserName:   cred.GetRemoteWin().GetConnectionParameters().GetUsername(),
			GuestSecretName: cred.GetRemoteWin().GetConnectionParameters().GetSecret().GetSecretName(),
		}
	case *configpb.SQLServerConfiguration_CredentialConfiguration_RemoteLinux:
		return &sqlserverutils.GuestConfig{
			ServerName:             cred.GetRemoteLinux().GetConnectionParameters().GetHost(),
			GuestUserName:          cred.GetRemoteLinux().GetConnectionParameters().GetUsername(),
			GuestPortNumber:        cred.GetRemoteLinux().GetConnectionParameters().GetPort(),
			LinuxRemote:            true,
			LinuxSSHPrivateKeyPath: cred.GetRemoteLinux().GetLinuxSshPrivateKeyPath(),
		}
	}
	return &sqlserverutils.GuestConfig{}
}
