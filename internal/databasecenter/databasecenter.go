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

// Package databasecenter provides functionality to send metadata to Database Center.
package databasecenter

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	anypb "google.golang.org/protobuf/types/known/anypb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	"github.com/GoogleCloudPlatform/agentcommunication_client"
	compute "google.golang.org/api/compute/v1"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/communication"
	wlagce "github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
	dcpb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/databasecenter"
)

const (
	endpoint = "" // endpoint override for database center, don't set if not needed
	// DB Center UAP channel name.
	channel = "databasecenter.googleapis.com/dbcenter-prod"
	// SQLFlexInstanceIDLabelKey is the GCE VM label for the SQL Flex instance ID.
	SQLFlexInstanceIDLabelKey = "sql-flex-instance-id"
	// MajorVersionKey is the key for the major version in metrics.
	MajorVersionKey = "major_version"
	// MinorVersionKey is the key for the minor version in metrics.
	MinorVersionKey = "minor_version"
	// NoRootPasswordKey is the key for the no root password signal in metrics.
	NoRootPasswordKey = "no_root_password"
	// ExposedToPublicAccessKey is the key for the exposed to public access signal in metrics.
	ExposedToPublicAccessKey = "exposed_to_public_access"
	// UnencryptedConnectionsKey is the key for the unencrypted connections signal in metrics.
	UnencryptedConnectionsKey = "unencrypted_connections"
	// DatabaseAuditingDisabledKey is the key for the database auditing disabled signal in metrics.
	DatabaseAuditingDisabledKey = "database_auditing_disabled"
	// LastBackupOldKey is the key for the last backup old signal in metrics.
	LastBackupOldKey = "last_backup_old"
	// NotProtectedByAutomaticFailoverKey is the key for the not protected by automatic failover signal in metrics.
	NotProtectedByAutomaticFailoverKey = "not_protected_by_automatic_failover"
	// NoAutomatedBackupPolicyKey is the key for the no automated backup policy signal in metrics.
	NoAutomatedBackupPolicyKey = "no_automated_backup_policy"
)

// EngineType is an enum for the type of database engine.
type EngineType string

const (
	// UNKNOWN  engine type.
	UNKNOWN EngineType = "UNKNOWN"
	// MYSQL engine type.
	MYSQL EngineType = "MYSQL"
	// POSTGRES engine type.
	POSTGRES EngineType = "POSTGRES"
	// SQLSERVER engine type.
	SQLSERVER EngineType = "SQLSERVER"
)

// DBCenterMetrics is a struct for database center metrics.
type DBCenterMetrics struct {
	EngineType EngineType
	Metrics    map[string]string
}

// CommunicationClient is an interface for communication client.
type CommunicationClient interface {
	EstablishACSConnection(ctx context.Context, endpoint string, channel string) (*client.Connection, error)
	SendAgentMessage(ctx context.Context, agentType string, messageType string, msg *anypb.Any, conn *client.Connection) error
}

// Client for sending metadata to database center.
type Client interface {
	SendMetadataToDatabaseCenter(ctx context.Context, metrics DBCenterMetrics) error
}

// gceClient provides an interface for interacting with the Google Compute Engine API.
type gceClient interface {
	GetInstance(project, zone, instance string) (*compute.Instance, error)
}

// newGCEClientFn creates a new GCE client instance.
var newGCEClientFn = func(ctx context.Context) (gceClient, error) {
	return wlagce.NewGCEClient(ctx)
}

// Client for sending metadata to database center.
type realClient struct {
	Config     *configpb.Configuration
	CommClient CommunicationClient
	conn       *client.Connection
}

// cloudSQLInstanceID returns the Cloud SQL instance ID from the VM's labels if present.
func cloudSQLInstanceID(ctx context.Context, config *configpb.Configuration) string {
	cloudProperties := config.GetCloudProperties()
	if cloudProperties == nil {
		return ""
	}
	projectID := cloudProperties.GetProjectId()
	zone := cloudProperties.GetZone()
	instanceName := cloudProperties.GetInstanceName()
	if projectID == "" || zone == "" || instanceName == "" {
		return ""
	}

	gceClient, err := newGCEClientFn(ctx)
	if err != nil {
		log.CtxLogger(ctx).Debugf("Failed to create GCE client to fetch VM labels: %v.", err)
		return ""
	}
	instance, err := gceClient.GetInstance(projectID, zone, instanceName)
	if err != nil {
		log.CtxLogger(ctx).Debugf("Failed to fetch instance from GCE to read labels: %v.", err)
		return ""
	}
	if instance != nil && instance.Labels[SQLFlexInstanceIDLabelKey] != "" {
		id := instance.Labels[SQLFlexInstanceIDLabelKey]
		log.CtxLogger(ctx).Infof("Found label %q=%q; treating instance as Cloud SQL.", SQLFlexInstanceIDLabelKey, id)
		return id
	}
	return ""
}

// NewClient creates a new database center client.
func NewClient(config *configpb.Configuration, commClient CommunicationClient) Client {
	if commClient == nil {
		commClient = &realCommunicationClient{}
	}
	return &realClient{
		Config:     config,
		CommClient: commClient,
	}
}

type realCommunicationClient struct{}

func (r *realCommunicationClient) EstablishACSConnection(ctx context.Context, endpoint string, channel string) (*client.Connection, error) {
	conn := communication.EstablishACSConnection(ctx, endpoint, channel)
	if conn == nil {
		return nil, fmt.Errorf("failed to establish ACS connection")
	}
	return conn, nil
}

func (r *realCommunicationClient) SendAgentMessage(ctx context.Context, agentType string, messageType string, msg *anypb.Any, conn *client.Connection) error {
	return communication.SendAgentMessage(ctx, agentType, messageType, msg, conn)
}

func (c *realClient) getEngineType(metrics DBCenterMetrics) dcpb.Engine {
	switch metrics.EngineType {
	case MYSQL:
		return dcpb.Engine_ENGINE_MYSQL
	case POSTGRES:
		return dcpb.Engine_ENGINE_POSTGRES
	case SQLSERVER:
		return dcpb.Engine_ENGINE_SQL_SERVER
	default:
		return dcpb.Engine_ENGINE_UNSPECIFIED
	}
}

// buildDatabaseResourceMetadataMessage builds the snapshot message.
func (c *realClient) buildDatabaseResourceMetadataMessage(ctx context.Context, metrics DBCenterMetrics) (*anypb.Any, string, error) {
	cloudProps := c.Config.GetCloudProperties()
	feedTime := timestamppb.New(time.Now())
	productType := dcpb.ProductType_PRODUCT_TYPE_COMPUTE_ENGINE
	resourceName := "//compute.googleapis.com/projects/" + cloudProps.GetProjectId() + "/zones/" + cloudProps.GetZone() + "/instances/" + cloudProps.GetInstanceName()
	if sqlFlexInstanceID := cloudSQLInstanceID(ctx, c.Config); sqlFlexInstanceID != "" {
		productType = dcpb.ProductType_PRODUCT_TYPE_CLOUD_SQL
		resourceName = "//cloudsql.googleapis.com/projects/" + cloudProps.GetProjectId() + "/instances/" + sqlFlexInstanceID
	}
	// construct an object of DatabaseResourceFeed proto.
	body, err := anypb.New(&dcpb.DatabaseResourceFeed{
		FeedTimestamp: feedTime,
		FeedType:      dcpb.DatabaseResourceFeed_RESOURCE_METADATA,
		Content: &dcpb.DatabaseResourceFeed_ResourceMetadata{
			ResourceMetadata: &dcpb.DatabaseResourceMetadata{
				Id: &dcpb.DatabaseResourceId{
					Provider:     dcpb.DatabaseResourceId_GCP,
					UniqueId:     cloudProps.GetInstanceId(),
					ResourceType: "compute.googleapis.com/Instance",
				},
				ResourceName: resourceName,

				ResourceContainer: "projects/" + cloudProps.GetNumericProjectId(),
				Location:          cloudProps.GetRegion(),
				CreationTime:      feedTime,
				UpdationTime:      feedTime,
				ExpectedState:     dcpb.DatabaseResourceMetadata_HEALTHY,
				CurrentState:      dcpb.DatabaseResourceMetadata_HEALTHY,
				InstanceType:      dcpb.InstanceType_SUB_RESOURCE_TYPE_PRIMARY,
				Product: &dcpb.Product{
					Type:         productType,
					Engine:       c.getEngineType(metrics),
					Version:      metrics.Metrics[MajorVersionKey],
					MinorVersion: metrics.Metrics[MinorVersionKey],
				},
				MachineConfiguration: &dcpb.MachineConfiguration{
					VcpuCount:         float64(cloudProps.GetVcpuCount()),
					MemorySizeInBytes: cloudProps.GetMemorySizeMb() * 1024 * 1024, // convert memory size to bytes
				},
			},
		},
	})

	if err != nil {
		return nil, "", fmt.Errorf("unable to create DatabaseResourceFeed: %w", err)
	}
	log.CtxLogger(ctx).Debugf("Sending message databaseresourcefeed: %v", body)
	return body, resourceName, nil
}

// Get the signal type from the metric key
func (c *realClient) getSignalType(key string) dcpb.SignalType {
	switch key {
	case NoRootPasswordKey:
		return dcpb.SignalType_SIGNAL_TYPE_NO_ROOT_PASSWORD
	case ExposedToPublicAccessKey:
		return dcpb.SignalType_SIGNAL_TYPE_EXPOSED_TO_PUBLIC_ACCESS
	case UnencryptedConnectionsKey:
		return dcpb.SignalType_SIGNAL_TYPE_UNENCRYPTED_CONNECTIONS
	case DatabaseAuditingDisabledKey:
		return dcpb.SignalType_SIGNAL_TYPE_DATABASE_AUDITING_DISABLED
	case LastBackupOldKey:
		return dcpb.SignalType_SIGNAL_TYPE_LAST_BACKUP_OLD
	case NotProtectedByAutomaticFailoverKey:
		return dcpb.SignalType_SIGNAL_TYPE_NOT_PROTECTED_BY_AUTOMATIC_FAILOVER
	case NoAutomatedBackupPolicyKey:
		return dcpb.SignalType_SIGNAL_TYPE_NO_AUTOMATED_BACKUP_POLICY
	default:
		return dcpb.SignalType_SIGNAL_TYPE_UNSPECIFIED
	}
}

// Get the signal value from the metric value
func (c *realClient) getSignalValue(value string) bool {
	return value == "true"
}

// buildConfigBasedSignalMessage builds the config based signal message.
func (c *realClient) buildConfigBasedSignalMessage(ctx context.Context, key string, value string) (*anypb.Any, error) {
	cloudProps := c.Config.GetCloudProperties()
	feedTime := timestamppb.New(time.Now())
	// construct an object of DatabaseResourceFeed proto.
	body, err := anypb.New(&dcpb.DatabaseResourceFeed{
		FeedTimestamp: feedTime,
		FeedType:      dcpb.DatabaseResourceFeed_CONFIG_BASED_SIGNAL_DATA,
		Content: &dcpb.DatabaseResourceFeed_ConfigBasedSignalData{
			ConfigBasedSignalData: &dcpb.ConfigBasedSignalData{
				ResourceId: &dcpb.DatabaseResourceId{
					Provider:     dcpb.DatabaseResourceId_GCP,
					UniqueId:     cloudProps.GetInstanceId(),
					ResourceType: "compute.googleapis.com/Instance",
				},
				FullResourceName: "//compute.googleapis.com/projects/" + cloudProps.GetProjectId() + "/zones/" + cloudProps.GetZone() + "/instances/" + cloudProps.GetInstanceName(),

				LastRefreshTime: feedTime,
				SignalType:      c.getSignalType(key),
				SignalMetadata:  &dcpb.ConfigBasedSignalData_SignalBoolValue{SignalBoolValue: c.getSignalValue(value)},
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create DatabaseResourceFeed: %v", err)
	}
	log.CtxLogger(ctx).Debugf("Sending message configbasedsignal: %v", body)
	return body, nil
}

// isCloudSQLResource reports whether the given resource name represents a Cloud SQL resource.
func isCloudSQLResource(resourceName string) bool {
	return strings.HasPrefix(resourceName, "//cloudsql.googleapis.com/")
}

// SendMetadataToDatabaseCenter sends metadata to database center.
func (c *realClient) SendMetadataToDatabaseCenter(ctx context.Context, metrics DBCenterMetrics) error {
	flag.Parse()
	log.CtxLogger(ctx).Debugw("Sending metadata to database center")
	client.DebugLogging = true
	// establish connection with UAP channel if not already established.
	if c.conn == nil {
		conn, err := c.CommClient.EstablishACSConnection(ctx, endpoint, channel)
		if err != nil {
			return fmt.Errorf("failed to establish ACS connection: %v", err)
		}
		c.conn = conn
	}

	msg, resourceName, err := c.buildDatabaseResourceMetadataMessage(ctx, metrics)
	if err != nil {
		return fmt.Errorf("failed to build database resource metadata message: %w", err)
	}
	err = c.CommClient.SendAgentMessage(ctx, string(metrics.EngineType), "databaseresourcefeed", msg, c.conn)
	if err != nil {
		return fmt.Errorf("failed to send metadata message to database center: %v", err)
	}

	isCloudSQL := isCloudSQLResource(resourceName)
	log.CtxLogger(ctx).Debugf("Send signals to database center")
	for key, value := range metrics.Metrics {
		log.CtxLogger(ctx).Debugf("Key: %v, Value: %v", key, value)
		// Skip major version and minor version signals.
		if key == MajorVersionKey || key == MinorVersionKey {
			continue
		}
		// Skip certain signals for Cloud SQL.
		if isCloudSQL && (key == ExposedToPublicAccessKey || key == UnencryptedConnectionsKey) {
			continue
		}
		msg, err := c.buildConfigBasedSignalMessage(ctx, key, value)
		if err != nil {
			return fmt.Errorf("failed to build config based signal message: %v", err)
		}
		err = c.CommClient.SendAgentMessage(ctx, string(metrics.EngineType), "configbasedsignal", msg, c.conn)
		if err != nil {
			return fmt.Errorf("failed to send config based signal message to database center: %v", err)
		}
	}
	return nil
}
