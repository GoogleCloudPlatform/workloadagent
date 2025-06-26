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
	"time"

	anypb "google.golang.org/protobuf/types/known/anypb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	"github.com/GoogleCloudPlatform/agentcommunication_client"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/communication"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
	dcpb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/databasecenter"
)

const (
	endpoint = "" // endpoint override for database center, don't set if not needed
	// TODO: Update the channel to prod channel once the integration is tested.
	channel = "dbcenter-autopush" // "channel for database center"
	// MajorVersionKey is the key for the major version in metrics.
	MajorVersionKey = "major_version"
	// MinorVersionKey is the key for the minor version in metrics.
	MinorVersionKey = "minor_version"
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

// Client for sending metadata to database center.
type realClient struct {
	Config     *configpb.Configuration
	CommClient CommunicationClient
	conn       *client.Connection
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

// buildCondorMessage builds the snapshot message.
func (c *realClient) buildCondorMessage(ctx context.Context, metrics DBCenterMetrics) (*anypb.Any, error) {
	cloudProps := c.Config.GetCloudProperties()
	feedTime := timestamppb.New(time.Now())
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
				ResourceName:      "//compute.googleapis.com/projects/" + cloudProps.GetProjectId() + "/zones/" + cloudProps.GetZone() + "/instances/" + cloudProps.GetInstanceName(),
				ResourceContainer: "projects/" + cloudProps.GetNumericProjectId(),
				Location:          cloudProps.GetRegion(),
				CreationTime:      feedTime,
				UpdationTime:      feedTime,
				ExpectedState:     dcpb.DatabaseResourceMetadata_HEALTHY,
				CurrentState:      dcpb.DatabaseResourceMetadata_HEALTHY,
				InstanceType:      dcpb.InstanceType_SUB_RESOURCE_TYPE_PRIMARY,
				Product: &dcpb.Product{
					Type:         dcpb.ProductType_PRODUCT_TYPE_COMPUTE_ENGINE,
					Engine:       c.getEngineType(metrics),
					Version:      metrics.Metrics[MajorVersionKey],
					MinorVersion: metrics.Metrics[MinorVersionKey],
				},
			},
		},
	})

	if err != nil {
		return nil, fmt.Errorf("unable to create DatabaseResourceFeed: %v", err)
	}
	log.CtxLogger(ctx).Debugf("Sending message databaseresourcefeed: %v", body)
	return body, nil
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

	msg, err := c.buildCondorMessage(ctx, metrics)
	if err != nil {
		return fmt.Errorf("failed to build condor message: %v", err)
	}
	err = c.CommClient.SendAgentMessage(ctx, "mysql", "databaseresourcefeed", msg, c.conn)
	if err != nil {
		return fmt.Errorf("failed to send message to database center: %v", err)
	}
	return nil
}
