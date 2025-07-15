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

// Package mongodbmetrics implements metric collection for the MongoDB workload agent service.
package mongodbmetrics

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"github.com/GoogleCloudPlatform/workloadagent/internal/databasecenter"
	"github.com/GoogleCloudPlatform/workloadagent/internal/workloadmanager"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/commandlineexecutor"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/secret"
)

const (
	versionKey = "version"
)

type gceInterface interface {
	GetSecret(ctx context.Context, projectID, secretName string) (string, error)
}

// MongoDBMetrics contains variables and methods to collect metrics for MongoDB databases running on the current host.
type MongoDBMetrics struct {
	execute        commandlineexecutor.Execute
	Config         *configpb.Configuration
	mongoClient    *mongo.Client
	WLMClient      workloadmanager.WLMWriter
	DBcenterClient databasecenter.Client
	RunCommand     func(ctx context.Context, client *mongo.Client, dbName string, cmd bson.D, resultStruct any) (any, error)
}

// password gets the password for the MongoDB database.
// If the password is set in the configuration, it is used directly (not recommended).
// Otherwise, if the secret configuration is set, the secret is fetched from GCE.
// Without either, the password is not set and requests to the MongoDB database will fail.
func (m *MongoDBMetrics) password(ctx context.Context, gceService gceInterface) (secret.String, error) {
	pw := ""
	if m.Config.GetMongoDbConfiguration().GetConnectionParameters().GetPassword() != "" {
		return secret.String(m.Config.GetMongoDbConfiguration().GetConnectionParameters().GetPassword()), nil
	}
	secretCfg := m.Config.GetMongoDbConfiguration().GetConnectionParameters().GetSecret()
	if secretCfg.GetSecretName() != "" && secretCfg.GetProjectId() != "" {
		var err error
		pw, err = gceService.GetSecret(ctx, secretCfg.GetProjectId(), secretCfg.GetSecretName())
		if err != nil {
			return secret.String(""), fmt.Errorf("failed to get secret: %w", err)
		}
	}
	return secret.String(pw), nil
}

// New creates a new MongoDBMetrics object initialized with default values.
func New(ctx context.Context, config *configpb.Configuration, wlmClient workloadmanager.WLMWriter, dbcenterClient databasecenter.Client, runCommand func(ctx context.Context, client *mongo.Client, dbName string, cmd bson.D, resultStruct any) (any, error)) *MongoDBMetrics {
	return &MongoDBMetrics{
		execute:        commandlineexecutor.ExecuteCommand,
		Config:         config,
		WLMClient:      wlmClient,
		DBcenterClient: dbcenterClient,
		RunCommand:     runCommand,
	}
}

func pingDB(ctx context.Context, client *mongo.Client) error {
	err := client.Ping(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to ping MongoDB during db connection initialization: %w", err)
	}
	log.CtxLogger(ctx).Info("Successfully pinged MongoDB database.")
	return nil
}

// InitDB initializes the MongoDB database connection.
func (m *MongoDBMetrics) InitDB(ctx context.Context, gceService gceInterface, serverSelectionTimeout time.Duration) error {
	var err error
	// Set client options
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	clientOptions.SetServerSelectionTimeout(serverSelectionTimeout)
	m.mongoClient, err = mongo.Connect(clientOptions)
	if err == nil && pingDB(ctx, m.mongoClient) == nil {
		log.CtxLogger(ctx).Debug("Was able to connect to MongoDB without credentials.")
	}
	// if authorization is enabled then we need to provide the username and password.
	user := m.Config.GetMongoDbConfiguration().GetConnectionParameters().GetUsername()
	pw, err := m.password(ctx, gceService)
	if err != nil {
		return fmt.Errorf("getting password from configuration or secret manager failed: %w", err)
	}
	clientOptions = options.Client().ApplyURI(fmt.Sprintf("mongodb://%s:%s@localhost:27017", user, pw.SecretValue()))
	clientOptions.SetServerSelectionTimeout(serverSelectionTimeout)
	m.mongoClient, err = mongo.Connect(clientOptions)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}
	return pingDB(ctx, m.mongoClient)
}

// DefaultRunCommand is the default implementation of the RunCommand function.
// It uses the Decode method of the SingleResult to decode the result into the receiver.
func DefaultRunCommand(ctx context.Context, client *mongo.Client, dbName string, cmd bson.D, receiver any) (any, error) {
	db := client.Database(dbName)
	err := db.RunCommand(ctx, cmd).Decode(&receiver)
	return receiver, err
}

func (m *MongoDBMetrics) version(ctx context.Context) (string, error) {
	var result any
	res, err := m.RunCommand(ctx, m.mongoClient, "admin", bson.D{bson.E{Key: "buildInfo", Value: 1}}, result)
	if err != nil {
		log.CtxLogger(ctx).Warnf("Failed to get db version: %w", err)
		return "", err
	}
	var version string
	for _, element := range res.(bson.D) {
		if element.Key == "version" {
			version = element.Value.(string)
			break
		}
	}
	if version == "" {
		log.CtxLogger(ctx).Debugw("Version is empty", "document contents", res)
	}
	log.CtxLogger(ctx).Debugf("Version: %s", version)
	return version, nil
}

// CollectMetricsOnce collects metrics for MongoDB databases running on the host.
func (m *MongoDBMetrics) CollectMetricsOnce(ctx context.Context, dwActivated bool) (*workloadmanager.WorkloadMetrics, error) {
	version, err := m.version(ctx)
	if err != nil {
		log.CtxLogger(ctx).Warnf("Failed to get work mem: %w", err)
		return nil, err
	}
	log.CtxLogger(ctx).Debugw("Finished collecting MongoDB metrics once. Next step is to send to WLM (DW).", versionKey, version)
	metrics := workloadmanager.WorkloadMetrics{
		WorkloadType: workloadmanager.MONGODB,
		Metrics: map[string]string{
			versionKey: version,
		},
	}
	if !dwActivated {
		log.CtxLogger(ctx).Debugw("Data Warehouse is not activated, not sending metrics to Data Warehouse")
		return &metrics, nil
	}
	res, err := workloadmanager.SendDataInsight(ctx, workloadmanager.SendDataInsightParams{
		WLMetrics:  metrics,
		CloudProps: m.Config.GetCloudProperties(),
		WLMService: m.WLMClient,
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		log.CtxLogger(ctx).Warn("SendDataInsight did not return an error but the WriteInsight response is nil")
		return &metrics, nil
	}
	log.CtxLogger(ctx).Debugw("WriteInsight response", "StatusCode", res.HTTPStatusCode)
	return &metrics, nil
}
