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

// Package redismetrics implements metric collection for the Redis workload agent service.
package redismetrics

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/GoogleCloudPlatform/workloadagent/internal/workloadmanager"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
	"github.com/GoogleCloudPlatform/workloadagent/internal/secret"
)

const (
	redisMain        = "master"
	redisWorker      = "slave"
	main             = "main"
	worker           = "worker"
	connectedWorkers = "connected_slaves:"
	linkStatus       = "master_link_status:"
	role             = "role:"
	save             = "save"
	appendonly       = "appendonly"
	yes              = "yes"
	up               = "up"
	defaultPort      = 6379
	persistenceKey   = "persistence_enabled"
	replicationKey   = "replication_enabled"
)

type gceInterface interface {
	GetSecret(ctx context.Context, projectID, secretName string) (string, error)
}

type dbInterface interface {
	Info(context.Context, ...string) *redis.StringCmd
	ConfigGet(context.Context, string) *redis.MapStringStringCmd
	String() string
}

// RedisMetrics contains variables and methods to collect metrics for Redis databases running on the current host.
type RedisMetrics struct {
	Config    *configpb.Configuration
	db        dbInterface
	WLMClient workloadmanager.WLMWriter
}

// password gets the password for the Redis database.
// If the password is set in the configuration, it is used directly (not recommended).
// Otherwise, if the secret configuration is set, the secret is fetched from GCE.
// Without either, the password is not set and requests to the Redis database will fail.
func (r *RedisMetrics) password(ctx context.Context, gceService gceInterface) (secret.String, error) {
	pw := ""
	if r.Config.GetRedisConfiguration().GetConnectionParameters().GetPassword() != "" {
		return secret.String(r.Config.GetRedisConfiguration().GetConnectionParameters().GetPassword()), nil
	}
	secretCfg := r.Config.GetRedisConfiguration().GetConnectionParameters().GetSecret()
	if secretCfg.GetSecretName() != "" && secretCfg.GetProjectId() != "" {
		var err error
		pw, err = gceService.GetSecret(ctx, secretCfg.GetProjectId(), secretCfg.GetSecretName())
		if err != nil {
			return secret.String(""), fmt.Errorf("failed to get secret: %v", err)
		}
	}
	return secret.String(pw), nil
}

// InitDB initializes the Redis database client.
func (r *RedisMetrics) InitDB(ctx context.Context, gceService gceInterface, wlmClient workloadmanager.WLMWriter) error {
	pw, err := r.password(ctx, gceService)
	if err != nil {
		return fmt.Errorf("failed to get password: %v", err)
	}
	port := r.Config.GetRedisConfiguration().GetConnectionParameters().GetPort()
	if port == 0 {
		port = defaultPort
	}
	r.db = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("localhost:%d", port),
		Password: pw.SecretValue(),
	})
	r.WLMClient = wlmClient
	return nil
}

func (r *RedisMetrics) replicationModeActive(ctx context.Context) bool {
	replication := r.db.Info(ctx, "replication")
	log.CtxLogger(ctx).Debugf("replication: %v", replication)
	lines := strings.Split(replication.String(), "\n")
	var err error
	var currentRole string
	for _, line := range lines {
		if strings.Contains(line, role) {
			switch {
			case strings.Contains(line, redisMain):
				log.CtxLogger(ctx).Debugf("Redis is running as main role.")
				currentRole = main
			case strings.Contains(line, redisWorker):
				log.CtxLogger(ctx).Debugf("Redis is running as worker role.")
				currentRole = worker
			default:
				log.CtxLogger(ctx).Debugf("Redis is running as an unknown role. Returning false by default.")
				return false
			}
		}
	}
	var numWorkers int
	for _, line := range lines {
		line = strings.TrimSpace(line)
		switch currentRole {
		case main:
			if strings.Contains(line, connectedWorkers) {
				connectedWorkers := strings.TrimPrefix(line, connectedWorkers)
				numWorkers, err = strconv.Atoi(connectedWorkers)
				if err != nil {
					log.CtxLogger(ctx).Debugf("Failed to parse info about connected workers: %v", err)
					return false
				}
				if numWorkers > 0 {
					return true
				}
				return false
			}
		case worker:
			if strings.Contains(line, linkStatus) {
				return strings.Contains(line, up)
			}
		}
	}
	return false
}

func (r *RedisMetrics) persistenceEnabled(ctx context.Context) bool {
	// Check RDB persistence.
	persistence := r.db.ConfigGet(ctx, save).Val()
	log.CtxLogger(ctx).Debugf("RDB persistence: %v", persistence)
	// Expected to be something like "3600 1 300 100 60 10000" if enabled. Empty string if disabled.
	if saveInfo, ok := persistence[save]; ok && saveInfo != "" {
		log.CtxLogger(ctx).Debugf("RDB persistence: %v", saveInfo)
		return true
	}

	// Check AOF persistence.
	persistence = r.db.ConfigGet(ctx, appendonly).Val()
	log.CtxLogger(ctx).Debugf("AOF persistence: %v", persistence)
	// Expected to be "yes" if enabled. "no" if disabled.
	if appendonlyInfo, ok := persistence[appendonly]; ok && appendonlyInfo == yes {
		return true
	}

	return false
}

// CollectMetricsOnce collects metrics for Redis databases running on the host.
func (r *RedisMetrics) CollectMetricsOnce(ctx context.Context) (*workloadmanager.WorkloadMetrics, error) {
	replicationOn := r.replicationModeActive(ctx)
	persistenceOn := r.persistenceEnabled(ctx)
	log.CtxLogger(ctx).Debugw("Finished collecting metrics once. Next step is to send to WLM (DW).", "replicationOn", replicationOn, "persistenceOn", persistenceOn)
	metrics := workloadmanager.WorkloadMetrics{
		WorkloadType: workloadmanager.REDIS,
		Metrics: map[string]string{
			replicationKey: strconv.FormatBool(replicationOn),
			persistenceKey: strconv.FormatBool(persistenceOn),
		},
	}
	res, err := workloadmanager.SendDataInsight(ctx, workloadmanager.SendDataInsightParams{
		WLMetrics:  metrics,
		CloudProps: r.Config.GetCloudProperties(),
		WLMService: r.WLMClient,
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
