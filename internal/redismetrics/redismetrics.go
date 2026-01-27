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
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/GoogleCloudPlatform/workloadagent/internal/ipinfo"
	"github.com/GoogleCloudPlatform/workloadagent/internal/workloadmanager"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/commandlineexecutor"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/osinfo"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/secret"
)

const (
	// RedisProcessName is the systemd process name for Redis on RHEL and SLES.
	RedisProcessName = "redis"
	// RedisServerProcessName is the systemd process name for Redis on Debian.
	RedisServerProcessName = "redis-server"
	redisMain              = "master"
	redisWorker            = "slave"
	main                   = "main"
	worker                 = "worker"
	connectedWorkers       = "connected_slaves:"
	linkStatus             = "master_link_status:"
	role                   = "role:"
	save                   = "save"
	appendonly             = "appendonly"
	yes                    = "yes"
	up                     = "up"
	defaultPort            = 6379
	persistenceKey         = "persistence_enabled"
	replicationKey         = "replication_enabled"
	serviceEnabledKey      = "service_enabled"
	serviceRestartKey      = "service_restart"
	replicationZonesKey    = "replication_zones"
	currentRoleKey         = "current_role"
	ip                     = "ip"
)

type gceInterface interface {
	GetSecret(ctx context.Context, projectID, secretName string) (string, error)
}

type dbInterface interface {
	Info(context.Context, ...string) *redis.StringCmd
	ConfigGet(context.Context, string) *redis.MapStringStringCmd
	Ping(context.Context) *redis.StatusCmd
	String() string
}

// RedisMetrics contains variables and methods to collect metrics for Redis databases running on the current host.
type RedisMetrics struct {
	Config      *configpb.Configuration
	db          dbInterface
	newClient   func(opt *redis.Options) dbInterface
	execute     commandlineexecutor.Execute
	WLMClient   workloadmanager.WLMWriter
	OSData      osinfo.Data
	CurrentRole string
}

// New creates a new RedisMetrics object initialized with default values.
func New(ctx context.Context, config *configpb.Configuration, wlmClient workloadmanager.WLMWriter, osData osinfo.Data) *RedisMetrics {
	return &RedisMetrics{
		Config:    config,
		execute:   commandlineexecutor.ExecuteCommand,
		WLMClient: wlmClient,
		OSData:    osData,
		newClient: func(opt *redis.Options) dbInterface {
			return redis.NewClient(opt)
		},
	}
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
func (r *RedisMetrics) InitDB(ctx context.Context, gceService gceInterface) error {
	pw, err := r.password(ctx, gceService)
	if err != nil {
		return fmt.Errorf("failed to get password: %v", err)
	}
	port := r.Config.GetRedisConfiguration().GetConnectionParameters().GetPort()
	if port == 0 {
		port = defaultPort
	}
	r.db = r.newClient(&redis.Options{
		Addr:     fmt.Sprintf("localhost:%d", port),
		Password: pw.SecretValue(),
	})
	err = r.db.Ping(ctx).Err()
	if err != nil {
		return fmt.Errorf("failed to ping Redis database: %v", err)
	}
	return nil
}

func (r *RedisMetrics) getCurrentRole(ctx context.Context) string {
	replication := r.db.Info(ctx, "replication")
	log.CtxLogger(ctx).Debugf("replication: %v", replication)
	lines := strings.Split(replication.String(), "\n")
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
				return ""
			}
		}
	}
	return currentRole
}

func getIP(s string) (string, error) {
	// s is something like "slave0:ip=12.345.67.890,port=6379,state=wait_bgsave,offset=0,lag=0"
	// We want the ip=12.345.67.890 part, specifically the part after the '='.
	re := regexp.MustCompile(`ip=([^,]+)`)
	match := re.FindStringSubmatch(s)
	if len(match) > 1 {
		return strings.TrimSpace(match[1]), nil
	}
	return "", errors.New("ip not found")
}

func (r *RedisMetrics) replicationZones(ctx context.Context, currentRole string, netLookupAddr func(ip string) ([]string, error)) []string {
	var workerIPs []string
	replication := r.db.Info(ctx, "replication")
	log.CtxLogger(ctx).Debugf("replication: %v", replication)
	lines := strings.Split(replication.String(), "\n")
	// There are only replication targets for the main role.
	if currentRole != main {
		return nil
	}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		// Replication target information
		if strings.HasPrefix(line, redisWorker) {
			ip, err := getIP(line)
			if err != nil {
				log.CtxLogger(ctx).Debugf("Failed to get IP from line: %v", err)
				continue
			}
			workerIPs = append(workerIPs, ip)
		}
	}
	zones := ipinfo.ZonesFromIPs(ctx, workerIPs, netLookupAddr)
	return zones
}

func (r *RedisMetrics) replicationModeActive(ctx context.Context, currentRole string) bool {
	replication := r.db.Info(ctx, "replication")
	log.CtxLogger(ctx).Debugf("replication: %v", replication)
	lines := strings.Split(replication.String(), "\n")
	var err error
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

func (r *RedisMetrics) serviceEnabled(ctx context.Context) bool {
	processName := RedisProcessName
	if r.OSData.OSVendor == "debian" {
		processName = RedisServerProcessName
	}
	res := r.execute(ctx, commandlineexecutor.Params{
		Executable: "systemctl",
		Args:       []string{"is-enabled", processName},
	})
	// is-enabled returns an exit code of 0 if the service is enabled,
	// and non-zero otherwise. Since the commandlineexecutor folds in non-zero
	// exit codes into the error, assume that the service is enabled if
	// the error is nil.
	if res.Error != nil {
		log.CtxLogger(ctx).Debugw("Redis service is not enabled", "error", res.Error)
		return false
	}
	return true
}

func (r *RedisMetrics) serviceRestart(ctx context.Context) bool {
	processName := RedisProcessName
	if r.OSData.OSVendor == "debian" {
		processName = RedisServerProcessName
	}
	res := r.execute(ctx, commandlineexecutor.Params{
		Executable: "systemctl",
		Args:       []string{"show", processName, "-p", "Restart"},
	})
	if res.Error != nil {
		log.CtxLogger(ctx).Debugw("Failed to check Redis service restart policy", "error", res.Error)
		return false
	}
	return strings.TrimSpace(res.StdOut) != "Restart=no"
}

// CollectMetricsOnce collects metrics for Redis databases running on the host.
func (r *RedisMetrics) CollectMetricsOnce(ctx context.Context, dwActivated bool) (*workloadmanager.WorkloadMetrics, error) {
	currentRole := r.getCurrentRole(ctx)
	replicationOn := r.replicationModeActive(ctx, currentRole)
	persistenceOn := r.persistenceEnabled(ctx)
	serviceEnabled := r.serviceEnabled(ctx)
	serviceRestart := r.serviceRestart(ctx)
	replicationZones := r.replicationZones(ctx, currentRole, net.LookupAddr)
	log.CtxLogger(ctx).Debugw("Finished collecting metrics once. Next step is to send to WLM (DW).",
		replicationKey, replicationOn,
		persistenceKey, persistenceOn,
		serviceEnabledKey, serviceEnabled,
		serviceRestartKey, serviceRestart,
		replicationZonesKey, strings.Join(replicationZones, ","),
		currentRoleKey, currentRole,
	)
	metrics := workloadmanager.WorkloadMetrics{
		WorkloadType: workloadmanager.REDIS,
		Metrics: map[string]string{
			replicationKey:      strconv.FormatBool(replicationOn),
			persistenceKey:      strconv.FormatBool(persistenceOn),
			serviceEnabledKey:   strconv.FormatBool(serviceEnabled),
			serviceRestartKey:   strconv.FormatBool(serviceRestart),
			replicationZonesKey: strings.Join(replicationZones, ","),
			currentRoleKey:      currentRole,
		},
	}
	if !dwActivated {
		log.CtxLogger(ctx).Debugw("Data Warehouse is not activated, not sending metrics to Data Warehouse")
		return &metrics, nil
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
