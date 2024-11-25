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

// Package mysqlmetrics implements metric collection for the MySQL workload agent service.
package mysqlmetrics

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/GoogleCloudPlatform/sapagent/shared/commandlineexecutor"
	"github.com/GoogleCloudPlatform/sapagent/shared/gce"
	"github.com/GoogleCloudPlatform/sapagent/shared/log"
	"github.com/GoogleCloudPlatform/workloadagent/internal/secret"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

type gceInterface interface {
	GetSecret(ctx context.Context, projectID, secretName string) (string, error)
}

// MySQLMetrics contains variables and methods to collect metrics for MySQL databases running on the current host.
type MySQLMetrics struct {
	Execute    commandlineexecutor.Execute
	Config     *configpb.Configuration
	GCEService gceInterface
	password   secret.String
}

// initPassword initializes the password for the MySQL database.
// If the password is set in the configuration, it is used directly (not recommended).
// Otherwise, if the secret configuration is set, the secret is fetched from GCE.
// Without either, the password is not set and requests to the MySQL database will fail.
func (m *MySQLMetrics) initPassword(ctx context.Context) error {
	pw := ""
	if m.Config.GetMysqlConfiguration().GetPassword() != "" {
		m.password = secret.String(m.Config.GetMysqlConfiguration().GetPassword())
		return nil
	}
	secretCfg := m.Config.GetMysqlConfiguration().GetSecret()
	if secretCfg.GetSecretName() != "" && secretCfg.GetProjectId() != "" {
		var err error
		pw, err = m.GCEService.GetSecret(ctx, secretCfg.GetProjectId(), secretCfg.GetSecretName())
		if err != nil {
			return fmt.Errorf("failed to get secret: %v", err)
		}
	}
	m.password = secret.String(pw)
	return nil
}

// New creates a new MySQLMetrics object with default values.
func New(ctx context.Context, config *configpb.Configuration) (*MySQLMetrics, error) {
	gceService, err := gce.NewGCEClient(ctx)
	if err != nil {
		usagemetrics.Error(usagemetrics.GCEServiceCreationFailure)
		return nil, fmt.Errorf("initializing GCE services: %w", err)
	}
	m := &MySQLMetrics{
		Execute:    commandlineexecutor.ExecuteCommand,
		Config:     config,
		GCEService: gceService,
	}
	err = m.initPassword(ctx)
	if err != nil {
		return nil, fmt.Errorf("initializing password: %w", err)
	}
	return m, nil
}

func (m *MySQLMetrics) bufferPoolSize(ctx context.Context) (int, error) {
	user := m.Config.GetMysqlConfiguration().GetUser()
	pw := fmt.Sprintf("-p='%s'", m.password)
	cmd := commandlineexecutor.Params{
		Executable: "sudo",
		Args:       []string{"mysql", "-u", user, pw, "-e", "SELECT @@innodb_buffer_pool_size"},
	}
	log.CtxLogger(ctx).Debugw("MySQL metric collection command", "command", cmd)
	res := m.Execute(ctx, cmd)
	log.CtxLogger(ctx).Debugw("MySQL metric collection result", "result", res)
	lines := strings.Split(res.StdOut, "\n")
	if len(lines) != 3 {
		return 0, fmt.Errorf("found wrong number of lines in buffer pool size: %d", len(lines))
	}
	fields := strings.Fields(lines[1])
	if len(fields) != 1 {
		return 0, fmt.Errorf("found wrong number of fields in buffer pool size: %d", len(fields))
	}
	size, err := strconv.Atoi(fields[0])
	if err != nil {
		return 0, fmt.Errorf("failed to convert buffer pool size to integer: %v", err)
	}
	return size, nil
}

func (m *MySQLMetrics) totalRAM(ctx context.Context) (int, error) {
	cmd := commandlineexecutor.Params{
		Executable: "grep",
		Args:       []string{"MemTotal", "/proc/meminfo"},
	}
	log.CtxLogger(ctx).Debugw("getTotalRAM command", "command", cmd)
	res := m.Execute(ctx, cmd)
	log.CtxLogger(ctx).Debugw("getTotalRAM result", "result", res)
	lines := strings.Split(res.StdOut, "\n")
	if len(lines) != 2 {
		return 0, fmt.Errorf("found wrong number of lines in total RAM: %d", len(lines))
	}
	fields := strings.Fields(lines[0])
	if len(fields) != 3 {
		return 0, fmt.Errorf("found wrong number of fields in total RAM: %d", len(fields))
	}
	ram, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0, fmt.Errorf("failed to convert total RAM to integer: %v", err)
	}
	units := fields[2]
	if strings.ToUpper(units) == "KB" {
		ram = ram * 1024
	}
	return ram, nil
}

// CollectMetricsOnce collects metrics for MySQL databases running on the host.
func (m *MySQLMetrics) CollectMetricsOnce(ctx context.Context) {
	bufferPoolSize, err := m.bufferPoolSize(ctx)
	if err != nil {
		log.CtxLogger(ctx).Warnf("Failed to get buffer pool size: %v", err)
	}
	totalRAM, err := m.totalRAM(ctx)
	if err != nil {
		log.CtxLogger(ctx).Warnf("Failed to get total RAM: %v", err)
	}
	// TODO: send these metrics to Data Warehouse.
	log.CtxLogger(ctx).Debugw("Buffer pool size: %s, total RAM: %s", bufferPoolSize, totalRAM)
}
