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

// Package commondiscovery performs common discovery operations for all services.
package commondiscovery

import (
	"context"
	"strings"

	"github.com/shirou/gopsutil/v3/process"
	"github.com/GoogleCloudPlatform/sapagent/shared/commandlineexecutor"
	"github.com/GoogleCloudPlatform/sapagent/shared/log"
)

// executeCommand abstracts the commandlineexecutor.ExecuteCommand for testability.
type executeCommand func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result

// readFile abstracts the file reading operation for testability.
type readFile func(string) ([]byte, error)

// hostname abstracts the os.Hostname for testability.
type hostname func() (string, error)

// DiscoveryService is used to perform common discovery operations.
type DiscoveryService struct {
	ExecuteCommand executeCommand
	ReadFile       readFile
	Hostname       hostname
}

// ProcessWrapper is a wrapper around process.Process to support testing.
type ProcessWrapper interface {
	Username() (string, error)
	Pid() int32
	Name() (string, error)
	CmdlineSlice() ([]string, error)
}

// gopsProcess implements the processWrapper for abstracting process.Process.
type gopsProcess struct {
	process *process.Process
}

// Username returns a username of the process.
func (p gopsProcess) Username() (string, error) {
	return p.process.Username()
}

// Pid returns the PID of the process.
func (p gopsProcess) Pid() int32 {
	return p.process.Pid
}

// Name returns the name of the process.
func (p gopsProcess) Name() (string, error) {
	return p.process.Name()
}

// CmdlineSlice returns the command line arguments of the process.
func (p gopsProcess) CmdlineSlice() ([]string, error) {
	return p.process.CmdlineSlice()
}

// ListAllProcesses returns a list of processes.
func (d DiscoveryService) ListAllProcesses(ctx context.Context) ([]ProcessWrapper, error) {
	ps, err := process.Processes()
	if err != nil {
		return nil, err
	}
	processes := make([]ProcessWrapper, len(ps))
	for i, p := range ps {
		processes[i] = &gopsProcess{process: p}
	}
	// printProcessInfo(ctx, processes)
	return processes, nil
}

func printProcessInfo(ctx context.Context, processes []ProcessWrapper) {
	for _, p := range processes {
		name, err := p.Name()
		if err != nil {
			log.CtxLogger(ctx).Debugw("Failed to get process name", "error", err)
			continue
		}
		username, err := p.Username()
		if err != nil {
			log.CtxLogger(ctx).Debugw("Failed to get process username", "error", err)
			continue
		}
		cmdline, err := p.CmdlineSlice()
		if err != nil {
			log.CtxLogger(ctx).Debugw("Failed to get process command line", "error", err)
			continue
		}
		log.CtxLogger(ctx).Debugw("Process info", "name", name, "pid", p.Pid(), "username", username, "cmdline", cmdline)
	}
}

// InitialDiscovery returns a list of all processes, whether MySQL is running, whether Oracle is running, and any errors encountered during the discovery process.
func (d DiscoveryService) InitialDiscovery(ctx context.Context) ([]ProcessWrapper, bool, bool, error) {
	isMySQLRunning := false
	isOracleRunning := false
	processes, err := d.ListAllProcesses(ctx)
	if err != nil {
		log.CtxLogger(ctx).Errorw("Failed to ListAllProcesses", "error", err)
		return nil, isMySQLRunning, isOracleRunning, err
	}
	for _, p := range processes {
		name, err := p.Name()
		if err != nil {
			log.CtxLogger(ctx).Debugw("Failed to get process name", "error", err)
			continue
		}
		if strings.HasPrefix(name, "mysql") {
			isMySQLRunning = true
		}
		if strings.HasPrefix(name, "ora_pmon_") {
			isOracleRunning = true
		}
	}
	return processes, isMySQLRunning, isOracleRunning, nil
}
