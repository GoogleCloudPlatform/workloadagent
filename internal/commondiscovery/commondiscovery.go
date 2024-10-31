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
	"errors"
	"strings"

	"github.com/shirou/gopsutil/v3/process"
	"github.com/GoogleCloudPlatform/sapagent/shared/commandlineexecutor"
	"github.com/GoogleCloudPlatform/sapagent/shared/log"
)

const (
	mySQLProcess   = "mysqld"
	oracleProcess  = "ora_pmon_"
	oracleListener = "tnslsnr"
)

// executeCommand abstracts the commandlineexecutor.ExecuteCommand for testability.
type executeCommand func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result

// readFile abstracts the file reading operation for testability.
type readFile func(string) ([]byte, error)

// hostname abstracts the os.Hostname for testability.
type hostname func() (string, error)

// processLister is a wrapper around []*process.Process.
type processLister interface {
	listAllProcesses() ([]ProcessWrapper, error)
}

// DefaultProcessLister implements the ProcessLister interface for listing processes.
type DefaultProcessLister struct{}

// listAllProcesses returns a list of processes.
func (DefaultProcessLister) listAllProcesses() ([]ProcessWrapper, error) {
	ps, err := process.Processes()
	if err != nil {
		return nil, err
	}
	processes := make([]ProcessWrapper, len(ps))
	for i, p := range ps {
		processes[i] = &gopsProcess{process: p}
	}
	return processes, nil
}

// DiscoveryService is used to perform common discovery operations.
type DiscoveryService struct {
	ProcessLister processLister
	ReadFile      readFile
	Hostname      hostname
}

// ProcessWrapper is a wrapper around process.Process to support testing.
type ProcessWrapper interface {
	Username() (string, error)
	Pid() int32
	Name() (string, error)
	CmdlineSlice() ([]string, error)
}

// Result holds the results of a common discovery operation.
type Result struct {
	Processes       []ProcessWrapper
	MySQLProcesses  []ProcessWrapper
	OracleProcesses []ProcessWrapper
}

// InitialDiscoveryResult holds the results of an initial discovery operation.
type InitialDiscoveryResult struct {
	IsMySQLRunning  bool
	IsOracleRunning bool
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

// CommonDiscovery returns a CommonDiscoveryResult and any errors encountered during the discovery process.
func (d DiscoveryService) CommonDiscovery(ctx context.Context) (Result, error) {
	mySQLProcesses := []ProcessWrapper{}
	oracleProcesses := []ProcessWrapper{}
	processes, err := d.ProcessLister.listAllProcesses()
	if err != nil {
		log.CtxLogger(ctx).Errorw("Failed to ListAllProcesses", "error", err)
		return Result{}, err
	}
	if len(processes) < 1 {
		log.CtxLogger(ctx).Error("No processes found")
		return Result{}, errors.New("no processes found")
	}
	for _, p := range processes {
		name, err := p.Name()
		if err != nil {
			log.CtxLogger(ctx).Debugw("Failed to get process name", "error", err, "process", p)
			continue
		}
		if name == mySQLProcess {
			mySQLProcesses = append(mySQLProcesses, p)
		} else if strings.HasPrefix(name, oracleProcess) || strings.HasPrefix(name, oracleListener) {
			oracleProcesses = append(oracleProcesses, p)
		}
	}
	discoveryResult := Result{
		Processes:       processes,
		MySQLProcesses:  mySQLProcesses,
		OracleProcesses: oracleProcesses,
	}
	return discoveryResult, nil
}
