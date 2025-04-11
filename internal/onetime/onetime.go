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

// Package onetime contains the common methods which will be used by multiple
// OTE features to avoid duplication.
package onetime

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap/zapcore"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/configuration"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
)

type (
	// InternallyInvokedOTE is a struct which contains necessary context for an
	// OTE when it is not invoked via command line.
	InternallyInvokedOTE struct {
		InvokedBy string
		Lp        log.Parameters
		Cp        *cpb.CloudProperties
	}

	// Options is a struct which contains necessary context for Init function to initialise the OTEs.
	Options struct {
		Help, Version  bool
		Name, LogLevel string
		Fs             *pflag.FlagSet
		IIOTE          *InternallyInvokedOTE
	}
)

// ConfigureUsageMetricsForOTE configures usage metrics for the Workload Agent in one time execution mode.
func ConfigureUsageMetricsForOTE(cp *cpb.CloudProperties, name, version string) {
	if cp == nil {
		log.Logger.Error("CloudProperties is nil, not configuring usage metrics for OTE.")
		return
	}
	if name == "" {
		name = configuration.AgentName
	}
	if version == "" {
		version = configuration.AgentVersion
	}

	usagemetrics.SetAgentProperties(&cpb.AgentProperties{
		Name:            name,
		Version:         version,
		LogUsageMetrics: true,
	})
	usagemetrics.SetCloudProperties(cp)
}

// SetupOneTimeLogging creates logging config for the agent's one time execution.
func SetupOneTimeLogging(params log.Parameters, subcommandName string, level zapcore.Level, logFilePath string) log.Parameters {
	oteLogDir := `/var/log/`
	if params.OSType == "windows" {
		oteLogDir = fmt.Sprintf(`%s\Google\google-cloud-workload-agent\logs\`, log.CreateWindowsLogBasePath())
	}
	if logFilePath != "" {
		if params.OSType == "windows" && !strings.HasSuffix(logFilePath, `\`) {
			logFilePath = logFilePath + `\`
		} else if params.OSType != "windows" && !strings.HasSuffix(logFilePath, `/`) {
			logFilePath = logFilePath + `/`
		}
		oteLogDir = logFilePath
	}
	params.Level = level
	logFileNamePrefix := fmt.Sprintf("google-cloud-workload-agent-%s", subcommandName)
	params.LogFileName = oteLogDir + logFileNamePrefix + ".log"
	params.CloudLogName = logFileNamePrefix
	params.LogFilePath = oteLogDir
	log.SetupLogging(params)
	// make all of the OTE log files global read + write
	os.Chmod(params.LogFileName, 0666)
	return params
}

// Persistent flags (defined at the ote command level)
var (
	logFile, logLevel string
	logToCloud        bool
)

// Register registers the persistent flags for the command.
func Register(osType string, cmd *cobra.Command) {
	cmd.PersistentFlags().StringVarP(&logFile, "log-file", "f", "", "Set the file path for logging")
	cmd.PersistentFlags().StringVarP(&logLevel, "log-level", "l", "info", "Set the logging level (debug, info, warn, error)")
	cmd.PersistentFlags().BoolVarP(&logToCloud, "log-to-cloud", "c", true, "Enable logging to the cloud")
}

// SetValues sets the persistent flags for the subcommand.
func SetValues(agentName string, lp *log.Parameters, cmd *cobra.Command, logName string) {
	flags := cmd.Flags()
	lp.LogFileName = log.OTEFilePath("google-cloud-workload-agent", logName, lp.OSType, "")
	logFileName, _ := flags.GetString("log-file")
	if logFileName != "" {
		lp.LogFileName = logFileName
	}

	lp.LogToCloud, _ = flags.GetBool("log-to-cloud")
	logLevel, _ := flags.GetString("log-level")
	lp.Level = log.StringLevelToZapcore(logLevel)
}
