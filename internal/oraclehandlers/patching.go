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

package oraclehandlers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/commandlineexecutor"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce/metadataserver"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"

	codepb "google.golang.org/genproto/googleapis/rpc/code"
	gpb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/guestactions"
)

var (
	osStat      = os.Stat
	osReadFile  = os.ReadFile
	osWriteFile = os.WriteFile
)

type startupMechanism int

const (
	startupUnknown startupMechanism = iota
	startupOracleRestart
	startupOratab
	startupSystemdFree
)

// DisableAutostart implements the oracle_disable_autostart guest action.
func DisableAutostart(ctx context.Context, command *gpb.Command, cloudProperties *metadataserver.CloudProperties) *gpb.CommandResult {
	params := command.GetAgentCommand().GetParameters()
	logger := log.CtxLogger(ctx)
	if result := validateParams(ctx, logger, command, params); result != nil {
		return result
	}
	logger = logger.With("oracle_sid", params["oracle_sid"], "oracle_home", params["oracle_home"], "oracle_user", params["oracle_user"])
	logger.Info("oracle_disable_autostart handler called")

	if err := disableAutostart(ctx, params); err != nil {
		logger.Warnw("DisableAutostart failed", "error", err)
		return commandResult(ctx, logger, command, "", "", codepb.Code_INTERNAL, err.Error(), err)
	}

	return commandResult(ctx, logger, command, "Autostart disabled successfully", "", codepb.Code_OK, "Autostart disabled successfully", nil)
}

func disableAutostart(ctx context.Context, params map[string]string) error {
	oracleSID := params["oracle_sid"]
	oracleHome := params["oracle_home"]
	oracleUser := params["oracle_user"]
	dbUniqueName := params["db_unique_name"]

	mechanism, err := detectStartupMechanism(ctx)
	if err != nil {
		return err
	}

	switch mechanism {
	case startupOracleRestart:
		srvctlPath := filepath.Join(oracleHome, "bin", "srvctl")
		disableRes := executeCommand(ctx, commandlineexecutor.Params{
			Executable: srvctlPath,
			Args:       []string{"disable", "database", "-d", dbUniqueName},
			User:       oracleUser,
			Env:        []string{"ORACLE_HOME=" + oracleHome, "ORACLE_SID=" + oracleSID, "LD_LIBRARY_PATH=" + filepath.Join(oracleHome, "lib")},
		})
		if disableRes.ExitCode != 0 {
			return fmt.Errorf("failed to disable database via srvctl: %s", disableRes.StdErr)
		}
	case startupOratab:
		if err := setAutostartInOratab("/etc/oratab", oracleSID, false); err != nil {
			return fmt.Errorf("failed to disable autostart in /etc/oratab: %w", err)
		}
	case startupSystemdFree:
		serviceName, err := getOracleFreeSystemdServiceName(ctx)
		if err != nil {
			return fmt.Errorf("failed to get oracle-free service name: %w", err)
		}
		res := executeCommand(ctx, commandlineexecutor.Params{
			Executable: "systemctl",
			Args:       []string{"disable", serviceName},
		})
		if res.ExitCode != 0 {
			return fmt.Errorf("failed to disable service %s: %s", serviceName, res.StdErr)
		}
	default:
		return fmt.Errorf("unknown startup mechanism")
	}

	return nil
}

// EnableAutostart implements the oracle_enable_autostart guest action.
func EnableAutostart(ctx context.Context, command *gpb.Command, cloudProperties *metadataserver.CloudProperties) *gpb.CommandResult {
	params := command.GetAgentCommand().GetParameters()
	logger := log.CtxLogger(ctx)
	if result := validateParams(ctx, logger, command, params); result != nil {
		return result
	}
	logger = logger.With("oracle_sid", params["oracle_sid"], "oracle_home", params["oracle_home"], "oracle_user", params["oracle_user"])
	logger.Info("oracle_enable_autostart handler called")

	if err := enableAutostart(ctx, params); err != nil {
		logger.Warnw("EnableAutostart failed", "error", err)
		return commandResult(ctx, logger, command, "", "", codepb.Code_INTERNAL, err.Error(), err)
	}

	return commandResult(ctx, logger, command, "Autostart enabled successfully", "", codepb.Code_OK, "Autostart enabled successfully", nil)
}

func enableAutostart(ctx context.Context, params map[string]string) error {
	oracleSID := params["oracle_sid"]
	oracleHome := params["oracle_home"]
	oracleUser := params["oracle_user"]
	dbUniqueName := params["db_unique_name"]

	state, err := detectStartupMechanism(ctx)
	if err != nil {
		return err
	}

	switch state {
	case startupOracleRestart:
		srvctlPath := filepath.Join(oracleHome, "bin", "srvctl")
		res := executeCommand(ctx, commandlineexecutor.Params{
			Executable: srvctlPath,
			Args:       []string{"enable", "database", "-d", dbUniqueName},
			User:       oracleUser,
			Env:        []string{"ORACLE_HOME=" + oracleHome, "ORACLE_SID=" + oracleSID, "LD_LIBRARY_PATH=" + filepath.Join(oracleHome, "lib")},
		})
		if res.ExitCode != 0 {
			return fmt.Errorf("failed to enable database via srvctl: %s", res.StdErr)
		}
	case startupOratab:
		if err := setAutostartInOratab("/etc/oratab", oracleSID, true); err != nil {
			return fmt.Errorf("failed to enable autostart in /etc/oratab: %w", err)
		}
	case startupSystemdFree:
		serviceName, err := getOracleFreeSystemdServiceName(ctx)
		if err != nil {
			return fmt.Errorf("failed to get oracle-free service name: %w", err)
		}
		res := executeCommand(ctx, commandlineexecutor.Params{
			Executable: "systemctl",
			Args:       []string{"enable", serviceName},
		})
		if res.ExitCode != 0 {
			return fmt.Errorf("failed to enable service %s: %s", serviceName, res.StdErr)
		}
	default:
		return fmt.Errorf("unknown autostart state: %d", state)
	}

	return nil
}

// RunDatapatch implements the oracle_run_datapatch guest action.
func RunDatapatch(ctx context.Context, command *gpb.Command, cloudProperties *metadataserver.CloudProperties) *gpb.CommandResult {
	log.CtxLogger(ctx).Info("oracle_run_datapatch handler called")
	// TODO: Implement oracle_run_datapatch handler.
	return &gpb.CommandResult{
		Command:  command,
		ExitCode: 1,
		Stdout:   "oracle_run_datapatch not implemented.",
	}
}

// DisableRestrictedMode implements the oracle_disable_restricted_mode guest action.
func DisableRestrictedMode(ctx context.Context, command *gpb.Command, cloudProperties *metadataserver.CloudProperties) *gpb.CommandResult {
	log.CtxLogger(ctx).Info("oracle_disable_restricted_mode handler called")
	// TODO: Implement oracle_disable_restricted_mode handler.
	return &gpb.CommandResult{
		Command:  command,
		ExitCode: 1,
		Stdout:   "oracle_disable_restricted_mode not implemented.",
	}
}

// StartListener implements the oracle_start_listener guest action.
func StartListener(ctx context.Context, command *gpb.Command, cloudProperties *metadataserver.CloudProperties) *gpb.CommandResult {
	log.CtxLogger(ctx).Info("oracle_start_listener handler called")
	// TODO: Implement oracle_start_listener handler.
	return &gpb.CommandResult{
		Command:  command,
		ExitCode: 1,
		Stdout:   "oracle_start_listener not implemented.",
	}
}

func detectStartupMechanism(ctx context.Context) (startupMechanism, error) {
	// Check for ASM (Oracle Restart)
	// ASM implies the presence of Oracle Grid Infrastructure.
	// In this configuration, 'Oracle Restart' (part of GI) manages the database lifecycle.
	// It ignores the autostart flags in /etc/oratab, relying instead on its own internal registry.
	// We detect this by looking for the unique ASM Process Monitor (pmon) process.
	pgrepRes := executeCommand(ctx, commandlineexecutor.Params{
		Executable: "pgrep",
		Args:       []string{"-f", "asm_pmon_+ASM"},
	})
	if pgrepRes.ExitCode == 0 {
		return startupOracleRestart, nil
	}

	// Check for Filesystem configuration.
	// For Filesystem deployments, the oracle-toolkit installs a custom systemd
	// service named 'dbora'.
	// This service executes a helper script that reads /etc/oratab and starts any instances
	// explicitly marked with a 'Y' flag.
	sysCtlRes := executeCommand(ctx, commandlineexecutor.Params{
		Executable: "systemctl",
		Args:       []string{"is-active", "--quiet", "dbora.service"},
	})
	if sysCtlRes.ExitCode == 0 {
		return startupOratab, nil
	}

	// Check for Oracle Free Edition
	// Oracle Database Free Edition packages provide their own native systemd service
	// (e.g., 'oracle-free-23c.service') and do not use the toolkit's 'dbora' service.
	if _, err := getOracleFreeSystemdServiceName(ctx); err == nil {
		return startupSystemdFree, nil
	}

	return startupUnknown, fmt.Errorf("unable to detect startup mechanism")
}

func getOracleFreeSystemdServiceName(ctx context.Context) (string, error) {
	listUnitsRes := executeCommand(ctx, commandlineexecutor.Params{
		Executable: "systemctl",
		Args:       []string{"list-units", "--all", "--plain", "--no-legend", "oracle-free*.service"},
	})
	if listUnitsRes.ExitCode != 0 {
		return "", fmt.Errorf("failed to list oracle-free services: %s", listUnitsRes.StdErr)
	}
	output := strings.TrimSpace(listUnitsRes.StdOut)
	if len(output) == 0 {
		return "", fmt.Errorf("no oracle-free service found")
	}
	// Take the first one found.
	fields := strings.Fields(output)
	if len(fields) > 0 {
		return fields[0], nil
	}
	return "", fmt.Errorf("failed to parse systemctl output")
}

// setAutostartInOratab updates the oratab file to set the autostart flag for the given SID.
func setAutostartInOratab(filePath string, targetSID string, enable bool) error {
	content, err := osReadFile(filePath)
	if err != nil {
		return err
	}

	lines := strings.Split(string(content), "\n")
	var outputLines []string

	newValue := "N"
	if enable {
		newValue = "Y"
	}

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			outputLines = append(outputLines, line)
			continue
		}

		// Format is $ORACLE_SID:$ORACLE_HOME:<N|Y>
		parts := strings.Split(line, ":")
		if len(parts) >= 3 && parts[0] == targetSID {
			parts[2] = newValue
			outputLines = append(outputLines, strings.Join(parts, ":"))
		} else {
			outputLines = append(outputLines, line)
		}
	}

	output := strings.Join(outputLines, "\n")
	info, err := osStat(filePath)
	if err != nil {
		return err
	}

	return osWriteFile(filePath, []byte(output), info.Mode())
}

// isAutostartEnabledInOratab parses the oratab file to see if the given SID is set to 'Y'
func isAutostartEnabledInOratab(filePath string, targetSID string) (bool, error) {
	content, err := osReadFile(filePath)
	if err != nil {
		return false, err
	}

	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)

		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Format is $ORACLE_SID:$ORACLE_HOME:<N|Y>
		parts := strings.Split(line, ":")

		if len(parts) >= 3 {
			currentSID := parts[0]
			autoStartFlag := parts[2]

			if currentSID == targetSID {
				return autoStartFlag == "Y", nil
			}
		}
	}

	return false, nil
}
