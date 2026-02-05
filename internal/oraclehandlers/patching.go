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

	"go.uber.org/zap"
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

func (m startupMechanism) String() string {
	switch m {
	case startupOracleRestart:
		return "Oracle Restart"
	case startupOratab:
		return "Oratab"
	case startupSystemdFree:
		return "Systemd unit for Oracle Free Edition"
	default:
		return "Unknown"
	}
}

// DisableAutostart implements the oracle_disable_autostart guest action.
func DisableAutostart(ctx context.Context, command *gpb.Command, cloudProperties *metadataserver.CloudProperties) *gpb.CommandResult {
	params := command.GetAgentCommand().GetParameters()
	logger := log.CtxLogger(ctx)
	if result := validateParams(ctx, logger, command, params, []string{"oracle_sid", "oracle_home", "oracle_user", "db_unique_name"}); result != nil {
		return result
	}

	logger = logger.With("oracle_sid", params["oracle_sid"], "oracle_home", params["oracle_home"], "oracle_user", params["oracle_user"])
	logger.Info("oracle_disable_autostart handler called")

	if err := disableAutostart(ctx, logger, params); err != nil {
		logger.Warnw("DisableAutostart failed", "error", err)
		return commandResult(ctx, logger, command, "", "", codepb.Code_INTERNAL, err.Error(), err)
	}

	return commandResult(ctx, logger, command, "Autostart disabled successfully", "", codepb.Code_OK, "Autostart disabled successfully", nil)
}

func disableAutostart(ctx context.Context, logger *zap.SugaredLogger, params map[string]string) error {
	oracleSID := params["oracle_sid"]
	oracleHome := params["oracle_home"]
	oracleUser := params["oracle_user"]
	dbUniqueName := params["db_unique_name"]

	mechanism, err := detectStartupMechanism(ctx, oracleHome, oracleUser, dbUniqueName)
	if err != nil {
		return err
	}

	switch mechanism {
	case startupOracleRestart:
		logger.Infow("Disabling database via srvctl")
		// The user running the command needs to have the correct group id and supplemental
		// groups set to ensure permission to execute the 'srvctl' command successfully.
		//
		// For Oracle Restart, the expected oracle_user is "grid",
		// oracle_sid is expected to be set to +ASM value, and oracle_home also needs
		// to be set to the asm folder (e.g.: /u01/app/19.3.0/grid).
		// We do not verify these values here; the caller is expected to provide the correct values.
		srvctlPath := filepath.Join(oracleHome, "bin", "srvctl")
		disableRes := executeCommand(ctx, commandlineexecutor.Params{
			Executable: srvctlPath,
			Args:       []string{"disable", "database", "-d", dbUniqueName},
			User:       oracleUser,
			Env:        []string{"ORACLE_HOME=" + oracleHome, "ORACLE_SID=" + oracleSID, "LD_LIBRARY_PATH=" + filepath.Join(oracleHome, "lib")},
		})
		if disableRes.ExitCode != 0 {
			return fmt.Errorf("failed to disable database via srvctl; stdout: %s; stderr: %s; exit code: %d", disableRes.StdOut, disableRes.StdErr, disableRes.ExitCode)
		}
		logger.Infow("Database disabled via srvctl", "stdout", disableRes.StdOut, "stderr", disableRes.StdErr, "exit_code", disableRes.ExitCode)
	case startupOratab:
		logger.Infow("Disabling autostart in /etc/oratab")
		if err := setAutostartInOratab("/etc/oratab", oracleSID, false); err != nil {
			return fmt.Errorf("failed to disable autostart in /etc/oratab: %w", err)
		}
		logger.Infow("Database autostart disabled in /etc/oratab")
	case startupSystemdFree:
		serviceName, err := findOracleFreeSystemdServiceName(ctx)
		if err != nil {
			return fmt.Errorf("failed to get oracle-free service name: %w", err)
		}
		logger.Infow("Disabling service via systemctl", "service", serviceName)
		// 'systemctl disable' for SysV services (like Oracle Free) delegates to chkconfig,
		// which attempts to modify symlinks in /etc/rc.d/. This requires the agent's systemd
		// service to have write access to /etc (e.g., ProtectSystem=no or ReadWritePaths including /etc/rc.d).
		res := executeCommand(ctx, commandlineexecutor.Params{
			Executable: "systemctl",
			Args:       []string{"disable", serviceName},
		})
		if res.ExitCode != 0 {
			return fmt.Errorf("failed to disable %s service; stdout: %s; stderr: %s; exit code: %d", serviceName, res.StdOut, res.StdErr, res.ExitCode)
		}
		logger.Infow("Service disabled via systemctl", "service", serviceName, "stdout", res.StdOut, "stderr", res.StdErr, "exit_code", res.ExitCode)
	default:
		logger.Warn("No startup mechanism detected; no action taken")
		return nil
	}
	return nil
}

// EnableAutostart implements the oracle_enable_autostart guest action.
func EnableAutostart(ctx context.Context, command *gpb.Command, cloudProperties *metadataserver.CloudProperties) *gpb.CommandResult {
	params := command.GetAgentCommand().GetParameters()
	logger := log.CtxLogger(ctx)
	if result := validateParams(ctx, logger, command, params, []string{"oracle_sid", "oracle_home", "oracle_user", "db_unique_name"}); result != nil {
		return result
	}

	logger = logger.With("oracle_sid", params["oracle_sid"], "oracle_home", params["oracle_home"], "oracle_user", params["oracle_user"])
	logger.Info("oracle_enable_autostart handler called")

	if err := enableAutostart(ctx, logger, params); err != nil {
		logger.Warnw("EnableAutostart failed", "error", err)
		return commandResult(ctx, logger, command, "", "", codepb.Code_INTERNAL, err.Error(), err)
	}

	return commandResult(ctx, logger, command, "Autostart enabled successfully", "", codepb.Code_OK, "Autostart enabled successfully", nil)
}

func enableAutostart(ctx context.Context, logger *zap.SugaredLogger, params map[string]string) error {
	oracleSID := params["oracle_sid"]
	oracleHome := params["oracle_home"]
	oracleUser := params["oracle_user"]
	dbUniqueName := params["db_unique_name"]

	state, err := detectStartupMechanism(ctx, oracleHome, oracleUser, dbUniqueName)
	if err != nil {
		return err
	}
	logger.Infow("Detected startup mechanism", "mechanism", state.String())

	switch state {
	case startupOracleRestart:
		logger.Infow("Enabling database via srvctl")
		// The user running the command needs to have the correct group id and supplemental
		// groups set to ensure permission to execute the 'srvctl' command successfully.
		//
		// For Oracle Restart, the expected oracle_user is "grid",
		// oracle_sid is expected to be set to +ASM value, and oracle_home also needs
		// to be set to the asm folder (e.g.: /u01/app/19.3.0/grid).
		// We do not verify these values here; the caller is expected to provide the correct values.
		srvctlPath := filepath.Join(oracleHome, "bin", "srvctl")
		res := executeCommand(ctx, commandlineexecutor.Params{
			Executable: srvctlPath,
			Args:       []string{"enable", "database", "-d", dbUniqueName},
			User:       oracleUser,
			Env:        []string{"ORACLE_HOME=" + oracleHome, "ORACLE_SID=" + oracleSID, "LD_LIBRARY_PATH=" + filepath.Join(oracleHome, "lib")},
		})
		if res.ExitCode != 0 {
			return fmt.Errorf("failed to enable %s database via srvctl; stdout: %s; stderr: %s; exit code: %d", dbUniqueName, res.StdOut, res.StdErr, res.ExitCode)
		}
		logger.Infow("Database enabled via srvctl", "stdout", res.StdOut, "stderr", res.StdErr, "exit_code", res.ExitCode)
	case startupOratab:
		logger.Infow("Enabling autostart in /etc/oratab")
		if err := setAutostartInOratab("/etc/oratab", oracleSID, true); err != nil {
			return fmt.Errorf("failed to enable autostart in /etc/oratab: %w", err)
		}
		logger.Infow("Database autostart enabled in /etc/oratab")
	case startupSystemdFree:
		serviceName, err := findOracleFreeSystemdServiceName(ctx)
		if err != nil {
			return fmt.Errorf("failed to get oracle-free service name: %w", err)
		}
		logger.Infow("Enabling service via systemctl", "service", serviceName)
		// 'systemctl enable' for SysV services (like Oracle Free) delegates to chkconfig,
		// which attempts to modify symlinks in /etc/rc.d/. This requires the agent's systemd
		// service to have write access to /etc (e.g., ProtectSystem=no or ReadWritePaths including /etc/rc.d).
		res := executeCommand(ctx, commandlineexecutor.Params{
			Executable: "systemctl",
			Args:       []string{"enable", serviceName},
		})
		if res.ExitCode != 0 {
			return fmt.Errorf("failed to enable %s service; stdout: %s; stderr: %s; exit code: %d", serviceName, res.StdOut, res.StdErr, res.ExitCode)
		}
		logger.Infow("Service enabled via systemctl", "service", serviceName, "stdout", res.StdOut, "stderr", res.StdErr, "exit_code", res.ExitCode)
	default:
		logger.Warn("No startup mechanism detected; no action taken")
		return nil
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

// DisableRestrictedSession implements the oracle_disable_restricted_mode guest action.
// It executes "ALTER SYSTEM DISABLE RESTRICTED SESSION" to allow normal users to log in.
func DisableRestrictedSession(ctx context.Context, command *gpb.Command, cloudProperties *metadataserver.CloudProperties) *gpb.CommandResult {
	params := command.GetAgentCommand().GetParameters()
	logger := log.CtxLogger(ctx)
	if result := validateParams(ctx, logger, command, params, []string{"oracle_sid", "oracle_home", "oracle_user"}); result != nil {
		return result
	}
	logger = logger.With("oracle_sid", params["oracle_sid"], "oracle_home", params["oracle_home"], "oracle_user", params["oracle_user"])
	logger.Infow("oracle_disable_restricted_mode handler called")

	sql := "ALTER SYSTEM DISABLE RESTRICTED SESSION;"
	stdout, stderr, err := runSQL(ctx, params, sql, 120, true)
	if err != nil {
		errMsg := fmt.Errorf("failed to disable restricted mode: %w", err)
		logger.Warnw("failed to disable restricted session", "stdout", stdout, "stderr", stderr, "error", errMsg)
		return commandResult(ctx, logger, command, stdout, stderr, codepb.Code_FAILED_PRECONDITION, errMsg.Error(), errMsg)
	}
	logger.Infow("Restricted session disabled successfully", "stdout", stdout)
	return commandResult(ctx, logger, command, stdout, stderr, codepb.Code_OK, "Restricted session disabled successfully", nil)
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

func detectStartupMechanism(ctx context.Context, oracleHome, oracleUser, dbUniqueName string) (startupMechanism, error) {
	// Check for Oracle Restart via srvctl
	// Oracle Restart (part of GI) manages the database lifecycle and ignores the autostart flags
	// in /etc/oratab, relying instead on its own internal registry.
	// We detect this by checking if the database is registered in Oracle Restart configuration.
	srvctlPath := filepath.Join(oracleHome, "bin", "srvctl")
	res := executeCommand(ctx, commandlineexecutor.Params{
		Executable: srvctlPath,
		Args:       []string{"config", "database", "-d", dbUniqueName},
		User:       oracleUser,
		Env:        []string{"ORACLE_HOME=" + oracleHome, "LD_LIBRARY_PATH=" + filepath.Join(oracleHome, "lib")},
	})
	if res.ExitCode == 0 {
		return startupOracleRestart, nil
	}

	// Check for Oracle Free Edition
	// Oracle Database Free Edition packages provide their own native systemd service
	// (e.g., 'oracle-free-23c.service') and do not use the toolkit's 'dbora' service.
	if _, err := findOracleFreeSystemdServiceName(ctx); err == nil {
		return startupSystemdFree, nil
	}

	// Default to Oratab
	// For all other cases, assume the autostart is governed by the /etc/oratab file.
	return startupOratab, nil
}

func findOracleFreeSystemdServiceName(ctx context.Context) (string, error) {
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
		return strings.TrimSuffix(fields[0], ".service"), nil
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

	for _, originalLine := range lines {
		trimmedLine := strings.TrimSpace(originalLine)
		if trimmedLine == "" || strings.HasPrefix(trimmedLine, "#") {
			outputLines = append(outputLines, originalLine)
			continue
		}

		// Format is $ORACLE_SID:$ORACLE_HOME:<N|Y>
		parts := strings.Split(trimmedLine, ":")
		if len(parts) >= 3 && parts[0] == targetSID {
			parts[2] = newValue
			outputLines = append(outputLines, strings.Join(parts, ":"))
		} else {
			outputLines = append(outputLines, originalLine)
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
