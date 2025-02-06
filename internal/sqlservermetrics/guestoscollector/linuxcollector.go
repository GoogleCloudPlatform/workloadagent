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

package guestoscollector

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/commandlineexecutor"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"

	"github.com/GoogleCloudPlatform/workloadagent/internal/sqlservermetrics/guestoscollector/remote"
	"github.com/GoogleCloudPlatform/workloadagent/internal/sqlservermetrics/sqlserverutils"
)

/*
The production library uses the ExecuteCommand function from commandlineexecutor and the
EvalSymlinks function from filepath.  We need to be able to mock these functions in our unit tests.
*/
var (
	symLinkCommand = filepath.EvalSymlinks
)

const (
	localSSDCommand                = "sudo lshw -class disk -json"
	localSSDCommandForSuse         = "sudo hwinfo --disk"
	powerPlanCommand               = "sudo tuned-adm active"
	dataDiskAllocationUnitsCommand = "sudo blockdev --getbsz /dev/"
	gcbdrAgentRunningCommand       = "sudo systemctl status udsagent | grep \"Active: \""
	persistentDisk                 = "PersistentDisk"
	ephemeralDisk                  = "EphemeralDisk"
)

// highPerformanceProfile are all tuned power profiles that will be considered high performance best practice
var highPerformanceProfile = map[string]bool{
	"mssql":                  true,
	"throughput-performance": true,
}

// HighPerformanceProfiles public getter for highPerformanceProfile
func HighPerformanceProfiles() map[string]bool { return highPerformanceProfile }

// LinuxCollector is the collector for linux systems.
type LinuxCollector struct {
	ipaddr                 string
	username               string
	privateKeyPath         string
	disks                  [](*sqlserverutils.Disks)
	physicalDriveToDiskMap map[string]string
	guestRuleCommandMap    map[string]commandExecutor
	lshwRegexMapping       map[string]*regexp.Regexp
	remote                 bool
	port                   int32
	remoteRunner           remote.Executor
}

type commandExecutor struct {
	command          string
	isRule           bool
	runCommand       func(context.Context, string) (string, error)
	runRemoteCommand func(context.Context, string, remote.Executor) (string, error)
}

type disk struct {
	logicalname string
	diskType    string
}

type lshwEntry struct {
	Product     string `json:"product"`
	LogicalName string `json:"logicalname"`
	Size        int    `json:"size"`
}

var lshwFieldsToParse = []string{
	"product", "logicalname", "size", "Device File", "Device", "Capacity",
}

func lshwFields() []string { return lshwFieldsToParse }

// NewLinuxCollector initializes and returns a new LinuxCollector object.
func NewLinuxCollector(disks []*sqlserverutils.Disks, ipAddr, username, privateKeyPath string, isRemote bool, port int32) *LinuxCollector {
	c := LinuxCollector{
		ipaddr:                 ipAddr,
		username:               username,
		privateKeyPath:         privateKeyPath,
		disks:                  disks,
		guestRuleCommandMap:    map[string]commandExecutor{},
		physicalDriveToDiskMap: map[string]string{},
		lshwRegexMapping:       map[string]*regexp.Regexp{},
		remote:                 isRemote,
		port:                   port,
	}

	if c.remote {
		c.remoteRunner = remote.NewRemote(c.ipaddr, c.username, c.port)
		c.setUpRegex()
		if err := c.remoteRunner.SetupKeys(c.privateKeyPath); err != nil {
			log.Logger.Error(err)
			c.remoteRunner = nil
		} else if err := c.remoteRunner.CreateClient(); err != nil {
			log.Logger.Error(err)
			c.remoteRunner = nil
		}
	}

	c.guestRuleCommandMap[UseLocalSSD] = commandExecutor{
		command: localSSDCommand,
		isRule:  false,
		runCommand: func(ctx context.Context, command string) (string, error) {
			// LocalSSDRule is collected differently, check DiskToDiskType method
			return "", nil
		},
		runRemoteCommand: func(ctx context.Context, command string, r remote.Executor) (string, error) {
			var isLinuxSuse bool
			lshwResult, err := remote.RunCommandWithPipes(command, r)
			if err != nil {
				lshwResult, err = remote.RunCommandWithPipes(localSSDCommandForSuse, r)
				if err != nil {
					return "", err
				}
				log.Logger.Debugw("Fetched the disk info by using hwinfo.")
				isLinuxSuse = true
			}

			var lshwFields lshwEntry
			if !isLinuxSuse {
				lshwFields, err = c.findLshwFields(lshwResult)
			} else {
				lshwFields, err = c.findHwinfoFields(lshwResult)
			}
			if err != nil {
				return "", err
			}

			diskType := Other.String()
			if lshwFields.Product == persistentDisk {
				diskType = PersistentSSD.String()
			} else if lshwFields.Product == ephemeralDisk && lshwFields.Size%402653184000 == 0 {
				diskType = LocalSSD.String()
			}

			c.physicalDriveToDiskMap[lshwFields.LogicalName] = diskType

			res, errMar := json.Marshal(c.physicalDriveToDiskMap)
			if errMar != nil {
				return "", errMar
			}
			return string(res), nil
		},
	}
	c.guestRuleCommandMap[PowerProfileSetting] = commandExecutor{
		command: powerPlanCommand,
		isRule:  true,
		runCommand: func(ctx context.Context, command string) (string, error) {
			res, err := commandLineExecutorWrapper(ctx, "/bin/sh", fmt.Sprintf(" -c '%s'", command), commandlineexecutor.ExecuteCommand)
			if err != nil {
				return "", fmt.Errorf("Check help docs, tuned package not installed or no power profile set. %v", err.Error())
			}
			return findPowerProfile(res)
		},
		runRemoteCommand: func(ctx context.Context, command string, r remote.Executor) (string, error) {
			s, err := r.CreateSession("")
			if err != nil {
				return "", err
			}
			defer s.Close()
			res, err := r.Run(command, s)
			if err != nil {
				return "", fmt.Errorf("Check help docs, tuned package not installed or no power profile set. %v", err.Error())
			}
			return findPowerProfile(res)
		},
	}
	c.guestRuleCommandMap[DataDiskAllocationUnits] = commandExecutor{
		command: dataDiskAllocationUnitsCommand,
		isRule:  true,
		runCommand: func(ctx context.Context, command string) (string, error) {
			if c.disks == nil || len(c.disks) == 0 {
				return "", fmt.Errorf("data disk allocation failed. no disks found")
			}

			type resultEle struct {
				BlockSize string
				Caption   string
			}

			var result []resultEle

			for _, disk := range c.disks {
				if disk.Mapping == "" {
					continue
				}
				fullCommand := command + disk.Mapping
				blockSize, err := commandLineExecutorWrapper(ctx, "/bin/sh", fmt.Sprintf(" -c '%s'", fullCommand), commandlineexecutor.ExecuteCommand)
				if err != nil {
					return "", err
				}
				result = append(result, resultEle{BlockSize: blockSize, Caption: disk.Mapping})
			}
			res, err := json.Marshal(result)
			if err != nil {
				return "", err
			}
			return string(res), nil
		},
		runRemoteCommand: func(ctx context.Context, command string, r remote.Executor) (string, error) {
			if c.physicalDriveToDiskMap == nil || len(c.physicalDriveToDiskMap) == 0 {
				return "", fmt.Errorf("data disk allocation failed. no disks found")
			}

			type resultEle struct {
				BlockSize string
				Caption   string
			}
			var result []resultEle

			for physicalDrive := range c.physicalDriveToDiskMap {
				fullCommand := command + physicalDrive
				s, err := r.CreateSession("")
				if err != nil {
					return "", err
				}
				blockSize, err := r.Run(fullCommand, s)
				s.Close()
				if err != nil || blockSize == "" {
					blockSize = "unknown"
				}
				result = append(result, resultEle{BlockSize: blockSize, Caption: physicalDrive})
			}
			res, err := json.Marshal(result)
			if err != nil {
				return "", err
			}
			return string(res), nil
		},
	}
	c.guestRuleCommandMap[GCBDRAgentRunning] = commandExecutor{
		command: gcbdrAgentRunningCommand,
		isRule:  true,
		runCommand: func(ctx context.Context, command string) (string, error) {
			res, err := commandLineExecutorWrapper(ctx, "/bin/sh", fmt.Sprintf(" -c '%s'", command), commandlineexecutor.ExecuteCommand)
			if err != nil || res == "" {
				return "false", nil
			}
			return c.gcbdrAgentRunning(res)
		},
		runRemoteCommand: func(ctx context.Context, command string, r remote.Executor) (string, error) {
			s, err := r.CreateSession("")
			if err != nil {
				return "", err
			}
			defer s.Close()
			res, err := r.Run(command, s)
			if err != nil || res == "" {
				return "false", nil
			}
			return c.gcbdrAgentRunning(res)
		},
	}
	return &c
}

// setUpRegex initializes the needed regex's to parse output of a remote lshw and hwinfo call
func (c *LinuxCollector) setUpRegex() {
	for _, field := range lshwFields() {
		if field == "size" {
			expression := fmt.Sprintf(`"%s" : (\d+?)[\D]`, field)
			reg := regexp.MustCompile(expression)
			c.lshwRegexMapping[field] = reg
		} else if field == "logicalname" || field == "product" {
			expression := fmt.Sprintf(`"%s" : "(.*?)"`, field)
			reg := regexp.MustCompile(expression)
			c.lshwRegexMapping[field] = reg
		} else if field == "Capacity" {
			expression := fmt.Sprintf(`%s: .*\((\d+?)[\D]`, field)
			reg := regexp.MustCompile(expression)
			c.lshwRegexMapping[field] = reg
		} else if field == "Device" {
			expression := fmt.Sprintf(`%s: "(.*?)"`, field)
			reg := regexp.MustCompile(expression)
			c.lshwRegexMapping[field] = reg
		} else {
			expression := fmt.Sprintf(`%s: ([^\s]+)`, field)
			reg := regexp.MustCompile(expression)
			c.lshwRegexMapping[field] = reg
		}
	}
}

// DiskToDiskType maps physical drive to disktype. EX: /dev/sda to local_ssd
func DiskToDiskType(fields map[string]string, disks []*sqlserverutils.Disks) {
	logicalToTypeMap := map[string]string{}
	for _, devices := range disks {
		var err error
		devices.Mapping, err = forLinux(devices.DeviceName)
		if err != nil {
			log.Logger.Warnw("No mapping for instance disk", "disk", devices.DeviceName, "error", err)
		} else {
			// EX: sda -> PERSISTENT
			logicalToTypeMap[devices.Mapping] = devices.DiskType
		}
		log.Logger.Debugw("Instance disk is mapped to device name", "devicename", devices.DeviceName, "mapping", devices.Mapping)
	}
	r, err := json.Marshal(logicalToTypeMap)
	if err != nil {
		log.Logger.Errorw("An error occurred while serializing disk info to JSON", "error", err)
	}
	if len(logicalToTypeMap) == 0 {
		fields[UseLocalSSD] = "unknown"
	} else {
		fields[UseLocalSSD] = string(r)
	}
}

/*
forLinux returns the name of the Linux physical disk mapped to "deviceName". (sda1, hda1, sdb1,
etc...)
*/
func forLinux(deviceName string) (string, error) {
	path, err := symLinkCommand("/dev/disk/by-id/google-" + deviceName)
	if err != nil {
		return "", err
	}

	if path != "" {
		path = strings.TrimSuffix(filepath.Base(path), "\n")
	}
	log.Logger.Debugw("Mapping for device", "name", deviceName, "mapping", path)
	return path, nil
}

func (c *LinuxCollector) findLshwFields(lshwResult string) (lshwEntry, error) {
	logicalName, logicalNameErr := c.findLshwFieldString(lshwResult, "logicalname")
	if logicalNameErr != nil {
		return lshwEntry{}, logicalNameErr
	}
	product, productErr := c.findLshwFieldString(lshwResult, "product")
	if productErr != nil {
		return lshwEntry{}, productErr
	}
	size, sizeErr := c.findLshwFieldInt(lshwResult, "size")
	if sizeErr != nil {
		return lshwEntry{}, sizeErr
	}

	return lshwEntry{LogicalName: logicalName, Product: product, Size: size}, nil
}

func (c *LinuxCollector) findHwinfoFields(lshwResult string) (lshwEntry, error) {
	logicalName, logicalNameErr := c.findLshwFieldString(lshwResult, "Device File")
	if logicalNameErr != nil {
		return lshwEntry{}, logicalNameErr
	}
	product, productErr := c.findLshwFieldString(lshwResult, "Device")
	if productErr != nil {
		return lshwEntry{}, productErr
	}
	size, sizeErr := c.findLshwFieldInt(lshwResult, "Capacity")
	if sizeErr != nil {
		return lshwEntry{}, sizeErr
	}

	return lshwEntry{LogicalName: logicalName, Product: product, Size: size}, nil
}

func (c *LinuxCollector) findLshwFieldString(lshwResult string, field string) (string, error) {
	// expression := fmt.Sprintf(`"%s" : "(.*?)"`, field)
	// reg := regexp.MustCompile(expression)
	reg, ok := c.lshwRegexMapping[field]
	if !ok {
		return "", fmt.Errorf("regexp did not find %s field", field)
	}
	match := reg.FindStringSubmatch(lshwResult)
	if len(match) <= 1 {
		return "", fmt.Errorf("regexp did not find %s field", field)
	}
	resultArr := strings.Split(match[1], "/")
	return resultArr[len(resultArr)-1], nil
}

func (c *LinuxCollector) findLshwFieldInt(lshwResult string, field string) (int, error) {
	// expression := fmt.Sprintf(`"%s" : (\d+?)[\D]`, field)
	// reg := regexp.MustCompile(expression)
	reg, ok := c.lshwRegexMapping[field]
	if !ok {
		return 0, fmt.Errorf("regexp did not find %s field", field)
	}
	match := reg.FindStringSubmatch(lshwResult)
	if len(match) <= 1 {
		return 0, fmt.Errorf("regexp did not find %s field", field)
	}
	result, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, fmt.Errorf("unable to convert %s from string to int: error %v", field, err)
	}

	return result, nil
}

// findPowerProfile takes input string of command tuned-adm active, and gets the power profile
func findPowerProfile(powerProfileFull string) (string, error) {
	powerProfile := strings.Split(powerProfileFull, ": ")

	if len(powerProfile) < 2 || powerProfile[0] != "Current active profile" {
		return "", fmt.Errorf(`Check help docs. Expected power profile format to be  "Current active profile: <profile>. Actual result: %v`, powerProfileFull)
	}
	if HighPerformanceProfiles()[powerProfile[1]] {
		return "High performance", nil
	}

	return powerProfile[1], nil
}

// CollectGuestRules collects os guest os rules
func (c *LinuxCollector) CollectGuestRules(ctx context.Context, timeout time.Duration) sqlserverutils.MetricDetails {
	details := sqlserverutils.MetricDetails{
		Name: "OS",
	}
	fields := map[string]string{}

	if !c.remote {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		ch := make(chan bool, 1)
		go func() {
			DiskToDiskType(fields, c.disks)
			ch <- true
		}()
		select {
		case <-ctxWithTimeout.Done():
			log.Logger.Errorf("DiskToDiskType() for local linux disktype timeout")
		case <-ch:
		}

	} else {
		if c.remoteRunner == nil {
			fields[UseLocalSSD] = "unknown"
			details.Fields = append(details.Fields, fields)
			log.Logger.Debugw("Remoterunner is nil. Remote collection attempted when ssh keys aren't set up correctly. Check customer support documentation.")
			return details
		}
		defer func() {
			log.Logger.Debug("Closing the remote runner client")
			if err := c.remoteRunner.Close(); err != nil {
				log.Logger.Errorw("Failed to close the client in remote runner", "error", err)
			}
		}()
	}

	for _, rule := range CollectionOSFields() {
		exe := c.guestRuleCommandMap[rule]
		func() {
			ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			ch := make(chan bool, 1)
			go func() {
				if c.remote {
					res, err := exe.runRemoteCommand(ctx, exe.command, c.remoteRunner)
					if err != nil {
						if strings.Contains(err.Error(), "Check help docs") {
							log.Logger.Warnw("Failed to run remote command. Install command on linux vm to collect more data", "command", exe.command, "error", err)
						} else {
							log.Logger.Errorw("Failed to run remote command", "command", exe.command, "error", err)
						}
						fields[rule] = "unknown"
						ch <- false
						return
					} else if res == "null" {
						fields[rule] = "unknown"
						ch <- false
						return
					}
					fields[rule] = res
				} else if exe.isRule { // local calls are only made if isrule is true
					res, err := exe.runCommand(ctx, exe.command)
					if err != nil {
						if strings.Contains(err.Error(), "Check help docs") {
							log.Logger.Warnw("Failed to run remote command. Install command on linux vm to collect more data", "command", exe.command, "error", err)
						} else {
							log.Logger.Errorw("Failed to run command", "command", exe.command, "error", err)
						}
						fields[rule] = "unknown"
						ch <- false
						return
					} else if res == "null" {
						fields[rule] = "unknown"
						ch <- false
						return
					}
					fields[rule] = res
				}
				ch <- true
			}()

			select {
			case <-ctxWithTimeout.Done():
				log.Logger.Errorf("Running linux guest rule %s timeout", rule)
			case <-ch:
			}

		}()

	}
	details.Fields = append(details.Fields, fields)
	return details
}

func (c *LinuxCollector) gcbdrAgentRunning(cmdOutput string) (string, error) {
	reg := regexp.MustCompile(`Active: (.*) since .*`)
	match := reg.FindStringSubmatch(cmdOutput)
	if len(match) <= 1 {
		return "", fmt.Errorf("regexp did not find gcbdr_agent status field")
	}
	return strconv.FormatBool(match[1] == "active (running)"), nil
}

// commandLineExecutorWrapper executes a windows or linux command with arguments given
func commandLineExecutorWrapper(ctx context.Context, executable string, argsToSplit string, exec commandlineexecutor.Execute) (string, error) {
	result := exec(ctx, commandlineexecutor.Params{
		Executable:  executable,
		ArgsToSplit: argsToSplit,
	})
	if result.Error != nil {
		return "", fmt.Errorf("Error when running CommandLineExecutor: %s", result.StdErr)
	}
	return strings.TrimSuffix(result.StdOut, "\n"), nil
}
