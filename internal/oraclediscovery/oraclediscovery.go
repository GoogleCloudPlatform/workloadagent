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

// Package oraclediscovery discovers Oracle databases.
package oraclediscovery

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	"github.com/shirou/gopsutil/v3/process"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	odpb "github.com/GoogleCloudPlatform/workloadagent/protos/oraclediscovery"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/commandlineexecutor"
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/log"
)

const (
	query = `SELECT JSON_OBJECT(
			'dbid' VALUE db.dbid,
			'name' VALUE db.name,
			'db_unique_name' VALUE db.db_unique_name,
			'con_id' VALUE db.con_id,
			'CDB' VALUE db.CDB,
			'created' VALUE TO_CHAR(db.created, 'YYYY-MM-DD'),
			'database_role' VALUE db.database_role,
			'parent_db_unique_name' VALUE dg.PARENT_DBUN,
			'instances' VALUE (
					SELECT JSON_ARRAYAGG(
							JSON_OBJECT(
											'instance_number' VALUE ins.instance_number,
											'name' VALUE ins.instance_name,
											'hostname' VALUE ins.host_name,
											'version' VALUE ins.version,
											'edition' VALUE ins.edition,
											'database_type' VALUE ins.database_type
							)
					) FROM V$INSTANCE ins
			)
	) AS json_output
	FROM V$DATABASE db
	LEFT JOIN V$DATAGUARD_CONFIG dg ON db.db_unique_name = dg.DB_UNIQUE_NAME;`

	sqlSettings = "SET PAGESIZE 0 LINESIZE 32000 LONG 50000 FEEDBACK OFF " +
		"ECHO OFF TERMOUT OFF HEADING OFF VERIFY OFF TRIMSPOOL ON"

	oraProcessPrefix = "ora_pmon_"
	oraListenerProc  = "tnslsnr"
)

var (
	errOracleHomeNotFound = errors.New("ORACLE_HOME not found")
	errOracleSIDNotFound  = errors.New("ORACLE_SID not found")
	errIncompleteData     = errors.New("required fields are missing")
	sqlPlusArgs           = []string{"-LOGON", "-SILENT", "-NOLOGINTIME", "/", "as", "sysdba"}
)

type database struct {
	DBID               int64      `json:"dbid"`
	Name               string     `json:"name"`
	DBUniqueName       string     `json:"db_unique_name"`
	Instances          []instance `json:"instances"`
	CDB                string     `json:"cdb"`
	ConID              int64      `json:"con_id"`
	ParentDBUniqueName string     `json:"parent_db_unique_name"`
	DatabaseRole       string     `json:"database_role"`
}

type instance struct {
	InstanceNumber int64  `json:"instance_number"`
	Name           string `json:"name"`
	Hostname       string `json:"hostname"`
	Version        string `json:"version"`
	Edition        string `json:"edition"`
	Type           string `json:"database_type"`
}

// executeCommand abstracts the commandlineexecutor.ExecuteCommand for testability.
type executeCommand func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result

// readFile abstracts the file reading operation for testability.
type readFile func(string) ([]byte, error)

// hostname abstracts the os.Hostname for testability.
type hostname func() (string, error)

// processLister is a wrapper around []*process.Process.
type processLister interface {
	listAllProcesses() ([]processWrapper, error)
}

// defaultProcessLister implements the ProcessLister interface for listing processes.
type defaultProcessLister struct{}

// listAllProcesses returns a list of processes.
func (defaultProcessLister) listAllProcesses() ([]processWrapper, error) {
	ps, err := process.Processes()
	if err != nil {
		return nil, err
	}
	processes := make([]processWrapper, len(ps))
	for i, p := range ps {
		processes[i] = &gopsProcess{process: p}
	}
	return processes, nil
}

// DiscoveryService is used to perform Oracle discovery operations.
type DiscoveryService struct {
	processLister  processLister
	executeCommand executeCommand
	readFile       readFile
	hostname       hostname
}

// processWrapper is a wrapper around process.Process to support testing.
type processWrapper interface {
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

// New creates a new DiscoveryService.
// The options are used to override the default behavior of the DiscoveryService.
func New(options ...func(*DiscoveryService)) *DiscoveryService {
	d := DiscoveryService{
		processLister:  &defaultProcessLister{},
		executeCommand: commandlineexecutor.ExecuteCommand,
		readFile:       os.ReadFile,
		hostname:       os.Hostname,
	}
	for _, option := range options {
		option(&d)
	}
	return &d
}

// Discover returns the discovery proto containing discovered databases, listeners, and host metadata.
func (d DiscoveryService) Discover(ctx context.Context, cloudProps *cpb.CloudProperties) (*odpb.Discovery, error) {
	discovery := &odpb.Discovery{}
	var err error
	discovery.Databases, err = d.discoverDatabases(ctx)
	if err != nil {
		return nil, fmt.Errorf("discovering databases: %w", err)
	}
	if len(discovery.Databases) == 0 {
		return nil, nil
	}
	discovery.Listeners, err = d.discoverListeners(ctx)
	if err != nil {
		return nil, fmt.Errorf("discovering listeners: %w", err)
	}
	if len(discovery.Listeners) == 0 {
		return nil, nil
	}
	discovery.Host, err = d.discoverHostMetadata(ctx, cloudProps)
	if err != nil {
		return nil, fmt.Errorf("discovering host metadata: %w", err)
	}
	return discovery, nil
}

func (d DiscoveryService) discoverHostMetadata(ctx context.Context, cloudProps *cpb.CloudProperties) (*odpb.Discovery_Host, error) {
	hostname, err := d.hostname()
	if err != nil {
		return nil, fmt.Errorf("getting hostname: %w", err)
	}
	return &odpb.Discovery_Host{
		Hostname: hostname,
		HostType: &odpb.Discovery_Host_Vm{
			Vm: &odpb.Discovery_Host_GcpVirtualMachine{
				InstanceId: cloudProps.GetInstanceId(),
			},
		},
	}, nil
}

// extractOracleEnvVars extracts the given environment variables from the given environ file.
func (d DiscoveryService) extractOracleEnvVars(environFilePath string, varsToExtract ...string) (map[string]string, error) {
	data, err := d.readFile(environFilePath)
	if err != nil {
		return nil, fmt.Errorf("reading environment variables from %q file: %w", environFilePath, err)
	}

	// The entries in /proc/[pid]/environ file are separated by the null character
	envVars := strings.Split(string(data), "\x00")
	results := make(map[string]string)
	varsSet := make(map[string]bool)
	for _, v := range varsToExtract {
		varsSet[v] = true
	}

	for _, envVar := range envVars {
		if envVar == "" {
			continue
		}
		split := strings.SplitN(envVar, "=", 2)
		if len(split) != 2 {
			return nil, fmt.Errorf("failed to split environment variable %q into key and value", envVar)
		}
		varName, varValue := split[0], split[1]
		if _, ok := varsSet[varName]; ok {
			results[varName] = varValue
		}
	}

	return results, nil
}

func (d DiscoveryService) discoverDatabases(ctx context.Context) ([]*odpb.Discovery_DatabaseRoot, error) {
	var dbroots []*odpb.Discovery_DatabaseRoot
	procs, err := d.findProcessesByName(ctx, oraProcessPrefix)
	if err != nil {
		return nil, err
	}
	log.CtxLogger(ctx).Debugf("found %d Oracle processes: %+v", len(procs), procs)
	for _, p := range procs {
		environFilePath := fmt.Sprintf("/proc/%d/environ", p.Pid())
		envVars, err := d.extractOracleEnvVars(environFilePath, "ORACLE_HOME", "ORACLE_SID")
		if err != nil || len(envVars) != 2 {
			log.CtxLogger(ctx).Warnw("Failed to extract ORACLE_HOME and ORACLE_SID environment variables", "error", err, "process_id", p.Pid(), "environment_file", environFilePath)
			continue
		}
		username, err := p.Username()
		if err != nil {
			log.CtxLogger(ctx).Warnw("Unable to extract username", "error", err, "process_id", p.Pid(), "SID", envVars["ORACLE_SID"])
			continue
		}
		db, err := d.executeSQLQuery(ctx, username, envVars)
		if err != nil {
			return nil, fmt.Errorf("fetching database information: %w", err)
		}
		dbroots = append(dbroots, createDBRoot(db, envVars))
	}
	return dbroots, nil
}

func (d DiscoveryService) executeSQLQuery(ctx context.Context, username string, envVars map[string]string) (database, error) {
	if _, ok := envVars["ORACLE_HOME"]; !ok {
		return database{}, errOracleHomeNotFound
	}
	if _, ok := envVars["ORACLE_SID"]; !ok {
		return database{}, errOracleSIDNotFound
	}

	var vars []string
	for key, value := range envVars {
		vars = append(vars, fmt.Sprintf("%s=%s", key, value))
	}
	params := commandlineexecutor.Params{
		Executable: fmt.Sprintf("%s/bin/sqlplus", envVars["ORACLE_HOME"]),
		Args:       sqlPlusArgs,
		Stdin:      fmt.Sprintf("%s\n%s", sqlSettings, query),
		Env:        vars,
		User:       username,
	}
	result := d.executeCommand(ctx, params)
	if result.Error != nil {
		log.CtxLogger(ctx).Errorw("Failed to execute command", "params", params, "result", result)
		return database{}, fmt.Errorf("executing command to fetch database information: %w", result.Error)
	}

	var db database
	if err := json.Unmarshal([]byte(result.StdOut), &db); err != nil {
		log.CtxLogger(ctx).Errorw("Failed to parse JSON output from SQL query", "params", params, "result", result)
		return database{}, fmt.Errorf("parsing JSON output from SQL query: %w", err)
	}

	return db, nil
}

// createDBRoot creates a database root proto message for the given database.
func createDBRoot(database database, envVars map[string]string) *odpb.Discovery_DatabaseRoot {
	db := createDatabase(database, envVars)
	dbRoot := &odpb.Discovery_DatabaseRoot{}
	if database.CDB == "YES" {
		cdbRoot := &odpb.Discovery_DatabaseRoot_ContainerDatabase{Root: db}
		dbRoot.TenancyType = &odpb.Discovery_DatabaseRoot_Cdb{Cdb: cdbRoot}
	} else {
		dbRoot.TenancyType = &odpb.Discovery_DatabaseRoot_Db{Db: db}
	}
	return dbRoot
}

func createDatabase(database database, envVars map[string]string) *odpb.Discovery_Database {
	db := &odpb.Discovery_Database{
		Name:               database.Name,
		DbUniqueName:       database.DBUniqueName,
		Dbid:               database.DBID,
		ConId:              database.ConID,
		ParentDbUniqueName: database.ParentDBUniqueName,
		DatabaseRole:       parseDatabaseRole(database.DatabaseRole),
	}

	for _, inst := range database.Instances {
		instance := &odpb.Discovery_Database_Instance{
			InstanceNumber: inst.InstanceNumber,
			Name:           inst.Name,
			Hostname:       inst.Hostname,
			Version:        inst.Version,
			Edition:        parseDatabaseEdition(inst.Edition),
			Type:           parseDatabaseType(inst.Type),
			OracleHome:     envVars["ORACLE_HOME"],
			OracleSid:      envVars["ORACLE_SID"],
		}
		db.Instances = append(db.Instances, instance)
	}
	return db
}

func (d DiscoveryService) discoverListeners(ctx context.Context) ([]*odpb.Discovery_Listener, error) {
	var listeners []*odpb.Discovery_Listener
	procs, err := d.findProcessesByName(ctx, oraListenerProc)
	if err != nil {
		return nil, err
	}
	log.CtxLogger(ctx).Debugf("Found %d Listener processes", len(procs))

	for _, p := range procs {
		environFilePath := fmt.Sprintf("/proc/%d/environ", p.Pid())
		envVars, err := d.extractOracleEnvVars(environFilePath, "ORACLE_HOME")
		if err != nil || len(envVars) == 0 {
			log.CtxLogger(ctx).Warnw("Failed to extract ORACLE_HOME environment variable", "error", err)
			continue
		}
		username, err := p.Username()
		if err != nil {
			log.CtxLogger(ctx).Warnw("Failed to extract username from process", "error", err, "process_id", p.Pid())
			continue
		}
		args, err := p.CmdlineSlice()
		if err != nil {
			log.CtxLogger(ctx).Warnw("Failed to extract command line arguments", "error", err, "process_id", p.Pid())
			continue
		}
		if len(args) < 2 {
			log.CtxLogger(ctx).Warnw("Listener alias not found in command line arguments", "process_id", p.Pid(), "args", args)
			continue
		}
		alias := args[1]
		listener, err := d.createListener(ctx, username, alias, envVars)
		if err != nil {
			return nil, err
		}
		listeners = append(listeners, listener)
	}
	return listeners, nil
}

func (d DiscoveryService) createListener(ctx context.Context, username string, alias string, envVars map[string]string) (*odpb.Discovery_Listener, error) {
	if _, ok := envVars["ORACLE_HOME"]; !ok {
		return nil, errOracleHomeNotFound
	}

	var vars []string
	for key, value := range envVars {
		vars = append(vars, fmt.Sprintf("%s=%s", key, value))
	}
	params := commandlineexecutor.Params{
		Executable: fmt.Sprintf("%s/bin/lsnrctl", envVars["ORACLE_HOME"]),
		Args:       []string{"status", alias},
		Env:        vars,
		User:       username,
	}
	result := d.executeCommand(ctx, params)
	if result.Error != nil {
		log.CtxLogger(ctx).Errorw("Failed to execute command", "params", params, "result", result)
		return nil, fmt.Errorf("executing command to get listener information: %w", result.Error)
	}

	listener, err := parseLsnrctlStatusOutput(ctx, strings.NewReader(result.StdOut), envVars)
	if err != nil {
		log.CtxLogger(ctx).Errorw("Failed to parse output from 'lsnrctl status' command", "params", params, "result", result)
		return nil, fmt.Errorf("parsing output from 'lsnrctl status' command: %w", err)
	}

	params = commandlineexecutor.Params{
		Executable: fmt.Sprintf("%s/bin/lsnrctl", envVars["ORACLE_HOME"]),
		Args:       []string{"services", alias},
		Env:        vars,
		User:       username,
	}
	result = d.executeCommand(ctx, params)
	if result.Error != nil {
		log.CtxLogger(ctx).Errorw("Failed to execute command", "params", params, "result", result)
		return nil, fmt.Errorf("executing command to get listener information: %w", result.Error)
	}

	err = populateInstanceHandlers(ctx, strings.NewReader(result.StdOut), listener)
	if err != nil {
		log.CtxLogger(ctx).Errorw("Failed to parse output from 'lsnrctl services' command", "params", params, "result", result)
		return nil, fmt.Errorf("parsing output from 'lsnrctl services' command: %w", err)
	}

	return listener, nil
}

// parseLsnrctlStatusOutput parses the output of the 'lsnrctl status' command and returns a
// Discovery_Listener proto message.
func parseLsnrctlStatusOutput(ctx context.Context, r io.Reader, envVars map[string]string) (*odpb.Discovery_Listener, error) {
	scanner := bufio.NewScanner(r)
	listener := &odpb.Discovery_Listener{}
	currentServiceName := ""

	reAlias := regexp.MustCompile(`Alias\s+(.*)`)
	reTraceLevel := regexp.MustCompile(`Trace Level\s+(.*)`)
	reSecurity := regexp.MustCompile(`Security\s+(.*)`)
	reStartDate := regexp.MustCompile(`Start Date\s+(\d{2}-\w{3}-\d{4} \d{2}:\d{2}:\d{2})`)
	reParameterFile := regexp.MustCompile(`Listener Parameter File\s+(.*)`)
	reLogFile := regexp.MustCompile(`Listener Log File\s+(.*)`)
	reService := regexp.MustCompile(`Service\s+"([^"]+)"\s+has\s+\d+\s+instance\(s\)`)
	reInstance := regexp.MustCompile(`Instance\s+"([^"]+)",\s+status\s+(\w+),`)
	reEndpoint := regexp.MustCompile(`\s*\(DESCRIPTION=\(ADDRESS=`)

	for scanner.Scan() {
		line := scanner.Text()

		switch {
		case reAlias.MatchString(line):
			alias := reAlias.FindStringSubmatch(line)[1]
			listener.Id = &odpb.Discovery_Listener_ListenerId{
				Alias:      alias,
				OracleHome: envVars["ORACLE_HOME"],
			}
		case reStartDate.MatchString(line):
			startDateStr := reStartDate.FindStringSubmatch(line)[1]
			startDate, err := time.Parse("02-Jan-2006 15:04:05", startDateStr)
			if err != nil {
				log.CtxLogger(ctx).Warnw("Failed to parse start date", "error", err, "date_string", startDateStr)
			}
			listener.StartTime = timestamppb.New(startDate)
		case reSecurity.MatchString(line):
			listener.Security = reSecurity.FindStringSubmatch(line)[1]
		case reParameterFile.MatchString(line):
			listener.ParameterFile = reParameterFile.FindStringSubmatch(line)[1]
		case reLogFile.MatchString(line):
			listener.LogFile = reLogFile.FindStringSubmatch(line)[1]
		case reTraceLevel.MatchString(line):
			listener.TraceLevel = reTraceLevel.FindStringSubmatch(line)[1]
		case reService.MatchString(line):
			currentServiceName = reService.FindStringSubmatch(line)[1]
			service := &odpb.Discovery_Listener_Service{
				Name: currentServiceName,
			}
			listener.Services = append(listener.Services, service)
		case reInstance.MatchString(line) && currentServiceName != "":
			matches := reInstance.FindStringSubmatch(line)
			instance := &odpb.Discovery_Listener_Service_DatabaseInstance{
				Name:   matches[1],
				Status: matches[2],
			}
			if len(listener.Services) > 0 {
				lastService := listener.Services[len(listener.Services)-1]
				lastService.Instances = append(lastService.Instances, instance)
			}
		case reEndpoint.MatchString(line):
			listener.Endpoints = append(listener.Endpoints, parseListenerEndpoint(ctx, line))
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	if listener.Id == nil {
		return nil, errIncompleteData
	}

	return listener, nil
}

func populateInstanceHandlers(ctx context.Context, r io.Reader, listener *odpb.Discovery_Listener) error {
	var (
		currentService    *odpb.Discovery_Listener_Service
		currentInstance   *odpb.Discovery_Listener_Service_DatabaseInstance
		currentHandler    *odpb.Discovery_Listener_Service_DatabaseInstance_Handler
		currentDispatcher *odpb.Discovery_Listener_Service_DatabaseInstance_Handler_Dispatcher
	)

	scanner := bufio.NewScanner(r)

	reService := regexp.MustCompile(`Service\s+"([^"]+)"\s+has\s+\d+\s+instance\(s\)`)
	reInstance := regexp.MustCompile(`Instance\s+"([^"]+)",\s+status\s+(\w+)`)
	reHandler := regexp.MustCompile(`"([^"]+)"\s+established:(\d+)\s+refused:(\d+)(\s+current:(\d+))?(\s+max:(\d+))?\s+state:(\w+)`)
	reDispatcher := regexp.MustCompile(`DISPATCHER\s+<machine:\s+([^,]+),\s+pid:\s+(\d+)>`)
	reAddress := regexp.MustCompile(`\(ADDRESS=\(PROTOCOL=(ipc|tcp|tcps|nmp)\)(\(HOST=([^)]+)\))?(\(PORT=(\d+)\))?(\(SERVER=([^)]+)\))?(\(PIPE=([^)]+)\))?(\(KEY=([^)]+)\))?\)`)

	findServiceByName := func(name string) *odpb.Discovery_Listener_Service {
		for _, service := range listener.Services {
			if service.Name == name {
				return service
			}
		}
		return nil
	}

	findInstanceByName := func(service *odpb.Discovery_Listener_Service, name string) *odpb.Discovery_Listener_Service_DatabaseInstance {
		for _, instance := range service.Instances {
			if instance.Name == name {
				return instance
			}
		}
		return nil
	}

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		switch {
		case reService.MatchString(line):
			serviceName := reService.FindStringSubmatch(line)[1]
			currentService = findServiceByName(serviceName)
		case reInstance.MatchString(line) && currentService != nil:
			instanceName := reInstance.FindStringSubmatch(line)[1]
			currentInstance = findInstanceByName(currentService, instanceName)
		case reHandler.MatchString(line) && currentInstance != nil:
			matches := reHandler.FindStringSubmatch(line)
			name := matches[1]
			state := matches[8]
			currentHandler = &odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
				Name:  name,
				State: parseHandlerState(state), // TODO: Integrate this into the metrics collection pipeline
			}
			if name == "DEDICATED" {
				currentHandler.Type = &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_DedicatedServer_{
					DedicatedServer: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_DedicatedServer{},
				}
			}
			currentInstance.Handlers = append(currentInstance.Handlers, currentHandler)
		case reDispatcher.MatchString(line) && currentHandler != nil:
			matches := reDispatcher.FindStringSubmatch(line)
			machineName := matches[1]
			pid, err := strconv.Atoi(matches[2])
			if err != nil {
				log.CtxLogger(ctx).Warnw("Failed to parse PID", "error", err, "pid_string", matches[2])
			}
			currentDispatcher = &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_Dispatcher{
				MachineName: machineName,
				Pid:         uint32(pid),
			}
			currentHandler.Type = &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_Dispatcher_{
				Dispatcher: currentDispatcher,
			}
		case reAddress.MatchString(line) && currentDispatcher != nil:
			matches := reAddress.FindStringSubmatch(line)
			port, err := strconv.Atoi(matches[5])
			if err != nil {
				log.CtxLogger(ctx).Warnw("Failed to parse port", "error", err, "port_string", matches[5])
			}
			currentDispatcher.Address = &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_Dispatcher_Address{
				Protocol: matches[1],
				Host:     matches[3],
				Port:     uint32(port),
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

// findProcessesByName finds and returns a slice of processes that match the given name.
func (d DiscoveryService) findProcessesByName(ctx context.Context, name string) ([]processWrapper, error) {
	allProcesses, err := d.processLister.listAllProcesses()
	if err != nil {
		return nil, err
	}

	var matchedProcesses []processWrapper
	for _, proc := range allProcesses {
		procName, err := proc.Name()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				// Silently skip to avoid cluttering logs as it's expected that processes come and go
				continue
			}
			log.CtxLogger(ctx).Warnw("Could not get the name of process, skipping", "process_id", proc.Pid(), "error", err)
			continue
		}
		if strings.HasPrefix(procName, name) {
			matchedProcesses = append(matchedProcesses, proc)
		}
	}

	return matchedProcesses, nil
}

func parseDatabaseRole(role string) odpb.Discovery_Database_DatabaseRole {
	switch strings.ToUpper(role) {
	case "PRIMARY":
		return odpb.Discovery_Database_DATABASE_ROLE_PRIMARY
	case "PHYSICAL STANDBY":
		return odpb.Discovery_Database_DATABASE_ROLE_PHYSICAL_STANDBY
	case "LOGICAL STANDBY":
		return odpb.Discovery_Database_DATABASE_ROLE_LOGICAL_STANDBY
	case "SNAPSHOT STANDBY":
		return odpb.Discovery_Database_DATABASE_ROLE_SNAPSHOT_STANDBY
	case "FAR SYNC":
		return odpb.Discovery_Database_DATABASE_ROLE_FAR_SYNC
	default:
		return odpb.Discovery_Database_DATABASE_ROLE_UNKNOWN
	}
}

func parseDatabaseEdition(edition string) odpb.Discovery_Database_Instance_DatabaseEdition {
	switch strings.ToUpper(edition) {
	case "CORE EE":
		return odpb.Discovery_Database_Instance_DATABASE_EDITION_CORE_EE
	case "EE":
		return odpb.Discovery_Database_Instance_DATABASE_EDITION_EE
	case "PO":
		return odpb.Discovery_Database_Instance_DATABASE_EDITION_PO
	case "SE":
		return odpb.Discovery_Database_Instance_DATABASE_EDITION_SE
	case "SE2":
		return odpb.Discovery_Database_Instance_DATABASE_EDITION_SE2
	case "XE":
		return odpb.Discovery_Database_Instance_DATABASE_EDITION_XE
	default:
		return odpb.Discovery_Database_Instance_DATABASE_EDITION_UNKNOWN
	}
}

func parseDatabaseType(dbType string) odpb.Discovery_Database_Instance_DatabaseType {
	switch strings.ToUpper(dbType) {
	case "RAC":
		return odpb.Discovery_Database_Instance_DATABASE_TYPE_RAC
	case "RACONENODE":
		return odpb.Discovery_Database_Instance_DATABASE_TYPE_RAC_ONE_NODE
	case "SINGLE":
		return odpb.Discovery_Database_Instance_DATABASE_TYPE_SINGLE
	default:
		return odpb.Discovery_Database_Instance_DATABASE_TYPE_UNKNOWN
	}
}

func parseHandlerState(state string) odpb.Discovery_Listener_Service_DatabaseInstance_Handler_State {
	switch strings.ToUpper(state) {
	case "READY":
		return odpb.Discovery_Listener_Service_DatabaseInstance_Handler_STATE_READY
	case "BLOCKED":
		return odpb.Discovery_Listener_Service_DatabaseInstance_Handler_STATE_BLOCKED
	default:
		return odpb.Discovery_Listener_Service_DatabaseInstance_Handler_STATE_UNKNOWN
	}
}

func parseListenerEndpoint(ctx context.Context, input string) *odpb.Discovery_Listener_Endpoint {
	endpoint := &odpb.Discovery_Listener_Endpoint{}
	var protocol, host, server, pipe, key string
	var port int
	var err error

	segments := strings.FieldsFunc(input, func(r rune) bool {
		return r == '(' || r == ')'
	})

	for _, segment := range segments {
		if segment == "" {
			continue
		}
		pair := strings.Split(segment, "=")
		if len(pair) != 2 {
			continue
		}
		value := pair[1]
		switch strings.ToUpper(pair[0]) {
		case "PROTOCOL":
			protocol = value
		case "KEY":
			key = value
		case "HOST":
			host = value
		case "PORT":
			port, err = strconv.Atoi(value)
			if err != nil {
				log.CtxLogger(ctx).Warnw("Failed to parse port", "error", err, "port_string", value)
			}
		case "SERVER":
			server = value
		case "PIPE":
			pipe = value
		}
		switch strings.ToUpper(protocol) {
		case "IPC":
			endpoint.Protocol = &odpb.Discovery_Listener_Endpoint_Ipc{
				Ipc: &odpb.Discovery_Listener_IPCProtocol{
					Key: key,
				},
			}
		case "TCP":
			endpoint.Protocol = &odpb.Discovery_Listener_Endpoint_Tcp{
				Tcp: &odpb.Discovery_Listener_TCPProtocol{
					Host: host,
					Port: int32(port),
				},
			}
		case "TCPS":
			endpoint.Protocol = &odpb.Discovery_Listener_Endpoint_Tcps{
				Tcps: &odpb.Discovery_Listener_TCPProtocol{
					Host: host,
					Port: int32(port),
				},
			}
		case "NMP":
			endpoint.Protocol = &odpb.Discovery_Listener_Endpoint_Nmp{
				Nmp: &odpb.Discovery_Listener_NMPProtocol{
					Server: server,
					Pipe:   pipe,
				},
			}
		}
	}
	return endpoint
}
