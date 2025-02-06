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

package oraclediscovery

import (
	"bytes"
	"context"
	"embed"
	"errors"
	"fmt"
	"strings"
	"testing"

	tpb "google.golang.org/protobuf/types/known/timestamppb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	odpb "github.com/GoogleCloudPlatform/workloadagent/protos/oraclediscovery"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/commandlineexecutor"
)

//go:embed testdata/*
var testData embed.FS

const (
	listenerStatusSuccessFilePath   = "testdata/lsnrctl_status_success.txt"
	listenerStatusFailureFilePath   = "testdata/lsnrctl_status_failure.txt"
	listenerServicesSuccessFilePath = "testdata/lsnrctl_services_success.txt"
)

type fakeProcessLister struct {
	processes []processStub
}

func (f fakeProcessLister) listAllProcesses() ([]processWrapper, error) {
	result := make([]processWrapper, len(f.processes))
	for i, p := range f.processes {
		result[i] = processWrapper(p)
	}
	return result, nil
}

// Stub is a no-op test double for psutil.Process.
type processStub struct {
	username string
	pid      int32
	name     string
	args     []string
}

// Username returns the username of the process.
func (p processStub) Username() (string, error) {
	return p.username, nil
}

// Pid returns the PID of the process.
func (p processStub) Pid() int32 {
	return p.pid
}

// Name returns the name of the process.
func (p processStub) Name() (string, error) {
	return p.name, nil
}

func (p processStub) CmdlineSlice() ([]string, error) {
	return p.args, nil
}

func processListerFunc(p []processStub) func(d *DiscoveryService) {
	return func(d *DiscoveryService) {
		d.processLister = fakeProcessLister{processes: p}
	}
}

func readFileFunc(s string, err error) func(d *DiscoveryService) {
	return func(d *DiscoveryService) {
		d.readFile = func(string) ([]byte, error) {
			return []byte(s), err
		}
	}
}

func hostnameFunc(s string, err error) func(d *DiscoveryService) {
	return func(d *DiscoveryService) {
		d.hostname = func() (string, error) {
			return s, err
		}
	}
}

func executeCommandFunc(t *testing.T, stdout string, wantErr error, listenerStatusFilePath string, listenerServicesFilePath string) func(d *DiscoveryService) {
	t.Helper()
	return func(d *DiscoveryService) {
		d.executeCommand = func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
			if wantErr != nil {
				return commandlineexecutor.Result{Error: wantErr, ExitCode: 1}
			}
			if params.Executable == "/opt/oracle/product/19c/db/bin/sqlplus" {
				return commandlineexecutor.Result{StdOut: stdout}
			}
			key := fmt.Sprintf("%s %s", params.Executable, params.Args[0])
			switch key {
			case "/opt/oracle/product/19c/db/bin/lsnrctl status":
				s, err := testData.ReadFile(listenerStatusFilePath)
				if err != nil {
					t.Fatalf("failed to read test data: %v", err)
				}
				return commandlineexecutor.Result{StdOut: string(s)}
			case "/opt/oracle/product/19c/db/bin/lsnrctl services":
				s, err := testData.ReadFile(listenerServicesFilePath)
				if err != nil {
					t.Fatalf("failed to read test data: %v", err)
				}
				return commandlineexecutor.Result{StdOut: string(s)}
			default:
				return commandlineexecutor.Result{Error: fmt.Errorf("unexpected command"), ExitCode: 1}
			}
		}
	}
}

func TestExtractOracleEnvVars(t *testing.T) {
	type testCase struct {
		name           string
		envFileContent string
		readFileErr    error
		want           map[string]string
		wantErr        bool
	}

	tests := []testCase{
		{
			name:           "Success",
			envFileContent: "ORACLE_HOME=/opt/oracle/product/19c/db\x00ORACLE_SID=orcl",
			want: map[string]string{
				"ORACLE_HOME": "/opt/oracle/product/19c/db",
				"ORACLE_SID":  "orcl",
			},
			wantErr: false,
		},
		{
			name:        "FileReadError",
			readFileErr: errors.New("simulated read error"),
			wantErr:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			keys := []string{}
			if tc.want != nil {
				for k := range tc.want {
					keys = append(keys, k)
				}
			}

			discovery := New(readFileFunc(tc.envFileContent, tc.readFileErr))
			got, err := discovery.extractOracleEnvVars("/non/existent/file", keys...)
			if (err != nil) != tc.wantErr {
				t.Errorf("extractOracleEnvVars() got error: %v, want: %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				if diff := cmp.Diff(tc.want, got); diff != "" {
					t.Errorf("extractOracleEnvVars() found mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestFindProcessesByName(t *testing.T) {
	tests := []struct {
		name       string
		input      []processStub
		wantNames  []string
		searchName string
	}{
		{
			name: "empty process list",
		},
		{
			name: "oracle and non-oracle processes",
			input: []processStub{
				{username: "user1", pid: 123, name: "ora_pmon_testdb1"},
				{username: "user2", pid: 456, name: "ora_pmon_testdb2"},
				{username: "user3", pid: 789, name: "not_oracle_process"},
			},
			searchName: "ora_pmon_",
			wantNames:  []string{"ora_pmon_testdb1", "ora_pmon_testdb2"},
		},
		{
			name: "no match",
			input: []processStub{
				{username: "user1", pid: 123, name: "not_oracle_process"},
			},
			searchName: "ora_pmon_",
		},
		{
			name: "process with no Name",
			input: []processStub{
				{username: "user1", pid: 123},
			},
			searchName: "ora_pmon_",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := New(processListerFunc(tc.input))
			processes, err := d.findProcessesByName(context.Background(), tc.searchName)
			if err != nil {
				t.Errorf("findProcessesByName() failed: %v", err)
			}

			for i, proc := range processes {
				gotName, _ := proc.Name()
				if !strings.Contains(gotName, tc.wantNames[i]) {
					t.Errorf("got name %s, want %s", tc.wantNames[i], gotName)
				}
			}
		})
	}
}

func TestCreateDatabase(t *testing.T) {
	envVars := map[string]string{
		"ORACLE_HOME": "/opt/oracle/product/19c/db",
		"ORACLE_SID":  "TEST",
	}
	testDatabase := database{
		DBID:         1234,
		Name:         "TestDB",
		DBUniqueName: "TestDB_UNIQUE",
		CDB:          "YES",
		ConID:        5678,
		Instances: []instance{
			{
				InstanceNumber: 1,
				Name:           "TestInstance",
				Hostname:       "testhost.example.com",
				Version:        "19.0.0.0.0",
				Edition:        "EE",
				Type:           "SINGLE",
			},
		},
	}

	want := &odpb.Discovery_Database{
		Dbid:         1234,
		Name:         "TestDB",
		DbUniqueName: "TestDB_UNIQUE",
		ConId:        5678,
		Instances: []*odpb.Discovery_Database_Instance{
			{
				InstanceNumber: 1,
				Name:           "TestInstance",
				Hostname:       "testhost.example.com",
				Version:        "19.0.0.0.0",
				OracleHome:     "/opt/oracle/product/19c/db",
				OracleSid:      "TEST",
				Edition:        *odpb.Discovery_Database_Instance_DATABASE_EDITION_EE.Enum(),
				Type:           *odpb.Discovery_Database_Instance_DATABASE_TYPE_SINGLE.Enum(),
			},
		},
	}

	got := createDatabase(testDatabase, envVars)
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("createDatabase() found mismatch (-want +got):\n%s", diff)
	}
}

func TestCreateDBRoot(t *testing.T) {
	tests := []struct {
		name    string
		db      database
		envVars map[string]string
		want    *odpb.Discovery_DatabaseRoot
	}{
		{
			name: "Single instance database",
			envVars: map[string]string{
				"ORACLE_HOME": "/opt/oracle/product/19c/db",
				"ORACLE_SID":  "TEST",
			},
			db: database{
				DBID:               1234,
				Name:               "TestDB",
				DBUniqueName:       "TestDB_UNIQUE",
				Instances:          []instance{},
				CDB:                "YES",
				ConID:              5678,
				ParentDBUniqueName: "",
				DatabaseRole:       "PRIMARY",
			},
			want: &odpb.Discovery_DatabaseRoot{
				TenancyType: &odpb.Discovery_DatabaseRoot_Cdb{
					Cdb: &odpb.Discovery_DatabaseRoot_ContainerDatabase{
						Root: &odpb.Discovery_Database{
							Dbid:         1234,
							Name:         "TestDB",
							DbUniqueName: "TestDB_UNIQUE",
							ConId:        5678,
							DatabaseRole: odpb.Discovery_Database_DATABASE_ROLE_PRIMARY,
							Instances:    []*odpb.Discovery_Database_Instance{},
						},
					},
				},
			},
		},
		{
			name: "Multi-instance database",
			envVars: map[string]string{
				"ORACLE_HOME": "/opt/oracle/product/19c/db",
				"ORACLE_SID":  "TEST",
			},
			db: database{
				DBID:         1234,
				Name:         "TestDB",
				DBUniqueName: "TestDB_UNIQUE",
				Instances: []instance{
					{
						InstanceNumber: 1,
						Name:           "TestInstance1",
						Hostname:       "testhost1.example.com",
						Version:        "19.0.0.0.0",
						Edition:        "EE",
						Type:           "SINGLE",
					},
					{
						InstanceNumber: 2,
						Name:           "TestInstance2",
						Hostname:       "testhost2.example.com",
						Version:        "19.0.0.0.0",
						Edition:        "EE",
						Type:           "SINGLE",
					},
				},
				CDB:   "YES",
				ConID: 5678,
			},
			want: &odpb.Discovery_DatabaseRoot{
				TenancyType: &odpb.Discovery_DatabaseRoot_Cdb{
					Cdb: &odpb.Discovery_DatabaseRoot_ContainerDatabase{
						Root: &odpb.Discovery_Database{
							Dbid:         1234,
							Name:         "TestDB",
							DbUniqueName: "TestDB_UNIQUE",
							ConId:        5678,
							Instances: []*odpb.Discovery_Database_Instance{
								{
									InstanceNumber: 1,
									Name:           "TestInstance1",
									Hostname:       "testhost1.example.com",
									Version:        "19.0.0.0.0",
									OracleHome:     "/opt/oracle/product/19c/db",
									OracleSid:      "TEST",
									Edition:        *odpb.Discovery_Database_Instance_DATABASE_EDITION_EE.Enum(),
									Type:           *odpb.Discovery_Database_Instance_DATABASE_TYPE_SINGLE.Enum(),
								},
								{
									InstanceNumber: 2,
									Name:           "TestInstance2",
									Hostname:       "testhost2.example.com",
									Version:        "19.0.0.0.0",
									OracleHome:     "/opt/oracle/product/19c/db",
									OracleSid:      "TEST",
									Edition:        *odpb.Discovery_Database_Instance_DATABASE_EDITION_EE.Enum(),
									Type:           *odpb.Discovery_Database_Instance_DATABASE_TYPE_SINGLE.Enum(),
								},
							},
						},
					},
				},
			},
		},
		{
			name: "non-CDB database",
			db: database{
				DBID:         1234,
				Name:         "TestDB",
				DBUniqueName: "TestDB_UNIQUE",
				Instances:    []instance{},
				CDB:          "NO",
				ConID:        5678,
			},
			want: &odpb.Discovery_DatabaseRoot{
				TenancyType: &odpb.Discovery_DatabaseRoot_Db{
					Db: &odpb.Discovery_Database{
						Dbid:         1234,
						Name:         "TestDB",
						DbUniqueName: "TestDB_UNIQUE",
						ConId:        5678,
						Instances:    []*odpb.Discovery_Database_Instance{},
					},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := createDBRoot(tc.db, tc.envVars)
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("createDBRoot() found mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestParseDatabaseType(t *testing.T) {
	tests := []struct {
		input string
		want  odpb.Discovery_Database_Instance_DatabaseType
	}{
		{"RAC", odpb.Discovery_Database_Instance_DATABASE_TYPE_RAC},
		{"RACONENODE", odpb.Discovery_Database_Instance_DATABASE_TYPE_RAC_ONE_NODE},
		{"Single", odpb.Discovery_Database_Instance_DATABASE_TYPE_SINGLE},
		{"Unknown", odpb.Discovery_Database_Instance_DATABASE_TYPE_UNKNOWN},
		{"", odpb.Discovery_Database_Instance_DATABASE_TYPE_UNKNOWN},
	}

	for _, tc := range tests {
		got := parseDatabaseType(tc.input)
		if got != tc.want {
			t.Errorf("getDatabaseType(%q) = %v; want %v", tc.input, got, tc.want)
		}
	}
}

func TestParseDatabaseEdition(t *testing.T) {
	tests := []struct {
		input string
		want  odpb.Discovery_Database_Instance_DatabaseEdition
	}{
		{"EE", odpb.Discovery_Database_Instance_DATABASE_EDITION_EE},
		{"SE", odpb.Discovery_Database_Instance_DATABASE_EDITION_SE},
		{"Core EE", odpb.Discovery_Database_Instance_DATABASE_EDITION_CORE_EE},
		{"PO", odpb.Discovery_Database_Instance_DATABASE_EDITION_PO},
		{"SE", odpb.Discovery_Database_Instance_DATABASE_EDITION_SE},
		{"SE2", odpb.Discovery_Database_Instance_DATABASE_EDITION_SE2},
		{"XE", odpb.Discovery_Database_Instance_DATABASE_EDITION_XE},
		{"", odpb.Discovery_Database_Instance_DATABASE_EDITION_UNKNOWN},
	}

	for _, tc := range tests {
		got := parseDatabaseEdition(tc.input)
		if got != tc.want {
			t.Errorf("getDatabaseEdition(%q) = %v; want %v", tc.input, got, tc.want)
		}
	}
}

func TestParseDatabaseRole(t *testing.T) {
	tests := []struct {
		input string
		want  odpb.Discovery_Database_DatabaseRole
	}{
		{"PRIMARY", odpb.Discovery_Database_DATABASE_ROLE_PRIMARY},
		{"PHYSICAL STANDBY", odpb.Discovery_Database_DATABASE_ROLE_PHYSICAL_STANDBY},
		{"LOGICAL STANDBY", odpb.Discovery_Database_DATABASE_ROLE_LOGICAL_STANDBY},
		{"SNAPSHOT STANDBY", odpb.Discovery_Database_DATABASE_ROLE_SNAPSHOT_STANDBY},
		{"FAR SYNC", odpb.Discovery_Database_DATABASE_ROLE_FAR_SYNC},
		{"UNKNOWN", odpb.Discovery_Database_DATABASE_ROLE_UNKNOWN},
	}

	for _, tc := range tests {
		got := parseDatabaseRole(tc.input)
		if got != tc.want {
			t.Errorf("getDatabaseRole(%q) = %v; want %v", tc.input, got, tc.want)
		}
	}
}

func TestParseHandlerState(t *testing.T) {
	tests := []struct {
		input string
		want  odpb.Discovery_Listener_Service_DatabaseInstance_Handler_State
	}{
		{"ready", odpb.Discovery_Listener_Service_DatabaseInstance_Handler_STATE_READY},
		{"blocked", odpb.Discovery_Listener_Service_DatabaseInstance_Handler_STATE_BLOCKED},
		{"unknown", odpb.Discovery_Listener_Service_DatabaseInstance_Handler_STATE_UNKNOWN},
	}

	for _, tc := range tests {
		got := parseHandlerState(tc.input)
		if got != tc.want {
			t.Errorf("parseHandlerState(%q) = %v; want %v", tc.input, got, tc.want)
		}
	}
}

func TestDiscovery(t *testing.T) {
	tests := []struct {
		name                     string
		envFileContent           string
		processes                []processStub
		want                     *odpb.Discovery
		sqlplusOutput            string
		listenerStatusFilePath   string
		listenerServicesFilePath string
		hostname                 string
		wantErr                  error
	}{
		{
			name:           "empty process list",
			envFileContent: "ORACLE_HOME=/opt/oracle/product/19c/db\x00ORACLE_SID=orcl",
		},
		{
			name: "No oracle processes",
			processes: []processStub{
				{username: "user1", pid: 123, name: "test"},
				{username: "user2", pid: 456, name: "tnslsnr", args: []string{"tnslsnr", "LISTENER"}},
			},
		},
		{
			name:     "Oracle and listener processes",
			hostname: "test_hostname",
			processes: []processStub{
				{username: "user1", pid: 123, name: "ora_pmon_orcl"},
				{username: "user2", pid: 456, name: "tnslsnr", args: []string{"tnslsnr", "LISTENER"}},
			},
			envFileContent: "ORACLE_HOME=/opt/oracle/product/19c/db\x00ORACLE_SID=orcl",
			sqlplusOutput: `
					{
						"dbid": 1234567890,
						"name": "test_db",
						"db_unique_name": "test_db_unique_name",
						"con_id": 0,
						"CDB": "NO",
						"created": "2024-05-20",
						"parent_db_unique_name": "test_parent_db_unique_name",
						"database_role": "PRIMARY",
						"instances": [
							{
								"instance_number": 1,
								"name": "test_instance",
								"hostname": "test_hostname",
								"version": "12.3.4.5.6",
								"edition": "EE",
								"database_type": "SINGLE",
								"oracle_home": "/opt/oracle/product/19c/db",
								"oracle_sid": "orcl"
							}
						]
					}`,
			want: &odpb.Discovery{
				Databases: []*odpb.Discovery_DatabaseRoot{
					{
						TenancyType: &odpb.Discovery_DatabaseRoot_Db{
							Db: &odpb.Discovery_Database{
								Dbid:               1234567890,
								Name:               "test_db",
								DbUniqueName:       "test_db_unique_name",
								ConId:              0,
								ParentDbUniqueName: "test_parent_db_unique_name",
								DatabaseRole:       odpb.Discovery_Database_DATABASE_ROLE_PRIMARY,
								Instances: []*odpb.Discovery_Database_Instance{
									{
										InstanceNumber: 1,
										Name:           "test_instance",
										Hostname:       "test_hostname",
										Version:        "12.3.4.5.6",
										Edition:        *odpb.Discovery_Database_Instance_DATABASE_EDITION_EE.Enum(),
										Type:           *odpb.Discovery_Database_Instance_DATABASE_TYPE_SINGLE.Enum(),
										OracleHome:     "/opt/oracle/product/19c/db",
										OracleSid:      "orcl",
									},
								},
							},
						},
					},
				},
				Host: &odpb.Discovery_Host{
					Hostname: "test_hostname",
					HostType: &odpb.Discovery_Host_Vm{
						Vm: &odpb.Discovery_Host_GcpVirtualMachine{
							InstanceId: "test_instance_id",
						},
					},
				},
				Listeners: []*odpb.Discovery_Listener{
					&odpb.Discovery_Listener{
						Id: &odpb.Discovery_Listener_ListenerId{
							Alias:      "LISTENER",
							OracleHome: "/opt/oracle/product/19c/db",
						},
						StartTime:     &tpb.Timestamp{Seconds: 1707530226},
						Security:      "ON: Local OS Authentication",
						TraceLevel:    "off",
						ParameterFile: "/u01/app/19.3.0/grid/network/admin/listener.ora",
						LogFile:       "/u01/app/grid/diag/tnslsnr/g322234287-s001/listener/alert/log.xml",
						Endpoints: []*odpb.Discovery_Listener_Endpoint{
							&odpb.Discovery_Listener_Endpoint{
								Protocol: &odpb.Discovery_Listener_Endpoint_Ipc{
									Ipc: &odpb.Discovery_Listener_IPCProtocol{
										Key: "LISTENER",
									},
								},
							},
							&odpb.Discovery_Listener_Endpoint{
								Protocol: &odpb.Discovery_Listener_Endpoint_Tcp{
									Tcp: &odpb.Discovery_Listener_TCPProtocol{
										Host: "172.16.115.2",
										Port: 1521,
									},
								},
							},
							&odpb.Discovery_Listener_Endpoint{
								Protocol: &odpb.Discovery_Listener_Endpoint_Tcp{
									Tcp: &odpb.Discovery_Listener_TCPProtocol{
										Host: "172.16.115.11",
										Port: 1521,
									},
								},
							},
						},
						Services: []*odpb.Discovery_Listener_Service{
							&odpb.Discovery_Listener_Service{
								Name: "+ASM",
								Instances: []*odpb.Discovery_Listener_Service_DatabaseInstance{
									&odpb.Discovery_Listener_Service_DatabaseInstance{
										Name:   "+ASM1",
										Status: "READY",
										Handlers: []*odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
											&odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
												Name: "DEDICATED",
												Type: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_DedicatedServer_{
													DedicatedServer: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_DedicatedServer{},
												},
												State: odpb.Discovery_Listener_Service_DatabaseInstance_Handler_STATE_READY,
											},
										},
									},
								},
							},
							&odpb.Discovery_Listener_Service{
								Name: "+ASM_DATA",
								Instances: []*odpb.Discovery_Listener_Service_DatabaseInstance{
									&odpb.Discovery_Listener_Service_DatabaseInstance{
										Name:   "+ASM1",
										Status: "READY",
										Handlers: []*odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
											&odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
												Name: "DEDICATED",
												Type: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_DedicatedServer_{
													DedicatedServer: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_DedicatedServer{},
												},
												State: odpb.Discovery_Listener_Service_DatabaseInstance_Handler_STATE_READY,
											},
										},
									},
								},
							},
							&odpb.Discovery_Listener_Service{
								Name: "+ASM_RECO",
								Instances: []*odpb.Discovery_Listener_Service_DatabaseInstance{
									&odpb.Discovery_Listener_Service_DatabaseInstance{
										Name:   "+ASM1",
										Status: "READY",
										Handlers: []*odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
											&odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
												Name: "DEDICATED",
												Type: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_DedicatedServer_{
													DedicatedServer: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_DedicatedServer{},
												},
												State: odpb.Discovery_Listener_Service_DatabaseInstance_Handler_STATE_READY,
											},
										},
									},
								},
							},
							&odpb.Discovery_Listener_Service{
								Name: "orcl",
								Instances: []*odpb.Discovery_Listener_Service_DatabaseInstance{
									&odpb.Discovery_Listener_Service_DatabaseInstance{
										Name:   "orcl1",
										Status: "READY",
										Handlers: []*odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
											&odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
												Name: "DEDICATED",
												Type: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_DedicatedServer_{
													DedicatedServer: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_DedicatedServer{},
												},
												State: odpb.Discovery_Listener_Service_DatabaseInstance_Handler_STATE_READY,
											},
										},
									},
								},
							},
							&odpb.Discovery_Listener_Service{
								Name: "orclXDB",
								Instances: []*odpb.Discovery_Listener_Service_DatabaseInstance{
									&odpb.Discovery_Listener_Service_DatabaseInstance{
										Name:   "orcl1",
										Status: "READY",
										Handlers: []*odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
											&odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
												Name: "D000",
												Type: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_Dispatcher_{
													Dispatcher: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_Dispatcher{
														MachineName: "g322234287-s001",
														Pid:         2739062,
														Address: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_Dispatcher_Address{
															Host:     "g322234287-s001",
															Port:     21411,
															Protocol: "tcp",
														},
													},
												},
												State: odpb.Discovery_Listener_Service_DatabaseInstance_Handler_STATE_READY,
											},
										},
									},
								},
							},
						},
					},
				},
			},
			listenerStatusFilePath:   listenerStatusSuccessFilePath,
			listenerServicesFilePath: listenerServicesSuccessFilePath,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := New(processListerFunc(tc.processes), executeCommandFunc(t, tc.sqlplusOutput, nil, tc.listenerStatusFilePath, tc.listenerServicesFilePath), readFileFunc(tc.envFileContent, nil), hostnameFunc(tc.hostname, nil))
			got, err := d.Discover(context.Background(), &cpb.CloudProperties{InstanceId: "test_instance_id"})
			if err != nil && !errors.Is(err, tc.wantErr) {
				t.Errorf("Discover() got error: %v, want: %v", err, tc.wantErr)
			}
			if err == nil && tc.wantErr != nil {
				t.Errorf("Discover() got nil error, want: %v", tc.wantErr)
			}
			if diff := cmp.Diff(tc.want, got, protocmp.IgnoreEmptyMessages(), protocmp.Transform()); diff != "" {
				t.Errorf("Discover() found mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDiscoverDatabases(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name           string
		envFileContent string
		readFile       func(string) ([]byte, error)
		want           []*odpb.Discovery_DatabaseRoot
		wantErr        error
		stdout         string
		processes      []processStub
	}{
		{
			name:   "No processes found",
			stdout: "{}",
		},
		{
			name:   "No env variables",
			stdout: "{}",
			processes: []processStub{
				processStub{pid: 123, name: "ora_pmon_orcl", username: "user1"},
			},
			envFileContent: "",
		},
		{
			name:           "Success",
			envFileContent: "ORACLE_HOME=/opt/oracle/product/19c/db\x00ORACLE_SID=orcl",
			processes:      []processStub{{username: "user1", pid: 123, name: "ora_pmon_orcl"}},
			stdout: `
			  {
			      "dbid": 1234567890,
			      "name": "test_db",
			      "db_unique_name": "test_db_unique_name",
			      "con_id": 0,
			      "CDB": "NO",
			      "created": "2024-05-20",
			      "parent_db_unique_name": "test_parent_db_unique_name",
			      "database_role": "PRIMARY",
			      "instances": [
			          {
			              "instance_number": 1,
			              "name": "test_instance",
			              "hostname": "test_hostname",
			              "version": "12.3.4.5.6",
			              "edition": "EE",
			              "database_type": "SINGLE",
			              "oracle_home": "/opt/oracle/product/19c/db",
			              "oracle_sid": "orcl"
			          }
			      ]
			  }`,
			want: []*odpb.Discovery_DatabaseRoot{
				{
					TenancyType: &odpb.Discovery_DatabaseRoot_Db{
						Db: &odpb.Discovery_Database{
							Dbid:               1234567890,
							Name:               "test_db",
							DbUniqueName:       "test_db_unique_name",
							ConId:              0,
							ParentDbUniqueName: "test_parent_db_unique_name",
							DatabaseRole:       odpb.Discovery_Database_DATABASE_ROLE_PRIMARY,
							Instances: []*odpb.Discovery_Database_Instance{
								{
									InstanceNumber: 1,
									Name:           "test_instance",
									Hostname:       "test_hostname",
									Version:        "12.3.4.5.6",
									Edition:        *odpb.Discovery_Database_Instance_DATABASE_EDITION_EE.Enum(),
									Type:           *odpb.Discovery_Database_Instance_DATABASE_TYPE_SINGLE.Enum(),
									OracleHome:     "/opt/oracle/product/19c/db",
									OracleSid:      "orcl",
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := New(processListerFunc(tc.processes), readFileFunc(tc.envFileContent, nil), executeCommandFunc(t, tc.stdout, nil, listenerStatusSuccessFilePath, listenerServicesSuccessFilePath))
			got, err := d.discoverDatabases(ctx)
			if err != nil && !errors.Is(err, tc.wantErr) {
				t.Errorf("discoverDatabases() got error: %v, want: %v", err, tc.wantErr)
			}
			if diff := cmp.Diff(tc.want, got, protocmp.IgnoreEmptyMessages(), protocmp.Transform()); diff != "" {
				t.Errorf("discoverDatabases() found mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestExecuteSQLQuery(t *testing.T) {
	tests := []struct {
		name    string
		envVars map[string]string
		want    database
		wantErr bool
		stdout  string
	}{
		{
			name:    "missing ORACLE_HOME env variable",
			wantErr: true,
		},
		{
			name:    "missing ORACLE_SID env variable",
			envVars: map[string]string{"ORACLE_HOME": "/opt/oracle/product/19c/db"},
			wantErr: true,
		},
		{
			name:    "Error unmarshaling JSON",
			envVars: map[string]string{"ORACLE_HOME": "/opt/oracle/product/19c/db", "ORACLE_SID": "orcl"},
			stdout:  "invalid_json",
			wantErr: true,
		},
		{
			envVars: map[string]string{"ORACLE_HOME": "/opt/oracle/product/19c/db", "ORACLE_SID": "orcl"},
			stdout: `
        {
            "dbid": 1234567890,
            "name": "test_db",
            "db_unique_name": "test_db_unique_name",
            "con_id": 0,
            "CDB": "NO",
            "created": "2024-05-20",
            "parent_db_unique_name": "test_parent_db_unique_name",
            "database_role": "PRIMARY",
            "instances": [
                {
                    "instance_number": 1,
                    "name": "test_instance",
                    "hostname": "test_hostname",
                    "version": "12.3.4.5.6",
                    "edition": "EE",
                    "database_type": "SINGLE",
                    "oracle_home": "/opt/oracle/product/19c/db",
                    "oracle_sid": "orcl"
                }
            ]
        }`,
			want: database{
				DBID:               1234567890,
				Name:               "test_db",
				DBUniqueName:       "test_db_unique_name",
				ConID:              0,
				CDB:                "NO",
				ParentDBUniqueName: "test_parent_db_unique_name",
				DatabaseRole:       "PRIMARY",
				Instances: []instance{
					{
						InstanceNumber: 1,
						Name:           "test_instance",
						Hostname:       "test_hostname",
						Version:        "12.3.4.5.6",
						Edition:        "EE",
						Type:           "SINGLE",
					},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := New(executeCommandFunc(t, tc.stdout, nil, listenerStatusSuccessFilePath, listenerServicesSuccessFilePath))
			got, err := d.executeSQLQuery(context.Background(), "", tc.envVars)
			if err != nil && !tc.wantErr {
				t.Errorf("executeSQLQuery() got error: %v, want: %v", err, tc.wantErr)
			}
			if diff := cmp.Diff(tc.want, got, protocmp.IgnoreEmptyMessages(), protocmp.Transform()); diff != "" {
				t.Errorf("executeSQLQuery() found mismatch (+got, -want):\n%s", diff)
			}
		})
	}
}

func TestCreateListener(t *testing.T) {
	tests := []struct {
		name                     string
		envVars                  map[string]string
		want                     *odpb.Discovery_Listener
		wantErr                  error
		listenerStatusFilePath   string
		listenerServicesFilePath string
	}{
		{
			name:    "missing ORACLE_HOME env variable",
			wantErr: errOracleHomeNotFound,
		},
		{
			name:                   "Failed to connect to the listener",
			envVars:                map[string]string{"ORACLE_HOME": "/opt/oracle/product/19c/db"},
			listenerStatusFilePath: listenerStatusFailureFilePath,
			wantErr:                errIncompleteData,
		},
		{
			name:    "Success",
			envVars: map[string]string{"ORACLE_HOME": "/opt/oracle/product/19c/db"},
			want: &odpb.Discovery_Listener{
				Id: &odpb.Discovery_Listener_ListenerId{
					Alias:      "LISTENER",
					OracleHome: "/opt/oracle/product/19c/db",
				},
				StartTime:     &tpb.Timestamp{Seconds: 1707530226},
				Security:      "ON: Local OS Authentication",
				TraceLevel:    "off",
				ParameterFile: "/u01/app/19.3.0/grid/network/admin/listener.ora",
				LogFile:       "/u01/app/grid/diag/tnslsnr/g322234287-s001/listener/alert/log.xml",
				Endpoints: []*odpb.Discovery_Listener_Endpoint{
					&odpb.Discovery_Listener_Endpoint{
						Protocol: &odpb.Discovery_Listener_Endpoint_Ipc{
							Ipc: &odpb.Discovery_Listener_IPCProtocol{
								Key: "LISTENER",
							},
						},
					},
					&odpb.Discovery_Listener_Endpoint{
						Protocol: &odpb.Discovery_Listener_Endpoint_Tcp{
							Tcp: &odpb.Discovery_Listener_TCPProtocol{
								Host: "172.16.115.2",
								Port: 1521,
							},
						},
					},
					&odpb.Discovery_Listener_Endpoint{
						Protocol: &odpb.Discovery_Listener_Endpoint_Tcp{
							Tcp: &odpb.Discovery_Listener_TCPProtocol{
								Host: "172.16.115.11",
								Port: 1521,
							},
						},
					},
				},
				Services: []*odpb.Discovery_Listener_Service{
					&odpb.Discovery_Listener_Service{
						Name: "+ASM",
						Instances: []*odpb.Discovery_Listener_Service_DatabaseInstance{
							&odpb.Discovery_Listener_Service_DatabaseInstance{
								Name:   "+ASM1",
								Status: "READY",
								Handlers: []*odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
									&odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
										Name: "DEDICATED",
										Type: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_DedicatedServer_{
											DedicatedServer: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_DedicatedServer{},
										},
										State: odpb.Discovery_Listener_Service_DatabaseInstance_Handler_STATE_READY,
									},
								},
							},
						},
					},
					&odpb.Discovery_Listener_Service{
						Name: "+ASM_DATA",
						Instances: []*odpb.Discovery_Listener_Service_DatabaseInstance{
							&odpb.Discovery_Listener_Service_DatabaseInstance{
								Name:   "+ASM1",
								Status: "READY",
								Handlers: []*odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
									&odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
										Name: "DEDICATED",
										Type: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_DedicatedServer_{
											DedicatedServer: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_DedicatedServer{},
										},
										State: odpb.Discovery_Listener_Service_DatabaseInstance_Handler_STATE_READY,
									},
								},
							},
						},
					},
					&odpb.Discovery_Listener_Service{
						Name: "+ASM_RECO",
						Instances: []*odpb.Discovery_Listener_Service_DatabaseInstance{
							&odpb.Discovery_Listener_Service_DatabaseInstance{
								Name:   "+ASM1",
								Status: "READY",
								Handlers: []*odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
									&odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
										Name: "DEDICATED",
										Type: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_DedicatedServer_{
											DedicatedServer: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_DedicatedServer{},
										},
										State: odpb.Discovery_Listener_Service_DatabaseInstance_Handler_STATE_READY,
									},
								},
							},
						},
					},
					&odpb.Discovery_Listener_Service{
						Name: "orcl",
						Instances: []*odpb.Discovery_Listener_Service_DatabaseInstance{
							&odpb.Discovery_Listener_Service_DatabaseInstance{
								Name:   "orcl1",
								Status: "READY",
								Handlers: []*odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
									&odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
										Name: "DEDICATED",
										Type: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_DedicatedServer_{
											DedicatedServer: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_DedicatedServer{},
										},
										State: odpb.Discovery_Listener_Service_DatabaseInstance_Handler_STATE_READY,
									},
								},
							},
						},
					},
					&odpb.Discovery_Listener_Service{
						Name: "orclXDB",
						Instances: []*odpb.Discovery_Listener_Service_DatabaseInstance{
							&odpb.Discovery_Listener_Service_DatabaseInstance{
								Name:   "orcl1",
								Status: "READY",
								Handlers: []*odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
									&odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
										Name: "D000",
										Type: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_Dispatcher_{
											Dispatcher: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_Dispatcher{
												MachineName: "g322234287-s001",
												Pid:         2739062,
												Address: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_Dispatcher_Address{
													Host:     "g322234287-s001",
													Port:     21411,
													Protocol: "tcp",
												},
											},
										},
										State: odpb.Discovery_Listener_Service_DatabaseInstance_Handler_STATE_READY,
									},
								},
							},
						},
					},
				},
			},
			listenerStatusFilePath:   listenerStatusSuccessFilePath,
			listenerServicesFilePath: listenerServicesSuccessFilePath,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := New(executeCommandFunc(t, "", nil, tc.listenerStatusFilePath, tc.listenerServicesFilePath))
			got, err := d.createListener(context.Background(), "testUser", "testListener", tc.envVars)
			if err != nil && !errors.Is(err, tc.wantErr) {
				t.Errorf("createListener() got error: %v, want: %v", err, tc.wantErr)
			}
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("createListener() found mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestParseLsnrctlStatusOutput(t *testing.T) {
	envVars := map[string]string{"ORACLE_HOME": "/opt/oracle/product/19c/db"}
	want := &odpb.Discovery_Listener{
		Id: &odpb.Discovery_Listener_ListenerId{
			Alias:      "LISTENER",
			OracleHome: "/opt/oracle/product/19c/db",
		},
		StartTime:     &tpb.Timestamp{Seconds: 1707530226},
		Security:      "ON: Local OS Authentication",
		TraceLevel:    "off",
		ParameterFile: "/u01/app/19.3.0/grid/network/admin/listener.ora",
		LogFile:       "/u01/app/grid/diag/tnslsnr/g322234287-s001/listener/alert/log.xml",
		Endpoints: []*odpb.Discovery_Listener_Endpoint{
			&odpb.Discovery_Listener_Endpoint{
				Protocol: &odpb.Discovery_Listener_Endpoint_Ipc{
					Ipc: &odpb.Discovery_Listener_IPCProtocol{
						Key: "LISTENER",
					},
				},
			},
			&odpb.Discovery_Listener_Endpoint{
				Protocol: &odpb.Discovery_Listener_Endpoint_Tcp{
					Tcp: &odpb.Discovery_Listener_TCPProtocol{
						Host: "172.16.115.2",
						Port: 1521,
					},
				},
			},
			&odpb.Discovery_Listener_Endpoint{
				Protocol: &odpb.Discovery_Listener_Endpoint_Tcp{
					Tcp: &odpb.Discovery_Listener_TCPProtocol{
						Host: "172.16.115.11",
						Port: 1521,
					},
				},
			},
		},
		Services: []*odpb.Discovery_Listener_Service{
			&odpb.Discovery_Listener_Service{
				Name: "+ASM",
				Instances: []*odpb.Discovery_Listener_Service_DatabaseInstance{
					&odpb.Discovery_Listener_Service_DatabaseInstance{
						Name:   "+ASM1",
						Status: "READY",
					},
				},
			},
			&odpb.Discovery_Listener_Service{
				Name: "+ASM_DATA",
				Instances: []*odpb.Discovery_Listener_Service_DatabaseInstance{
					&odpb.Discovery_Listener_Service_DatabaseInstance{
						Name:   "+ASM1",
						Status: "READY",
					},
				},
			},
			&odpb.Discovery_Listener_Service{
				Name: "+ASM_RECO",
				Instances: []*odpb.Discovery_Listener_Service_DatabaseInstance{
					&odpb.Discovery_Listener_Service_DatabaseInstance{
						Name:   "+ASM1",
						Status: "READY",
					},
				},
			},
			&odpb.Discovery_Listener_Service{
				Name: "orcl",
				Instances: []*odpb.Discovery_Listener_Service_DatabaseInstance{
					&odpb.Discovery_Listener_Service_DatabaseInstance{
						Name:   "orcl1",
						Status: "READY",
					},
				},
			},
			&odpb.Discovery_Listener_Service{
				Name: "orclXDB",
				Instances: []*odpb.Discovery_Listener_Service_DatabaseInstance{
					&odpb.Discovery_Listener_Service_DatabaseInstance{
						Name:   "orcl1",
						Status: "READY",
					},
				},
			},
		},
	}

	statusFile, err := testData.ReadFile(listenerStatusSuccessFilePath)
	if err != nil {
		t.Fatalf("failed to read test data: %v", err)
	}
	got, err := parseLsnrctlStatusOutput(context.Background(), bytes.NewReader(statusFile), envVars)
	if err != nil {
		t.Errorf("parseLsnrctlStatusOutput() returned unexpected error: %v", err)
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("parseLsnrctlStatusOutput() found mismatch (-want +got):\n%s", diff)
	}
}

func TestDiscoverListeners(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name                     string
		envFileContent           string
		want                     []*odpb.Discovery_Listener
		wantErr                  error
		processes                []processStub
		listenerStatusFilePath   string
		listenerServicesFilePath string
	}{
		{
			name:                     "No listeners found",
			want:                     nil,
			listenerStatusFilePath:   listenerStatusSuccessFilePath,
			listenerServicesFilePath: listenerServicesSuccessFilePath,
		},
		{
			name:           "missing ORACLE_HOME env variable",
			envFileContent: "",
			processes: []processStub{
				processStub{pid: 123, name: "tnslsnr", username: "user1", args: []string{"tnslsnr", "listener1"}},
			},
			wantErr:                  errOracleHomeNotFound,
			listenerStatusFilePath:   listenerStatusSuccessFilePath,
			listenerServicesFilePath: listenerServicesSuccessFilePath,
		},
		{
			name: "Failed to connect to the listener",
			processes: []processStub{
				processStub{pid: 123, name: "tnslsnr", username: "user1", args: []string{"tnslsnr", "listener1"}},
			},
			envFileContent:           "ORACLE_HOME=/opt/oracle/product/19c/db\x00ORACLE_SID=orcl",
			wantErr:                  errIncompleteData,
			listenerStatusFilePath:   listenerStatusFailureFilePath,
			listenerServicesFilePath: listenerServicesSuccessFilePath,
		},
		{
			name: "listener and non-listener processes",
			processes: []processStub{
				processStub{pid: 123, name: "tnslsnr", username: "user1", args: []string{"tnslsnr", "listener1"}},
				processStub{pid: 456, name: "non_listener_process", username: "user2"},
			},
			envFileContent: "ORACLE_HOME=/opt/oracle/product/19c/db\x00ORACLE_SID=orcl",
			want: []*odpb.Discovery_Listener{
				&odpb.Discovery_Listener{
					Id: &odpb.Discovery_Listener_ListenerId{
						Alias:      "LISTENER",
						OracleHome: "/opt/oracle/product/19c/db",
					},
					StartTime:     &tpb.Timestamp{Seconds: 1707530226},
					Security:      "ON: Local OS Authentication",
					TraceLevel:    "off",
					ParameterFile: "/u01/app/19.3.0/grid/network/admin/listener.ora",
					LogFile:       "/u01/app/grid/diag/tnslsnr/g322234287-s001/listener/alert/log.xml",
					Endpoints: []*odpb.Discovery_Listener_Endpoint{
						&odpb.Discovery_Listener_Endpoint{
							Protocol: &odpb.Discovery_Listener_Endpoint_Ipc{
								Ipc: &odpb.Discovery_Listener_IPCProtocol{
									Key: "LISTENER",
								},
							},
						},
						&odpb.Discovery_Listener_Endpoint{
							Protocol: &odpb.Discovery_Listener_Endpoint_Tcp{
								Tcp: &odpb.Discovery_Listener_TCPProtocol{
									Host: "172.16.115.2",
									Port: 1521,
								},
							},
						},
						&odpb.Discovery_Listener_Endpoint{
							Protocol: &odpb.Discovery_Listener_Endpoint_Tcp{
								Tcp: &odpb.Discovery_Listener_TCPProtocol{
									Host: "172.16.115.11",
									Port: 1521,
								},
							},
						},
					},
					Services: []*odpb.Discovery_Listener_Service{
						&odpb.Discovery_Listener_Service{
							Name: "+ASM",
							Instances: []*odpb.Discovery_Listener_Service_DatabaseInstance{
								&odpb.Discovery_Listener_Service_DatabaseInstance{
									Name:   "+ASM1",
									Status: "READY",
									Handlers: []*odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
										&odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
											Name: "DEDICATED",
											Type: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_DedicatedServer_{
												DedicatedServer: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_DedicatedServer{},
											},
											State: odpb.Discovery_Listener_Service_DatabaseInstance_Handler_STATE_READY,
										},
									},
								},
							},
						},
						&odpb.Discovery_Listener_Service{
							Name: "+ASM_DATA",
							Instances: []*odpb.Discovery_Listener_Service_DatabaseInstance{
								&odpb.Discovery_Listener_Service_DatabaseInstance{
									Name:   "+ASM1",
									Status: "READY",
									Handlers: []*odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
										&odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
											Name: "DEDICATED",
											Type: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_DedicatedServer_{
												DedicatedServer: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_DedicatedServer{},
											},
											State: odpb.Discovery_Listener_Service_DatabaseInstance_Handler_STATE_READY,
										},
									},
								},
							},
						},
						&odpb.Discovery_Listener_Service{
							Name: "+ASM_RECO",
							Instances: []*odpb.Discovery_Listener_Service_DatabaseInstance{
								&odpb.Discovery_Listener_Service_DatabaseInstance{
									Name:   "+ASM1",
									Status: "READY",
									Handlers: []*odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
										&odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
											Name: "DEDICATED",
											Type: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_DedicatedServer_{
												DedicatedServer: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_DedicatedServer{},
											},
											State: odpb.Discovery_Listener_Service_DatabaseInstance_Handler_STATE_READY,
										},
									},
								},
							},
						},
						&odpb.Discovery_Listener_Service{
							Name: "orcl",
							Instances: []*odpb.Discovery_Listener_Service_DatabaseInstance{
								&odpb.Discovery_Listener_Service_DatabaseInstance{
									Name:   "orcl1",
									Status: "READY",
									Handlers: []*odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
										&odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
											Name: "DEDICATED",
											Type: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_DedicatedServer_{
												DedicatedServer: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_DedicatedServer{},
											},
											State: odpb.Discovery_Listener_Service_DatabaseInstance_Handler_STATE_READY,
										},
									},
								},
							},
						},
						&odpb.Discovery_Listener_Service{
							Name: "orclXDB",
							Instances: []*odpb.Discovery_Listener_Service_DatabaseInstance{
								&odpb.Discovery_Listener_Service_DatabaseInstance{
									Name:   "orcl1",
									Status: "READY",
									Handlers: []*odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
										&odpb.Discovery_Listener_Service_DatabaseInstance_Handler{
											Name: "D000",
											Type: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_Dispatcher_{
												Dispatcher: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_Dispatcher{
													MachineName: "g322234287-s001",
													Pid:         2739062,
													Address: &odpb.Discovery_Listener_Service_DatabaseInstance_Handler_Dispatcher_Address{
														Host:     "g322234287-s001",
														Port:     21411,
														Protocol: "tcp",
													},
												},
											},
											State: odpb.Discovery_Listener_Service_DatabaseInstance_Handler_STATE_READY,
										},
									},
								},
							},
						},
					},
				},
			},
			listenerStatusFilePath:   listenerStatusSuccessFilePath,
			listenerServicesFilePath: listenerServicesSuccessFilePath,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := New(processListerFunc(tc.processes), readFileFunc(tc.envFileContent, nil), executeCommandFunc(t, "", nil, tc.listenerStatusFilePath, tc.listenerServicesFilePath))
			got, err := d.discoverListeners(ctx)
			if err != nil && tc.wantErr == nil {
				t.Errorf("discoverListeners() returned unexpected error: %v", err)
			} else if err != nil && !errors.Is(err, tc.wantErr) {
				t.Errorf("discoverListeners() returned error: %v, want: %v", err, tc.wantErr)
			}

			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("discoverListeners() found mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestParseListenerEndpointProtocol(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  *odpb.Discovery_Listener_Endpoint
	}{
		{
			name:  "IPC protocol",
			input: "(DESCRIPTION=(ADDRESS=(PROTOCOL=ipc)(KEY=foo)))",
			want: &odpb.Discovery_Listener_Endpoint{
				Protocol: &odpb.Discovery_Listener_Endpoint_Ipc{
					Ipc: &odpb.Discovery_Listener_IPCProtocol{
						Key: "foo",
					},
				},
			},
		},
		{
			name:  "TCP protocol",
			input: "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=localhost)(PORT=1234)))",
			want: &odpb.Discovery_Listener_Endpoint{
				Protocol: &odpb.Discovery_Listener_Endpoint_Tcp{
					Tcp: &odpb.Discovery_Listener_TCPProtocol{
						Host: "localhost",
						Port: 1234,
					},
				},
			},
		},
		{
			name:  "TCP protocol with IP",
			input: "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=12.34.56.78)(PORT=1234)))",
			want: &odpb.Discovery_Listener_Endpoint{
				Protocol: &odpb.Discovery_Listener_Endpoint_Tcp{
					Tcp: &odpb.Discovery_Listener_TCPProtocol{
						Host: "12.34.56.78",
						Port: 1234,
					},
				},
			},
		},
		{
			name:  "TCPS protocol",
			input: "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps)(HOST=localhost)(PORT=1235)))",
			want: &odpb.Discovery_Listener_Endpoint{
				Protocol: &odpb.Discovery_Listener_Endpoint_Tcps{
					Tcps: &odpb.Discovery_Listener_TCPProtocol{
						Host: "localhost",
						Port: 1235,
					},
				},
			},
		},
		{
			name:  "NMP protocol",
			input: "(DESCRIPTION=(ADDRESS=(PROTOCOL=nmp)(SERVER=localhost)(PIPE=MyPipe)))",
			want: &odpb.Discovery_Listener_Endpoint{
				Protocol: &odpb.Discovery_Listener_Endpoint_Nmp{
					Nmp: &odpb.Discovery_Listener_NMPProtocol{
						Server: "localhost",
						Pipe:   "MyPipe",
					},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := parseListenerEndpoint(context.Background(), tc.input)
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("parseListenerEndpointProtocol() found mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDiscoverHostMetadata(t *testing.T) {
	cloudProps := &cpb.CloudProperties{
		InstanceId: "test_instance_id",
	}
	d := New(hostnameFunc("test_hostname", nil))
	got, err := d.discoverHostMetadata(context.Background(), cloudProps)
	if err != nil {
		t.Errorf("discoverHostMetadata failed: %v", err)
	}
	want := &odpb.Discovery_Host{
		Hostname: "test_hostname",
		HostType: &odpb.Discovery_Host_Vm{
			Vm: &odpb.Discovery_Host_GcpVirtualMachine{
				InstanceId: "test_instance_id",
			},
		},
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("discoverHostMetadata() found mismatch (-want +got):\n%s", diff)
	}
}
