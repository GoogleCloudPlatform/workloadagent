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

package oracle

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"github.com/GoogleCloudPlatform/workloadagent/internal/servicecommunication"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce/metadataserver"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/guestactions"

	mrpb "google.golang.org/genproto/googleapis/monitoring/v3"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	odpb "github.com/GoogleCloudPlatform/workloadagent/protos/oraclediscovery"
	gapb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/guestactions"
)

// fakeGuestActionsManager is a test double for guestActionsManager.
type fakeGuestActionsManager struct {
	startCalled bool
	startOpts   guestactions.Options
	started     chan struct{}
	onStart     sync.Once
}

// Start captures the options passed and marks itself as called.
func (f *fakeGuestActionsManager) Start(ctx context.Context, a any) {
	f.startCalled = true
	f.startOpts = a.(guestactions.Options)
	if f.started != nil {
		f.onStart.Do(func() {
			close(f.started)
		})
	}
}

// fakeDiscoveryClient is a test double for DiscoveryClient.
type fakeDiscoveryClient struct {
	discoverCalled bool
	discoverErr    error
}

func (f *fakeDiscoveryClient) Discover(ctx context.Context, cloudProps *cpb.CloudProperties, processes []servicecommunication.ProcessWrapper) (*odpb.Discovery, error) {
	f.discoverCalled = true
	return nil, f.discoverErr
}

// fakeMetricCollector is a test double for MetricCollector.
type fakeMetricCollector struct {
	healthMetricsCalled  bool
	defaultMetricsCalled bool
}

func (f *fakeMetricCollector) SendHealthMetricsToCloudMonitoring(ctx context.Context) []*mrpb.TimeSeries {
	f.healthMetricsCalled = true
	return nil
}

func (f *fakeMetricCollector) SendDefaultMetricsToCloudMonitoring(ctx context.Context) []*mrpb.TimeSeries {
	f.defaultMetricsCalled = true
	return nil
}

type fakeProcess struct {
	name string
}

func (p fakeProcess) Name() (string, error) {
	return p.name, nil
}

func (p fakeProcess) Exe() (string, error) {
	return "", nil
}

func (p fakeProcess) Cmdline() (string, error) {
	return "", nil
}

func (p fakeProcess) CmdlineSlice() ([]string, error) {
	return nil, nil
}

func (p fakeProcess) Pid() int32 {
	return 0
}

func (p fakeProcess) PPID() (int32, error) {
	return 0, nil
}

func (p fakeProcess) Username() (string, error) {
	return "", nil
}

func (p fakeProcess) CreateTime() (int64, error) {
	return 0, nil
}

func (p fakeProcess) Environ() ([]string, error) {
	return nil, nil
}

func (p fakeProcess) String() string {
	return ""
}

func TestConvertCloudProperties(t *testing.T) {
	tests := []struct {
		name string
		cp   *cpb.CloudProperties
		want *metadataserver.CloudProperties
	}{
		{
			name: "NilCloudProperties",
			cp:   nil,
			want: nil,
		},
		{
			name: "NonNilCloudProperties",
			cp: &cpb.CloudProperties{
				ProjectId:           "test-project",
				NumericProjectId:    "12345",
				InstanceId:          "test-instance",
				Zone:                "us-central1-a",
				InstanceName:        "test-instance-name",
				Image:               "test-image",
				MachineType:         "n1-standard-1",
				Region:              "us-central1",
				ServiceAccountEmail: "test-sa@google.com",
				Scopes:              []string{"scope1", "scope2"},
			},
			want: &metadataserver.CloudProperties{
				ProjectID:           "test-project",
				NumericProjectID:    "12345",
				InstanceID:          "test-instance",
				Zone:                "us-central1-a",
				InstanceName:        "test-instance-name",
				Image:               "test-image",
				MachineType:         "n1-standard-1",
				Region:              "us-central1",
				ServiceAccountEmail: "test-sa@google.com",
				Scopes:              []string{"scope1", "scope2"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := convertCloudProperties(tc.cp)
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("convertCloudProperties(%v) returned an unexpected diff (-want +got): %v", tc.cp, diff)
			}
		})
	}
}

func TestOracleCommandKey(t *testing.T) {
	tests := []struct {
		name        string
		cmd         *gapb.Command
		cp          *metadataserver.CloudProperties
		wantKey     string
		wantTimeout time.Duration
		wantLock    bool
	}{
		{
			name:        "NilAgentCommand",
			cmd:         &gapb.Command{},
			cp:          &metadataserver.CloudProperties{InstanceID: "test-instance"},
			wantKey:     "",
			wantTimeout: 0,
			wantLock:    false,
		},
		{
			name: "NilParameters",
			cmd: &gapb.Command{
				CommandType: &gapb.Command_AgentCommand{
					AgentCommand: &gapb.AgentCommand{
						Command: "oracle_start_database",
					},
				},
			},
			cp:          &metadataserver.CloudProperties{InstanceID: "test-instance"},
			wantKey:     "",
			wantTimeout: 0,
			wantLock:    false,
		},
		{
			name: "EmptyParameters",
			cmd: &gapb.Command{
				CommandType: &gapb.Command_AgentCommand{
					AgentCommand: &gapb.AgentCommand{
						Command:    "oracle_start_database",
						Parameters: map[string]string{},
					},
				},
			},
			cp:          &metadataserver.CloudProperties{InstanceID: "test-instance"},
			wantKey:     "",
			wantTimeout: 0,
			wantLock:    false,
		},
		{
			name: "MissingOracleHome",
			cmd: &gapb.Command{
				CommandType: &gapb.Command_AgentCommand{
					AgentCommand: &gapb.AgentCommand{
						Command: "oracle_start_database",
						Parameters: map[string]string{
							"oracle_sid": "orcl",
						},
					},
				},
			},
			cp:          &metadataserver.CloudProperties{InstanceID: "test-instance"},
			wantKey:     "",
			wantTimeout: 0,
			wantLock:    false,
		},
		{
			name: "MissingOracleSID",
			cmd: &gapb.Command{
				CommandType: &gapb.Command_AgentCommand{
					AgentCommand: &gapb.AgentCommand{
						Command: "oracle_start_database",
						Parameters: map[string]string{
							"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
						},
					},
				},
			},
			cp:          &metadataserver.CloudProperties{InstanceID: "test-instance"},
			wantKey:     "",
			wantTimeout: 0,
			wantLock:    false,
		},
		{
			name: "SIDAndHome_NilCloudProperties",
			cmd: &gapb.Command{
				CommandType: &gapb.Command_AgentCommand{
					AgentCommand: &gapb.AgentCommand{
						Command: "oracle_health_check",
						Parameters: map[string]string{
							"oracle_sid":  "orcl",
							"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
						},
					},
				},
			},
			cp:          nil,
			wantKey:     "",
			wantTimeout: 0,
			wantLock:    false,
		},
		{
			name: "SIDAndHome_CloudProperties",
			cmd: &gapb.Command{
				CommandType: &gapb.Command_AgentCommand{
					AgentCommand: &gapb.AgentCommand{
						Command: "oracle_health_check",
						Parameters: map[string]string{
							"oracle_sid":  "orcl",
							"oracle_home": "/u01/app/oracle/product/19.3.0/dbhome_1",
						},
					},
				},
			},
			cp:          &metadataserver.CloudProperties{InstanceID: "test-instance"},
			wantKey:     "test-instance:/u01/app/oracle/product/19.3.0/dbhome_1:orcl",
			wantTimeout: 24 * time.Hour,
			wantLock:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotKey, gotTimeout, gotLock := oracleCommandKey(context.Background(), tc.cmd, tc.cp)
			if gotKey != tc.wantKey || gotTimeout != tc.wantTimeout || gotLock != tc.wantLock {
				t.Errorf("oracleCommandKey(%v, %v) = (%q, %v, %v), want (%q, %v, %v)", tc.cmd, tc.cp, gotKey, gotTimeout, gotLock, tc.wantKey, tc.wantTimeout, tc.wantLock)
			}
		})
	}
}

func TestRunGuestActions(t *testing.T) {
	// Keep track of the original newGuestActionsManager and restore it after the test.
	originalNewGuestActionsManager := newGuestActionsManager
	defer func() {
		newGuestActionsManager = originalNewGuestActionsManager
	}()

	fakeGA := &fakeGuestActionsManager{}
	newGuestActionsManager = func() guestActionsManager {
		return fakeGA
	}

	cloudProps := &cpb.CloudProperties{ProjectId: "test-project"}
	handlers := map[string]guestactions.GuestActionHandler{
		"test_handler": func(ctx context.Context, cmd *gapb.Command, cp *metadataserver.CloudProperties) *gapb.CommandResult {
			return nil
		},
	}
	args := runGuestActionsArgs{
		s: &Service{
			CloudProps: cloudProps,
		},
		handlers: handlers,
	}

	runGuestActions(context.Background(), args)

	if !fakeGA.startCalled {
		t.Errorf("runGuestActions() did not call Start()")
	}

	if fakeGA.startOpts.Channel != defaultChannel {
		t.Errorf("runGuestActions() called Start() with channel %q, want %q", fakeGA.startOpts.Channel, defaultChannel)
	}
	if fakeGA.startOpts.CloudProperties.ProjectID != cloudProps.ProjectId {
		t.Errorf("runGuestActions() called Start() with CloudProperties.ProjectID %q, want %q", fakeGA.startOpts.CloudProperties.ProjectID, cloudProps.ProjectId)
	}
	if fakeGA.startOpts.CommandConcurrencyKey == nil {
		t.Errorf("runGuestActions() called Start() with nil CommandConcurrencyKey, want non-nil")
	}
}

func TestGuestActionHandlers(t *testing.T) {
	handlers := guestActionHandlers()
	expectedHandlers := []string{
		"oracle_data_guard_switchover",
		"oracle_disable_autostart",
		"oracle_disable_restricted_session",
		"oracle_enable_autostart",
		"oracle_health_check",
		"oracle_run_datapatch",
		"oracle_run_discovery",
		"oracle_start_database",
		"oracle_start_listener",
		"oracle_stop_database",
	}

	if len(handlers) != len(expectedHandlers) {
		t.Errorf("getGuestActionHandlers() returned %d handlers, want %d", len(handlers), len(expectedHandlers))
	}

	for _, h := range expectedHandlers {
		if _, ok := handlers[h]; !ok {
			t.Errorf("getGuestActionHandlers() missing handler for %q", h)
		}
	}
}

func TestInitializeDependencies(t *testing.T) {
	t.Run("InitializeNilDependencies", func(t *testing.T) {
		s := &Service{}
		s.initializeDependencies()
		if s.discovery == nil {
			t.Errorf("initializeDependencies() did not initialize discovery")
		}
		if s.newMetricCollector == nil {
			t.Errorf("initializeDependencies() did not initialize newMetricCollector")
		}
	})

	t.Run("DoNotOverwriteExistingDependencies", func(t *testing.T) {
		fakeDiscovery := &fakeDiscoveryClient{}
		fakeMetricCollectorFactory := func(context.Context, *cpb.Configuration) (MetricCollector, error) {
			return &fakeMetricCollector{}, nil
		}
		s := &Service{
			discovery:          fakeDiscovery,
			newMetricCollector: fakeMetricCollectorFactory,
		}
		s.initializeDependencies()
		if s.discovery != fakeDiscovery {
			t.Errorf("initializeDependencies() overwrote existing discovery dependency")
		}
		if reflect.ValueOf(s.newMetricCollector).Pointer() != reflect.ValueOf(fakeMetricCollectorFactory).Pointer() {
			t.Errorf("initializeDependencies() overwrote existing newMetricCollector dependency")
		}
	})
}

func TestWaitForWorkload(t *testing.T) {
	tests := []struct {
		name                  string
		enabled               *bool
		setProcessPresent     bool // if true, isProcessPresent will be set to true after a short delay
		initialProcessPresent bool
		want                  bool
	}{
		{
			name:                  "EnabledNilProcessEventuallyPresent",
			enabled:               nil,
			setProcessPresent:     true,
			initialProcessPresent: false,
			want:                  true,
		},
		{
			name:                  "EnabledNilProcessNeverPresent",
			enabled:               nil,
			setProcessPresent:     false,
			initialProcessPresent: false,
			want:                  false,
		},
		{
			name:                  "EnabledNilProcessPresentInitially",
			enabled:               nil,
			setProcessPresent:     false,
			initialProcessPresent: true,
			want:                  true,
		},
		{
			name:    "EnabledTrue",
			enabled: proto.Bool(true),
			want:    true,
		},
		{
			name:    "EnabledFalse",
			enabled: proto.Bool(false),
			want:    false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &Service{
				Config: &cpb.Configuration{
					OracleConfiguration: &cpb.OracleConfiguration{Enabled: tc.enabled},
				},
				isProcessPresent: tc.initialProcessPresent,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()

			if tc.setProcessPresent {
				go func() {
					time.Sleep(50 * time.Millisecond)
					s.processesMutex.Lock()
					s.isProcessPresent = true
					s.processesMutex.Unlock()
				}()
			}

			got := s.waitForWorkload(ctx)
			if got != tc.want {
				t.Errorf("waitForWorkload() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCheckServiceCommunication(t *testing.T) {
	tests := []struct {
		name                 string
		message              *servicecommunication.Message
		wantIsProcessPresent bool
		wantProcesses        []servicecommunication.ProcessWrapper
	}{
		{
			name: "discoveryWithNoProcess",
			message: &servicecommunication.Message{
				Origin:          servicecommunication.Discovery,
				DiscoveryResult: servicecommunication.DiscoveryResult{Processes: []servicecommunication.ProcessWrapper{}},
			},
			wantIsProcessPresent: false,
			wantProcesses:        []servicecommunication.ProcessWrapper{},
		},
		{
			name: "discoveryWithOracleProcess",
			message: &servicecommunication.Message{
				Origin: servicecommunication.Discovery,
				DiscoveryResult: servicecommunication.DiscoveryResult{
					Processes: []servicecommunication.ProcessWrapper{
						fakeProcess{name: "ora_pmon_ORCL"},
					},
				},
			},
			wantIsProcessPresent: true,
			wantProcesses: []servicecommunication.ProcessWrapper{
				fakeProcess{name: "ora_pmon_ORCL"},
			},
		},
		{
			name: "discoveryWithNonOracleProcess",
			message: &servicecommunication.Message{
				Origin: servicecommunication.Discovery,
				DiscoveryResult: servicecommunication.DiscoveryResult{
					Processes: []servicecommunication.ProcessWrapper{
						fakeProcess{name: "other_process"},
					},
				},
			},
			wantIsProcessPresent: false,
			wantProcesses: []servicecommunication.ProcessWrapper{
				fakeProcess{name: "other_process"},
			},
		},
		{
			name: "otherMessageType",
			message: &servicecommunication.Message{
				Origin: servicecommunication.DWActivation,
			},
			wantIsProcessPresent: false,
			wantProcesses:        nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ch := make(chan *servicecommunication.Message, 1)
			s := &Service{
				CommonCh: ch,
			}

			ch <- tc.message
			s.checkServiceCommunication(context.Background())

			if s.isProcessPresent != tc.wantIsProcessPresent {
				t.Errorf("checkServiceCommunication() isProcessPresent = %v, want %v", s.isProcessPresent, tc.wantIsProcessPresent)
			}

			if diff := cmp.Diff(tc.wantProcesses, s.processes, protocmp.Transform(), cmp.AllowUnexported(fakeProcess{})); diff != "" {
				t.Errorf("checkServiceCommunication() processes returned diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRunDiscovery(t *testing.T) {
	fdc := &fakeDiscoveryClient{}
	s := &Service{
		discovery: fdc,
		Config: &cpb.Configuration{
			OracleConfiguration: &cpb.OracleConfiguration{
				OracleDiscovery: &cpb.OracleDiscovery{
					UpdateFrequency: durationpb.New(1 * time.Second),
				},
			},
		},
		processes: []servicecommunication.ProcessWrapper{fakeProcess{name: "ora_pmon_"}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// allow one run of discover
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	runDiscovery(ctx, runDiscoveryArgs{s})
	if !fdc.discoverCalled {
		t.Errorf("runDiscovery() did not call Discover")
	}
}

func TestStartDiscoveryRoutine(t *testing.T) {
	tests := []struct {
		name               string
		enabled            bool
		wantRoutineStarted bool
	}{
		{
			name:               "Enabled",
			enabled:            true,
			wantRoutineStarted: true,
		},
		{
			name:               "Disabled",
			enabled:            false,
			wantRoutineStarted: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &Service{
				Config: &cpb.Configuration{
					OracleConfiguration: &cpb.OracleConfiguration{
						OracleDiscovery: &cpb.OracleDiscovery{
							Enabled:         proto.Bool(tc.enabled),
							UpdateFrequency: durationpb.New(1 * time.Hour),
						},
					},
				},
				discovery: &fakeDiscoveryClient{},
				processes: []servicecommunication.ProcessWrapper{fakeProcess{name: "ora_pmon_"}},
			}
			ctx, cancel := context.WithCancel(context.Background())
			s.startDiscoveryRoutine(ctx)
			cancel()
			gotRoutineStarted := s.discoveryRoutine != nil
			if gotRoutineStarted != tc.wantRoutineStarted {
				t.Errorf("startDiscoveryRoutine() routine started = %t, want %t", gotRoutineStarted, tc.wantRoutineStarted)
			}
		})
	}
}

func TestStartMetricCollectionRoutine(t *testing.T) {
	tests := []struct {
		name               string
		enabled            bool
		wantRoutineStarted bool
	}{
		{
			name:               "Enabled",
			enabled:            true,
			wantRoutineStarted: true,
		},
		{
			name:               "Disabled",
			enabled:            false,
			wantRoutineStarted: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &Service{
				Config: &cpb.Configuration{
					OracleConfiguration: &cpb.OracleConfiguration{
						OracleMetrics: &cpb.OracleMetrics{
							Enabled:             proto.Bool(tc.enabled),
							CollectionFrequency: durationpb.New(1 * time.Hour),
						},
					},
				},
				newMetricCollector: func(context.Context, *cpb.Configuration) (MetricCollector, error) {
					return &fakeMetricCollector{}, nil
				},
			}
			ctx, cancel := context.WithCancel(context.Background())
			s.startMetricCollectionRoutine(ctx)
			cancel()
			gotRoutineStarted := s.metricCollectionRoutine != nil
			if gotRoutineStarted != tc.wantRoutineStarted {
				t.Errorf("startMetricCollectionRoutine() routine started = %t, want %t", gotRoutineStarted, tc.wantRoutineStarted)
			}
		})
	}
}

func TestStartGuestActionsRoutine(t *testing.T) {
	originalNewGuestActionsManager := newGuestActionsManager
	defer func() {
		newGuestActionsManager = originalNewGuestActionsManager
	}()

	fakeGA := &fakeGuestActionsManager{
		started: make(chan struct{}),
	}
	newGuestActionsManager = func() guestActionsManager {
		return fakeGA
	}

	s := &Service{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.startGuestActionsRoutine(ctx)

	if s.guestActionsRoutine == nil {
		t.Errorf("startGuestActionsRoutine() routine got nil, want not nil")
	}

	select {
	case <-fakeGA.started:
	case <-time.After(1 * time.Second):
		t.Errorf("guestActionsManager.Start() was not called within timeout")
	}
}

func TestRunMetricCollection(t *testing.T) {
	fmc := &fakeMetricCollector{}
	s := &Service{
		newMetricCollector: func(context.Context, *cpb.Configuration) (MetricCollector, error) {
			return fmc, nil
		},
		Config: &cpb.Configuration{
			OracleConfiguration: &cpb.OracleConfiguration{
				OracleMetrics: &cpb.OracleMetrics{
					CollectionFrequency: durationpb.New(100 * time.Millisecond),
				},
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	runMetricCollection(ctx, runMetricCollectionArgs{s})

	if !fmc.healthMetricsCalled {
		t.Errorf("runMetricCollection() did not call SendHealthMetricsToCloudMonitoring")
	}
	if !fmc.defaultMetricsCalled {
		t.Errorf("runMetricCollection() did not call SendDefaultMetricsToCloudMonitoring")
	}
}

func TestStart(t *testing.T) {
	tests := []struct {
		name                           string
		oracleConfig                   *cpb.OracleConfiguration
		wantDiscoveryRoutineStarted    bool
		wantMetricRoutineStarted       bool
		wantGuestActionsRoutineStarted bool
	}{
		{
			name: "oracleDisabled",
			oracleConfig: &cpb.OracleConfiguration{
				Enabled: proto.Bool(false),
				OracleDiscovery: &cpb.OracleDiscovery{
					Enabled: proto.Bool(true),
				},
				OracleMetrics: &cpb.OracleMetrics{
					Enabled: proto.Bool(true),
				},
			},
			wantDiscoveryRoutineStarted:    false,
			wantMetricRoutineStarted:       false,
			wantGuestActionsRoutineStarted: false,
		},
		{
			name: "oracleEnabled",
			oracleConfig: &cpb.OracleConfiguration{
				Enabled: proto.Bool(true),
				OracleDiscovery: &cpb.OracleDiscovery{
					Enabled:         proto.Bool(true),
					UpdateFrequency: durationpb.New(1 * time.Hour),
				},
				OracleMetrics: &cpb.OracleMetrics{
					Enabled:             proto.Bool(true),
					CollectionFrequency: durationpb.New(1 * time.Hour),
				},
			},
			wantDiscoveryRoutineStarted:    true,
			wantMetricRoutineStarted:       true,
			wantGuestActionsRoutineStarted: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			originalNewGuestActionsManager := newGuestActionsManager
			defer func() {
				newGuestActionsManager = originalNewGuestActionsManager
			}()
			newGuestActionsManager = func() guestActionsManager {
				return &fakeGuestActionsManager{}
			}

			ch := make(chan *servicecommunication.Message)
			s := &Service{
				Config: &cpb.Configuration{
					OracleConfiguration: tc.oracleConfig,
				},
				CommonCh:  ch,
				discovery: &fakeDiscoveryClient{},
				newMetricCollector: func(context.Context, *cpb.Configuration) (MetricCollector, error) {
					return &fakeMetricCollector{}, nil
				},
				processes: []servicecommunication.ProcessWrapper{fakeProcess{name: "ora_pmon_"}},
			}

			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				s.Start(ctx, nil)
			}()
			time.Sleep(100 * time.Millisecond) // give Start() time to run
			cancel()

			if got := s.discoveryRoutine != nil; got != tc.wantDiscoveryRoutineStarted {
				t.Errorf("Start() discoveryRoutine started = %t, want %t", got, tc.wantDiscoveryRoutineStarted)
			}
			if got := s.metricCollectionRoutine != nil; got != tc.wantMetricRoutineStarted {
				t.Errorf("Start() metricCollectionRoutine started = %t, want %t", got, tc.wantMetricRoutineStarted)
			}
			if got := s.guestActionsRoutine != nil; got != tc.wantGuestActionsRoutineStarted {
				t.Errorf("Start() guestActionsRoutine started = %t, want %t", got, tc.wantGuestActionsRoutineStarted)
			}
		})
	}
}

func TestStart_InitializesDependencies(t *testing.T) {
	s := &Service{
		Config: &cpb.Configuration{
			OracleConfiguration: &cpb.OracleConfiguration{
				Enabled: proto.Bool(false),
			},
		},
		CommonCh: make(chan *servicecommunication.Message),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Calling Start with disabled config will return early, but should initialize dependencies.
	s.Start(ctx, nil)

	if s.discovery == nil {
		t.Errorf("Start() did not initialize discovery")
	}
	if s.newMetricCollector == nil {
		t.Errorf("Start() did not initialize newMetricCollector")
	}
}
