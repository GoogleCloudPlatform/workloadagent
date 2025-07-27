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

package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"google.golang.org/protobuf/proto"
	"github.com/GoogleCloudPlatform/workloadagent/internal/databasecenter"
	"github.com/GoogleCloudPlatform/workloadagent/internal/postgresmetrics"
	"github.com/GoogleCloudPlatform/workloadagent/internal/servicecommunication"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	"github.com/GoogleCloudPlatform/workloadagent/internal/workloadmanager"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce"

	durationpb "google.golang.org/protobuf/types/known/durationpb"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

// fakeMetrics is a test double for MetricsInterface.
type fakeMetrics struct {
	InitDBCalled chan bool
	InitDBErr    error

	CollectWlmCalled chan bool
	CollectWlmErr    error

	CollectDBCenterCalled chan bool
	CollectDBCenterErr    error
}

func newFakeMetrics() *fakeMetrics {
	return &fakeMetrics{
		InitDBCalled:          make(chan bool, 1),
		CollectWlmCalled:      make(chan bool, 5),
		CollectDBCenterCalled: make(chan bool, 5),
	}
}

func (f *fakeMetrics) InitDB(ctx context.Context, gceService postgresmetrics.GceInterface) error {
	select {
	case f.InitDBCalled <- true:
	default:
	}
	return f.InitDBErr
}

func (f *fakeMetrics) CollectWlmMetricsOnce(ctx context.Context, dwActivated bool) (*workloadmanager.WorkloadMetrics, error) {
	select {
	case f.CollectWlmCalled <- true:
	default:
	}
	return nil, f.CollectWlmErr
}

func (f *fakeMetrics) CollectDBCenterMetricsOnce(ctx context.Context) error {
	select {
	case f.CollectDBCenterCalled <- true:
	default:
	}
	return f.CollectDBCenterErr
}

// Stub is a no-op test double for psutil.Process.
type processStub struct {
	username string
	pid      int32
	name     string
	args     []string
	environ  []string
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

func (p processStub) Environ() ([]string, error) {
	return p.environ, nil
}

func (p processStub) String() string {
	return fmt.Sprintf("processStub{username: %q, pid: %d, name: %q, args: %v}", p.username, p.pid, p.name, p.args)
}

func TestIsWorkloadPresent(t *testing.T) {
	tests := []struct {
		name string
		s    *Service
		want bool
	}{
		{
			name: "Present",
			s: &Service{postgresProcesses: []servicecommunication.ProcessWrapper{
				processStub{
					username: "postgres_user",
					pid:      1234,
					name:     "postgres",
					args:     []string{"-D", "/var/lib/postgresql/15/main", "-c", "config_file=/etc/postgresql/15/main/postgresql.conf"},
					environ:  []string{"POSTGRES_HOME=/usr/lib/postgresql/15/main", "POSTGRES_DATADIR=/var/lib/postgresql/15/main"},
				},
			}},
			want: true,
		},
		{
			name: "NotPresent",
			s:    &Service{postgresProcesses: []servicecommunication.ProcessWrapper{}},
			want: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := test.s.isWorkloadPresent()
			if got != test.want {
				t.Errorf("isWorkloadPresent() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestIdentifyPostgresProcesses(t *testing.T) {
	tests := []struct {
		name string
		s    *Service
		want int
	}{
		{
			name: "MixedProcesses",
			s: &Service{
				processes: servicecommunication.DiscoveryResult{
					Processes: []servicecommunication.ProcessWrapper{
						processStub{
							username: "postgres_user",
							pid:      1234,
							name:     "postgres",
							args:     []string{"-D", "/var/lib/postgresql/15/main", "-c", "config_file=/etc/postgresql/15/main/postgresql.conf"},
							environ:  []string{"POSTGRES_HOME=/usr/lib/postgresql/15/main", "POSTGRES_DATADIR=/var/lib/postgresql/15/main"},
						},
						processStub{
							username: "test_user",
							pid:      1234,
							name:     "test_name",
						},
					},
				},
			},
			want: 1,
		},
		{
			name: "OnePostgresProcess",
			s: &Service{
				processes: servicecommunication.DiscoveryResult{
					Processes: []servicecommunication.ProcessWrapper{
						processStub{
							username: "postgres_user",
							pid:      1234,
							name:     "postgres",
							args:     []string{"-D", "/var/lib/postgresql/15/main", "-c", "config_file=/etc/postgresql/15/main/postgresql.conf"},
							environ:  []string{"POSTGRES_HOME=/usr/lib/postgresql/15/main", "POSTGRES_DATADIR=/var/lib/postgresql/15/main"},
						},
					}}},
			want: 1,
		},
		{
			name: "OneNotPostgresProcess",
			s: &Service{
				processes: servicecommunication.DiscoveryResult{
					Processes: []servicecommunication.ProcessWrapper{
						processStub{
							username: "test_user",
							pid:      1234,
							name:     "test_name",
						},
					}}},
			want: 0,
		},
		{
			name: "ZeroProcesses",
			s:    &Service{processes: servicecommunication.DiscoveryResult{Processes: []servicecommunication.ProcessWrapper{}}},
			want: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.s.identifyPostgresProcesses(context.Background())
			got := len(test.s.postgresProcesses)
			if got != test.want {
				t.Errorf("length of Postgres processes = %v, want %v", got, test.want)
			}
		})
	}
}

func TestCheckServiceCommunicationMissingOrigin(t *testing.T) {
	ch := make(chan *servicecommunication.Message, 1)
	result := servicecommunication.Message{}

	ch <- &result
	s := &Service{CommonCh: ch}
	s.checkServiceCommunication(context.Background())

	want := 0
	t.Run("OriginMissing", func(t *testing.T) {
		got := len(s.processes.Processes)
		if got != want {
			t.Errorf("checkServiceCommunication() = %v, want %v", got, want)
		}
	})

}

func TestCheckServiceCommunication(t *testing.T) {
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name   string
		ctx    context.Context
		result servicecommunication.Message
		ch     chan *servicecommunication.Message
		want   int
	}{
		{
			name: "DiscoveryWorkloadPresent",
			ctx:  context.Background(),
			result: servicecommunication.Message{
				Origin: servicecommunication.Discovery,
				DiscoveryResult: servicecommunication.DiscoveryResult{
					Processes: []servicecommunication.ProcessWrapper{
						processStub{
							username: "postgres_user",
							pid:      1234,
							name:     "postgres",
							args:     []string{"-D", "/var/lib/postgresql/15/main", "-c", "config_file=/etc/postgresql/15/main/postgresql.conf"},
							environ:  []string{"POSTGRES_HOME=/usr/lib/postgresql/15/main", "POSTGRES_DATADIR=/var/lib/postgresql/15/main"},
						},
					},
				},
			},
			ch:   make(chan *servicecommunication.Message, 1),
			want: 1,
		},
		{
			name:   "ContextEnded",
			ctx:    cancelledCtx,
			result: servicecommunication.Message{},
			ch:     make(chan *servicecommunication.Message, 1),
			want:   0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := &Service{CommonCh: test.ch}
			test.ch <- &test.result
			s.checkServiceCommunication(test.ctx)
			got := len(s.processes.Processes)
			if got != test.want {
				t.Errorf("checkServiceCommunication() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestCheckServiceCommunicationDWActivation(t *testing.T) {
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name   string
		result servicecommunication.Message
		ctx    context.Context
		ch     chan *servicecommunication.Message
		want   bool
	}{
		{
			name: "DWActivation",
			ctx:  context.Background(),
			result: servicecommunication.Message{
				Origin: servicecommunication.DWActivation,
				DWActivationResult: servicecommunication.DataWarehouseActivationResult{
					Activated: true,
				},
			},
			ch:   make(chan *servicecommunication.Message, 1),
			want: true,
		},
		{
			name: "ContextEnded",
			ctx:  cancelledCtx,
			result: servicecommunication.Message{
				Origin: servicecommunication.DWActivation,
				DWActivationResult: servicecommunication.DataWarehouseActivationResult{
					Activated: true,
				},
			},
			ch:   make(chan *servicecommunication.Message, 1),
			want: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := &Service{CommonCh: test.ch}
			test.ch <- &test.result
			s.checkServiceCommunication(test.ctx)
			got := s.dwActivated
			if got != test.want {
				t.Errorf("checkServiceCommunication() = %v, want %v", got, test.want)
			}
		})
	}
}

// Can only test the case where the context is cancelled.
// In other cases, the test will hang because this method is meant to run perpetually.
func TestStart(t *testing.T) {
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name string
		s    *Service
		ctx  context.Context
		want int
	}{
		{
			name: "ContextEnded",
			s:    &Service{},
			ctx:  cancelledCtx,
			want: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.s.Start(test.ctx, nil)
			got := len(test.s.processes.Processes)
			if got != test.want {
				t.Errorf("Start() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestString(t *testing.T) {
	s := &Service{}
	got := s.String()
	// Unlikely to intentionally change.
	want := "Postgres Service"
	if got != want {
		t.Errorf("String() = %v, want %v", got, want)
	}
}

func TestErrorCode(t *testing.T) {
	s := &Service{}
	got := s.ErrorCode()
	want := usagemetrics.PostgresServiceError
	if got != want {
		t.Errorf("ErrorCode() = %v, want %v", got, want)
	}
}

func TestExpectedMinDuration(t *testing.T) {
	s := &Service{}
	got := s.ExpectedMinDuration()
	want := 20 * time.Second
	if got != want {
		t.Errorf("ExpectedMinDuration() = %v, want %v", got, want)
	}
}

func TestStart_PostgresDisabled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &Service{
		Config: &configpb.Configuration{
			PostgresConfiguration: &configpb.PostgresConfiguration{
				Enabled: proto.Bool(false),
			},
		},
	}
	s.Start(ctx, nil)
}

func TestStart_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ch := make(chan *servicecommunication.Message)
	s := &Service{
		Config: &configpb.Configuration{
			PostgresConfiguration: &configpb.PostgresConfiguration{
				Enabled: proto.Bool(true),
			},
		},
		CommonCh:    ch,
		dwActivated: true,
		postgresProcesses: []servicecommunication.ProcessWrapper{
			processStub{
				username: "postgres_user",
				pid:      1234,
				name:     "postgres",
			},
		},
	}
	s.Start(ctx, nil)
}

func TestRunDiscovery_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runDiscovery(ctx, runDiscoveryArgs{s: &Service{}})
}

func TestRunDiscovery_InvalidArgs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDiscovery(ctx, "invalid args")
}

func TestRunWlmMetricCollection_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runWlmMetricCollection(ctx, runWlmMetricCollectionArgs{s: &Service{}})
}

func TestRunWlmMetricCollection_InvalidArgs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runWlmMetricCollection(ctx, "invalid args")
}

func TestRunDBCenterMetricCollection_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runDBCenterMetricCollection(ctx, runWlmMetricCollectionArgs{s: &Service{}})
}

func TestRunDBCenterMetricCollection_InvalidArgs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDBCenterMetricCollection(ctx, "invalid args")
}

func TestGetDbCenterMetricCollectionFrequency(t *testing.T) {
	tests := []struct {
		name     string
		args     runDBCenterMetricCollectionArgs
		wantFreq time.Duration
	}{
		{
			name:     "nil_service",
			args:     runDBCenterMetricCollectionArgs{},
			wantFreq: dbCenterMetricCollectionFrequencyDefault,
		},
		{
			name: "nil_config",
			args: runDBCenterMetricCollectionArgs{
				s: &Service{},
			},
			wantFreq: dbCenterMetricCollectionFrequencyDefault,
		},
		{
			name: "config_with_nil_postgres_config",
			args: runDBCenterMetricCollectionArgs{
				s: &Service{
					Config: &configpb.Configuration{},
				},
			},
			wantFreq: dbCenterMetricCollectionFrequencyDefault,
		},
		{
			name: "config_with_nil_collection_frequency",
			args: runDBCenterMetricCollectionArgs{
				s: &Service{
					Config: &configpb.Configuration{
						PostgresConfiguration: &configpb.PostgresConfiguration{},
					},
				},
			},
			wantFreq: dbCenterMetricCollectionFrequencyDefault,
		},
		{
			name: "config_with_valid_collection_frequency",
			args: runDBCenterMetricCollectionArgs{
				s: &Service{
					Config: &configpb.Configuration{
						PostgresConfiguration: &configpb.PostgresConfiguration{
							DbcenterCollectionFrequency: durationpb.New(30 * time.Minute),
						},
					},
				},
			},
			wantFreq: 30 * time.Minute,
		},
		{
			name: "config_with_very_small_collection_frequency",
			args: runDBCenterMetricCollectionArgs{
				s: &Service{
					Config: &configpb.Configuration{
						PostgresConfiguration: &configpb.PostgresConfiguration{
							DbcenterCollectionFrequency: durationpb.New(1 * time.Second),
						},
					},
				},
			},
			wantFreq: dbCenterMetricCollectionFrequencyMin,
		},
		{
			name: "config_with_very_large_collection_frequency",
			args: runDBCenterMetricCollectionArgs{
				s: &Service{
					Config: &configpb.Configuration{
						PostgresConfiguration: &configpb.PostgresConfiguration{
							DbcenterCollectionFrequency: durationpb.New(6*time.Hour + 1*time.Second),
						},
					},
				},
			},
			wantFreq: dbCenterMetricCollectionFrequencyMax,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotFreq := getDbCenterMetricCollectionFrequency(tc.args)
			if gotFreq != tc.wantFreq {
				t.Errorf("getMetricCollectionFrequency(%v) = %v, want %v", tc.args, gotFreq, tc.wantFreq)
			}
		})
	}
}

func TestRunDBCenterMetricCollection_Success(t *testing.T) {
	// Save original functions
	origNewTicker := newTicker
	origNewPostgresMetrics := newPostgresMetrics
	origNewGCEClient := newGCEClient

	// Defer restoration
	defer func() {
		newTicker = origNewTicker
		newPostgresMetrics = origNewPostgresMetrics
		newGCEClient = origNewGCEClient
	}()

	fakeClock := clockwork.NewFakeClock()
	// Stub functions
	newTicker = func(d time.Duration) *time.Ticker {
		if d != dbCenterMetricCollectionFrequencyDefault {
			t.Errorf("NewTicker called with wrong duration: got %v, want %v", d, dbCenterMetricCollectionFrequencyDefault)
		}
		return &time.Ticker{C: fakeClock.NewTicker(d).Chan()}
	}

	mockMetrics := newFakeMetrics()
	newPostgresMetrics = func(ctx context.Context, config *configpb.Configuration, wlmClient workloadmanager.WLMWriter, dbcenterClient databasecenter.Client) MetricsInterface {
		return mockMetrics
	}
	newGCEClient = func(ctx context.Context) (*gce.GCE, error) { return nil, nil }

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go runDBCenterMetricCollection(ctx, runDBCenterMetricCollectionArgs{s: &Service{Config: &configpb.Configuration{}}})

	// Wait for InitDB to be called
	select {
	case <-mockMetrics.InitDBCalled:
	case <-ctx.Done():
		t.Fatalf("runDBCenterMetricCollection: InitDB not called within timeout: %v", ctx.Err())
	}

	// First tick
	fakeClock.Advance(dbCenterMetricCollectionFrequencyDefault + 1*time.Second)
	select {
	case <-mockMetrics.CollectDBCenterCalled:
	case <-ctx.Done():
		t.Fatalf("runDBCenterMetricCollection: CollectDBCenterMetricsOnce not called after first tick: %v", ctx.Err())
	}

	// Second tick
	fakeClock.Advance(dbCenterMetricCollectionFrequencyDefault + 1*time.Second)
	select {
	case <-mockMetrics.CollectDBCenterCalled:
	case <-ctx.Done():
		t.Fatalf("runDBCenterMetricCollection: CollectDBCenterMetricsOnce not called after second tick: %v", ctx.Err())
	}
}

func TestRunDBCenterMetricCollection_InitDBError(t *testing.T) {
	// Save original functions
	origNewTicker := newTicker
	origNewPostgresMetrics := newPostgresMetrics
	origNewGCEClient := newGCEClient

	// Defer restoration
	defer func() {
		newTicker = origNewTicker
		newPostgresMetrics = origNewPostgresMetrics
		newGCEClient = origNewGCEClient
	}()
	fakeClock := clockwork.NewFakeClock()
	// Stub functions
	newTicker = func(d time.Duration) *time.Ticker {
		return &time.Ticker{C: fakeClock.NewTicker(d).Chan()}
	}

	mockMetrics := newFakeMetrics()
	mockMetrics.InitDBErr = errors.New("InitDB error")
	newPostgresMetrics = func(ctx context.Context, config *configpb.Configuration, wlmClient workloadmanager.WLMWriter, dbcenterClient databasecenter.Client) MetricsInterface {
		return mockMetrics
	}
	newGCEClient = func(ctx context.Context) (*gce.GCE, error) { return nil, nil }

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		runDBCenterMetricCollection(ctx, runDBCenterMetricCollectionArgs{s: &Service{Config: &configpb.Configuration{}}})
		close(done)
	}()

	// Wait for InitDB to be called
	select {
	case <-mockMetrics.InitDBCalled:
	case <-ctx.Done():
		t.Fatalf("runDBCenterMetricCollection: InitDB not called within timeout: %v", ctx.Err())
	}
	// Advance the clock to make sure that CollectDBCenterMetricsOnce is never called.
	fakeClock.Advance(dbCenterMetricCollectionFrequencyDefault + 1*time.Second)
	select {
	case <-mockMetrics.CollectDBCenterCalled:
		t.Fatalf("runDBCenterMetricCollection: CollectDBCenterMetricsOnce should not be called if InitDB fails")
	case <-time.After(100 * time.Millisecond):
		// This is the expected case, CollectDBCenterMetricsOnce should not be called.
	}
}

func TestRunWlmMetricCollection_Success(t *testing.T) {
	// Save original functions
	origNewTicker := newTicker
	origNewPostgresMetrics := newPostgresMetrics
	origNewGCEClient := newGCEClient

	// Defer restoration
	defer func() {
		newTicker = origNewTicker
		newPostgresMetrics = origNewPostgresMetrics
		newGCEClient = origNewGCEClient
	}()

	fakeClock := clockwork.NewFakeClock()
	// Stub functions
	newTicker = func(d time.Duration) *time.Ticker {
		return &time.Ticker{C: fakeClock.NewTicker(d).Chan()}
	}

	mockMetrics := newFakeMetrics()
	newPostgresMetrics = func(ctx context.Context, config *configpb.Configuration, wlmClient workloadmanager.WLMWriter, dbcenterClient databasecenter.Client) MetricsInterface {
		return mockMetrics
	}
	newGCEClient = func(ctx context.Context) (*gce.GCE, error) { return nil, nil }

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go runWlmMetricCollection(ctx, runWlmMetricCollectionArgs{s: &Service{Config: &configpb.Configuration{}}})

	// Wait for InitDB to be called
	select {
	case <-mockMetrics.InitDBCalled:
	case <-ctx.Done():
		t.Fatalf("runWlmMetricCollection: InitDB not called within timeout: %v", ctx.Err())
	}

	// First tick
	fakeClock.Advance(wlmMetricCollectionFrequencyDefault + 1*time.Second)
	select {
	case <-mockMetrics.CollectWlmCalled:
	case <-ctx.Done():
		t.Fatalf("runWlmMetricCollection: CollectWlmMetricsOnce not called after first tick: %v", ctx.Err())
	}

	// Second tick
	fakeClock.Advance(wlmMetricCollectionFrequencyDefault + 1*time.Second)
	select {
	case <-mockMetrics.CollectWlmCalled:
	case <-ctx.Done():
		t.Fatalf("runWlmMetricCollection: CollectWlmMetricsOnce not called after second tick: %v", ctx.Err())
	}
}

func TestRunWlmMetricCollection_InitDBError(t *testing.T) {
	// Save original functions
	origNewTicker := newTicker
	origNewPostgresMetrics := newPostgresMetrics
	origNewGCEClient := newGCEClient

	// Defer restoration
	defer func() {
		newTicker = origNewTicker
		newPostgresMetrics = origNewPostgresMetrics
		newGCEClient = origNewGCEClient
	}()
	fakeClock := clockwork.NewFakeClock()
	// Stub functions
	newTicker = func(d time.Duration) *time.Ticker {
		return &time.Ticker{C: fakeClock.NewTicker(d).Chan()}
	}

	mockMetrics := newFakeMetrics()
	mockMetrics.InitDBErr = errors.New("InitDB error")
	newPostgresMetrics = func(ctx context.Context, config *configpb.Configuration, wlmClient workloadmanager.WLMWriter, dbcenterClient databasecenter.Client) MetricsInterface {
		return mockMetrics
	}
	newGCEClient = func(ctx context.Context) (*gce.GCE, error) { return nil, nil }

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		runWlmMetricCollection(ctx, runWlmMetricCollectionArgs{s: &Service{Config: &configpb.Configuration{}}})
		close(done)
	}()

	// Wait for InitDB to be called
	select {
	case <-mockMetrics.InitDBCalled:
	case <-ctx.Done():
		t.Fatalf("runWlmMetricCollection: InitDB not called within timeout: %v", ctx.Err())
	}
	// Advance the clock to make sure that CollectWlmMetricsOnce is never called.
	fakeClock.Advance(wlmMetricCollectionFrequencyDefault + 1*time.Second)
	select {
	case <-mockMetrics.CollectWlmCalled:
		t.Fatalf("runWlmMetricCollection: CollectWlmMetricsOnce should not be called if InitDB fails")
	case <-time.After(100 * time.Millisecond):
		// This is the expected case, CollectWlmMetricsOnce should not be called.
	}
}
