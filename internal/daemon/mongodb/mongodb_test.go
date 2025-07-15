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

package mongodb

import (
	"context"
	"fmt"
	"testing"
	"time"

	durationpb "google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/proto"
	"github.com/GoogleCloudPlatform/workloadagent/internal/servicecommunication"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

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
			s: &Service{mongodbProcesses: []servicecommunication.ProcessWrapper{
				processStub{
					username: "mongodb_user",
					pid:      1234,
					name:     "mongodb",
					args:     []string{"-D", "/var/lib/mongodb/15/main", "-c", "config_file=/etc/mongodb/15/main/mongodb.conf"},
					environ:  []string{"MONGODB_HOME=/usr/lib/mongodb/15/main", "MONGODB_DATADIR=/var/lib/mongodb/15/main"},
				},
			}},
			want: true,
		},
		{
			name: "NotPresent",
			s:    &Service{mongodbProcesses: []servicecommunication.ProcessWrapper{}},
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

func TestIdentifyMongoDBProcesses(t *testing.T) {
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
							username: "mongodb_user",
							pid:      1234,
							name:     "mongodb",
							args:     []string{"-D", "/var/lib/mongodb/15/main", "-c", "config_file=/etc/mongodb/15/main/mongodb.conf"},
							environ:  []string{"MONGODB_HOME=/usr/lib/mongodb/15/main", "MONGODB_DATADIR=/var/lib/mongodb/15/main"},
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
			name: "OneMongoDBProcess",
			s: &Service{
				processes: servicecommunication.DiscoveryResult{
					Processes: []servicecommunication.ProcessWrapper{
						processStub{
							username: "mongodb_user",
							pid:      1234,
							name:     "mongodb",
							args:     []string{"-D", "/var/lib/mongodb/15/main", "-c", "config_file=/etc/mongodb/15/main/mongodb.conf"},
							environ:  []string{"MONGODB_HOME=/usr/lib/mongodb/15/main", "MONGODB_DATADIR=/var/lib/mongodb/15/main"},
						},
					}}},
			want: 1,
		},
		{
			name: "OneNotMongoDBProcess",
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
			test.s.identifyMongoDBProcesses(context.Background())
			got := len(test.s.mongodbProcesses)
			if got != test.want {
				t.Errorf("length of MongoDB processes = %v, want %v", got, test.want)
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
							username: "mongodb_user",
							pid:      1234,
							name:     "mongodb",
							args:     []string{"-D", "/var/lib/mongodb/15/main", "-c", "config_file=/etc/mongodb/15/main/mongodb.conf"},
							environ:  []string{"MONGODB_HOME=/usr/lib/mongodb/15/main", "MONGODB_DATADIR=/var/lib/mongodb/15/main"},
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
	want := "MongoDB Service"
	if got != want {
		t.Errorf("String() = %v, want %v", got, want)
	}
}

func TestErrorCode(t *testing.T) {
	s := &Service{}
	got := s.ErrorCode()
	want := usagemetrics.MongoDBServiceError
	if got != want {
		t.Errorf("ErrorCode() = %v, want %v", got, want)
	}
}

func TestExpectedMinDuration(t *testing.T) {
	s := &Service{}
	got := s.ExpectedMinDuration()
	want := 0 * time.Second
	if got != want {
		t.Errorf("ExpectedMinDuration() = %v, want %v", got, want)
	}
}

func TestStart_MongoDBDisabled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &Service{
		Config: &configpb.Configuration{
			MongoDbConfiguration: &configpb.MongoDBConfiguration{
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
			MongoDbConfiguration: &configpb.MongoDBConfiguration{
				Enabled: proto.Bool(true),
			},
		},
		CommonCh:    ch,
		dwActivated: true,
		mongodbProcesses: []servicecommunication.ProcessWrapper{
			processStub{
				username: "mongodb_user",
				pid:      1234,
				name:     "mongodb",
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

func TestRunMetricCollection_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runMetricCollection(ctx, runMetricCollectionArgs{s: &Service{}})
}

func TestRunMetricCollection_InvalidArgs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runMetricCollection(ctx, "invalid args")
}

func TestGetMetricCollectionFrequency(t *testing.T) {
	tests := []struct {
		name     string
		args     runMetricCollectionArgs
		wantFreq time.Duration
	}{
		{
			name:     "nil_service",
			args:     runMetricCollectionArgs{},
			wantFreq: metricCollectionFrequencyDefault,
		},
		{
			name: "nil_config",
			args: runMetricCollectionArgs{
				s: &Service{},
			},
			wantFreq: metricCollectionFrequencyDefault,
		},
		{
			name: "config_with_nil_mongodb_config",
			args: runMetricCollectionArgs{
				s: &Service{
					Config: &configpb.Configuration{},
				},
			},
			wantFreq: metricCollectionFrequencyDefault,
		},
		{
			name: "config_with_nil_collection_frequency",
			args: runMetricCollectionArgs{
				s: &Service{
					Config: &configpb.Configuration{
						MongoDbConfiguration: &configpb.MongoDBConfiguration{},
					},
				},
			},
			wantFreq: metricCollectionFrequencyDefault,
		},
		{
			name: "config_with_valid_collection_frequency",
			args: runMetricCollectionArgs{
				s: &Service{
					Config: &configpb.Configuration{
						MongoDbConfiguration: &configpb.MongoDBConfiguration{
							CollectionFrequency: durationpb.New(30 * time.Minute),
						},
					},
				},
			},
			wantFreq: 30 * time.Minute,
		},
		{
			name: "config_with_very_small_collection_frequency",
			args: runMetricCollectionArgs{
				s: &Service{
					Config: &configpb.Configuration{
						MongoDbConfiguration: &configpb.MongoDBConfiguration{
							CollectionFrequency: durationpb.New(1 * time.Second),
						},
					},
				},
			},
			wantFreq: metricCollectionFrequencyMin,
		},
		{
			name: "config_with_very_large_collection_frequency",
			args: runMetricCollectionArgs{
				s: &Service{
					Config: &configpb.Configuration{
						MongoDbConfiguration: &configpb.MongoDBConfiguration{
							CollectionFrequency: durationpb.New(6*time.Hour + 1*time.Second),
						},
					},
				},
			},
			wantFreq: metricCollectionFrequencyMax,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotFreq := metricCollectionFrequency(tc.args)
			if gotFreq != tc.wantFreq {
				t.Errorf("getMetricCollectionFrequency(%v) = %v, want %v", tc.args, gotFreq, tc.wantFreq)
			}
		})
	}
}
