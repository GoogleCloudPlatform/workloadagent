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

// Package redis tests the Redis workload agent service.
package redis

import (
	"context"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/workloadagent/internal/servicecommunication"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
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

func TestIsWorkloadPresent(t *testing.T) {
	tests := []struct {
		name string
		s    *Service
		want bool
	}{
		{
			name: "Present",
			s: &Service{redisProcesses: []servicecommunication.ProcessWrapper{
				processStub{
					username: "redis_user",
					pid:      1234,
					name:     "redis-server",
					args:     []string{"--port 6379", "--bind 0.0.0.0"},
					environ:  []string{"REDIS_PORT=6379", "REDIS_BIND=0.0.0.0"},
				},
			}},
			want: true,
		},
		{
			name: "NotPresent",
			s:    &Service{redisProcesses: []servicecommunication.ProcessWrapper{}},
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

func TestIdentifyRedisProcesses(t *testing.T) {
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
							username: "redis_user",
							pid:      1234,
							name:     "redis-server",
							args:     []string{"--port 6379", "--bind 0.0.0.0"},
							environ:  []string{"REDIS_PORT=6379", "REDIS_BIND=0.0.0.0"},
						},
						processStub{
							username: "test_user",
							pid:      1234,
							name:     "test_name",
						},
					}}},
			want: 1,
		},
		{
			name: "OneRedisProcess",
			s: &Service{
				processes: servicecommunication.DiscoveryResult{
					Processes: []servicecommunication.ProcessWrapper{
						processStub{
							username: "redis_user",
							pid:      1234,
							name:     "redis-server",
							args:     []string{"--port 6379", "--bind 0.0.0.0"},
							environ:  []string{"REDIS_PORT=6379", "REDIS_BIND=0.0.0.0"},
						},
					}}},
			want: 1,
		},
		{
			name: "OneNotRedisProcess",
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
			test.s.identifyRedisProcesses(context.Background())
			got := len(test.s.redisProcesses)
			if got != test.want {
				t.Errorf("isWorkloadPresent() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestCheckServiceCommunicationMissingOrigin(t *testing.T) {
	ch := make(chan *servicecommunication.Message, 1)
	result := servicecommunication.Message{
		Origin: servicecommunication.UnspecifiedMessageOrigin,
		DiscoveryResult: servicecommunication.DiscoveryResult{
			Processes: []servicecommunication.ProcessWrapper{
				processStub{
					username: "redis_user",
					pid:      1234,
					name:     "redis-server",
					args:     []string{"--port 6379", "--bind 0.0.0.0"},
					environ:  []string{"REDIS_PORT=6379", "REDIS_BIND=0.0.0.0"},
				},
			},
		},
		DWActivationResult: servicecommunication.DataWarehouseActivationResult{
			Activated: true,
		},
	}

	ch <- &result
	s := &Service{CommonCh: ch}
	s.checkServiceCommunication(context.Background())

	t.Run("OriginMissing", func(t *testing.T) {
		got := len(s.processes.Processes)
		want := 0
		if got != want {
			t.Errorf("checkServiceCommunication() = %v, want %v", got, want)
		}
		gotBool := s.dwActivated
		wantBool := false
		if gotBool != wantBool {
			t.Errorf("checkServiceCommunication() = %v, want %v", gotBool, wantBool)
		}
	})

}

func TestCheckServiceCommunicationDiscovery(t *testing.T) {
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name   string
		ch     chan *servicecommunication.Message
		ctx    context.Context
		result servicecommunication.Message
		want   int
	}{
		{
			name: "WorkloadPresent",
			ch:   make(chan *servicecommunication.Message, 1),
			ctx:  context.Background(),
			result: servicecommunication.Message{
				Origin: servicecommunication.Discovery,
				DiscoveryResult: servicecommunication.DiscoveryResult{
					Processes: []servicecommunication.ProcessWrapper{
						processStub{
							username: "redis_user",
							pid:      1234,
							name:     "redis-server",
							args:     []string{"--port 6379", "--bind 0.0.0.0"},
							environ:  []string{"REDIS_PORT=6379", "REDIS_BIND=0.0.0.0"},
						},
					},
				},
			},
			want: 1,
		},
		{
			name: "ContextEnded",
			ch:   make(chan *servicecommunication.Message, 1),
			ctx:  cancelledCtx,
			result: servicecommunication.Message{
				Origin: servicecommunication.Discovery,
				DiscoveryResult: servicecommunication.DiscoveryResult{
					Processes: []servicecommunication.ProcessWrapper{
						processStub{
							username: "redis_user",
							pid:      1234,
							name:     "redis-server",
							args:     []string{"--port 6379", "--bind 0.0.0.0"},
							environ:  []string{"REDIS_PORT=6379", "REDIS_BIND=0.0.0.0"},
						},
					},
				},
			},
			want: 0,
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
		ch     chan *servicecommunication.Message
		ctx    context.Context
		result servicecommunication.Message
		want   bool
	}{
		{
			name: "DWActivated",
			ch:   make(chan *servicecommunication.Message, 1),
			ctx:  context.Background(),
			result: servicecommunication.Message{
				Origin: servicecommunication.DWActivation,
				DWActivationResult: servicecommunication.DataWarehouseActivationResult{
					Activated: true,
				},
			},
			want: true,
		},
		{
			name: "ContextEnded",
			ch:   make(chan *servicecommunication.Message, 1),
			ctx:  cancelledCtx,
			result: servicecommunication.Message{
				Origin: servicecommunication.DWActivation,
				DWActivationResult: servicecommunication.DataWarehouseActivationResult{
					Activated: true,
				},
			},
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
	want := "Redis Service"
	if got != want {
		t.Errorf("String() = %v, want %v", got, want)
	}
}

func TestErrorCode(t *testing.T) {
	s := &Service{}
	got := s.ErrorCode()
	want := usagemetrics.RedisServiceError
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
