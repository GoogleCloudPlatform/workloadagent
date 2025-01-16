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

// Package mysql tests the MySQL workload agent service.
package mysql

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
			s: &Service{mySQLProcesses: []servicecommunication.ProcessWrapper{
				processStub{
					username: "mysql_user",
					pid:      1234,
					name:     "mysqld",
					args:     []string{"--basedir=/usr/local/mysql", "--datadir=/var/lib/mysql", "--socket=/var/lib/mysql/mysql.sock"},
					environ:  []string{"MYSQL_HOME=/usr/local/mysql", "MYSQL_DATADIR=/var/lib/mysql", "MYSQL_SOCKET=/var/lib/mysql/mysql.sock"},
				},
			}},
			want: true,
		},
		{
			name: "NotPresent",
			s:    &Service{mySQLProcesses: []servicecommunication.ProcessWrapper{}},
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

func TestIdentifyMySQLProcesses(t *testing.T) {
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
							username: "mysql_user",
							pid:      1234,
							name:     "mysqld",
							args:     []string{"--basedir=/usr/local/mysql", "--datadir=/var/lib/mysql", "--socket=/var/lib/mysql/mysql.sock"},
							environ:  []string{"MYSQL_HOME=/usr/local/mysql", "MYSQL_DATADIR=/var/lib/mysql", "MYSQL_SOCKET=/var/lib/mysql/mysql.sock"},
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
			name: "OneMySQLProcess",
			s: &Service{
				processes: servicecommunication.DiscoveryResult{
					Processes: []servicecommunication.ProcessWrapper{
						processStub{
							username: "mysql_user",
							pid:      1234,
							name:     "mysqld",
							args:     []string{"--basedir=/usr/local/mysql", "--datadir=/var/lib/mysql", "--socket=/var/lib/mysql/mysql.sock"},
							environ:  []string{"MYSQL_HOME=/usr/local/mysql", "MYSQL_DATADIR=/var/lib/mysql", "MYSQL_SOCKET=/var/lib/mysql/mysql.sock"},
						},
					}}},
			want: 1,
		},
		{
			name: "OneNotMySQLProcess",
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
			test.s.identifyMySQLProcesses(context.Background())
			got := len(test.s.mySQLProcesses)
			if got != test.want {
				t.Errorf("length of MySQL processes = %v, want %v", got, test.want)
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
							username: "mysql_user",
							pid:      1234,
							name:     "mysqld",
							args:     []string{"--basedir=/usr/local/mysql", "--datadir=/var/lib/mysql", "--socket=/var/lib/mysql/mysql.sock"},
							environ:  []string{"MYSQL_HOME=/usr/local/mysql", "MYSQL_DATADIR=/var/lib/mysql", "MYSQL_SOCKET=/var/lib/mysql/mysql.sock"},
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
	want := "MySQL Service"
	if got != want {
		t.Errorf("String() = %v, want %v", got, want)
	}
}

func TestErrorCode(t *testing.T) {
	s := &Service{}
	got := s.ErrorCode()
	want := usagemetrics.MySQLServiceError
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
