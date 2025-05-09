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

package discovery

import (
	"context"
	"errors"
	"testing"
	"time"

	dpb "google.golang.org/protobuf/types/known/durationpb"
	"github.com/google/go-cmp/cmp"
	"github.com/shirou/gopsutil/v3/process"
	"github.com/GoogleCloudPlatform/workloadagent/internal/servicecommunication"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

type errorProneProcessLister struct {
	processes []processStub
}

func (f errorProneProcessLister) listAllProcesses() ([]servicecommunication.ProcessWrapper, error) {
	return nil, errors.New("test error")
}

type fakeProcessLister struct {
	processes []processStub
}

func (f fakeProcessLister) listAllProcesses() ([]servicecommunication.ProcessWrapper, error) {
	result := make([]servicecommunication.ProcessWrapper, len(f.processes))
	for i, p := range f.processes {
		result[i] = servicecommunication.ProcessWrapper(p)
	}
	return result, nil
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

func TestUsername(t *testing.T) {
	tests := []struct {
		name string
		p    gopsProcess
	}{
		{
			name: "Username",
			p:    gopsProcess{process: &process.Process{}},
		},
	}
	for _, tc := range tests {
		_, err := tc.p.Username()
		if err == nil {
			t.Errorf("TestUsername() with name %s got nil error but expected an error", tc.name)
		}
	}
}

func TestPid(t *testing.T) {
	tests := []struct {
		name string
		p    gopsProcess
	}{
		{
			name: "Pid",
			p:    gopsProcess{process: &process.Process{}},
		},
	}
	for _, tc := range tests {
		got := tc.p.Pid()
		if got != 0 {
			t.Errorf("TestPid() with name %s expected 0 but got %d", tc.name, got)
		}
	}
}

func TestName(t *testing.T) {
	tests := []struct {
		name string
		p    gopsProcess
	}{
		{
			name: "Name",
			p:    gopsProcess{process: &process.Process{}},
		},
	}
	for _, tc := range tests {
		_, err := tc.p.Name()
		if err == nil {
			t.Errorf("TestName() with name %s got nil error but expected an error", tc.name)
		}
	}
}

func TestCmdlineSlice(t *testing.T) {
	tests := []struct {
		name string
		p    gopsProcess
	}{
		{
			name: "CmdlineSlice",
			p:    gopsProcess{process: &process.Process{}},
		},
	}
	for _, tc := range tests {
		_, err := tc.p.CmdlineSlice()
		if err == nil {
			t.Errorf("TestCmdlineSlice() with name %s got nil error but expected an error", tc.name)
		}
	}
}

func TestErrorCode(t *testing.T) {
	tests := []struct {
		name string
		d    Service
		want int
	}{
		{
			name: "ErrorCode",
			d:    Service{},
			want: usagemetrics.CommonDiscoveryFailure,
		},
	}
	for _, tc := range tests {
		code := tc.d.ErrorCode()
		if code != tc.want {
			t.Errorf("TestErrorCode() with name %s got %d but expected %d", tc.name, code, tc.want)
		}
	}
}

func TestExpectedMinDuration(t *testing.T) {
	tests := []struct {
		name string
		d    Service
		want time.Duration
	}{
		{
			name: "ExpectedMinDuration",
			d:    Service{},
			want: 0,
		},
	}
	for _, tc := range tests {
		duration := tc.d.ExpectedMinDuration()
		if duration != tc.want {
			t.Errorf("TestExpectedMinDuration() with name %s got %d but expected %d", tc.name, duration, tc.want)
		}
	}
}

func TestListAllProcesses(t *testing.T) {
	tests := []struct {
		name string
		d    *Service
		want []processStub
	}{
		{
			name: "ListAllProcesses",
			d: &Service{
				ProcessLister: fakeProcessLister{processes: []processStub{
					{username: "user1", pid: 123, name: "test"},
					{username: "user2", pid: 456, name: "tnslsnr", args: []string{"tnslsnr", "LISTENER"}},
				}},
			},
			want: []processStub{
				{username: "user1", pid: 123, name: "test"},
				{username: "user2", pid: 456, name: "tnslsnr", args: []string{"tnslsnr", "LISTENER"}},
			},
		},
	}

	for _, tc := range tests {
		processes, gotErr := tc.d.ProcessLister.listAllProcesses()
		if gotErr != nil {
			t.Errorf("TestListAllProcesses() with name %s  got error: %v, want: nil", tc.name, gotErr)
		}
		for i, proc := range processes {
			wantProc := tc.want[i]
			wantName := wantProc.name
			gotName, _ := proc.Name()
			if gotName != wantName {
				t.Errorf("TestListAllProcesses() with name %s got name %s, want %s", tc.name, wantName, gotName)
			}
			wantPid := wantProc.pid
			gotPid := proc.Pid()
			if gotPid != wantPid {
				t.Errorf("TestListAllProcesses() with name %s got pid %d, want %d", tc.name, wantPid, gotPid)
			}
			wantUsername := wantProc.username
			gotUsername, _ := proc.Username()
			if gotUsername != wantUsername {
				t.Errorf("TestListAllProcesses() with name %s got username %s, want %s", tc.name, wantUsername, gotUsername)
			}
			wantArgs := wantProc.args
			gotArgs, _ := proc.CmdlineSlice()
			if diff := cmp.Diff(wantArgs, gotArgs); diff != "" {
				t.Errorf("TestListAllProcesses() with name %s got args %v, want %v", tc.name, gotArgs, wantArgs)
			}
		}
	}
}

func ValidateResult(gotProcesses []servicecommunication.ProcessWrapper, wantProcesses []servicecommunication.ProcessWrapper, testName string, t *testing.T) {
	if len(gotProcesses) != len(wantProcesses) {
		t.Errorf("TestCommonDiscovery() with name %s got %d processes, want %d", testName, len(gotProcesses), len(wantProcesses))
	}
	for i, proc := range gotProcesses {
		wantProc := wantProcesses[i]
		wantName, _ := wantProc.Name()
		gotName, _ := proc.Name()
		if gotName != wantName {
			t.Errorf("TestCommonDiscovery() with name %s got name %s, want %s", testName, wantName, gotName)
		}
		wantPid := wantProc.Pid()
		gotPid := proc.Pid()
		if gotPid != wantPid {
			t.Errorf("TestCommonDiscovery() with name %s got pid %d, want %d", testName, wantPid, gotPid)
		}
		wantUsername, _ := wantProc.Username()
		gotUsername, _ := proc.Username()
		if gotUsername != wantUsername {
			t.Errorf("TestCommonDiscovery() with name %s got username %s, want %s", testName, wantUsername, gotUsername)
		}
		wantArgs, _ := wantProc.CmdlineSlice()
		gotArgs, _ := proc.CmdlineSlice()
		if diff := cmp.Diff(wantArgs, gotArgs); diff != "" {
			t.Errorf("TestCommonDiscovery() with name %s got args %v, want %v", testName, gotArgs, wantArgs)
		}
	}
}

func TestCommonDiscoveryLoop(t *testing.T) {
	tests := []struct {
		name    string
		d       *Service
		want    servicecommunication.DiscoveryResult
		wantErr error
	}{
		{
			name: "UnrelatedProcess",
			d: &Service{
				ProcessLister: fakeProcessLister{processes: []processStub{
					{username: "user1", pid: 123, name: "test"},
				}},
			},
			want: servicecommunication.DiscoveryResult{
				Processes: []servicecommunication.ProcessWrapper{
					processStub{username: "user1", pid: 123, name: "test"},
				},
			},
		},
		{
			name: "MySQLProcess",
			d: &Service{
				ProcessLister: fakeProcessLister{processes: []processStub{
					{username: "user1", pid: 123, name: "mysqld"},
				}},
			},
			want: servicecommunication.DiscoveryResult{
				Processes: []servicecommunication.ProcessWrapper{
					processStub{username: "user1", pid: 123, name: "mysqld"},
				},
			},
		},
		{
			name: "OracleProcess",
			d: &Service{
				ProcessLister: fakeProcessLister{processes: []processStub{
					{username: "user1", pid: 123, name: "test"},
					{username: "user2", pid: 456, name: "tnslsnr", args: []string{"tnslsnr", "LISTENER"}},
					{username: "user2", pid: 789, name: "ora_pmon_orcl"},
				}},
			},
			want: servicecommunication.DiscoveryResult{
				Processes: []servicecommunication.ProcessWrapper{
					processStub{username: "user1", pid: 123, name: "test"},
					processStub{username: "user2", pid: 456, name: "tnslsnr", args: []string{"tnslsnr", "LISTENER"}},
					processStub{username: "user2", pid: 789, name: "ora_pmon_orcl"},
				},
			},
		},
		{
			name: "AllTypesOfProcesses",
			d: &Service{
				ProcessLister: fakeProcessLister{processes: []processStub{
					{username: "user1", pid: 123, name: "test"},
					{username: "user1", pid: 234, name: "mysqld"},
					{username: "user2", pid: 456, name: "tnslsnr", args: []string{"tnslsnr", "LISTENER"}},
					{username: "user2", pid: 789, name: "ora_pmon_orcl"},
				}},
			},
			want: servicecommunication.DiscoveryResult{
				Processes: []servicecommunication.ProcessWrapper{
					processStub{username: "user1", pid: 123, name: "test"},
					processStub{username: "user1", pid: 234, name: "mysqld"},
					processStub{username: "user2", pid: 456, name: "tnslsnr", args: []string{"tnslsnr", "LISTENER"}},
					processStub{username: "user2", pid: 789, name: "ora_pmon_orcl"},
				},
			},
		},
		{
			name: "EmptyProcesses",
			d: &Service{
				ProcessLister: fakeProcessLister{processes: []processStub{}},
			},
			want: servicecommunication.DiscoveryResult{
				Processes: []servicecommunication.ProcessWrapper{},
			},
			wantErr: errors.New("no processes found"),
		},
		{
			name: "ProcessListerError",
			d: &Service{
				ProcessLister: errorProneProcessLister{processes: []processStub{}},
			},
			want: servicecommunication.DiscoveryResult{
				Processes: []servicecommunication.ProcessWrapper{},
			},
			wantErr: errors.New("test error"),
		},
		{
			name: "EnvVars",
			d: &Service{
				ProcessLister: fakeProcessLister{processes: []processStub{
					{username: "user2", pid: 456, name: "testprocess", args: []string{"testprocess", "flag"}, environ: []string{"VAR1=val1", "VAR2=val2"}},
				}},
			},
			want: servicecommunication.DiscoveryResult{
				Processes: []servicecommunication.ProcessWrapper{
					processStub{username: "user2", pid: 456, name: "testprocess", args: []string{"testprocess", "flag"}, environ: []string{"VAR1=val1", "VAR2=val2"}},
				},
			},
		},
	}

	ctx := context.Background()
	for _, tc := range tests {
		result, gotErr := tc.d.commonDiscoveryLoop(ctx)
		if gotErr != nil {
			if tc.wantErr == nil {
				t.Errorf("TestCommonDiscovery() with name %s  got error: %v, want: nil", tc.name, gotErr)
			} else if gotErr.Error() != tc.wantErr.Error() {
				t.Errorf("TestCommonDiscovery() with name %s  got error: %v, want: %v", tc.name, gotErr, tc.wantErr)
			}
		}
		ValidateResult(result.Processes, tc.want.Processes, tc.name, t)
	}
}

func TestCommonDiscoveryUnbufferedChannels(t *testing.T) {
	tests := []struct {
		name string
		d    *Service
		chs  []chan<- *servicecommunication.Message
	}{
		{
			name: "UnbufferedChannelsDoNotHang",
			d: &Service{
				ProcessLister: fakeProcessLister{processes: []processStub{
					{username: "user1", pid: 123, name: "test"},
				}},
			},
			chs: []chan<- *servicecommunication.Message{make(chan<- *servicecommunication.Message), make(chan<- *servicecommunication.Message), make(chan<- *servicecommunication.Message)},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range tests {
		// This test is just checking that the common discovery loop does not hang when the channels are unbuffered.
		tc.d.CommonDiscovery(ctx, tc.chs)
	}
}

func TestCommonDiscoveryFullChannel(t *testing.T) {
	name := "OneChannelFullOneChannelNotBlocked"
	d := &Service{
		Config: &cpb.Configuration{
			CommonDiscovery: &cpb.CommonDiscovery{
				// every 0.1 seconds
				CollectionFrequency: &dpb.Duration{Nanos: 1000000000 * 0.1},
			},
		},
		ProcessLister: fakeProcessLister{processes: []processStub{
			{username: "user1", pid: 123, name: "test"},
		}},
	}
	ch1 := make(chan *servicecommunication.Message, 1)
	ch2 := make(chan *servicecommunication.Message, 1)
	chs := []chan<- *servicecommunication.Message{ch1, ch2}
	want := servicecommunication.Message{
		DiscoveryResult: servicecommunication.DiscoveryResult{
			Processes: []servicecommunication.ProcessWrapper{
				processStub{username: "user1", pid: 123, name: "test"},
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range 10 {
		d.CommonDiscovery(ctx, chs)
		// Ignore the second channel to make sure will not block the first channel.
		result := <-ch1
		ValidateResult(result.DiscoveryResult.Processes, want.DiscoveryResult.Processes, name, t)
	}
}

func TestCommonDiscovery(t *testing.T) {
	ch1 := make(chan *servicecommunication.Message, 1)
	ch2 := make(chan *servicecommunication.Message, 1)
	sendChs := []chan<- *servicecommunication.Message{ch1, ch2}
	receiveChs := []<-chan *servicecommunication.Message{ch1, ch2}
	tests := []struct {
		name        string
		d           *Service
		want        servicecommunication.Message
		iterations  int
		maxDuration time.Duration
		minDuration time.Duration
	}{
		{
			name: "MultipleChannels",
			d: &Service{
				ProcessLister: fakeProcessLister{processes: []processStub{
					{username: "user1", pid: 123, name: "test"},
				}},
			},
			want: servicecommunication.Message{
				DiscoveryResult: servicecommunication.DiscoveryResult{
					Processes: []servicecommunication.ProcessWrapper{
						processStub{username: "user1", pid: 123, name: "test"},
					},
				},
			},
			iterations:  1,
			maxDuration: 3 * time.Second,
			minDuration: 0 * time.Second,
		},
		{
			name: "SingleChannel",
			d: &Service{
				ProcessLister: fakeProcessLister{processes: []processStub{
					{username: "user1", pid: 123, name: "test"},
				}},
			},
			want: servicecommunication.Message{
				DiscoveryResult: servicecommunication.DiscoveryResult{
					Processes: []servicecommunication.ProcessWrapper{
						processStub{username: "user1", pid: 123, name: "test"},
					},
				},
			},
			iterations:  1,
			maxDuration: 3 * time.Second,
			minDuration: 0 * time.Second,
		},
		{
			name: "ZeroChannelsDoesNotHang",
			d: &Service{
				ProcessLister: fakeProcessLister{processes: []processStub{
					{username: "user1", pid: 123, name: "test"},
				}},
			},
			want: servicecommunication.Message{
				DiscoveryResult: servicecommunication.DiscoveryResult{
					Processes: []servicecommunication.ProcessWrapper{
						processStub{username: "user1", pid: 123, name: "test"},
					},
				},
			},
			iterations:  1,
			maxDuration: 3 * time.Second,
			minDuration: 0 * time.Second,
		},
		{
			name: "MultipleIterations",
			d: &Service{
				ProcessLister: fakeProcessLister{processes: []processStub{
					{username: "user1", pid: 123, name: "test"},
				}},
				Config: &cpb.Configuration{
					CommonDiscovery: &cpb.CommonDiscovery{
						// 200 milliseconds collection frequency
						CollectionFrequency: &dpb.Duration{Nanos: 1000 * 1000 * 200},
					},
				},
				InitialInterval: 30 * time.Millisecond,
			},
			want: servicecommunication.Message{
				DiscoveryResult: servicecommunication.DiscoveryResult{
					Processes: []servicecommunication.ProcessWrapper{
						processStub{username: "user1", pid: 123, name: "test"},
					},
				},
			},
			iterations:  10,
			maxDuration: 10 * time.Second,
			minDuration: 1 * time.Second,
		},
		{
			name: "MultipleIterationsWithInitialInterval",
			d: &Service{
				ProcessLister: fakeProcessLister{processes: []processStub{
					{username: "user1", pid: 123, name: "test"},
				}},
				Config: &cpb.Configuration{
					CommonDiscovery: &cpb.CommonDiscovery{
						CollectionFrequency: &dpb.Duration{Seconds: 1},
					},
				},
				InitialInterval: 10 * time.Millisecond,
			},
			want: servicecommunication.Message{
				DiscoveryResult: servicecommunication.DiscoveryResult{
					Processes: []servicecommunication.ProcessWrapper{
						processStub{username: "user1", pid: 123, name: "test"},
					},
				},
			},
			iterations:  4,
			maxDuration: 3 * time.Second,
			minDuration: 0 * time.Second,
		},
	}

	for _, tc := range tests {
		start := time.Now()
		ctx, cancel := context.WithCancel(context.Background())
		go tc.d.CommonDiscovery(ctx, sendChs)
		for range tc.iterations {
			for _, ch := range receiveChs {
				result := <-ch
				ValidateResult(result.DiscoveryResult.Processes, tc.want.DiscoveryResult.Processes, tc.name, t)
			}
		}
		cancel()
		elapsed := time.Since(start)
		if elapsed > time.Duration(tc.maxDuration) {
			t.Errorf("TestCommonDiscovery() with name %s took %v, want less than %v", tc.name, elapsed, tc.maxDuration)
		}
		if elapsed < time.Duration(tc.minDuration) {
			t.Errorf("TestCommonDiscovery() with name %s took %v, want at least %v", tc.name, elapsed, tc.minDuration)
		}
	}
}
