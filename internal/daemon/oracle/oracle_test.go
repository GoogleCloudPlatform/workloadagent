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
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce/metadataserver"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/guestactions"
	gapb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/guestactions"
)

// fakeGuestActionsManager is a test double for guestActionsManager.
type fakeGuestActionsManager struct {
	startCalled bool
	startOpts   guestactions.Options
}

// Start captures the options passed and marks itself as called.
func (f *fakeGuestActionsManager) Start(ctx context.Context, a any) {
	f.startCalled = true
	f.startOpts = a.(guestactions.Options)
}

func TestConvertCloudProperties(t *testing.T) {
	tests := []struct {
		name string
		cp   *cpb.CloudProperties
		want *metadataserver.CloudProperties
	}{
		{
			name: "nil cloud properties",
			cp:   nil,
			want: nil,
		},
		{
			name: "non-nil cloud properties",
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
		wantKey     string
		wantTimeout time.Duration
		wantLock    bool
	}{
		{
			name:        "nil agent command",
			cmd:         &gapb.Command{},
			wantKey:     "",
			wantTimeout: 0,
			wantLock:    false,
		},
		{
			name: "nil parameters",
			cmd: &gapb.Command{
				CommandType: &gapb.Command_AgentCommand{
					AgentCommand: &gapb.AgentCommand{
						Command: "oracle_start_database",
					},
				},
			},
			wantKey:     "",
			wantTimeout: 0,
			wantLock:    false,
		},
		{
			name: "empty parameters",
			cmd: &gapb.Command{
				CommandType: &gapb.Command_AgentCommand{
					AgentCommand: &gapb.AgentCommand{
						Command:    "oracle_start_database",
						Parameters: map[string]string{},
					},
				},
			},
			wantKey:     "",
			wantTimeout: 0,
			wantLock:    false,
		},
		{
			name: "missing oracle_home",
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
			wantKey:     "",
			wantTimeout: 0,
			wantLock:    false,
		},
		{
			name: "missing oracle_sid",
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
			wantKey:     "",
			wantTimeout: 0,
			wantLock:    false,
		},
		{
			name: "with oracle_sid and oracle_home",
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
			wantKey:     "orcl:/u01/app/oracle/product/19.3.0/dbhome_1",
			wantTimeout: 24 * time.Hour,
			wantLock:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotKey, gotTimeout, gotLock := oracleCommandKey(tc.cmd)
			if gotKey != tc.wantKey || gotTimeout != tc.wantTimeout || gotLock != tc.wantLock {
				t.Errorf("oracleCommandKey(%v) = (%q, %v, %v), want (%q, %v, %v)", tc.cmd, gotKey, gotTimeout, gotLock, tc.wantKey, tc.wantTimeout, tc.wantLock)
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
		"oracle_disable_restricted_mode",
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
