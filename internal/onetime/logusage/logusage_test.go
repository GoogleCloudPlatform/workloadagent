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

package logusage

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/spf13/cobra"

	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

func TestLogUsageHandler(t *testing.T) {
	// Prevent requests to the compute endpoint during test execution
	usagemetrics.SetAgentProperties(&cpb.AgentProperties{
		LogUsageMetrics: false,
	})

	tests := []struct {
		name     string
		logUsage *LogUsage
		want     error
	}{
		{
			name: "EmptyUsageStatus",
			logUsage: &LogUsage{
				status: "",
			},
			want: cmpopts.AnyError,
		},
		{
			name: "UnknownStatus",
			logUsage: &LogUsage{
				status: "UNKNOWN",
			},
			want: nil,
		},
		{
			name: "ErrorWithInvalidErrorCode",
			logUsage: &LogUsage{
				status:     "ERROR",
				usageError: 0,
			},
			want: cmpopts.AnyError,
		},
		{
			name: "ErrorWithValidErrorCode",
			logUsage: &LogUsage{
				status:     "ERROR",
				usageError: 1,
			},
			want: nil,
		},
		{
			name: "ActionWithEmptyActionCode",
			logUsage: &LogUsage{
				status: "ACTION",
				action: 0,
			},
			want: cmpopts.AnyError,
		},
		{
			name: "AgentUpdatedWithAgentVersion",
			logUsage: &LogUsage{
				status:       "UPDATED",
				agentVersion: "1.2.3",
			},
			want: nil,
		},
		{
			name: "AgentUpdatedWithoutAgentVersion",
			logUsage: &LogUsage{
				status: "UPDATED",
			},
			want: cmpopts.AnyError,
		},
		{
			name: "Success",
			logUsage: &LogUsage{
				status: "ACTION",
				action: 1,
			},
			want: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd := &cobra.Command{
				Use: "logusage",
				RunE: func(cmd *cobra.Command, args []string) error {
					return test.logUsage.logUsageHandler(cmd, &cpb.CloudProperties{})
				},
			}
			got := test.logUsage.logUsageHandler(cmd, &cpb.CloudProperties{})
			if !cmp.Equal(got, test.want, cmpopts.EquateErrors()) {
				t.Errorf("logUsageHandler(%v) got: %v, want: %v", test.logUsage, got, test.want)
			}
		})

	}
}

func TestLogUsageStatus(t *testing.T) {
	// Prevent requests to the compute endpoint during test execution
	usagemetrics.SetAgentProperties(&cpb.AgentProperties{
		LogUsageMetrics: false,
	})

	tests := []struct {
		name              string
		status            string
		agentVersion      string
		agentPriorVersion string
		actionID          int
		errorID           int
		want              error
	}{
		{
			name:   "Running",
			status: "RUNNING",
			want:   nil,
		},
		{
			name:   "Started",
			status: "STARTED",
			want:   nil,
		},
		{
			name:   "Stopped",
			status: "STOPPED",
			want:   nil,
		},
		{
			name:   "Configured",
			status: "CONFIGURED",
			want:   nil,
		},
		{
			name:   "Misconfigured",
			status: "MISCONFIGURED",
			want:   nil,
		},
		{
			name:    "Error",
			status:  "ERROR",
			errorID: 1,
			want:    nil,
		},
		{
			name:   "Installed",
			status: "INSTALLED",
			want:   nil,
		},
		{
			name:         "UpdatedWithAgentVersion",
			status:       "UPDATED",
			agentVersion: "1.2.3",
			want:         nil,
		},
		{
			name:              "UpdatedWithDifferentPriorVersionAndAgentVersion",
			status:            "UPDATED",
			agentPriorVersion: "1.2.3",
			agentVersion:      "1.2.4",
			want:              nil,
		},
		{
			name:              "UpdatedWithPriorVersion",
			status:            "UPDATED",
			agentPriorVersion: "1.2.3",
			want:              nil,
		},
		{
			name:   "Uninstalled",
			status: "UNINSTALLED",
			want:   nil,
		},
		{
			name:     "Action",
			status:   "ACTION",
			actionID: 1,
			want:     nil,
		},
		{
			name:   "InvalidStatusReturnsError",
			status: "INVALID",
			want:   cmpopts.AnyError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			l := &LogUsage{
				status:       test.status,
				action:       test.actionID,
				usageError:   test.errorID,
				agentVersion: test.agentVersion,
			}
			got := l.logUsageStatus(&cpb.CloudProperties{})
			if !cmp.Equal(got, test.want, cmpopts.EquateErrors()) {
				t.Errorf("logUsageStatus(%q, %d, %d) got: %v, want nil", test.status, test.actionID, test.errorID, got)
			}
		})
	}
}
