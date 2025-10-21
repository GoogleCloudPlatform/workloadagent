/*
Copyright 2023 Google LLC

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

package onetime

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/spf13/cobra"
	"go.uber.org/zap/zapcore"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
)

func TestSetValues(t *testing.T) {
	tests := []struct {
		name     string
		setFlags func(*cobra.Command)
		lp       *log.Parameters
		logName  string
		want     *log.Parameters
	}{
		{
			name:     "defaultValues",
			setFlags: func(cmd *cobra.Command) {},
			lp: &log.Parameters{
				OSType: "linux",
			},
			logName: "test",
			want: &log.Parameters{
				OSType:      "linux",
				LogFileName: "/var/log/google-cloud-workload-agent-test.log",
				LogToCloud:  true,
				Level:       zapcore.InfoLevel,
			},
		},
		{
			name: "overrideValues",
			setFlags: func(cmd *cobra.Command) {
				flags := cmd.PersistentFlags()
				flags.Set("log-file", "C:\\Override\\test.log")
				flags.Set("log-level", "warn")
				flags.Set("log-to-cloud", "false")
			},
			lp: &log.Parameters{
				OSType: "windows",
			},
			logName: "test",
			want: &log.Parameters{
				OSType:      "windows",
				LogFileName: `C:\Override\test.log`,
				LogToCloud:  false,
				Level:       zapcore.WarnLevel,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			Register(test.lp.OSType, cmd)
			test.setFlags(cmd)
			defer func(cmd *cobra.Command) {
				flags := cmd.PersistentFlags()
				flags.Set("log-file", "")
				flags.Set("log-level", "info")
				flags.Set("log-to-cloud", "true")
			}(cmd)
			SetValues("google-cloud-workload-agent", test.lp, cmd, test.logName)
			if diff := cmp.Diff(test.want, test.lp); diff != "" {
				t.Errorf("SetValues() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
