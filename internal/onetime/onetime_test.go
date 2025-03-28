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

	"go.uber.org/zap/zapcore"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
)

func TestSetupOneTimeLogging(t *testing.T) {
	tests := []struct {
		name              string
		os                string
		subCommandName    string
		logFilePath       string
		ProgramDataEnvVar string
		want              string
		wantcloudlogname  string
	}{
		{
			name:             "Windows",
			os:               "windows",
			subCommandName:   "logusage",
			logFilePath:      "",
			want:             `C:\Program Files\Google\google-cloud-workload-agent\logs\google-cloud-workload-agent-logusage.log`,
			wantcloudlogname: "google-cloud-workload-agent-logusage",
		},
		{
			name:              "WindowsWithProgramDataEnvVar",
			os:                "windows",
			subCommandName:    "logusage",
			logFilePath:       ``,
			ProgramDataEnvVar: `C:\ProgramData`,
			want:              `C:\ProgramData\Google\google-cloud-workload-agent\logs\google-cloud-workload-agent-logusage.log`,
			wantcloudlogname:  "google-cloud-workload-agent-logusage",
		},
		{
			name:              "WindowsWithEmptyProgramDataEnvVar",
			os:                "windows",
			subCommandName:    "logusage",
			logFilePath:       ``,
			ProgramDataEnvVar: ``,
			want:              `C:\Program Files\Google\google-cloud-workload-agent\logs\google-cloud-workload-agent-logusage.log`,
			wantcloudlogname:  "google-cloud-workload-agent-logusage",
		},
		{
			name:             "WindowsWithPath",
			os:               "windows",
			subCommandName:   "logusage",
			logFilePath:      `C:\tmp\`,
			want:             `C:\tmp\google-cloud-workload-agent-logusage.log`,
			wantcloudlogname: "google-cloud-workload-agent-logusage",
		},
		{
			name:             "WindowsWithPathNoSlash",
			os:               "windows",
			subCommandName:   "logusage",
			logFilePath:      `C:\tmp`,
			want:             `C:\tmp\google-cloud-workload-agent-logusage.log`,
			wantcloudlogname: "google-cloud-workload-agent-logusage",
		},
		{
			name:             "Linux",
			os:               "linux",
			subCommandName:   "snapshot",
			logFilePath:      "",
			want:             `/var/log/google-cloud-workload-agent-snapshot.log`,
			wantcloudlogname: "google-cloud-workload-agent-snapshot",
		},
		{
			name:             "LinuxWithPath",
			os:               "linux",
			subCommandName:   "snapshot",
			logFilePath:      `/tmp/`,
			want:             `/tmp/google-cloud-workload-agent-snapshot.log`,
			wantcloudlogname: "google-cloud-workload-agent-snapshot",
		},
		{
			name:             "LinuxWithPathNoSlash",
			os:               "linux",
			subCommandName:   "snapshot",
			logFilePath:      `/tmp`,
			want:             `/tmp/google-cloud-workload-agent-snapshot.log`,
			wantcloudlogname: "google-cloud-workload-agent-snapshot",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("PROGRAMDATA", test.ProgramDataEnvVar)
			defer t.Setenv("PROGRAMDATA", "")
			lp := log.Parameters{
				LogToCloud: false,
				OSType:     test.os,
				Level:      2,
			}
			gotparams := SetupOneTimeLogging(lp, test.subCommandName, zapcore.ErrorLevel, test.logFilePath)
			if gotparams.CloudLogName != test.wantcloudlogname {
				t.Errorf("SetupOneTimeLogging(%s,%s) cloudlogname is incorrect, got: %s, want: %s", test.os, test.subCommandName, gotparams.CloudLogName, test.wantcloudlogname)
			}

			got := log.GetLogFile()
			if got != test.want {
				t.Errorf("SetupOneTimeLogging(%s,%s)=%s, want: %s", test.os, test.subCommandName, got, test.want)
			}
		})
	}
}
