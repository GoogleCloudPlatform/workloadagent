//go:build windows

/*
Copyright 2026 Google LLC

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

package sqlservermetrics

import (
	"testing"
)

func TestBuildFindCommand(t *testing.T) {
	tests := []struct {
		name         string
		physicalPath string
		want         string
		wantErr      bool
	}{
		{
			name:         "normal path",
			physicalPath: "/var/opt/mssql/data/master.mdf",
			want:         `sudo find '/var/opt/mssql/data/' -type f -iname 'master.mdf' -print`,
		},
		{
			name:         "path with spaces",
			physicalPath: "/var/opt/mssql/my data/my db.mdf",
			want:         `sudo find '/var/opt/mssql/my data/' -type f -iname 'my db.mdf' -print`,
		},
		{
			// Payload from b/584900208.
			name:         "double quote command injection",
			physicalPath: `/var/opt/mssql/data/x"; sudo bash -c "id>/tmp/pwned" #x.mdf`,
			want:         `sudo find '/var/opt/mssql/data/x"; sudo bash -c "id>/tmp/' -type f -iname 'pwned" #x.mdf' -print`,
		},
		{
			// find option injection that would delete files as root if unquoted.
			name:         "find option injection",
			physicalPath: "/ -delete -newer /x.mdf",
			want:         `sudo find '/ -delete -newer /' -type f -iname 'x.mdf' -print`,
		},
		{
			name:         "command substitution",
			physicalPath: "/var/opt/mssql/data/$(id)`id`.mdf",
			want:         "sudo find '/var/opt/mssql/data/' -type f -iname '$(id)`id`.mdf' -print",
		},
		{
			name:         "single quote",
			physicalPath: "/var/opt/mssql/data/it's'; id #.mdf",
			want:         `sudo find '/var/opt/mssql/data/' -type f -iname 'it'\''s'\''; id #.mdf' -print`,
		},
		{
			name:         "directory starting with dash",
			physicalPath: "-delete/x.mdf",
			wantErr:      true,
		},
		{
			name:         "relative path",
			physicalPath: "data/x.mdf",
			wantErr:      true,
		},
		{
			name:         "no directory",
			physicalPath: "x.mdf",
			wantErr:      true,
		},
		{
			name:         "windows path",
			physicalPath: `C:\data\x.mdf`,
			wantErr:      true,
		},
		{
			name:         "no file name",
			physicalPath: "/var/opt/mssql/data/",
			wantErr:      true,
		},
		{
			name:         "pipe",
			physicalPath: "/var/opt/mssql/data/x.mdf|id",
			wantErr:      true,
		},
		{
			name:         "newline",
			physicalPath: "/var/opt/mssql/data/x.mdf\nid",
			wantErr:      true,
		},
		{
			name:         "empty",
			physicalPath: "",
			wantErr:      true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildFindCommand(tc.physicalPath)
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("buildFindCommand(%q) returned error: %v, want error: %t", tc.physicalPath, err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("buildFindCommand(%q) = %q, want %q", tc.physicalPath, got, tc.want)
			}
		})
	}
}

func TestBuildDfCommand(t *testing.T) {
	tests := []struct {
		name       string
		findOutput string
		want       string
		wantErr    bool
	}{
		{
			name:       "single result",
			findOutput: "/var/opt/mssql/data/master.mdf\n",
			want:       `sudo df --output=target '/var/opt/mssql/data/master.mdf' | tail -n 1`,
		},
		{
			name:       "single result without newline",
			findOutput: "/var/opt/mssql/data/master.mdf",
			want:       `sudo df --output=target '/var/opt/mssql/data/master.mdf' | tail -n 1`,
		},
		{
			name:       "multiple results uses first line",
			findOutput: "/var/opt/mssql/data/master.mdf\n/tmp/id\n",
			want:       `sudo df --output=target '/var/opt/mssql/data/master.mdf' | tail -n 1`,
		},
		{
			name:       "CRLF output",
			findOutput: "/var/opt/mssql/data/master.mdf\r\n",
			want:       `sudo df --output=target '/var/opt/mssql/data/master.mdf' | tail -n 1`,
		},
		{
			name:       "command substitution in file name",
			findOutput: "/var/opt/mssql/data/$(id>/tmp/pwned).mdf\n",
			want:       `sudo df --output=target '/var/opt/mssql/data/$(id>/tmp/pwned).mdf' | tail -n 1`,
		},
		{
			name:       "single quote in file name",
			findOutput: "/var/opt/mssql/data/x'; id #.mdf\n",
			want:       `sudo df --output=target '/var/opt/mssql/data/x'\''; id #.mdf' | tail -n 1`,
		},
		{
			name:       "pipe in file name",
			findOutput: "/var/opt/mssql/data/x|id.mdf\n",
			wantErr:    true,
		},
		{
			name:       "empty output",
			findOutput: "",
			wantErr:    true,
		},
		{
			name:       "empty first line",
			findOutput: "\n/var/opt/mssql/data/master.mdf\n",
			wantErr:    true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildDfCommand(tc.findOutput)
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("buildDfCommand(%q) returned error: %v, want error: %t", tc.findOutput, err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("buildDfCommand(%q) = %q, want %q", tc.findOutput, got, tc.want)
			}
		})
	}
}
