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

package status

import (
	"bytes"
	"testing"
)

func TestStatusCmd(t *testing.T) {
	cmd := NewCommand()
	cmd.SetArgs([]string{})
	cmd.SilenceUsage = true // disable usage info message
	var buf bytes.Buffer
	cmd.SetOut(&buf)

	err := cmd.Execute()
	if err != nil {
		t.Fatalf("status command failed: %v", err)
	}

	expectedOutput := "status\n"
	if buf.String() != expectedOutput {
		t.Errorf("status command output: got %q, want %q", buf.String(), expectedOutput)
	}
}
