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

package secret

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSecretString(t *testing.T) {
	var s String = "password"
	result := s.String()
	if !strings.Contains(result, redactedValue) {
		t.Errorf("String() = %s, want %s", result, redactedValue)
	}
}

func TestSecretStringUnmarshalJSON(t *testing.T) {
	type jsonStruct struct {
		S String `json:"password"`
	}

	jsonInput := `{"password": "superSecretPassword"}`
	var result jsonStruct
	err := json.Unmarshal([]byte(jsonInput), &result)
	if err != nil {
		t.Errorf("expected Unmarshal not to error: %s, got: %s", err, result)
	}
	if result.S != "superSecretPassword" {
		t.Errorf("expected Unmarshal to retain secret field value, got: %s", result.S)
	}
}

func TestSecretStringMarshalJSON(t *testing.T) {
	type jsonStruct struct {
		S String `json:"password"`
	}
	resultBytes, err := json.Marshal(jsonStruct{S: "superSecretPassword"})
	result := string(resultBytes)
	if err != nil {
		t.Errorf("expected marshal not to error, got: %s", result)
	}
	if !strings.Contains(result, redactedValue) {
		t.Fatalf("expected Marshal to redact secret field, got: %s", result)
	}
}
