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

// TODO: Move this to the common shared directory.

// Package secret provides a custom string type that hides sensitive data from output.
package secret

import "encoding/json"

const redactedValue = "__redacted__"

// String represents a string holding sensitive data.
type String string

// String implements the fmt.Stringer interface to prevent sensitive data from being printed.
func (s String) String() string {
	return redactedValue
}

// SecretValue returns the secret value.
func (s String) SecretValue() string {
	return string(s)
}

// UnmarshalJSON unmarshals a secret value from JSON.
func (s *String) UnmarshalJSON(b []byte) error {
	var value string
	if err := json.Unmarshal(b, &value); err != nil {
		return err
	}

	*s = String(value)
	return nil
}

// MarshalJSON ensures the sensitive string is redacted in JSON.
func (s String) MarshalJSON() ([]byte, error) {
	return json.Marshal(redactedValue)
}
