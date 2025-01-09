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

// Package servicecommunication provides common types and functions for communicating between services.
package servicecommunication

// ProcessWrapper is a wrapper around process.Process to support testing.
type ProcessWrapper interface {
	Username() (string, error)
	Pid() int32
	Name() (string, error)
	CmdlineSlice() ([]string, error)
	Environ() ([]string, error)
}

// DiscoveryResult holds the results of a discovery operation.
type DiscoveryResult struct {
	Processes []ProcessWrapper
}

// MessageOrigin is the origin of the message.
type MessageOrigin int

const (
	// UnspecifiedMessageOrigin is the default message origin.
	UnspecifiedMessageOrigin MessageOrigin = iota
	// Discovery is the message origin for discovery.
	Discovery
)

// Message is the message type used to communicate between services.
type Message struct {
	Origin          MessageOrigin
	DiscoveryResult DiscoveryResult
}
