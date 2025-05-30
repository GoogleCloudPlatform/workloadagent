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

// Package sqlserverutils contains utility data structures and functions for sql server metrics.
package sqlserverutils

// MetricDetails represents collected details results.
type MetricDetails struct {
	Name   string
	Fields []map[string]string
}

// Disks contains information about a device name and the disk type.
type Disks struct {
	DeviceName string
	DiskType   string
	Mapping    string
}

// SQLConfig .
type SQLConfig struct {
	Host       string
	Username   string
	SecretName string
	PortNumber int32
	ProjectID  string
}

// GuestConfig .
type GuestConfig struct {
	ServerName             string
	GuestUserName          string
	GuestSecretName        string
	GuestPortNumber        int32
	LinuxRemote            bool
	LinuxSSHPrivateKeyPath string
	ProjectID              string
}
