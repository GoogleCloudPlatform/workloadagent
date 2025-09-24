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

// Package status provides information on the agent, configuration, IAM and functional statuses.

package status

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/gax-go"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/testing/protocmp"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/configuration"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/commandlineexecutor"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/statushelper"

	arpb "google.golang.org/genproto/googleapis/devtools/artifactregistry/v1"
	ar "cloud.google.com/go/artifactregistry/apiv1"
	spb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/status"
)

type fakeARClient struct {
	statushelper.ARClientInterface
	packages []*arpb.Package
	versions []*arpb.Version
	err      error
}

// versionIterator implements the iterator interface for versions.
type versionIterator struct {
	versions []*arpb.Version
	err      error
	idx      int
}

func (it *versionIterator) Next() (*arpb.Version, error) {
	if it.err != nil {
		return nil, it.err
	}
	if it.idx >= len(it.versions) {
		return nil, iterator.Done
	}
	v := it.versions[it.idx]
	it.idx++
	return v, nil
}

func (c *fakeARClient) ListPackages(ctx context.Context, req *arpb.ListPackagesRequest, opts ...gax.CallOption) *ar.PackageIterator {
	var fetched bool
	return &ar.PackageIterator{
		InternalFetch: func(pageSize int, pageToken string) (res []*arpb.Package, nextPageToken string, err error) {
			if c.err != nil {
				return nil, "", c.err
			}
			if fetched {
				return nil, "", nil
			}
			fetched = true
			return c.packages, "", nil
		},
	}
}

func (c *fakeARClient) ListVersions(ctx context.Context, req *arpb.ListVersionsRequest, opts ...gax.CallOption) statushelper.VersionIterator {
	return &versionIterator{versions: c.versions, err: c.err}
}

func (c *fakeARClient) Close() error { return nil }

func newFakeARClient(packages []*arpb.Package, versions []*arpb.Version, err error) statushelper.ARClientInterface {
	return &fakeARClient{
		packages: packages,
		versions: versions,
		err:      err,
	}
}

func TestNewARClient(t *testing.T) {
	ctx := context.Background()
	// Force the client to fail by pointing to a non-existent credentials file.
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "non-existent-file")

	client, err := newARClient(ctx)
	if err == nil {
		if c, ok := client.(io.Closer); ok {
			c.Close()
		}
		t.Fatal("newARClient() succeeded, want error")
	}
	if client != nil {
		t.Error("newARClient() returned a non-nil client on error")
	}
}

func TestAgentStatus(t *testing.T) {
	tests := []struct {
		name       string
		exec       commandlineexecutor.Execute
		arClient   statushelper.ARClientInterface
		cloudProps *cpb.CloudProperties
		readFile   func(string) ([]byte, error)
		want       *spb.AgentStatus
	}{
		{
			name: "SuccessWithSingleVersion",
			exec: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				if strings.Contains(params.ArgsToSplit, "is-enabled") {
					return commandlineexecutor.Result{StdOut: "enabled", ExitCode: 0}
				}
				if strings.Contains(params.ArgsToSplit, "is-active") {
					return commandlineexecutor.Result{StdOut: "active", ExitCode: 0}
				}
				if params.Executable == "uname" {
					return commandlineexecutor.Result{StdOut: "5.10.0", ExitCode: 0}
				}
				return commandlineexecutor.Result{}
			},
			arClient: newFakeARClient(
				[]*arpb.Package{{Name: "projects/workload-agent-products/locations/us/repositories/google-cloud-workload-agent-x86-64/packages/google-cloud-workload-agent"}},
				[]*arpb.Version{{Name: "1.2.3"}},
				nil,
			),
			readFile: func(string) ([]byte, error) {
				return []byte(`{}`), nil
			},
			cloudProps: &cpb.CloudProperties{
				ProjectId:  "test-project",
				Zone:       "test-zone",
				InstanceId: "test-instance",
				Scopes:     []string{requiredScope},
			},
			want: &spb.AgentStatus{
				AgentName:                       agentPackageName,
				InstalledVersion:                fmt.Sprintf("%s-%s", configuration.AgentVersion, configuration.AgentBuildChange),
				AvailableVersion:                "1.2.3",
				SystemdServiceEnabled:           spb.State_SUCCESS_STATE,
				SystemdServiceRunning:           spb.State_SUCCESS_STATE,
				CloudApiAccessFullScopesGranted: spb.State_SUCCESS_STATE,
				ConfigurationFilePath:           configuration.LinuxConfigPath,
				ConfigurationValid:              spb.State_SUCCESS_STATE,
				InstanceUri:                     "projects/test-project/zones/test-zone/instances/test-instance",
				KernelVersion:                   &spb.KernelVersion{RawString: "5.10.0"},
			},
		},
		{
			name: "SuccessWithMultipleVersions",
			exec: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				if strings.Contains(params.ArgsToSplit, "is-enabled") {
					return commandlineexecutor.Result{StdOut: "enabled", ExitCode: 0}
				}
				if strings.Contains(params.ArgsToSplit, "is-active") {
					return commandlineexecutor.Result{StdOut: "active", ExitCode: 0}
				}
				if params.Executable == "uname" {
					return commandlineexecutor.Result{StdOut: "5.10.0", ExitCode: 0}
				}
				return commandlineexecutor.Result{}
			},
			arClient: newFakeARClient(
				[]*arpb.Package{{Name: "projects/workload-agent-products/locations/us/repositories/google-cloud-workload-agent-x86-64/packages/google-cloud-workload-agent"}},
				[]*arpb.Version{
					{Name: "1.0.0"},
					{Name: "1.1-764777575"},
					{Name: "0.9.0"},
					{Name: "1.2.0"},
				},
				nil,
			),
			readFile: func(string) ([]byte, error) {
				return []byte(`{}`), nil
			},
			cloudProps: &cpb.CloudProperties{
				ProjectId:  "test-project",
				Zone:       "test-zone",
				InstanceId: "test-instance",
				Scopes:     []string{requiredScope},
			},
			want: &spb.AgentStatus{
				AgentName:                       agentPackageName,
				InstalledVersion:                fmt.Sprintf("%s-%s", configuration.AgentVersion, configuration.AgentBuildChange),
				AvailableVersion:                "1.2.0",
				SystemdServiceEnabled:           spb.State_SUCCESS_STATE,
				SystemdServiceRunning:           spb.State_SUCCESS_STATE,
				CloudApiAccessFullScopesGranted: spb.State_SUCCESS_STATE,
				ConfigurationFilePath:           configuration.LinuxConfigPath,
				ConfigurationValid:              spb.State_SUCCESS_STATE,
				InstanceUri:                     "projects/test-project/zones/test-zone/instances/test-instance",
				KernelVersion:                   &spb.KernelVersion{RawString: "5.10.0"},
			},
		},
		{
			name: "Failure",
			exec: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				if strings.Contains(params.ArgsToSplit, "is-enabled") {
					return commandlineexecutor.Result{StdOut: "disabled", ExitCode: 1}
				}
				if strings.Contains(params.ArgsToSplit, "is-active") {
					return commandlineexecutor.Result{StdOut: "inactive", ExitCode: 3}
				}
				if params.Executable == "uname" {
					return commandlineexecutor.Result{StdOut: "5.10.0", ExitCode: 0}
				}
				return commandlineexecutor.Result{}
			},
			arClient: newFakeARClient(
				[]*arpb.Package{{Name: "projects/workload-agent-products/locations/us/repositories/google-cloud-workload-agent-x86-64/packages/google-cloud-workload-agent"}},
				[]*arpb.Version{{Name: "1.2.3"}},
				nil,
			),
			readFile: func(string) ([]byte, error) {
				return []byte(`{}`), nil
			},
			cloudProps: &cpb.CloudProperties{
				ProjectId:  "test-project",
				Zone:       "test-zone",
				InstanceId: "test-instance",
				Scopes:     []string{"wrong-scope"},
			},
			want: &spb.AgentStatus{
				AgentName:                       agentPackageName,
				InstalledVersion:                fmt.Sprintf("%s-%s", configuration.AgentVersion, configuration.AgentBuildChange),
				AvailableVersion:                "1.2.3",
				SystemdServiceEnabled:           spb.State_FAILURE_STATE,
				SystemdServiceRunning:           spb.State_FAILURE_STATE,
				CloudApiAccessFullScopesGranted: spb.State_FAILURE_STATE,
				ConfigurationFilePath:           configuration.LinuxConfigPath,
				ConfigurationValid:              spb.State_SUCCESS_STATE,
				InstanceUri:                     "projects/test-project/zones/test-zone/instances/test-instance",
				KernelVersion:                   &spb.KernelVersion{RawString: "5.10.0"},
			},
		},
		{
			name: "Error",
			exec: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				return commandlineexecutor.Result{
					StdErr: "command failed",
					Error:  errors.New("command failed"),
				}
			},
			arClient:   newFakeARClient(nil, nil, errors.New("AR error")),
			cloudProps: nil,
			readFile: func(string) ([]byte, error) {
				return nil, errors.New("file read error")
			},
			want: &spb.AgentStatus{
				AgentName:                       agentPackageName,
				InstalledVersion:                fmt.Sprintf("%s-%s", configuration.AgentVersion, configuration.AgentBuildChange),
				AvailableVersion:                "Error: could not fetch latest version",
				SystemdServiceEnabled:           spb.State_ERROR_STATE,
				SystemdServiceRunning:           spb.State_ERROR_STATE,
				CloudApiAccessFullScopesGranted: spb.State_ERROR_STATE,
				ConfigurationFilePath:           configuration.LinuxConfigPath,
				ConfigurationValid:              spb.State_FAILURE_STATE,
				ConfigurationErrorMessage:       "file read error",
				KernelVersion:                   nil,
			},
		},
		{
			name: "ARError",
			exec: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				if strings.Contains(params.ArgsToSplit, "is-enabled") {
					return commandlineexecutor.Result{StdOut: "enabled", ExitCode: 0}
				}
				if strings.Contains(params.ArgsToSplit, "is-active") {
					return commandlineexecutor.Result{StdOut: "active", ExitCode: 0}
				}
				if params.Executable == "uname" {
					return commandlineexecutor.Result{StdOut: "5.10.0", ExitCode: 0}
				}
				return commandlineexecutor.Result{}
			},
			arClient: newFakeARClient(nil, nil, errors.New("AR error")),
			readFile: func(string) ([]byte, error) {
				return []byte(`{}`), nil
			},
			cloudProps: &cpb.CloudProperties{
				ProjectId:  "test-project",
				Zone:       "test-zone",
				InstanceId: "test-instance",
				Scopes:     []string{requiredScope},
			},
			want: &spb.AgentStatus{
				AgentName:                       agentPackageName,
				InstalledVersion:                fmt.Sprintf("%s-%s", configuration.AgentVersion, configuration.AgentBuildChange),
				AvailableVersion:                "Error: could not fetch latest version",
				SystemdServiceEnabled:           spb.State_SUCCESS_STATE,
				SystemdServiceRunning:           spb.State_SUCCESS_STATE,
				CloudApiAccessFullScopesGranted: spb.State_SUCCESS_STATE,
				ConfigurationFilePath:           configuration.LinuxConfigPath,
				ConfigurationValid:              spb.State_SUCCESS_STATE,
				InstanceUri:                     "projects/test-project/zones/test-zone/instances/test-instance",
				KernelVersion:                   &spb.KernelVersion{RawString: "5.10.0"},
			},
		},
		{
			name: "InvalidConfigFile",
			exec: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				if strings.Contains(params.ArgsToSplit, "is-enabled") {
					return commandlineexecutor.Result{StdOut: "enabled", ExitCode: 0}
				}
				if strings.Contains(params.ArgsToSplit, "is-active") {
					return commandlineexecutor.Result{StdOut: "active", ExitCode: 0}
				}
				if params.Executable == "uname" {
					return commandlineexecutor.Result{StdOut: "5.10.0", ExitCode: 0}
				}
				return commandlineexecutor.Result{}
			},
			arClient: newFakeARClient(
				[]*arpb.Package{{Name: "projects/workload-agent-products/locations/us/repositories/google-cloud-workload-agent-x86-64/packages/google-cloud-workload-agent"}},
				[]*arpb.Version{{Name: "1.2.3"}},
				nil,
			),
			readFile: func(string) ([]byte, error) {
				return []byte(`{invalid-json}`), nil
			},
			cloudProps: &cpb.CloudProperties{
				ProjectId:  "test-project",
				Zone:       "test-zone",
				InstanceId: "test-instance",
				Scopes:     []string{requiredScope},
			},
			want: &spb.AgentStatus{
				AgentName:                       agentPackageName,
				InstalledVersion:                fmt.Sprintf("%s-%s", configuration.AgentVersion, configuration.AgentBuildChange),
				AvailableVersion:                "1.2.3",
				SystemdServiceEnabled:           spb.State_SUCCESS_STATE,
				SystemdServiceRunning:           spb.State_SUCCESS_STATE,
				CloudApiAccessFullScopesGranted: spb.State_SUCCESS_STATE,
				ConfigurationFilePath:           configuration.LinuxConfigPath,
				ConfigurationValid:              spb.State_FAILURE_STATE,
				ConfigurationErrorMessage:       "proto: syntax error (line 1:2): invalid value invalid-json",
				InstanceUri:                     "projects/test-project/zones/test-zone/instances/test-instance",
				KernelVersion:                   &spb.KernelVersion{RawString: "5.10.0"},
			},
		},
		{
			name: "WindowsPath",
			exec: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				if strings.Contains(params.ArgsToSplit, "query") {
					return commandlineexecutor.Result{StdOut: "STATE              : 4  RUNNING", ExitCode: 0}
				}
				if strings.Contains(params.ArgsToSplit, "qc") {
					return commandlineexecutor.Result{StdOut: "START_TYPE         : 2   AUTO_START", ExitCode: 0}
				}
				return commandlineexecutor.Result{}
			},
			arClient: newFakeARClient(
				[]*arpb.Package{{Name: "projects/workload-agent-products/locations/us/repositories/google-cloud-workload-agent-x86-64/packages/google-cloud-workload-agent"}},
				[]*arpb.Version{{Name: "1.2.3"}},
				nil,
			),
			readFile: func(string) ([]byte, error) {
				return []byte(`{}`), nil
			},
			cloudProps: &cpb.CloudProperties{
				ProjectId:  "test-project",
				Zone:       "test-zone",
				InstanceId: "test-instance",
				Scopes:     []string{requiredScope},
			},
			want: &spb.AgentStatus{
				AgentName:                       agentPackageName,
				InstalledVersion:                fmt.Sprintf("%s-%s", configuration.AgentVersion, configuration.AgentBuildChange),
				AvailableVersion:                "1.2.3",
				SystemdServiceEnabled:           spb.State_SUCCESS_STATE,
				SystemdServiceRunning:           spb.State_SUCCESS_STATE,
				CloudApiAccessFullScopesGranted: spb.State_SUCCESS_STATE,
				ConfigurationFilePath:           configuration.WindowsConfigPath,
				ConfigurationValid:              spb.State_SUCCESS_STATE,
				InstanceUri:                     "projects/test-project/zones/test-zone/instances/test-instance",
				KernelVersion:                   nil,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "WindowsPath" {
				if runtime.GOOS != "windows" {
					t.Skipf("Skipping windows test on non-windows OS: %s", runtime.GOOS)
				}
			} else if runtime.GOOS != "linux" {
				t.Skipf("Skipping linux test on non-linux OS: %s", runtime.GOOS)
			}

			ctx := context.Background()
			got := agentStatus(ctx, tc.arClient, tc.exec, tc.cloudProps, "", tc.readFile)
			got.ConfigurationErrorMessage = strings.ReplaceAll(got.ConfigurationErrorMessage, "\u00a0", " ")
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("agentStatus() returned unexpected diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGetRepositoryLocation(t *testing.T) {
	tests := []struct {
		name string
		cp   *cpb.CloudProperties
		want string
	}{
		{
			name: "NilCloudProps",
			cp:   nil,
			want: "us",
		},
		{
			name: "EmptyZone",
			cp:   &cpb.CloudProperties{},
			want: "us",
		},
		{
			name: "USZone",
			cp:   &cpb.CloudProperties{Zone: "us-central1-a"},
			want: "us",
		},
		{
			name: "EuropeZone",
			cp:   &cpb.CloudProperties{Zone: "europe-west1-b"},
			want: "europe",
		},
		{
			name: "AsiaZone",
			cp:   &cpb.CloudProperties{Zone: "asia-southeast1-c"},
			want: "asia",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := getRepositoryLocation(tc.cp)
			if got != tc.want {
				t.Errorf("getRepositoryLocation(%v) = %q, want %q", tc.cp, got, tc.want)
			}
		})
	}
}
