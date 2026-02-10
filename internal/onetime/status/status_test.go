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
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/gax-go"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/api/serviceusage/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/configuration"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/commandlineexecutor"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/permissions"
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

type fakeIAMService struct {
	projectPermissions    map[string][]string
	bucketPermissions     map[string][]string
	diskPermissions       map[string][]string
	instancePermissions   map[string][]string
	secretPermissions     map[string][]string
	projectPermissionErr  error
	bucketPermissionErr   error
	diskPermissionErr     error
	instancePermissionErr error
	secretPermissionErr   error
}

func (f *fakeIAMService) CheckIAMPermissionsOnProject(ctx context.Context, projectID string, permissions []string) ([]string, error) {
	if f.projectPermissionErr != nil {
		return nil, f.projectPermissionErr
	}
	return f.projectPermissions[projectID], nil
}

func (f *fakeIAMService) CheckIAMPermissionsOnBucket(ctx context.Context, bucketName string, permissions []string) ([]string, error) {
	if f.bucketPermissionErr != nil {
		return nil, f.bucketPermissionErr
	}
	return f.bucketPermissions[bucketName], nil
}

func (f *fakeIAMService) CheckIAMPermissionsOnDisk(ctx context.Context, projectID, zone, diskName string, permissions []string) ([]string, error) {
	if f.diskPermissionErr != nil {
		return nil, f.diskPermissionErr
	}
	key := fmt.Sprintf("%s/%s/%s", projectID, zone, diskName)
	return f.diskPermissions[key], nil
}

func (f *fakeIAMService) CheckIAMPermissionsOnInstance(ctx context.Context, projectID, zone, instanceName string, permissions []string) ([]string, error) {
	if f.instancePermissionErr != nil {
		return nil, f.instancePermissionErr
	}
	key := fmt.Sprintf("%s/%s/%s", projectID, zone, instanceName)
	return f.instancePermissions[key], nil
}

func (f *fakeIAMService) CheckIAMPermissionsOnSecret(ctx context.Context, projectID, secretName string, permissions []string) ([]string, error) {
	if f.secretPermissionErr != nil {
		return nil, f.secretPermissionErr
	}
	key := fmt.Sprintf("%s/%s", projectID, secretName)
	return f.secretPermissions[key], nil
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
	oldServiceUsageNewService := serviceUsageNewService
	t.Cleanup(func() { serviceUsageNewService = oldServiceUsageNewService })
	serviceUsageNewService = func(ctx context.Context) (*serviceusage.Service, error) {
		return nil, errors.New("credentials: could not find default credentials. See https://cloud.google.com/docs/authentication/external/set-up-adc for more information")
	}
	oldNewGCEClient := newGCEClient
	newGCEClient = func(ctx context.Context) (gceInterface, error) {
		return &fakeGCEClient{
			getSecret: func(ctx context.Context, projectID, secretName string) (string, error) {
				return "password", nil
			},
		}, nil
	}
	t.Cleanup(func() { newGCEClient = oldNewGCEClient })
	oldOsOpen := osOpen
	t.Cleanup(func() { osOpen = oldOsOpen })
	osOpen = func(name string) (io.ReadCloser, error) {
		return nil, os.ErrNotExist
	}

	tests := []struct {
		name       string
		exec       commandlineexecutor.Execute
		arClient   statushelper.ARClientInterface
		iamClient  permissions.IAMService
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
			iamClient: &fakeIAMService{
				projectPermissions: map[string][]string{
					"test-project": []string{"secretmanager.versions.access"},
				},
			},
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
				Services: []*spb.ServiceStatus{
					{
						Name:            "Secret Manager",
						State:           spb.State_SUCCESS_STATE,
						FullyFunctional: spb.State_SUCCESS_STATE,
						IamPermissions: []*spb.IAMPermission{
							{Name: "secretmanager.versions.access", Granted: spb.State_SUCCESS_STATE},
						},
					},
					{
						Name:            "Workload Manager API",
						State:           spb.State_FAILURE_STATE,
						ErrorMessage:    "failed to create Service Usage client",
						FullyFunctional: spb.State_FAILURE_STATE,
					},
				},
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
			iamClient: &fakeIAMService{},
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
				Services: []*spb.ServiceStatus{
					{
						Name:            "Secret Manager",
						State:           spb.State_SUCCESS_STATE,
						FullyFunctional: spb.State_SUCCESS_STATE,
						IamPermissions: []*spb.IAMPermission{
							{Name: "secretmanager.versions.access", Granted: spb.State_FAILURE_STATE},
						},
					},
					{
						Name:            "Workload Manager API",
						State:           spb.State_FAILURE_STATE,
						ErrorMessage:    "failed to create Service Usage client",
						FullyFunctional: spb.State_FAILURE_STATE,
					},
				},
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
			iamClient: &fakeIAMService{},
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
				Services: []*spb.ServiceStatus{
					{
						Name:            "Secret Manager",
						State:           spb.State_SUCCESS_STATE,
						FullyFunctional: spb.State_SUCCESS_STATE,
						IamPermissions: []*spb.IAMPermission{
							{Name: "secretmanager.versions.access", Granted: spb.State_FAILURE_STATE},
						},
					},
					{
						Name:            "Workload Manager API",
						State:           spb.State_FAILURE_STATE,
						ErrorMessage:    "failed to create Service Usage client",
						FullyFunctional: spb.State_FAILURE_STATE,
					},
				},
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
			iamClient:  &fakeIAMService{projectPermissionErr: errors.New("missing ProjectID in entityDetails")},
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
				Services: []*spb.ServiceStatus{
					{
						Name:            "Secret Manager",
						State:           spb.State_FAILURE_STATE,
						ErrorMessage:    "Cloud properties not available",
						FullyFunctional: spb.State_FAILURE_STATE,
					},
					{
						Name:            "Workload Manager API",
						State:           spb.State_FAILURE_STATE,
						ErrorMessage:    "Cloud properties not available",
						FullyFunctional: spb.State_FAILURE_STATE,
					},
				},
			},
		},
		{
			name: "IAMError",
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
			iamClient: &fakeIAMService{
				projectPermissionErr: errors.New("IAM check failed"),
			},
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
				Services: []*spb.ServiceStatus{
					{
						Name:            "Secret Manager",
						State:           spb.State_FAILURE_STATE,
						ErrorMessage:    "failed to check permissions for service SECRET_MANAGER on entity Project: IAM check failed",
						FullyFunctional: spb.State_FAILURE_STATE,
					},
					{
						Name:            "Workload Manager API",
						State:           spb.State_FAILURE_STATE,
						ErrorMessage:    "failed to create Service Usage client",
						FullyFunctional: spb.State_FAILURE_STATE,
					},
				},
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
			arClient:  newFakeARClient(nil, nil, errors.New("AR error")),
			iamClient: &fakeIAMService{},
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
				Services: []*spb.ServiceStatus{
					{
						Name:            "Secret Manager",
						State:           spb.State_SUCCESS_STATE,
						FullyFunctional: spb.State_SUCCESS_STATE,
						IamPermissions: []*spb.IAMPermission{
							{Name: "secretmanager.versions.access", Granted: spb.State_FAILURE_STATE},
						},
					},
					{
						Name:            "Workload Manager API",
						State:           spb.State_FAILURE_STATE,
						ErrorMessage:    "failed to create Service Usage client",
						FullyFunctional: spb.State_FAILURE_STATE,
					},
				},
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
			iamClient: &fakeIAMService{},
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
				Services: []*spb.ServiceStatus{
					{
						Name:            "Secret Manager",
						State:           spb.State_SUCCESS_STATE,
						FullyFunctional: spb.State_SUCCESS_STATE,
						IamPermissions: []*spb.IAMPermission{
							{Name: "secretmanager.versions.access", Granted: spb.State_FAILURE_STATE},
						},
					},
					{
						Name:            "Workload Manager API",
						State:           spb.State_FAILURE_STATE,
						ErrorMessage:    "failed to create Service Usage client",
						FullyFunctional: spb.State_FAILURE_STATE,
					},
				},
			},
		},
		{
			name: "KernelVersionError",
			exec: func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
				if strings.Contains(params.ArgsToSplit, "is-enabled") {
					return commandlineexecutor.Result{StdOut: "enabled", ExitCode: 0}
				}
				if strings.Contains(params.ArgsToSplit, "is-active") {
					return commandlineexecutor.Result{StdOut: "active", ExitCode: 0}
				}
				if params.Executable == "uname" {
					return commandlineexecutor.Result{StdErr: "uname failed", ExitCode: 1, Error: errors.New("uname failed")}
				}
				return commandlineexecutor.Result{}
			},
			arClient: newFakeARClient(
				[]*arpb.Package{{Name: "projects/workload-agent-products/locations/us/repositories/google-cloud-workload-agent-x86-64/packages/google-cloud-workload-agent"}},
				[]*arpb.Version{{Name: "1.2.3"}},
				nil,
			),
			iamClient: &fakeIAMService{},
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
				KernelVersion:                   nil,
				Services: []*spb.ServiceStatus{
					{
						Name:            "Secret Manager",
						State:           spb.State_SUCCESS_STATE,
						FullyFunctional: spb.State_SUCCESS_STATE,
						IamPermissions: []*spb.IAMPermission{
							{Name: "secretmanager.versions.access", Granted: spb.State_FAILURE_STATE},
						},
					},
					{
						Name:            "Workload Manager API",
						State:           spb.State_FAILURE_STATE,
						ErrorMessage:    "failed to create Service Usage client",
						FullyFunctional: spb.State_FAILURE_STATE,
					},
				},
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
			iamClient: &fakeIAMService{},
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
				Services: []*spb.ServiceStatus{
					{
						Name:            "Secret Manager",
						State:           spb.State_SUCCESS_STATE,
						FullyFunctional: spb.State_SUCCESS_STATE,
						IamPermissions: []*spb.IAMPermission{
							{Name: "secretmanager.versions.access", Granted: spb.State_FAILURE_STATE},
						},
					},
					{
						Name:            "Workload Manager API",
						State:           spb.State_FAILURE_STATE,
						ErrorMessage:    "failed to create Service Usage client",
						FullyFunctional: spb.State_FAILURE_STATE,
					},
				},
			},
		},
		{
			name: "PermissionsCheck_SecretManagerError",
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
			iamClient: &fakeIAMService{
				projectPermissions: map[string][]string{
					"test-project": []string{},
				},
			},
			readFile: func(string) ([]byte, error) {
				// Config with a secret configuration
				config := `{
					"redis_configuration": {
						"enabled": true,
						"connection_parameters": {
							"secret": {
								"project_id": "test-project",
								"secret_name": "test-secret"
							}
						}
					}
				}`
				return []byte(config), nil
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
				ConfigurationFilePath:           "/etc/google-cloud-workload-agent/configuration.json",
				ConfigurationValid:              spb.State_SUCCESS_STATE,
				InstanceUri:                     "projects/test-project/zones/test-zone/instances/test-instance",
				KernelVersion:                   &spb.KernelVersion{RawString: "5.10.0"},
				Services: []*spb.ServiceStatus{
					{
						Name:            "Secret Manager",
						State:           spb.State_SUCCESS_STATE,
						FullyFunctional: spb.State_SUCCESS_STATE,
						IamPermissions: []*spb.IAMPermission{
							{Name: "secretmanager.versions.access", Granted: spb.State_FAILURE_STATE},
						},
					},
					{
						Name:            "Workload Manager API",
						State:           spb.State_FAILURE_STATE,
						ErrorMessage:    "failed to create Service Usage client",
						FullyFunctional: spb.State_FAILURE_STATE,
					},
					{
						Name:         "redis",
						State:        spb.State_FAILURE_STATE,
						ErrorMessage: "could not read OS info: file does not exist",
					},
				},
			},
		},
		{
			name: "PermissionsCheck_FallbackProjectID",
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
			iamClient: &fakeIAMService{
				projectPermissions: map[string][]string{
					"fallback-project": []string{"secretmanager.versions.access"},
				},
			},
			readFile: func(string) ([]byte, error) {
				// Config with a secret configuration MISSING project_id
				config := `{
					"redis_configuration": {
						"enabled": true,
						"connection_parameters": {
							"secret": {
								"secret_name": "test-secret-no-project"
							}
						}
					}
				}`
				return []byte(config), nil
			},
			cloudProps: &cpb.CloudProperties{
				ProjectId:  "fallback-project",
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
				ConfigurationFilePath:           "/etc/google-cloud-workload-agent/configuration.json",
				ConfigurationValid:              spb.State_SUCCESS_STATE,
				InstanceUri:                     "projects/fallback-project/zones/test-zone/instances/test-instance",
				KernelVersion:                   &spb.KernelVersion{RawString: "5.10.0"},
				Services: []*spb.ServiceStatus{
					{
						Name:            "Secret Manager",
						State:           spb.State_SUCCESS_STATE,
						FullyFunctional: spb.State_SUCCESS_STATE,
						IamPermissions: []*spb.IAMPermission{
							{Name: "secretmanager.versions.access", Granted: spb.State_SUCCESS_STATE},
						},
					},
					{
						Name:            "Workload Manager API",
						State:           spb.State_FAILURE_STATE,
						ErrorMessage:    "failed to create Service Usage client",
						FullyFunctional: spb.State_FAILURE_STATE,
					},
					{
						Name:         "redis",
						State:        spb.State_FAILURE_STATE,
						ErrorMessage: "could not read OS info: file does not exist",
					},
				},
			},
		},
		{
			name: "DatabaseConnectivityEnabled",
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
			iamClient: &fakeIAMService{},
			readFile: func(string) ([]byte, error) {
				// Config with MySQL enabled
				config := `{
					"mysql_configuration": {
						"enabled": true
					}
				}`
				return []byte(config), nil
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
				ConfigurationFilePath:           "/etc/google-cloud-workload-agent/configuration.json",
				ConfigurationValid:              spb.State_SUCCESS_STATE,
				InstanceUri:                     "projects/test-project/zones/test-zone/instances/test-instance",
				KernelVersion:                   &spb.KernelVersion{RawString: "5.10.0"},
				Services: []*spb.ServiceStatus{
					{
						Name:            "Secret Manager",
						State:           spb.State_SUCCESS_STATE,
						FullyFunctional: spb.State_SUCCESS_STATE,
						IamPermissions: []*spb.IAMPermission{
							{Name: "secretmanager.versions.access", Granted: spb.State_FAILURE_STATE},
						},
					},
					{
						Name:            "Workload Manager API",
						State:           spb.State_FAILURE_STATE,
						ErrorMessage:    "failed to create Service Usage client",
						FullyFunctional: spb.State_FAILURE_STATE,
					},
					{
						Name:         "mysql",
						State:        spb.State_FAILURE_STATE,
						ErrorMessage: "failed to ping MySQL connection: dial tcp 127.0.0.1:3306: connect: connection refused",
					},
				},
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
			got := agentStatus(ctx, tc.arClient, tc.iamClient, tc.exec, tc.cloudProps, "", tc.readFile)
			got.ConfigurationErrorMessage = strings.ReplaceAll(got.ConfigurationErrorMessage, "\u00a0", " ")

			for _, s := range got.Services {
				if s.Name == "Workload Manager API" && s.ErrorMessage != "" {
					for _, w := range tc.want.Services {
						if w.Name == "Workload Manager API" && w.ErrorMessage != "" {
							s.ErrorMessage = w.ErrorMessage
							break
						}
					}
				}
			}

			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("agentStatus() returned unexpected diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAgentStatus_GCEClientError(t *testing.T) {
	oldNewGCEClient := newGCEClient
	newGCEClient = func(ctx context.Context) (gceInterface, error) {
		return nil, errors.New("gce client creation failed")
	}
	t.Cleanup(func() { newGCEClient = oldNewGCEClient })

	ctx := context.Background()
	arClient := newFakeARClient(nil, nil, nil)
	iamClient := &fakeIAMService{}
	exec := func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
		return commandlineexecutor.Result{}
	}
	cloudProps := &cpb.CloudProperties{
		ProjectId: "test-project",
		Zone:      "test-zone",
	}
	readFile := func(string) ([]byte, error) {
		return []byte(`{}`), nil
	}

	status := agentStatus(ctx, arClient, iamClient, exec, cloudProps, "", readFile)

	found := false
	for _, s := range status.Services {
		if s.Name == "GCE Connectivity" {
			found = true
			if s.State != spb.State_FAILURE_STATE {
				t.Errorf("GCE Connectivity state = %v, want %v", s.State, spb.State_FAILURE_STATE)
			}
			expectedMsg := "could not create GCE client"
			if s.ErrorMessage != expectedMsg {
				t.Errorf("GCE Connectivity error message = %q, want %q", s.ErrorMessage, expectedMsg)
			}
		}
	}
	if !found {
		t.Error("GCE Connectivity service status not found")
	}
}

func TestCheckAndSetStatus(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		s := checkAndSetStatus("test-service", func() error {
			return nil
		})
		if s.State != spb.State_SUCCESS_STATE {
			t.Errorf("checkAndSetStatus success case failed: got state %v, want SUCCESS", s.State)
		}
		if s.FullyFunctional != spb.State_SUCCESS_STATE {
			t.Errorf("checkAndSetStatus success case failed: got functional %v, want SUCCESS", s.FullyFunctional)
		}
	})

	t.Run("Failure", func(t *testing.T) {
		s := checkAndSetStatus("test-service", func() error {
			return errors.New("test error")
		})
		if s.State != spb.State_FAILURE_STATE {
			t.Errorf("checkAndSetStatus failure case failed: got state %v, want ERROR", s.State)
		}
		if s.ErrorMessage != "test error" {
			t.Errorf("checkAndSetStatus failure case failed: got error message %q, want 'test error'", s.ErrorMessage)
		}
	})
}

func TestCheckIAMPermissions(t *testing.T) {
	ctx := context.Background()
	services := map[string]string{"SECRET_MANAGER": "Secret Manager"}
	cloudProps := &cpb.CloudProperties{ProjectId: "test-project"}

	tests := []struct {
		name            string
		iamClient       permissions.IAMService
		cloudProps      *cpb.CloudProperties
		permissionsYAML []byte
		want            []*spb.ServiceStatus
		wantErrMsg      string
	}{
		{
			name:            "NilCloudProps",
			iamClient:       &fakeIAMService{},
			cloudProps:      nil,
			permissionsYAML: iamPermissionsYAML,
			want: []*spb.ServiceStatus{
				{
					Name:            "Secret Manager",
					State:           spb.State_FAILURE_STATE,
					ErrorMessage:    "Cloud properties not available",
					FullyFunctional: spb.State_FAILURE_STATE,
				},
			},
		},
		{
			name:            "InvalidPermissionsYAML",
			iamClient:       &fakeIAMService{},
			cloudProps:      cloudProps,
			permissionsYAML: []byte("invalid"),
			want: []*spb.ServiceStatus{
				{
					Name:            "Secret Manager",
					State:           spb.State_FAILURE_STATE,
					FullyFunctional: spb.State_FAILURE_STATE,
				},
			},
			wantErrMsg: "IAM permission configuration error",
		},
		{
			name: "IAMCheckError",
			iamClient: &fakeIAMService{
				projectPermissionErr: errors.New("IAM check failed"),
			},
			cloudProps:      cloudProps,
			permissionsYAML: iamPermissionsYAML,
			want: []*spb.ServiceStatus{
				{
					Name:            "Secret Manager",
					State:           spb.State_FAILURE_STATE,
					ErrorMessage:    "failed to check permissions for service SECRET_MANAGER on entity Project: IAM check failed",
					FullyFunctional: spb.State_FAILURE_STATE,
				},
			},
		},
		{
			name: "SuccessPermissionGranted",
			iamClient: &fakeIAMService{
				projectPermissions: map[string][]string{
					"test-project": []string{"secretmanager.versions.access"},
				},
			},
			cloudProps:      cloudProps,
			permissionsYAML: iamPermissionsYAML,
			want: []*spb.ServiceStatus{
				{
					Name:            "Secret Manager",
					State:           spb.State_SUCCESS_STATE,
					FullyFunctional: spb.State_SUCCESS_STATE,
					IamPermissions: []*spb.IAMPermission{
						{Name: "secretmanager.versions.access", Granted: spb.State_SUCCESS_STATE},
					},
				},
			},
		},
		{
			name: "SuccessPermissionNotGranted",
			iamClient: &fakeIAMService{
				projectPermissions: map[string][]string{
					"test-project": []string{},
				},
			},
			cloudProps:      cloudProps,
			permissionsYAML: iamPermissionsYAML,
			want: []*spb.ServiceStatus{
				{
					Name:            "Secret Manager",
					State:           spb.State_SUCCESS_STATE,
					FullyFunctional: spb.State_SUCCESS_STATE,
					IamPermissions: []*spb.IAMPermission{
						{Name: "secretmanager.versions.access", Granted: spb.State_FAILURE_STATE},
					},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := checkIAMPermissions(ctx, tc.iamClient, tc.cloudProps, services, tc.permissionsYAML)
			if tc.wantErrMsg != "" {
				if diff := cmp.Diff(tc.want[0], got[0], protocmp.Transform(), protocmp.IgnoreFields(&spb.ServiceStatus{}, "error_message")); diff != "" || !strings.Contains(got[0].ErrorMessage, tc.wantErrMsg) {
					t.Errorf("checkIAMPermissions() returned unexpected diff (-want +got):\n%s", diff)
				}
			} else {
				if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
					t.Errorf("checkIAMPermissions() returned unexpected diff (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestCheckAPIEnablement(t *testing.T) {
	ctx := context.Background()
	apis := map[string]string{"workloadmanager.googleapis.com": "Workload Manager API"}
	cloudProps := &cpb.CloudProperties{ProjectId: "test-project"}

	tests := []struct {
		name       string
		handler    http.HandlerFunc
		want       []*spb.ServiceStatus
		wantErrMsg string
	}{
		{
			name: "APIEnabled",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if strings.Contains(r.URL.Path, "workloadmanager.googleapis.com") {
					fmt.Fprintln(w, `{"state": "ENABLED"}`)
					return
				}
				http.Error(w, "not found", http.StatusNotFound)
			},
			want: []*spb.ServiceStatus{
				{
					Name:            "Workload Manager API",
					State:           spb.State_SUCCESS_STATE,
					FullyFunctional: spb.State_SUCCESS_STATE,
				},
			},
		},
		{
			name: "APIDisabled",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if strings.Contains(r.URL.Path, "workloadmanager.googleapis.com") {
					fmt.Fprintln(w, `{"state": "DISABLED"}`)
					return
				}
				http.Error(w, "not found", http.StatusNotFound)
			},
			want: []*spb.ServiceStatus{
				{
					Name:            "Workload Manager API",
					State:           spb.State_FAILURE_STATE,
					ErrorMessage:    "API is not enabled",
					FullyFunctional: spb.State_FAILURE_STATE,
				},
			},
		},
		{
			name: "APIPermissionDenied",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "permission denied", http.StatusForbidden)
			},
			want: []*spb.ServiceStatus{
				{
					Name:            "Workload Manager API",
					State:           spb.State_FAILURE_STATE,
					FullyFunctional: spb.State_FAILURE_STATE,
				},
			},
			wantErrMsg: "permission denied or service not found",
		},
		{
			name: "APIOtherError",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "internal server error", http.StatusInternalServerError)
			},
			want: []*spb.ServiceStatus{
				{
					Name:            "Workload Manager API",
					State:           spb.State_FAILURE_STATE,
					FullyFunctional: spb.State_FAILURE_STATE,
				},
			},
			wantErrMsg: "failed to get service",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(tc.handler)
			defer server.Close()

			oldServiceUsageNewService := serviceUsageNewService
			t.Cleanup(func() { serviceUsageNewService = oldServiceUsageNewService })
			serviceUsageNewService = func(ctx context.Context) (*serviceusage.Service, error) {
				return serviceusage.NewService(ctx, option.WithHTTPClient(server.Client()), option.WithEndpoint(server.URL))
			}

			got := checkAPIEnablement(ctx, cloudProps, apis)
			if tc.wantErrMsg != "" {
				if diff := cmp.Diff(tc.want[0], got[0], protocmp.Transform(), protocmp.IgnoreFields(&spb.ServiceStatus{}, "error_message")); diff != "" || !strings.Contains(got[0].ErrorMessage, tc.wantErrMsg) {
					t.Errorf("checkAPIEnablement() returned unexpected diff (-want +got):\n%s\nErrorMessage: %s", diff, got[0].ErrorMessage)
				}
			} else {
				if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
					t.Errorf("checkAPIEnablement() returned unexpected diff (-want +got):\n%s", diff)
				}
			}
		})
	}
}

type fakeGCEClient struct {
	getSecret func(ctx context.Context, projectID, secretName string) (string, error)
}

func (f *fakeGCEClient) GetSecret(ctx context.Context, projectID, secretName string) (string, error) {
	if f.getSecret != nil {
		return f.getSecret(ctx, projectID, secretName)
	}
	return "", errors.New("GetSecret not implemented")
}

func TestCheckDatabaseConnectivity(t *testing.T) {
	oldOsOpen := osOpen
	t.Cleanup(func() { osOpen = oldOsOpen })
	osOpen = func(name string) (io.ReadCloser, error) {
		return nil, os.ErrNotExist
	}

	tests := []struct {
		name       string
		config     *cpb.Configuration
		gce        gceInterface
		wantStatus []*spb.ServiceStatus
	}{
		{
			name: "MySQLFailure",
			config: &cpb.Configuration{
				MysqlConfiguration: &cpb.MySQLConfiguration{
					Enabled: proto.Bool(true),
				},
			},
			gce: &fakeGCEClient{
				getSecret: func(ctx context.Context, projectID, secretName string) (string, error) {
					return "mysql-password", nil
				},
			},
			wantStatus: []*spb.ServiceStatus{
				{
					Name:         "mysql",
					State:        spb.State_FAILURE_STATE,
					ErrorMessage: "failed to ping MySQL connection: dial tcp 127.0.0.1:3306: connect: connection refused",
				},
			},
		},
		{
			name: "PostgresFailure",
			config: &cpb.Configuration{
				PostgresConfiguration: &cpb.PostgresConfiguration{
					Enabled: proto.Bool(true),
				},
			},
			gce: &fakeGCEClient{
				getSecret: func(ctx context.Context, projectID, secretName string) (string, error) {
					return "postgres-password", nil
				},
			},
			wantStatus: []*spb.ServiceStatus{
				{
					Name:         "postgres",
					State:        spb.State_FAILURE_STATE,
					ErrorMessage: "failed to ping Postgres connection: dial tcp [::1]:5432: connect: connection refused",
				},
			},
		},
		{
			name: "MongoDbFailure",
			config: &cpb.Configuration{
				MongoDbConfiguration: &cpb.MongoDBConfiguration{
					Enabled: proto.Bool(true),
				},
			},
			gce: &fakeGCEClient{
				getSecret: func(ctx context.Context, projectID, secretName string) (string, error) {
					return "mongodb-password", nil
				},
			},
			wantStatus: []*spb.ServiceStatus{
				{
					Name:         "mongodb",
					State:        spb.State_FAILURE_STATE,
					ErrorMessage: "failed to connect to MongoDB: error validating uri: username required if URI contains user info",
				},
			},
		},
		{
			name: "RedisFailure",
			config: &cpb.Configuration{
				RedisConfiguration: &cpb.RedisConfiguration{
					Enabled: proto.Bool(true),
				},
			},
			gce: &fakeGCEClient{
				getSecret: func(ctx context.Context, projectID, secretName string) (string, error) {
					return "redis-password", nil
				},
			},
			wantStatus: []*spb.ServiceStatus{
				{
					Name:         "redis",
					State:        spb.State_FAILURE_STATE,
					ErrorMessage: "could not read OS info: file does not exist",
				},
			},
		},
		{
			name: "SQLServerMissingSecret",
			config: &cpb.Configuration{
				SqlserverConfiguration: &cpb.SQLServerConfiguration{
					Enabled: proto.Bool(true),
					CredentialConfigurations: []*cpb.SQLServerConfiguration_CredentialConfiguration{
						{
							ConnectionParameters: []*cpb.ConnectionParameters{
								{
									Host:     "sql-1-vm",
									Port:     1433,
									Username: "test-user",
								},
							},
						},
					},
				},
			},
			gce: &fakeGCEClient{},
			wantStatus: []*spb.ServiceStatus{
				{
					Name:         "sqlserver",
					State:        spb.State_FAILURE_STATE,
					ErrorMessage: "invalid value for \"secret_name\" for host: sql-1-vm",
				},
			},
		},
		{
			name: "SQLServerMissingUsername",
			config: &cpb.Configuration{
				SqlserverConfiguration: &cpb.SQLServerConfiguration{
					Enabled: proto.Bool(true),
					CredentialConfigurations: []*cpb.SQLServerConfiguration_CredentialConfiguration{
						{
							ConnectionParameters: []*cpb.ConnectionParameters{
								{
									Host: "sql-1-vm",
									Port: 1433,
									Secret: &cpb.SecretRef{
										SecretName: "test-secret",
									},
								},
							},
						},
					},
				},
			},
			gce: &fakeGCEClient{},
			wantStatus: []*spb.ServiceStatus{
				{
					Name:         "sqlserver",
					State:        spb.State_FAILURE_STATE,
					ErrorMessage: "invalid value for \"user_name\" for host: sql-1-vm",
				},
			},
		},
		{
			name: "AllEnabled",
			config: &cpb.Configuration{
				MysqlConfiguration: &cpb.MySQLConfiguration{
					Enabled: proto.Bool(true),
				},
				PostgresConfiguration: &cpb.PostgresConfiguration{
					Enabled: proto.Bool(true),
				},
				MongoDbConfiguration: &cpb.MongoDBConfiguration{
					Enabled: proto.Bool(true),
				},
				RedisConfiguration: &cpb.RedisConfiguration{
					Enabled: proto.Bool(true),
				},
			},
			gce: &fakeGCEClient{
				getSecret: func(ctx context.Context, projectID, secretName string) (string, error) {
					return "password", nil
				},
			},
			wantStatus: []*spb.ServiceStatus{
				{
					Name:         "mysql",
					State:        spb.State_FAILURE_STATE,
					ErrorMessage: "failed to ping MySQL connection: dial tcp 127.0.0.1:3306: connect: connection refused",
				},
				{
					Name:         "postgres",
					State:        spb.State_FAILURE_STATE,
					ErrorMessage: "failed to ping Postgres connection: dial tcp [::1]:5432: connect: connection refused",
				},
				{
					Name:         "mongodb",
					State:        spb.State_FAILURE_STATE,
					ErrorMessage: "failed to connect to MongoDB: error validating uri: username required if URI contains user info",
				},
				{
					Name:         "redis",
					State:        spb.State_FAILURE_STATE,
					ErrorMessage: "could not read OS info: file does not exist",
				},
			},
		},
		{
			name: "RedisDisabled",
			config: &cpb.Configuration{
				RedisConfiguration: &cpb.RedisConfiguration{
					Enabled: proto.Bool(false),
				},
			},
			gce:        &fakeGCEClient{},
			wantStatus: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			gotStatus := checkDatabaseConnectivity(ctx, tc.config, tc.gce, &cpb.CloudProperties{ProjectId: "test-project"})
			if diff := cmp.Diff(tc.wantStatus, gotStatus, protocmp.Transform()); diff != "" {
				t.Errorf("checkDatabaseConnectivity(%v) returned unexpected diff (-want +got):\n%s", tc.config, diff)
			}
		})
	}
}

func TestAgentStatus_ValidConfig(t *testing.T) {
	ctx := context.Background()
	arClient := newFakeARClient(nil, nil, nil)
	iamClient := &fakeIAMService{}
	exec := func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
		return commandlineexecutor.Result{}
	}
	cloudProps := &cpb.CloudProperties{
		ProjectId: "test-project",
		Zone:      "test-zone",
	}
	readFile := func(string) ([]byte, error) {
		return []byte(`{"mysql_configuration": {"enabled": true}}`), nil
	}

	status := agentStatus(ctx, arClient, iamClient, exec, cloudProps, "", readFile)

	if status.ConfigurationValid != spb.State_SUCCESS_STATE {
		t.Errorf("ConfigurationValid = %v, want %v", status.ConfigurationValid, spb.State_SUCCESS_STATE)
	}
	if status.ConfigurationErrorMessage != "" {
		t.Errorf("ConfigurationErrorMessage = %q, want empty", status.ConfigurationErrorMessage)
	}
}

func TestAgentStatus_ConfigurationValidity(t *testing.T) {
	ctx := context.Background()
	arClient := newFakeARClient(nil, nil, nil)
	iamClient := &fakeIAMService{}
	exec := func(ctx context.Context, params commandlineexecutor.Params) commandlineexecutor.Result {
		return commandlineexecutor.Result{}
	}
	cloudProps := &cpb.CloudProperties{
		ProjectId: "test-project",
		Zone:      "test-zone",
	}

	tests := []struct {
		name     string
		readFile func(string) ([]byte, error)
		want     spb.State
		errMsg   string
	}{
		{
			name: "InvalidConfig",
			readFile: func(string) ([]byte, error) {
				return []byte(`{invalid-json}`), nil
			},
			want:   spb.State_FAILURE_STATE,
			errMsg: "proto: syntax error",
		},
		{
			name: "ReadFileError",
			readFile: func(string) ([]byte, error) {
				return nil, errors.New("read error")
			},
			want:   spb.State_FAILURE_STATE,
			errMsg: "read error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			status := agentStatus(ctx, arClient, iamClient, exec, cloudProps, "", tc.readFile)
			status.ConfigurationErrorMessage = strings.ReplaceAll(status.ConfigurationErrorMessage, "\u00a0", " ")
			if status.ConfigurationValid != tc.want {
				t.Errorf("ConfigurationValid = %v, want %v", status.ConfigurationValid, tc.want)
			}
			if tc.errMsg != "" && !strings.Contains(status.ConfigurationErrorMessage, tc.errMsg) {
				t.Errorf("ConfigurationErrorMessage = %q, want containing %q", status.ConfigurationErrorMessage, tc.errMsg)
			}

			// Verify that no database connectivity checks were performed because the configuration is invalid.
			// Only Secret Manager and Workload Manager API services should be present (from IAM and API checks).
			for _, s := range status.Services {
				if s.Name == "mysql" || s.Name == "postgres" || s.Name == "mongodb" || s.Name == "redis" || s.Name == "sqlserver" {
					t.Errorf("Service %q found in status.Services, but database checks should be skipped for invalid configuration", s.Name)
				}
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
		{
			name: "NoHyphenZone",
			cp:   &cpb.CloudProperties{Zone: "uscentral1"},
			want: "us",
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
