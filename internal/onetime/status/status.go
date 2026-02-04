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
	_ "embed"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/artifactregistry/apiv1"
	"github.com/spf13/cobra"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/serviceusage/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/configuration"
	"github.com/GoogleCloudPlatform/workloadagent/internal/mongodbmetrics"
	"github.com/GoogleCloudPlatform/workloadagent/internal/mysqlmetrics"
	"github.com/GoogleCloudPlatform/workloadagent/internal/oraclemetrics"
	"github.com/GoogleCloudPlatform/workloadagent/internal/postgresmetrics"
	"github.com/GoogleCloudPlatform/workloadagent/internal/redismetrics"
	"github.com/GoogleCloudPlatform/workloadagent/internal/sqlservermetrics/sqlcollector"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/commandlineexecutor"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/iam"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/osinfo"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/permissions"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/statushelper"

	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	spb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/status"
)

const (
	agentPackageName = "google-cloud-workload-agent"
	requiredScope    = "https://www.googleapis.com/auth/cloud-platform"
)

//go:embed iam-permissions.yaml
var iamPermissionsYAML []byte

var serviceUsageNewService = func(ctx context.Context) (*serviceusage.Service, error) {
	return serviceusage.NewService(ctx)
}

var newGCEClient = func(ctx context.Context) (gceInterface, error) {
	return gce.NewGCEClient(ctx)
}

// NewCommand creates a new status command.
func NewCommand(cloudProps *cpb.CloudProperties) *cobra.Command {
	var config string
	var compact bool
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Print the status of the agent",
		Long:  "Print the status of the agent, including version, service status, and configuration validity.",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			arClient, err := newARClient(ctx)
			if err != nil {
				return err
			}
			if c, ok := arClient.(*statushelper.ArtifactRegistryClient); ok {
				defer c.Client.Close()
			}

			iamClient, err := iam.NewIAMClient(ctx)
			if err != nil {
				return err
			}

			status := agentStatus(ctx, arClient, iamClient, commandlineexecutor.ExecuteCommand, cloudProps, config, os.ReadFile)
			statushelper.PrintStatus(ctx, status, compact)
			return nil
		},
	}
	cmd.Flags().StringVar(&config, "config", "", "Configuration path override")
	cmd.Flags().BoolVar(&compact, "compact", false, "Compact output")
	return cmd
}

// newARClient creates a new artifact registry client.
func newARClient(ctx context.Context) (statushelper.ARClientInterface, error) {
	arClient, err := artifactregistry.NewClient(ctx)
	if err != nil {
		log.CtxLogger(ctx).Errorw("Could not create artifact registry client", "error", err)
		return nil, err
	}
	return &statushelper.ArtifactRegistryClient{Client: arClient}, nil
}

// agentStatus returns the agent version, enabled/running, config path, and the
// configuration as parsed by the agent.
func agentStatus(ctx context.Context, arClient statushelper.ARClientInterface, iamClient permissions.IAMService, exec commandlineexecutor.Execute, cloudProps *cpb.CloudProperties, config string, readFile func(string) ([]byte, error)) *spb.AgentStatus {
	agentStatus := &spb.AgentStatus{
		AgentName:        agentPackageName,
		InstalledVersion: fmt.Sprintf("%s-%s", configuration.AgentVersion, configuration.AgentBuildChange),
	}

	var err error
	if cloudProps == nil {
		log.CtxLogger(ctx).Errorw("Could not fetch cloud properties from metadata server. This may be because the agent is not running on a GCE VM, or the metadata server is not reachable.")
		agentStatus.CloudApiAccessFullScopesGranted = spb.State_ERROR_STATE
		agentStatus.AvailableVersion = "Error: could not fetch latest version"
	} else {
		agentStatus.AvailableVersion, err = statushelper.LatestVersionArtifactRegistry(ctx, arClient, "workload-agent-products", getRepositoryLocation(cloudProps), "google-cloud-workload-agent-x86-64", agentPackageName)
		if err != nil {
			log.CtxLogger(ctx).Errorw("Could not fetch latest version", "error", err)
			agentStatus.AvailableVersion = "Error: could not fetch latest version"
		}
		if slices.Contains(cloudProps.GetScopes(), requiredScope) {
			agentStatus.CloudApiAccessFullScopesGranted = spb.State_SUCCESS_STATE
		} else {
			agentStatus.CloudApiAccessFullScopesGranted = spb.State_FAILURE_STATE
		}
		agentStatus.InstanceUri = fmt.Sprintf("projects/%s/zones/%s/instances/%s", cloudProps.GetProjectId(), cloudProps.GetZone(), cloudProps.GetInstanceId())
	}

	agentStatus.SystemdServiceEnabled = spb.State_FAILURE_STATE
	agentStatus.SystemdServiceRunning = spb.State_FAILURE_STATE
	enabled, running, err := statushelper.CheckAgentEnabledAndRunning(ctx, agentPackageName, runtime.GOOS, exec)
	if err != nil {
		log.CtxLogger(ctx).Errorw("Could not check agent enabled and running", "error", err)
		agentStatus.SystemdServiceEnabled = spb.State_ERROR_STATE
		agentStatus.SystemdServiceRunning = spb.State_ERROR_STATE
	} else {
		if enabled {
			agentStatus.SystemdServiceEnabled = spb.State_SUCCESS_STATE
		}
		if running {
			agentStatus.SystemdServiceRunning = spb.State_SUCCESS_STATE
		}
	}

	path := config
	if len(path) == 0 {
		switch runtime.GOOS {
		case "linux":
			path = configuration.LinuxConfigPath
		case "windows":
			path = configuration.WindowsConfigPath
		}
	}
	agentStatus.ConfigurationFilePath = path
	configProto := &cpb.Configuration{}
	validate := func() error {
		content, err := readFile(path)
		if err != nil {
			return err
		}
		return protojson.Unmarshal(content, configProto)
	}
	agentStatus.ConfigurationValid = spb.State_SUCCESS_STATE
	if err := validate(); err != nil {
		agentStatus.ConfigurationValid = spb.State_FAILURE_STATE
		agentStatus.ConfigurationErrorMessage = err.Error()
	}

	// Check IAM permissions for configured services
	iamServices := map[string]string{
		"SECRET_MANAGER": "Secret Manager",
	}
	agentStatus.Services = append(agentStatus.Services, checkIAMPermissions(ctx, iamClient, cloudProps, iamServices, iamPermissionsYAML)...)

	// Check if configured APIs are enabled
	apis := map[string]string{
		"workloadmanager.googleapis.com": "Workload Manager API",
	}
	agentStatus.Services = append(agentStatus.Services, checkAPIEnablement(ctx, cloudProps, apis)...)

	// Check database connectivity if the configuration is valid
	if agentStatus.GetConfigurationValid() == spb.State_SUCCESS_STATE {
		gceService, err := newGCEClient(ctx)
		if err != nil {
			log.CtxLogger(ctx).Errorw("Could not create GCE client, skipping database connectivity checks.", "error", err)
			agentStatus.Services = append(agentStatus.Services, &spb.ServiceStatus{
				Name:         "GCE Connectivity",
				State:        spb.State_FAILURE_STATE,
				ErrorMessage: fmt.Sprintf("Could not create GCE client: %v", err),
			})
		} else {
			checkDatabaseConnectivity(ctx, configProto, gceService, agentStatus, cloudProps)
		}
	}

	agentStatus.KernelVersion, err = statushelper.KernelVersion(ctx, runtime.GOOS, exec)
	if err != nil && runtime.GOOS == "linux" {
		log.CtxLogger(ctx).Errorw("Could not fetch kernel version", "error", err)
	}
	return agentStatus
}

// checkIAMPermissions checks the IAM permissions for the given services.
func checkIAMPermissions(ctx context.Context, iamClient permissions.IAMService, cloudProps *cpb.CloudProperties, services map[string]string, permissionsYAML []byte) []*spb.ServiceStatus {
	var statuses []*spb.ServiceStatus
	if cloudProps == nil {
		for _, displayName := range services {
			statuses = append(statuses, &spb.ServiceStatus{
				Name:            displayName,
				State:           spb.State_FAILURE_STATE,
				ErrorMessage:    "Cloud properties not available",
				FullyFunctional: spb.State_FAILURE_STATE,
			})
		}
		return statuses
	}

	checker, parseErr := permissions.NewPermissionsChecker(permissionsYAML)
	if parseErr != nil {
		log.CtxLogger(ctx).Errorw("Failed to parse IAM permissions", "error", parseErr)
		for _, displayName := range services {
			statuses = append(statuses, &spb.ServiceStatus{
				Name:            displayName,
				State:           spb.State_FAILURE_STATE,
				ErrorMessage:    fmt.Sprintf("IAM permission configuration error: %v", parseErr),
				FullyFunctional: spb.State_FAILURE_STATE,
			})
		}
		return statuses
	}

	for serviceName, displayName := range services {
		status := &spb.ServiceStatus{
			Name: displayName,
		}
		resource := &permissions.ResourceDetails{
			ProjectID: cloudProps.GetProjectId(),
		}
		grantedPermissions, err := checker.FetchServicePermissionsStatus(ctx, iamClient, serviceName, resource)
		if err != nil {
			log.CtxLogger(ctx).Warnw("Failed to check IAM permissions", "service", serviceName, "error", err)
			status.State = spb.State_FAILURE_STATE
			status.ErrorMessage = err.Error()
			status.FullyFunctional = spb.State_FAILURE_STATE
		} else {
			log.CtxLogger(ctx).Debugw("IAM Permissions", "service", serviceName, "permissions", grantedPermissions)
			status.State = spb.State_SUCCESS_STATE
			status.FullyFunctional = spb.State_SUCCESS_STATE
			for permission, granted := range grantedPermissions {
				state := spb.State_FAILURE_STATE
				if granted {
					state = spb.State_SUCCESS_STATE
				}
				status.IamPermissions = append(status.IamPermissions, &spb.IAMPermission{
					Name:    permission,
					Granted: state,
				})
			}
			// Sort permissions for deterministic output (map iteration is random)
			slices.SortFunc(status.IamPermissions, func(a, b *spb.IAMPermission) int {
				return strings.Compare(a.Name, b.Name)
			})
		}
		statuses = append(statuses, status)
	}
	// Sort statuses by name for deterministic output
	slices.SortFunc(statuses, func(a, b *spb.ServiceStatus) int {
		return strings.Compare(a.Name, b.Name)
	})
	return statuses
}

// checkAPIEnablement checks if the required APIs are enabled.
func checkAPIEnablement(ctx context.Context, cloudProps *cpb.CloudProperties, apis map[string]string) []*spb.ServiceStatus {
	var statuses []*spb.ServiceStatus
	if cloudProps == nil {
		for _, name := range apis {
			statuses = append(statuses, &spb.ServiceStatus{
				Name:            name,
				State:           spb.State_FAILURE_STATE,
				ErrorMessage:    "Cloud properties not available",
				FullyFunctional: spb.State_FAILURE_STATE,
			})
		}
		return statuses
	}
	usageService, err := serviceUsageNewService(ctx)
	if err != nil {
		log.CtxLogger(ctx).Errorw("Failed to create Service Usage client", "error", err)
		for _, name := range apis {
			statuses = append(statuses, &spb.ServiceStatus{
				Name:            name,
				State:           spb.State_FAILURE_STATE,
				ErrorMessage:    fmt.Sprintf("Failed to create Service Usage client: %v", err),
				FullyFunctional: spb.State_FAILURE_STATE,
			})
		}
		return statuses
	}

	for serviceName, displayName := range apis {
		status := &spb.ServiceStatus{
			Name: displayName,
		}
		// Construct the resource name for the service.
		name := fmt.Sprintf("projects/%s/services/%s", cloudProps.GetProjectId(), serviceName)

		// Get the service details.
		svc, err := usageService.Services.Get(name).Context(ctx).Do()
		if err != nil {
			// Handle potential errors, such as permission issues or the service not existing.
			if gErr, ok := err.(*googleapi.Error); ok && gErr.Code == 403 {
				err = fmt.Errorf("permission denied or service not found for %s: %v", serviceName, err)
			} else {
				err = fmt.Errorf("failed to get service %s: %v", serviceName, err)
			}
			log.CtxLogger(ctx).Warnw("Failed to check API status", "service", serviceName, "error", err)
			status.State = spb.State_FAILURE_STATE
			status.ErrorMessage = err.Error()
			status.FullyFunctional = spb.State_FAILURE_STATE
		} else {
			if svc.State == "ENABLED" {
				status.State = spb.State_SUCCESS_STATE
				status.FullyFunctional = spb.State_SUCCESS_STATE
			} else {
				status.State = spb.State_FAILURE_STATE
				status.ErrorMessage = "API is not enabled"
				status.FullyFunctional = spb.State_FAILURE_STATE
			}
		}
		statuses = append(statuses, status)
	}
	// Sort statuses by name for deterministic output
	slices.SortFunc(statuses, func(a, b *spb.ServiceStatus) int {
		return strings.Compare(a.Name, b.Name)
	})
	return statuses
}

type gceInterface interface {
	GetSecret(ctx context.Context, projectID, secretName string) (string, error)
}

func checkAndSetStatus(name string, checkFn func() error) *spb.ServiceStatus {
	s := &spb.ServiceStatus{Name: name}
	if err := checkFn(); err != nil {
		s.State = spb.State_ERROR_STATE
		s.ErrorMessage = err.Error()
	} else {
		s.State = spb.State_SUCCESS_STATE
		s.FullyFunctional = spb.State_SUCCESS_STATE
	}
	return s
}

func checkDatabaseConnectivity(ctx context.Context, cfg *cpb.Configuration, gceService gceInterface, status *spb.AgentStatus, cloudProps *cpb.CloudProperties) {
	// Database Connectivity Checks for MySQL
	if cfg.GetMysqlConfiguration().GetEnabled() {
		status.Services = append(status.Services, checkAndSetStatus("mysql", func() error {
			metrics := mysqlmetrics.New(ctx, cfg, nil, nil)
			return metrics.InitDB(ctx, gceService)
		}))
	}

	// Database Connectivity Checks for PostgreSQL
	if cfg.GetPostgresConfiguration().GetEnabled() {
		status.Services = append(status.Services, checkAndSetStatus("postgres", func() error {
			metrics := postgresmetrics.New(ctx, cfg, nil, nil)
			return metrics.InitDB(ctx, gceService)
		}))
	}

	// Database Connectivity Checks for MongoDB
	if cfg.GetMongoDbConfiguration().GetEnabled() {
		status.Services = append(status.Services, checkAndSetStatus("mongodb", func() error {
			metrics := mongodbmetrics.New(ctx, cfg, nil, nil, mongodbmetrics.DefaultRunCommand)
			return metrics.InitDB(ctx, gceService, 10*time.Second)
		}))
	}

	// Database Connectivity Checks for Redis
	if cfg.GetRedisConfiguration().GetEnabled() {
		status.Services = append(status.Services, checkAndSetStatus("redis", func() error {
			osOpenFile := func(f string) (io.ReadCloser, error) { return os.Open(f) }
			osData, err := osinfo.ReadData(ctx, osOpenFile, runtime.GOOS, osinfo.OSReleaseFilePath)
			if err != nil {
				return fmt.Errorf("could not read OS info: %w", err)
			}
			metrics := redismetrics.New(ctx, cfg, nil, osData)
			return metrics.InitDB(ctx, gceService)
		}))
	}

	// Database Connectivity Checks for Oracle
	if cfg.GetOracleConfiguration().GetEnabled() {
		status.Services = append(status.Services, checkAndSetStatus("oracle", func() error {
			metrics := oraclemetrics.MetricCollector{
				Config:     cfg,
				GCEService: gceService,
			}
			return metrics.PingTest(ctx)
		}))
	}

	// Database Connectivity Checks for SQL Server
	if cfg.GetSqlserverConfiguration().GetEnabled() {
		status.Services = append(status.Services, checkAndSetStatus("sqlserver", func() error {
			return sqlcollector.PingSQLServer(ctx, cfg.GetSqlserverConfiguration(), gceService, cloudProps)
		}))
	}
}

// getRepositoryLocation returns the repository location based on the cloud properties.
func getRepositoryLocation(cp *cpb.CloudProperties) string {
	if cp.GetZone() == "" {
		return "us"
	}
	zone := cp.GetZone()
	idx := strings.LastIndex(zone, "-")
	if idx == -1 {
		return "us"
	}
	region := zone[:idx]
	if strings.HasPrefix(region, "us-") {
		return "us"
	}
	if strings.HasPrefix(region, "europe-") {
		return "europe"
	}
	if strings.HasPrefix(region, "asia-") {
		return "asia"
	}
	return region
}
