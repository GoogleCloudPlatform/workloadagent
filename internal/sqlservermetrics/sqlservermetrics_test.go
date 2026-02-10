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
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

type fakeGCEClient struct {
	getSecret func(ctx context.Context, projectID, secretName string) (string, error)
}

func (f *fakeGCEClient) GetSecret(ctx context.Context, projectID, secretName string) (string, error) {
	if f.getSecret != nil {
		return f.getSecret(ctx, projectID, secretName)
	}
	return "", errors.New("GetSecret not implemented")
}

func TestPingDB(t *testing.T) {
	tests := []struct {
		name       string
		config     *configpb.SQLServerConfiguration
		gce        GCEInterface
		cloudProps *configpb.CloudProperties
		wantErr    bool
		wantErrMsg string
	}{
		{
			name: "PingDBConnectionFailure",
			config: &configpb.SQLServerConfiguration{
				CredentialConfigurations: []*configpb.SQLServerConfiguration_CredentialConfiguration{
					{
						ConnectionParameters: []*configpb.ConnectionParameters{
							{
								Host:     "localhost",
								Username: "user",
								Port:     1433,
								Secret: &configpb.SecretRef{
									SecretName: "secret",
								},
							},
						},
					},
				},
			},
			gce: &fakeGCEClient{
				getSecret: func(ctx context.Context, projectID, secretName string) (string, error) {
					return "password", nil
				},
			},
			cloudProps: &configpb.CloudProperties{
				ProjectId: "test-project",
			},
			wantErr:    true,
			wantErrMsg: "failed to ping host",
		},
		{
			name: "GetSecretFailure",
			config: &configpb.SQLServerConfiguration{
				CredentialConfigurations: []*configpb.SQLServerConfiguration_CredentialConfiguration{
					{
						ConnectionParameters: []*configpb.ConnectionParameters{
							{
								Host:     "localhost",
								Username: "user",
								Port:     1433,
								Secret: &configpb.SecretRef{
									SecretName: "secret",
								},
							},
						},
					},
				},
			},
			gce: &fakeGCEClient{
				getSecret: func(ctx context.Context, projectID, secretName string) (string, error) {
					return "", errors.New("secret error")
				},
			},
			cloudProps: &configpb.CloudProperties{
				ProjectId: "test-project",
			},
			wantErr:    true,
			wantErrMsg: "failed to get secret",
		},
		{
			name: "SuccessWithExplicitProjectID",
			config: &configpb.SQLServerConfiguration{
				CredentialConfigurations: []*configpb.SQLServerConfiguration_CredentialConfiguration{
					{
						ConnectionParameters: []*configpb.ConnectionParameters{
							{
								Host:     "localhost",
								Username: "user",
								Port:     1433,
								Secret: &configpb.SecretRef{
									ProjectId:  "explicit-project",
									SecretName: "secret",
								},
							},
						},
					},
				},
			},
			gce: &fakeGCEClient{
				getSecret: func(ctx context.Context, projectID, secretName string) (string, error) {
					if projectID != "explicit-project" {
						return "", fmt.Errorf("got projectID %q, want %q", projectID, "explicit-project")
					}
					return "password", nil
				},
			},
			cloudProps: &configpb.CloudProperties{
				ProjectId: "fallback-project",
			},
			wantErr:    true, // Fails on actual Ping because localhost is not listening
			wantErrMsg: "failed to ping host",
		},
		{
			name: "SuccessWithFallbackProjectID",
			config: &configpb.SQLServerConfiguration{
				CredentialConfigurations: []*configpb.SQLServerConfiguration_CredentialConfiguration{
					{
						ConnectionParameters: []*configpb.ConnectionParameters{
							{
								Host:     "localhost",
								Username: "user",
								Port:     1433,
								Secret: &configpb.SecretRef{
									SecretName: "secret",
								},
							},
						},
					},
				},
			},
			gce: &fakeGCEClient{
				getSecret: func(ctx context.Context, projectID, secretName string) (string, error) {
					if projectID != "fallback-project" {
						return "", fmt.Errorf("got projectID %q, want %q", projectID, "fallback-project")
					}
					return "password", nil
				},
			},
			cloudProps: &configpb.CloudProperties{
				ProjectId: "fallback-project",
			},
			wantErr:    true, // Fails on actual Ping because localhost is not listening
			wantErrMsg: "failed to ping host",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &SQLServerMetrics{Config: tc.config}
			err := s.PingDB(context.Background(), tc.gce, tc.cloudProps)
			if (err != nil) != tc.wantErr {
				t.Errorf("PingDB() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr && tc.wantErrMsg != "" {
				if err == nil {
					t.Errorf("PingDB() expected error containing %q, got nil", tc.wantErrMsg)
				} else if !strings.Contains(err.Error(), tc.wantErrMsg) {
					t.Errorf("PingDB() error = %v, want error containing %q", err, tc.wantErrMsg)
				}
			}
		})
	}
}
