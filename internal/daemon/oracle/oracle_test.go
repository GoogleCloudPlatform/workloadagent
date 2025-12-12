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

package oracle

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce/metadataserver"
)

func TestConvertCloudProperties(t *testing.T) {
	tests := []struct {
		name string
		cp   *cpb.CloudProperties
		want *metadataserver.CloudProperties
	}{
		{
			name: "nil cloud properties",
			cp:   nil,
			want: nil,
		},
		{
			name: "non-nil cloud properties",
			cp: &cpb.CloudProperties{
				ProjectId:           "test-project",
				NumericProjectId:    "12345",
				InstanceId:          "test-instance",
				Zone:                "us-central1-a",
				InstanceName:        "test-instance-name",
				Image:               "test-image",
				MachineType:         "n1-standard-1",
				Region:              "us-central1",
				ServiceAccountEmail: "test-sa@google.com",
				Scopes:              []string{"scope1", "scope2"},
			},
			want: &metadataserver.CloudProperties{
				ProjectID:           "test-project",
				NumericProjectID:    "12345",
				InstanceID:          "test-instance",
				Zone:                "us-central1-a",
				InstanceName:        "test-instance-name",
				Image:               "test-image",
				MachineType:         "n1-standard-1",
				Region:              "us-central1",
				ServiceAccountEmail: "test-sa@google.com",
				Scopes:              []string{"scope1", "scope2"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := convertCloudProperties(tc.cp)
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("convertCloudProperties(%v) returned an unexpected diff (-want +got): %v", tc.cp, diff)
			}
		})
	}
}
