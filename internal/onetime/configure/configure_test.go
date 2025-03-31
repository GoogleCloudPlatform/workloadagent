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

package configure

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/testing/protocmp"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/configuration"

	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

func TestLoadWAConfiguration(t *testing.T) {
	tests := []struct {
		name       string
		cloudProps *cpb.CloudProperties
		rf         configuration.ReadConfigFile
		lf         loadFunc
		want       *cpb.Configuration
		wantErr    error
	}{
		{
			name:       "Success",
			cloudProps: &cpb.CloudProperties{},
			rf:         func(string) ([]byte, error) { return []byte{}, nil },
			lf: func(string, configuration.ReadConfigFile, *cpb.CloudProperties) (*cpb.Configuration, error) {
				return &cpb.Configuration{}, nil
			},
			want: &cpb.Configuration{},
		},
		{
			name:       "LoadFailure",
			cloudProps: &cpb.CloudProperties{},
			rf:         func(string) ([]byte, error) { return []byte{}, nil },
			lf: func(string, configuration.ReadConfigFile, *cpb.CloudProperties) (*cpb.Configuration, error) {
				return nil, cmpopts.AnyError
			},
			wantErr: cmpopts.AnyError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, gotErr := loadWAConfiguration(tc.cloudProps, tc.rf, tc.lf)
			if !cmp.Equal(gotErr, tc.wantErr, cmpopts.EquateErrors()) {
				t.Errorf("LoadWAConfiguration(%v, %v, %v)=%v, want %v", tc.cloudProps, tc.rf, tc.lf, gotErr, tc.wantErr)
			}
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("LoadWAConfiguration(%v, %v, %v) returned an unexpected diff (-want +got): %v", tc.cloudProps, tc.rf, tc.lf, diff)
			}
		})
	}
}
