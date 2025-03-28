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

package cliconfig

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/proto"

	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
)

// mockMarshaller implements Marshaller for testing.
type mockMarshaller struct {
	ReturnError error
	Called      bool
	InputProto  proto.Message
}

func (m *mockMarshaller) Marshal(msg proto.Message) ([]byte, error) {
	m.Called = true
	m.InputProto = proto.Clone(msg)
	if m.ReturnError != nil {
		return nil, m.ReturnError
	}
	// Return dummy data, not relevant for this error test
	return []byte("{}"), nil
}

func fakeWriteFileSuccess() WriteConfigFile {
	return func(name string, content []byte, perm os.FileMode) error {
		return nil
	}
}

func fakeWriteFileFail() WriteConfigFile {
	return func(name string, content []byte, perm os.FileMode) error {
		return cmpopts.AnyError
	}
}

func TestWriteFileErrors(t *testing.T) {
	tempPath := path.Join(t.TempDir(), "/configuration.json")
	cp := &cpb.CloudProperties{
		ProjectId:        "test-project",
		InstanceId:       "1234567890",
		Zone:             "us-central1-a",
		InstanceName:     "test-instance",
		Image:            "test-image",
		NumericProjectId: "1234567890",
		Region:           "us-central1",
		MachineType:      "test-machine-type",
	}
	cfg := &cpb.Configuration{
		CloudProperties: cp,
		OracleConfiguration: &cpb.OracleConfiguration{
			Enabled: proto.Bool(true),
		},
		SqlserverConfiguration: &cpb.SQLServerConfiguration{
			Enabled: proto.Bool(true),
		},
	}
	tests := []struct {
		name      string
		path      string
		cfg       *cpb.Configuration
		configure *Configure
		wantErr   error
	}{
		{
			name: "Success",
			path: tempPath,
			cfg:  cfg,
			configure: &Configure{
				Configuration: cfg,
				Path:          tempPath,
				marshaller:    &mockMarshaller{},
				fileWriter:    fakeWriteFileSuccess(),
			},
		},
		{
			name: "NilConfigFailure",
			path: tempPath,
			cfg:  nil,
			configure: &Configure{
				Configuration: nil,
				Path:          tempPath,
				marshaller:    &mockMarshaller{},
				fileWriter:    fakeWriteFileSuccess(),
			},
			wantErr: cmpopts.AnyError,
		},
		{
			name: "MarshalFailure",
			path: tempPath,
			cfg:  cfg,
			configure: &Configure{
				Configuration: cfg,
				Path:          tempPath,
				marshaller:    &mockMarshaller{ReturnError: cmpopts.AnyError},
				fileWriter:    fakeWriteFileSuccess(),
			},
			wantErr: cmpopts.AnyError,
		},
		{
			name: "WriteFileFailure",
			path: tempPath,
			cfg:  cfg,
			configure: &Configure{
				Configuration: cfg,
				Path:          tempPath,
				marshaller:    &mockMarshaller{},
				fileWriter:    fakeWriteFileFail(),
			},
			wantErr: cmpopts.AnyError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotErr := tc.configure.WriteFile(context.Background())
			if !cmp.Equal(gotErr, tc.wantErr, cmpopts.EquateErrors()) {
				t.Errorf("writeFile(%v, %v)=%v, want %v", tc.configure, tc.path, gotErr, tc.wantErr)
			}
		})
	}
}
