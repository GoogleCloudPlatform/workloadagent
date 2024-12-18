/*
Copyright 2023 Google LLC

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

package wlm

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/api/workloadmanager/v1"
	"google.golang.org/protobuf/testing/protocmp"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/configuration"
	"github.com/GoogleCloudPlatform/workloadagent/internal/sqlservermetrics/sqlserverutils"
)

func TestInitializeSQLServerValidation(t *testing.T) {
	mockProjectID := "testProjectID"
	mockInstance := "testInstance"
	want := &workloadmanager.SqlserverValidation{
		AgentVersion: configuration.AgentVersion,
		ProjectId:    mockProjectID,
		Instance:     mockInstance,
		ValidationDetails: []*workloadmanager.SqlserverValidationValidationDetail{
			&workloadmanager.SqlserverValidationValidationDetail{
				Type:    "SQLSERVER_VALIDATION_TYPE_UNSPECIFIED",
				Details: []*workloadmanager.SqlserverValidationDetails{},
			},
		},
	}
	got := InitializeSQLServerValidation(mockProjectID, mockInstance)
	if diff := cmp.Diff(got, want, protocmp.Transform()); diff != "" {
		t.Errorf("InitializeSQLServerValidation() returned wrong result (-got +want):\n%s", diff)
	}
}

func TestInitializeWriteInsightRequest(t *testing.T) {
	mockInstanceID := "testInstanceID"
	mockSqlserverValidation := &workloadmanager.SqlserverValidation{}
	want := &workloadmanager.WriteInsightRequest{
		Insight: &workloadmanager.Insight{
			SqlserverValidation: mockSqlserverValidation,
			InstanceId:          mockInstanceID,
		},
	}

	got := InitializeWriteInsightRequest(mockSqlserverValidation, mockInstanceID)
	if diff := cmp.Diff(got, want, protocmp.Transform()); diff != "" {
		t.Errorf("InitializeWriteInsightRequest() returned wrong result (-got +want):\n%s", diff)
	}
}

func TestUpdateValidationDetails(t *testing.T) {
	mockedSqlserverValidation := &workloadmanager.SqlserverValidation{}
	mockedDetails := []sqlserverutils.MetricDetails{
		{
			Name: "testDetailName",
			Fields: []map[string]string{
				map[string]string{
					"testField": "testValue",
				},
			},
		},
	}
	want := &workloadmanager.SqlserverValidation{
		ValidationDetails: []*workloadmanager.SqlserverValidationValidationDetail{
			{Type: "testDetailName",
				Details: []*workloadmanager.SqlserverValidationDetails{{Fields: map[string]string{"testField": "testValue"}}}},
		},
	}

	got := UpdateValidationDetails(mockedSqlserverValidation, mockedDetails)
	if diff := cmp.Diff(got, want); diff != "" {
		t.Errorf("UpdateValidationDetails() returned wrong result (-got +want):\n%s", diff)
	}
}

func TestUpdateRequest(t *testing.T) {
	w := WLM{}
	input := &workloadmanager.WriteInsightRequest{
		RequestId: "testRequestId",
	}
	w.UpdateRequest(input)
	if diff := cmp.Diff(input, w.Request); diff != "" {
		t.Errorf("UpdateRequest() returned wrong result (-got +want):\n%s", diff)
	}
}

func TestMockWLMService(t *testing.T) {
	w := MockWlmService{}
	if _, err := w.SendRequest(""); err == nil {
		t.Errorf("Mocked SendRequest() returned no error. Want an error to present")
	}
	w.UpdateRequest(w.InitializeMockWriteInsightRequest())
	if _, err := w.SendRequest(""); err != nil {
		t.Errorf("Mocked SendRequest() returned unexpected error: %v", err)
	}
}
