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
	"fmt"

	"google.golang.org/api/googleapi"
	"google.golang.org/api/workloadmanager/v1"
)

// MockWlmService mocks WorkloadManagerService for testing usage.
type MockWlmService struct {
	MockError    bool
	MockHTTPCode int
	Request      *workloadmanager.WriteInsightRequest
}

// SendRequest mock function.
func (m *MockWlmService) SendRequest(location string) (*workloadmanager.WriteInsightResponse, error) {
	if m.Request == nil {
		return nil, fmt.Errorf("any error")
	}
	err := fmt.Errorf("any error")
	if !m.MockError {
		err = nil
	}
	return &workloadmanager.WriteInsightResponse{
		ServerResponse: googleapi.ServerResponse{
			HTTPStatusCode: m.MockHTTPCode,
		},
	}, err

}

// UpdateRequest mock function.
func (m *MockWlmService) UpdateRequest(writeInsightRequest *workloadmanager.WriteInsightRequest) {
	m.Request = writeInsightRequest
}

// InitializeMockWriteInsightRequest mock function.
func (m *MockWlmService) InitializeMockWriteInsightRequest() *workloadmanager.WriteInsightRequest {
	return &workloadmanager.WriteInsightRequest{}
}
