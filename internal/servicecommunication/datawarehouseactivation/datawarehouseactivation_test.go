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

package datawarehouseactivation

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/api/googleapi"
	"github.com/GoogleCloudPlatform/workloadagent/internal/servicecommunication"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/gce/wlm"
	dwpb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/datawarehouse"
)

type fakeWLMWriter struct {
	writeInsightResponse *wlm.WriteInsightResponse
	writeInsightError    error
}

func (f *fakeWLMWriter) WriteInsight(project, location string, writeInsightRequest *dwpb.WriteInsightRequest) error {
	return f.writeInsightError
}

func (f *fakeWLMWriter) WriteInsightAndGetResponse(project, location string, writeInsightRequest *dwpb.WriteInsightRequest) (*wlm.WriteInsightResponse, error) {
	return f.writeInsightResponse, f.writeInsightError
}

var ActivatedClient *fakeWLMWriter = &fakeWLMWriter{writeInsightResponse: &wlm.WriteInsightResponse{ServerResponse: googleapi.ServerResponse{HTTPStatusCode: 201}}}
var NilResponseClient *fakeWLMWriter = &fakeWLMWriter{writeInsightResponse: nil}

func TestDataWarehouseActivationCheck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan *servicecommunication.Message)
	defer close(ch)

	service := Service{}
	service.Client = ActivatedClient
	go service.DataWarehouseActivationCheck(ctx, []chan<- *servicecommunication.Message{ch})

	select {
	case <-time.After(10 * time.Second):
		t.Errorf("DataWarehouseActivationCheck did not publish a message within the timeout")
	case msg := <-ch:
		if msg.DWActivationResult.Activated != true {
			t.Errorf("DataWarehouseActivationCheck published message with DWStatus = %v, want true", msg.DWActivationResult.Activated)
		}
	}
}

func TestDataWarehouseActivationCheck_InvalidArgs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	service := Service{}
	service.DataWarehouseActivationCheck(ctx, "invalid")
}

func TestErrorCode(t *testing.T) {
	service := Service{}
	if service.ErrorCode() != usagemetrics.DataWarehouseActivationServiceFailure {
		t.Errorf("ErrorCode() = %v, want %v", service.ErrorCode(), usagemetrics.DataWarehouseActivationServiceFailure)
	}
}

func TestExpectedMinDuration(t *testing.T) {
	service := Service{}
	if service.ExpectedMinDuration() != 0 {
		t.Errorf("ExpectedMinDuration() = %v, want 0", service.ExpectedMinDuration())
	}
}

func TestDataWarehouseActivation(t *testing.T) {
	tests := []struct {
		name              string
		s                 *Service
		startingStatus    activationStatus
		want              servicecommunication.DataWarehouseActivationResult
		wantStatusChanged bool
	}{
		{
			name: "NilResponse",
			s: &Service{
				Client: NilResponseClient,
			},
			startingStatus:    notYetChecked,
			want:              servicecommunication.DataWarehouseActivationResult{Activated: false},
			wantStatusChanged: true,
		},
		{
			name: "Activated",
			s: &Service{
				Client: ActivatedClient,
			},
			startingStatus:    notYetChecked,
			want:              servicecommunication.DataWarehouseActivationResult{Activated: true},
			wantStatusChanged: true,
		},
		{
			name: "AlreadyActivated",
			s: &Service{
				Client: ActivatedClient,
			},
			startingStatus:    activated,
			want:              servicecommunication.DataWarehouseActivationResult{Activated: true},
			wantStatusChanged: false,
		},
		{
			name: "AlreadyNotActivated",
			s: &Service{
				Client: NilResponseClient,
			},
			startingStatus:    notActivated,
			want:              servicecommunication.DataWarehouseActivationResult{Activated: false},
			wantStatusChanged: false,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range tests {
		result, statusChanged := tc.s.dwActivationLoop(ctx, tc.startingStatus)
		if !cmp.Equal(result, tc.want) {
			t.Errorf("dwActivationLoop() = %v, want %v", result, tc.want)
		}
		if statusChanged != tc.wantStatusChanged {
			t.Errorf("dwActivationLoop() status changed = %v, want %v", statusChanged, tc.wantStatusChanged)
		}
	}
}
