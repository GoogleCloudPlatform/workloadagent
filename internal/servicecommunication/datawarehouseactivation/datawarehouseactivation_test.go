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
	"github.com/GoogleCloudPlatform/workloadagent/internal/servicecommunication"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
)

func TestDataWarehouseActivationCheck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan *servicecommunication.Message)
	defer close(ch)

	service := Service{}
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

func TestDwActivationLoop(t *testing.T) {
	ctx := context.Background()

	service := Service{}
	result, err := service.dwActivationLoop(ctx)
	if err != nil {
		t.Fatalf("dwActivationLoop() returned an unexpected error: %v", err)
	}
	if !cmp.Equal(result, servicecommunication.DataWarehouseActivationResult{Activated: true}) {
		t.Errorf("dwActivationLoop() = %v, want %v", result, servicecommunication.DataWarehouseActivationResult{Activated: true})
	}
}
