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

// Package datawarehouseactivation performs data warehouse activation operations for all services.
package datawarehouseactivation

import (
	"context"
	"time"

	"github.com/GoogleCloudPlatform/workloadagent/internal/servicecommunication"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	"github.com/GoogleCloudPlatform/workloadagent/internal/workloadmanager"
	cpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"
)

// Service is used to perform data warehouse activation checks.
type Service struct {
	Config *cpb.Configuration
	Client workloadmanager.WLMWriter
}

type activationStatus int

const (
	notYetChecked activationStatus = iota
	activated
	notActivated
)

var statusToString = map[activationStatus]string{
	notYetChecked: "not yet checked",
	activated:     "activated",
	notActivated:  "not activated",
}

func (s activationStatus) String() string {
	return statusToString[s]
}

// checkActivation checks if the data warehouse is activated by sending an empty insight.
// The data warehouse returns a 201 status code if it is activated.
func (s Service) checkActivation(ctx context.Context) bool {
	res, err := workloadmanager.SendDataInsight(ctx, workloadmanager.SendDataInsightParams{
		WLMetrics: workloadmanager.WorkloadMetrics{
			WorkloadType: workloadmanager.UNKNOWN,
			Metrics:      map[string]string{},
		},
		CloudProps: s.Config.GetCloudProperties(),
		WLMService: s.Client,
	})
	if err != nil {
		log.CtxLogger(ctx).Debugw("Context for DW Status being not activated.", "error", err)
		return false
	}
	if res == nil {
		return false
	}
	log.CtxLogger(ctx).Debugw("WriteInsight response", "StatusCode", res.HTTPStatusCode)
	return res.HTTPStatusCode == 201
}

func (s Service) dwActivationLoop(ctx context.Context, currentStatus activationStatus) (servicecommunication.DataWarehouseActivationResult, bool) {
	log.CtxLogger(ctx).Infow("dwActivationLoop started.", "Current status", currentStatus)
	dwActivationStatus := s.checkActivation(ctx)
	status := notActivated
	if dwActivationStatus {
		status = activated
	}
	statusChanged := (status != currentStatus)
	if statusChanged {
		// Only log the status at info level if there is new information.
		log.CtxLogger(ctx).Infow("Updated data warehouse activation status.", "Status", status)
	}
	return servicecommunication.DataWarehouseActivationResult{Activated: dwActivationStatus}, statusChanged
}

func (s Service) sendActivationResult(ctx context.Context, chs map[string]chan<- *servicecommunication.Message, result servicecommunication.DataWarehouseActivationResult) {
	var fullChs []string
	msg := &servicecommunication.Message{Origin: servicecommunication.DWActivation, DWActivationResult: result}
	for key, ch := range chs {
		select {
		case ch <- msg:
		default:
			fullChs = append(fullChs, key)
		}
	}
	if len(fullChs) > 0 {
		log.CtxLogger(ctx).Debugf("DataWarehouseActivationCheck found %d full channels that it was unable to write to. Service(s) with full channels: %v", len(fullChs), fullChs)
	}
}

// DataWarehouseActivationCheck runs the data warehouse activation check periodically
// and publishes the result to the workload agent service channels.
func (s Service) DataWarehouseActivationCheck(ctx context.Context, a any) {
	currentStatus := notYetChecked
	log.CtxLogger(ctx).Info("DataWarehouseActivationCheck started")
	var chs map[string]chan<- *servicecommunication.Message
	var ok bool
	if chs, ok = a.(map[string]chan<- *servicecommunication.Message); !ok {
		log.CtxLogger(ctx).Warn("args is not of type chan servicecommunication.Message")
		return
	}
	frequency := 5 * time.Minute
	ticker := time.NewTicker(frequency)
	defer ticker.Stop()
	for {
		result, statusChanged := s.dwActivationLoop(ctx, currentStatus)
		// If the status has changed, send the new information across the channels.
		if statusChanged {
			currentStatus = notActivated
			if result.Activated {
				currentStatus = activated
			}
			s.sendActivationResult(ctx, chs, result)
		}

		// We can stop checking if the data warehouse is activated once we know that it is.
		if result.Activated {
			return
		}
		select {
		case <-ctx.Done():
			log.CtxLogger(ctx).Info("DataWarehouseActivationCheck cancellation requested")
			return
		case <-ticker.C:
			log.CtxLogger(ctx).Debug("DataWarehouseActivationCheck ticker fired")
			continue
		}
	}
}

// ErrorCode returns the error code for DataWarehouseActivationCheck.
func (s Service) ErrorCode() int {
	return usagemetrics.DataWarehouseActivationServiceFailure
}

// ExpectedMinDuration returns the expected minimum duration for DataWarehouseActivationCheck.
// Used by the recovery handler to determine if the service ran long enough to be considered
// successful.
func (s Service) ExpectedMinDuration() time.Duration {
	return 0
}
