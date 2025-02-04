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
	"github.com/GoogleCloudPlatform/workloadagentplatform/integration/common/shared/log"
)

// Service is used to perform data warehouse activation checks.
type Service struct {
	Config *cpb.Configuration
	Client workloadmanager.WLMWriter
}

// checkActivation checks if the data warehouse is activated by sending an empty insight.
// The data warehouse returns a 201 status code if it is activated.
func (s Service) checkActivation(ctx context.Context) (bool, error) {
	res, err := workloadmanager.SendDataInsight(ctx, workloadmanager.SendDataInsightParams{
		WLMetrics: workloadmanager.WorkloadMetrics{
			WorkloadType: workloadmanager.UNKNOWN,
			Metrics:      map[string]string{},
		},
		CloudProps: s.Config.GetCloudProperties(),
		WLMService: s.Client,
	})
	if err != nil {
		log.CtxLogger(ctx).Infow("Encountered error checking DW activation", "error", err)
		return false, err
	}
	if res == nil {
		log.CtxLogger(ctx).Warn("SendDataInsight did not return an error but the WriteInsight response is nil")
		return false, nil
	}
	log.CtxLogger(ctx).Debugw("WriteInsight response", "StatusCode", res.HTTPStatusCode)
	return res.HTTPStatusCode == 201, nil
}

func (s Service) dwActivationLoop(ctx context.Context) (servicecommunication.DataWarehouseActivationResult, error) {
	dwActivationStatus, err := s.checkActivation(ctx)
	if err != nil {
		return servicecommunication.DataWarehouseActivationResult{}, err
	}
	return servicecommunication.DataWarehouseActivationResult{Activated: dwActivationStatus}, nil
}

// DataWarehouseActivationCheck runs the data warehouse activation check periodically
// and publishes the result to the workload agent service channels.
func (s Service) DataWarehouseActivationCheck(ctx context.Context, a any) {
	log.CtxLogger(ctx).Info("DataWarehouseActivationCheck started")
	var chs []chan<- *servicecommunication.Message
	var ok bool
	if chs, ok = a.([]chan<- *servicecommunication.Message); !ok {
		log.CtxLogger(ctx).Warn("args is not of type chan servicecommunication.Message")
		return
	}
	frequency := 5 * time.Minute
	ticker := time.NewTicker(frequency)
	defer ticker.Stop()
	for {
		result, err := s.dwActivationLoop(ctx)
		if err != nil {
			log.CtxLogger(ctx).Errorw("Failed to perform data warehouse activation check", "error", err)
			return
		}
		log.CtxLogger(ctx).Infof("DataWarehouseActivationCheck found Data Warehouse Activation Status: %v", result.Activated)

		fullChs := 0
		msg := &servicecommunication.Message{Origin: servicecommunication.DWActivation, DWActivationResult: result}
		for _, ch := range chs {
			select {
			case ch <- msg:
			default:
				fullChs++
			}
		}
		if fullChs > 0 {
			log.CtxLogger(ctx).Infof("DataWarehouseActivationCheck found %d full channels that it was unable to write to.", fullChs)
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
