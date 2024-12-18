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

// Package wlm contains types and functions to interact with WorkloadManager cloud APIs.
package wlm

import (
	"context"
	"fmt"

	"google.golang.org/api/option"
	wlmngr "google.golang.org/api/workloadmanager/v1"
	"github.com/GoogleCloudPlatform/workloadagent/internal/daemon/configuration"
	"github.com/GoogleCloudPlatform/workloadagent/internal/sqlservermetrics/sqlserverutils"
)

const (
	basePath = "https://autopush-workloadmanager-datawarehouse.sandbox.googleapis.com/"
)

// WorkloadManagerService the interface of WLM.
type WorkloadManagerService interface {
	SendRequest(string) (*wlmngr.WriteInsightResponse, error)
	UpdateRequest(*wlmngr.WriteInsightRequest)
}

// WLM struct which contains workloadmanager service.
type WLM struct {
	wlmService *wlmngr.Service
	Request    *wlmngr.WriteInsightRequest
}

// NewWorkloadManager creates new WLM and it return non-nil error if any error was caught.
func NewWorkloadManager(ctx context.Context) (*WLM, error) {
	wlm, err := wlmngr.NewService(ctx, option.WithEndpoint(basePath))
	if err != nil {
		return nil, fmt.Errorf("%v error creating WLM client", err)
	}
	return &WLM{wlmService: wlm}, nil
}

// SendRequest sends request to wlmngr.
func (wlm *WLM) SendRequest(location string) (*wlmngr.WriteInsightResponse, error) {
	return wlm.wlmService.Projects.Locations.Insights.WriteInsight(location, wlm.Request).Do()
}

// UpdateRequest updates WLM request.
func (wlm *WLM) UpdateRequest(writeInsightRequest *wlmngr.WriteInsightRequest) {
	wlm.Request = writeInsightRequest
}

// InitializeSQLServerValidation initializes and returns SqlserverValidation.
func InitializeSQLServerValidation(projectID, instance string) *wlmngr.SqlserverValidation {
	return &wlmngr.SqlserverValidation{
		AgentVersion: configuration.AgentVersion,
		ProjectId:    projectID,
		Instance:     instance,
		ValidationDetails: []*wlmngr.SqlserverValidationValidationDetail{
			&wlmngr.SqlserverValidationValidationDetail{
				Type:    "SQLSERVER_VALIDATION_TYPE_UNSPECIFIED",
				Details: []*wlmngr.SqlserverValidationDetails{},
			},
		},
	}
}

// InitializeWriteInsightRequest initializes and returns WriteInsightRequest.
func InitializeWriteInsightRequest(sqlservervalidation *wlmngr.SqlserverValidation, instanceID string) *wlmngr.WriteInsightRequest {
	return &wlmngr.WriteInsightRequest{
		Insight: &wlmngr.Insight{
			SqlserverValidation: sqlservervalidation,
			InstanceId:          instanceID,
		},
	}
}

// UpdateValidationDetails update ValidationDetails in SqlserverValidation.
func UpdateValidationDetails(sqlservervalidation *wlmngr.SqlserverValidation, details []sqlserverutils.MetricDetails) *wlmngr.SqlserverValidation {
	sqlservervalidation.ValidationDetails = []*wlmngr.SqlserverValidationValidationDetail{}
	for _, detail := range details {
		d := []*wlmngr.SqlserverValidationDetails{}
		for _, f := range detail.Fields {
			d = append(d, &wlmngr.SqlserverValidationDetails{
				Fields: f,
			})
		}
		sqlservervalidation.ValidationDetails = append(sqlservervalidation.ValidationDetails, &wlmngr.SqlserverValidationValidationDetail{
			Type:    detail.Name,
			Details: d,
		})
	}
	return sqlservervalidation
}
