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

// Package openshiftmetrics implements metric collection for the OpenShift workload agent service.
package openshiftmetrics

import (
	"context"
	"fmt"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"google.golang.org/protobuf/encoding/protojson"
	"github.com/GoogleCloudPlatform/workloadagent/internal/openshiftmetrics/clients/openshift"
	"github.com/GoogleCloudPlatform/workloadagent/internal/workloadmanager"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"

	tspb "google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	ompb "github.com/GoogleCloudPlatform/workloadagent/protos/openshiftmetrics"
	dwpb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/datawarehouse"
)

// OpenShiftMetrics contains variables and methods to collect metrics for OpenShift running on the current host.
type OpenShiftMetrics struct {
	WLMClient       workloadmanager.WLMWriter
	K8sClient       *kubernetes.Clientset
	OpenShiftClient *openshift.Client
}

// MetricVersioning contains the versioning information for the metric agent and payload.
type MetricVersioning struct {
	PayloadVersion string // The version of the metric payload.
	AgentVersion   string // The version of the agent that generated the metric payload.
}

// New initializes and returns the MetricCollector struct.
func New(ctx context.Context, config *configpb.Configuration, wlmClient workloadmanager.WLMWriter) *OpenShiftMetrics {
	return &OpenShiftMetrics{WLMClient: wlmClient}
}

// Init initializes the OpenShiftMetrics client and all dependencies.
func (o *OpenShiftMetrics) Init(ctx context.Context) error {
	// We make the assumption that the agent will be running within an OpenShift cluster so that no
	// additonal credentials have to be provided to call the K8 APIs.
	// TODO: Use credentials from config as an alternative. Not crucial for MVP.
	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	o.K8sClient = clientset

	// creates the openshift client
	// TODO: Add openshift/api to the K8 clientset.
	openshiftClient, err := openshift.New(config)
	if err != nil {
		return err
	}
	o.OpenShiftClient = openshiftClient

	return nil
}

// CollectMetrics collects metrics about the Openshift cluster.
//
// This is the entry point for collecting all OCP metrics.
func (o *OpenShiftMetrics) CollectMetrics(ctx context.Context, versionData MetricVersioning) (*ompb.OpenshiftMetricsPayload, error) {
	// This is the base payload that will be sent to the WLM API.
	payload := &ompb.OpenshiftMetricsPayload{
		Version:      versionData.PayloadVersion,
		AgentVersion: versionData.AgentVersion,
	}
	logger := log.CtxLogger(ctx)
	logger.Debugw("Base metric payload", "payload", payload)

	// Modify payload with collected data in the following section. Failing to collect metrics should
	// not fail the entire collection process.
	if err := o.collectCusterVersionData(ctx, payload); err != nil {
		logger.Errorw("Failed to collect cluster version data", "error", err)
	}
	if err := o.collectNamespaceData(ctx, payload); err != nil {
		logger.Errorw("Failed to collect namespace data", "error", err)
	}

	logger.Debugw("Metric payload after collection", "payload", payload)

	return payload, nil
}

// SendMetricsToWLM sends the metrics to the WLM API.
func (o *OpenShiftMetrics) SendMetricsToWLM(ctx context.Context, config *configpb.Configuration, payload *ompb.OpenshiftMetricsPayload) error {
	logger := log.CtxLogger(ctx)

	if payload.GetClusterId() == "" {
		return fmt.Errorf("cluster id is required")
	}

	// Convert the payload to a google.protobuf.Struct for sending to the WLM API.
	marshalOpts := protojson.MarshalOptions{
		UseProtoNames: true,
	}
	jsonPayload, err := marshalOpts.Marshal(payload)
	if err != nil {
		return err
	}
	validationDetails, err := jsonStringToStruct(string(jsonPayload))
	if err != nil {
		return err
	}

	writeInsightRequest := &dwpb.WriteInsightRequest{
		Insight: &dwpb.Insight{
			InstanceId: config.GetCloudProperties().GetInstanceId(),
			OpenShiftValidation: &dwpb.OpenShiftValidation{
				ClusterId:         payload.GetClusterId(),
				ValidationDetails: validationDetails,
			},
		},
	}
	logger.Debugw("Generated WriteInsightRequest", "writeInsightRequest", writeInsightRequest)

	resp, err := o.WLMClient.WriteInsightAndGetResponse(config.GetCloudProperties().GetProjectId(), config.GetCloudProperties().GetRegion(), writeInsightRequest)
	if err != nil {
		return err
	}
	logger.Debugw("WriteInsightAndGetResponse response", "response", resp)
	return nil
}

// collectCusterVersionData collects the cluster version data from the cluster.
func (o *OpenShiftMetrics) collectCusterVersionData(ctx context.Context, payload *ompb.OpenshiftMetricsPayload) error {
	clusterVersion, err := o.OpenShiftClient.GetClusterVersion()
	if err != nil {
		log.CtxLogger(ctx).Errorw("Failed to get cluster version", "error", err)
		return err
	}
	if len(clusterVersion.Items) == 0 {
		return fmt.Errorf("no cluster versions found")
	}
	clusterID := clusterVersion.Items[0].Spec.ClusterID
	payload.ClusterId = clusterID
	return nil
}

// collectNamespaceData collects the namespace data from the cluster.
func (o *OpenShiftMetrics) collectNamespaceData(ctx context.Context, payload *ompb.OpenshiftMetricsPayload) error {
	namespaces, err := o.K8sClient.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	var namespaceList []*ompb.Namespace
	for _, namespace := range namespaces.Items {
		namespaceList = append(namespaceList, &ompb.Namespace{
			Metadata: &ompb.ResourceMetadata{
				Name:              namespace.Name,
				Uid:               string(namespace.UID),
				ResourceVersion:   namespace.ResourceVersion,
				Generation:        namespace.Generation,
				CreationTimestamp: tspb.New(namespace.CreationTimestamp.Time),
				Labels:            namespace.Labels,
				Annotations:       namespace.Annotations,
			},
		})
	}

	payload.Namespaces = &ompb.ResourceListContainer{
		Kind:           namespaces.Kind,
		ApiVersion:     namespaces.APIVersion,
		Metadata:       &ompb.ResourceListContainer_Metadata{ResourceVersion: namespaces.ResourceVersion},
		ContainerItems: &ompb.ResourceListContainer_Namespaces{Namespaces: &ompb.NamespaceList{Items: namespaceList}},
	}

	return nil
}
