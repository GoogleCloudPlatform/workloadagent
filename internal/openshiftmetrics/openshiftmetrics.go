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
	"reflect"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"google.golang.org/protobuf/encoding/protojson"
	"github.com/GoogleCloudPlatform/workloadagent/internal/openshiftmetrics/clients/openshift"
	"github.com/GoogleCloudPlatform/workloadagent/internal/usagemetrics"
	"github.com/GoogleCloudPlatform/workloadagent/internal/workloadmanager"
	"github.com/GoogleCloudPlatform/workloadagentplatform/sharedlibraries/log"

	tspb "google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	configpb "github.com/GoogleCloudPlatform/workloadagent/protos/configuration"
	ompb "github.com/GoogleCloudPlatform/workloadagent/protos/openshiftmetrics"
	dwpb "github.com/GoogleCloudPlatform/workloadagentplatform/sharedprotos/datawarehouse"
)

const (
	// WLMNamespace is the namespace that the WLM agent is installed in.
	WLMNamespace = "workloadmanager"
	// Gibibyte is the number of bytes in a GiB.
	Gibibyte = 1024 * 1024 * 1024
)

// OpenShiftMetrics contains variables and methods to collect metrics for OpenShift running on the current host.
type OpenShiftMetrics struct {
	WLMClient       workloadmanager.WLMWriter
	K8sClient       *kubernetes.Clientset
	OpenShiftClient *openshift.Client
	// APIExtensionsClient is the client for the API extensions group.
	APIExtensionsClient *apiextensions.Clientset
	// DynamicClient is the client for dynamic resources.
	DynamicClient dynamic.Interface
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

	apiExtensionsClient, err := apiextensions.NewForConfig(config)
	if err != nil {
		return err
	}
	o.APIExtensionsClient = apiExtensionsClient

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return err
	}
	o.DynamicClient = dynamicClient

	return nil
}

// CollectMetrics collects metrics about the Openshift cluster.
//
// This is the entry point for collecting all OCP metrics.
func (o *OpenShiftMetrics) CollectMetrics(ctx context.Context, versionData MetricVersioning) (*ompb.OpenshiftMetricsPayload, error) {
	// This is the base payload that will be sent to the WLM API.
	payload := &ompb.OpenshiftMetricsPayload{
		Version:       versionData.PayloadVersion,
		AgentVersion:  versionData.AgentVersion,
		ScanTimestamp: tspb.Now(),
	}
	logger := log.CtxLogger(ctx)
	logger.Debugw("Base metric payload", "payload", payload)

	// Modify payload with collected data in the following section. Failing to collect metrics should
	// not fail the entire collection process.
	if err := o.collectCusterVersionData(ctx, payload); err != nil {
		logger.Warnw("Failed to collect cluster version data", "error", err)
		usagemetrics.Error(usagemetrics.OpenShiftMetricCollectionFailure)
	}

	namespaces, err := o.collectNamespaceData(ctx, payload)
	if err != nil {
		logger.Warnw("Failed to collect namespace data", "error", err)
		usagemetrics.Error(usagemetrics.OpenShiftMetricCollectionFailure)
	}

	if err := o.collectDeploymentData(ctx, namespaces, payload); err != nil {
		logger.Warnw("Failed to collect deployment data", "error", err)
		usagemetrics.Error(usagemetrics.OpenShiftMetricCollectionFailure)
	}

	if err := o.collectPersistentVolumeClaims(ctx, namespaces, payload); err != nil {
		logger.Warnw("Failed to collect persistent volume claims data", "error", err)
		usagemetrics.Error(usagemetrics.OpenShiftMetricCollectionFailure)
	}

	if err := o.collectStorageClasses(ctx, payload); err != nil {
		logger.Warnw("Failed to collect storage classes data", "error", err)
		usagemetrics.Error(usagemetrics.OpenShiftMetricCollectionFailure)
	}

	// TODO: Clean this up once we have a better way to handle config map we need.
	if err := o.collectConfigMaps(ctx, []string{WLMNamespace}, payload); err != nil {
		logger.Warnw("Failed to collect config maps data", "error", err)
		usagemetrics.Error(usagemetrics.OpenShiftMetricCollectionFailure)
	}

	if err := o.collectCSIDrivers(ctx, payload); err != nil {
		logger.Warnw("Failed to collect CSI drivers data", "error", err)
		usagemetrics.Error(usagemetrics.OpenShiftMetricCollectionFailure)
	}

	if err := o.collectCloudCredentialConfig(ctx, payload); err != nil {
		logger.Warnw("Failed to collect cloud credential config data", "error", err)
		usagemetrics.Error(usagemetrics.OpenShiftMetricCollectionFailure)
	}

	if err := o.collectCustomResourceDefinitions(ctx, payload); err != nil {
		logger.Warnw("Failed to collect custom resource definitions data", "error", err)
		usagemetrics.Error(usagemetrics.OpenShiftMetricCollectionFailure)
	}

	if err := o.collectNodes(ctx, payload); err != nil {
		logger.Warnw("Failed to collect nodes data", "error", err)
		usagemetrics.Error(usagemetrics.OpenShiftMetricCollectionFailure)
	}

	if err := o.collectDaemonSets(ctx, namespaces, payload); err != nil {
		logger.Warnw("Failed to collect daemon sets data", "error", err)
	}

	if err := o.collectSecretProviderClasses(ctx, namespaces, payload); err != nil {
		logger.Warnw("Failed to collect secret provider classes data", "error", err)
	}

	if err := o.collectAPIServers(ctx, payload); err != nil {
		logger.Warnw("Failed to collect api servers data", "error", err)
	}

	if err := o.collectSecretStores(ctx, namespaces, payload); err != nil {
		logger.Warnw("Failed to collect secret stores data", "error", err)
	}

	if err := o.collectExternalSecrets(ctx, namespaces, payload); err != nil {
		logger.Warnw("Failed to collect external secrets data", "error", err)
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

	logger.Debugw("Openshift metrics payload size", "size (bytes)", len(string(jsonPayload)))

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
		log.CtxLogger(ctx).Warnw("Failed to get cluster version", "error", err)
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
func (o *OpenShiftMetrics) collectNamespaceData(ctx context.Context, payload *ompb.OpenshiftMetricsPayload) ([]string, error) {
	namespaces, err := o.K8sClient.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var namespaceList []*ompb.Namespace
	var retNamespaces []string
	for _, namespace := range namespaces.Items {
		retNamespaces = append(retNamespaces, namespace.Name)

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

	return retNamespaces, nil
}

// collectDeploymentData collects the deployment data from the cluster.
func (o *OpenShiftMetrics) collectDeploymentData(ctx context.Context, namespaces []string, payload *ompb.OpenshiftMetricsPayload) error {
	var deploymentList []*ompb.Deployment
	var kind, apiVersion, resourceVersion string

	for _, namespace := range namespaces {
		deployments, err := o.K8sClient.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return err
		}

		kind = deployments.Kind
		apiVersion = deployments.APIVersion
		resourceVersion = deployments.ResourceVersion

		for _, deployment := range deployments.Items {
			var containers []*ompb.Container
			var initContainers []*ompb.Container
			var volumes []*ompb.Volume
			var topologySpreadConstraints []*ompb.TopologySpreadConstraints
			var preferredDuringSchedulingIgnoredDuringExecution []*ompb.Affinity_PodAntiAffinity_PreferredDuringSchedulingIgnoredDuringExecution

			for _, container := range deployment.Spec.Template.Spec.Containers {
				var env []*ompb.Env
				var envFrom []*ompb.EnvFrom
				var volumeMounts []*ompb.VolumeMount
				for _, e := range container.Env {
					var valueFrom *ompb.Env_ValueFrom
					if e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil {
						valueFrom = &ompb.Env_ValueFrom{
							SecretKeyRef: &ompb.Env_ValueFrom_SecretKeyRef{
								Name: e.ValueFrom.SecretKeyRef.Name,
							},
						}
					}
					env = append(env, &ompb.Env{
						Name:      e.Name,
						Value:     e.Value,
						ValueFrom: valueFrom,
					})
				}
				for _, e := range container.EnvFrom {
					var secretRef *ompb.EnvFrom_SecretRef
					if e.SecretRef != nil {
						secretRef = &ompb.EnvFrom_SecretRef{
							Name: e.SecretRef.Name,
						}
					}
					envFrom = append(envFrom, &ompb.EnvFrom{
						SecretRef: secretRef,
					})
				}
				for _, v := range container.VolumeMounts {
					volumeMounts = append(volumeMounts, &ompb.VolumeMount{
						Name:      v.Name,
						MountPath: v.MountPath,
					})
				}
				containers = append(containers, &ompb.Container{
					Env:          env,
					EnvFrom:      envFrom,
					VolumeMounts: volumeMounts,
				})
			}

			for _, container := range deployment.Spec.Template.Spec.InitContainers {
				var env []*ompb.Env
				var envFrom []*ompb.EnvFrom
				var volumeMounts []*ompb.VolumeMount
				for _, e := range container.Env {
					var valueFrom *ompb.Env_ValueFrom
					if e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil {
						valueFrom = &ompb.Env_ValueFrom{
							SecretKeyRef: &ompb.Env_ValueFrom_SecretKeyRef{
								Name: e.ValueFrom.SecretKeyRef.Name,
							},
						}
					}
					env = append(env, &ompb.Env{
						ValueFrom: valueFrom,
					})
				}
				for _, e := range container.EnvFrom {
					var secretRef *ompb.EnvFrom_SecretRef
					if e.SecretRef != nil {
						secretRef = &ompb.EnvFrom_SecretRef{
							Name: e.SecretRef.Name,
						}
					}
					envFrom = append(envFrom, &ompb.EnvFrom{
						SecretRef: secretRef,
					})
				}
				for _, v := range container.VolumeMounts {
					volumeMounts = append(volumeMounts, &ompb.VolumeMount{
						Name:      v.Name,
						MountPath: v.MountPath,
					})
				}
				initContainers = append(initContainers, &ompb.Container{
					Env:          env,
					EnvFrom:      envFrom,
					VolumeMounts: volumeMounts,
				})
			}

			for _, volume := range deployment.Spec.Template.Spec.Volumes {
				var secret *ompb.Volume_Secret
				if volume.Secret != nil {
					secret = &ompb.Volume_Secret{
						SecretName: volume.Secret.SecretName,
					}
				}
				var projected *ompb.Volume_Projected
				if volume.Projected != nil {
					projected = &ompb.Volume_Projected{
						Sources: true,
					}
				}
				volumes = append(volumes, &ompb.Volume{
					Name:      volume.Name,
					Secret:    secret,
					Projected: projected,
				})
			}

			for _, constraint := range deployment.Spec.Template.Spec.TopologySpreadConstraints {
				topologySpreadConstraints = append(topologySpreadConstraints, &ompb.TopologySpreadConstraints{
					TopologyKey: constraint.TopologyKey,
				})
			}

			if deployment.Spec.Template.Spec.Affinity != nil && deployment.Spec.Template.Spec.Affinity.PodAntiAffinity != nil {
				for _, preferred := range deployment.Spec.Template.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
					preferredDuringSchedulingIgnoredDuringExecution = append(preferredDuringSchedulingIgnoredDuringExecution, &ompb.Affinity_PodAntiAffinity_PreferredDuringSchedulingIgnoredDuringExecution{
						PodAffinityTerm: &ompb.Affinity_PodAntiAffinity_PreferredDuringSchedulingIgnoredDuringExecution_PodAffinityTerm{
							TopologyKey: preferred.PodAffinityTerm.TopologyKey,
						},
					})
				}
			}

			deploymentList = append(deploymentList, &ompb.Deployment{
				Metadata: &ompb.ResourceMetadata{
					Name:              deployment.Name,
					Uid:               string(deployment.UID),
					ResourceVersion:   deployment.ResourceVersion,
					Generation:        deployment.Generation,
					CreationTimestamp: tspb.New(deployment.CreationTimestamp.Time),
					Labels:            deployment.Labels,
					Annotations:       deployment.Annotations,
					Namespace:         namespace,
				},
				Spec: &ompb.Deployment_Spec{
					PodTemplate: &ompb.PodTemplate{
						Metadata: &ompb.ResourceMetadata{
							Name:              deployment.Spec.Template.Name,
							Uid:               string(deployment.Spec.Template.UID),
							ResourceVersion:   deployment.Spec.Template.ResourceVersion,
							Generation:        deployment.Spec.Template.Generation,
							CreationTimestamp: tspb.New(deployment.Spec.Template.CreationTimestamp.Time),
							Labels:            deployment.Spec.Template.Labels,
							Annotations:       deployment.Spec.Template.Annotations,
						},
						Spec: &ompb.PodSpec{
							Containers:                containers,
							InitContainers:            initContainers,
							Volumes:                   volumes,
							TopologySpreadConstraints: topologySpreadConstraints,
							Affinity: &ompb.Affinity{
								PodAntiAffinity: &ompb.Affinity_PodAntiAffinity{
									PreferredDuringSchedulingIgnoredDuringExecution: preferredDuringSchedulingIgnoredDuringExecution,
								},
							},
						},
					},
				},
			})
		}
	}

	// Use the kind, API version, and resource version from the deployments in the last namespace we
	// query.
	payload.Deployments = &ompb.ResourceListContainer{
		Kind:           kind,
		ApiVersion:     apiVersion,
		Metadata:       &ompb.ResourceListContainer_Metadata{ResourceVersion: resourceVersion},
		ContainerItems: &ompb.ResourceListContainer_Deployments{Deployments: &ompb.DeploymentList{Items: deploymentList}},
	}

	return nil
}

// collectPersistentVolumeClaims collects the persistent volume claims from the cluster.
func (o *OpenShiftMetrics) collectPersistentVolumeClaims(ctx context.Context, namespaces []string, payload *ompb.OpenshiftMetricsPayload) error {
	var pvcList []*ompb.PersistentVolumeClaim
	var kind, apiVersion, resourceVersion string

	for _, namespace := range namespaces {
		pvcs, err := o.K8sClient.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return err
		}

		kind = pvcs.Kind
		apiVersion = pvcs.APIVersion
		resourceVersion = pvcs.ResourceVersion

		for _, pvc := range pvcs.Items {
			var specAccessModes []string
			for _, mode := range pvc.Spec.AccessModes {
				specAccessModes = append(specAccessModes, string(mode))
			}

			var statusAccessModes []string
			for _, mode := range pvc.Status.AccessModes {
				statusAccessModes = append(statusAccessModes, string(mode))
			}

			var volumeMode *string
			if pvc.Spec.VolumeMode != nil {
				vm := string(*pvc.Spec.VolumeMode)
				volumeMode = &vm
			}

			pvcProto := &ompb.PersistentVolumeClaim{
				Metadata: &ompb.ResourceMetadata{
					Name:              pvc.Name,
					Uid:               string(pvc.UID),
					ResourceVersion:   pvc.ResourceVersion,
					Generation:        pvc.Generation,
					CreationTimestamp: tspb.New(pvc.CreationTimestamp.Time),
					Labels:            pvc.Labels,
					Annotations:       pvc.Annotations,
					Namespace:         namespace,
				},
				Spec: &ompb.PersistentVolumeClaim_Spec{
					AccessModes:      specAccessModes,
					Resources:        &ompb.PersistentVolumeClaim_Resources{},
					VolumeName:       pvc.Spec.VolumeName,
					StorageClassName: pvc.Spec.StorageClassName,
					VolumeMode:       volumeMode,
				},
				Status: &ompb.PersistentVolumeClaim_Status{
					Phase:       string(pvc.Status.Phase),
					AccessModes: statusAccessModes,
				},
			}

			if pvc.Spec.Resources.Requests != nil {
				pvcProto.Spec.Resources.Requests = &ompb.PersistentVolumeClaim_Requests{
					Storage: pvc.Spec.Resources.Requests.Storage().Value() / Gibibyte,
				}
			}

			if pvc.Status.Capacity != nil {
				pvcProto.Status.Capacity = &ompb.PersistentVolumeClaim_Capacity{
					Storage: pvc.Status.Capacity.Storage().Value() / Gibibyte,
				}
			}
			pvcList = append(pvcList, pvcProto)
		}
	}

	// Use the kind, API version, and resource version from the persistent volume claim in the last
	// namespace we query.
	payload.PersistentVolumeClaims = &ompb.ResourceListContainer{
		Kind:           kind,
		ApiVersion:     apiVersion,
		Metadata:       &ompb.ResourceListContainer_Metadata{ResourceVersion: resourceVersion},
		ContainerItems: &ompb.ResourceListContainer_PersistentVolumeClaims{PersistentVolumeClaims: &ompb.PersistentVolumeClaimList{Items: pvcList}},
	}

	return nil
}

// collectStorageClasses collects the storage classes from the cluster.
func (o *OpenShiftMetrics) collectStorageClasses(ctx context.Context, payload *ompb.OpenshiftMetricsPayload) error {
	storageClasses, err := o.K8sClient.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	var storageClassList []*ompb.StorageClass
	for _, storageClass := range storageClasses.Items {
		var reclaimPolicy *string
		if storageClass.ReclaimPolicy != nil {
			rp := string(*storageClass.ReclaimPolicy)
			reclaimPolicy = &rp
		}

		var volumeBindingMode *string
		if storageClass.VolumeBindingMode != nil {
			vbm := string(*storageClass.VolumeBindingMode)
			volumeBindingMode = &vbm
		}

		storageClassList = append(storageClassList, &ompb.StorageClass{
			Metadata: &ompb.ResourceMetadata{
				Name:              storageClass.Name,
				Uid:               string(storageClass.UID),
				ResourceVersion:   storageClass.ResourceVersion,
				Generation:        storageClass.Generation,
				CreationTimestamp: tspb.New(storageClass.CreationTimestamp.Time),
				Labels:            storageClass.Labels,
				Annotations:       storageClass.Annotations,
			},
			Provisioner:          storageClass.Provisioner,
			Parameters:           storageClass.Parameters,
			ReclaimPolicy:        reclaimPolicy,
			AllowVolumeExpansion: storageClass.AllowVolumeExpansion,
			VolumeBindingMode:    volumeBindingMode,
		})
	}

	// Use the kind, API version, and resource version from the persistent volume claim in the last
	// namespace we query.
	payload.StorageClasses = &ompb.ResourceListContainer{
		Kind:           storageClasses.Kind,
		ApiVersion:     storageClasses.APIVersion,
		Metadata:       &ompb.ResourceListContainer_Metadata{ResourceVersion: storageClasses.ResourceVersion},
		ContainerItems: &ompb.ResourceListContainer_StorageClasses{StorageClasses: &ompb.StorageClassList{Items: storageClassList}},
	}

	return nil
}

// collectConfigMaps collects the config maps from the cluster.
func (o *OpenShiftMetrics) collectConfigMaps(ctx context.Context, namespaces []string, payload *ompb.OpenshiftMetricsPayload) error {
	var configMapList []*ompb.ConfigMap
	var kind, apiVersion, resourceVersion string

	for _, namespace := range namespaces {
		configMaps, err := o.K8sClient.CoreV1().ConfigMaps(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return err
		}

		kind = configMaps.Kind
		apiVersion = configMaps.APIVersion
		resourceVersion = configMaps.ResourceVersion

		for _, configMap := range configMaps.Items {
			configMapList = append(configMapList, &ompb.ConfigMap{
				Metadata: &ompb.ResourceMetadata{
					Name:              configMap.Name,
					Uid:               string(configMap.UID),
					ResourceVersion:   configMap.ResourceVersion,
					Generation:        configMap.Generation,
					CreationTimestamp: tspb.New(configMap.CreationTimestamp.Time),
					Labels:            configMap.Labels,
					Annotations:       configMap.Annotations,
					Namespace:         namespace,
				},
				Data: configMap.Data,
			})
		}
	}

	// Use the kind, API version, and resource version from the config map in the last namespace we
	// query.
	payload.ConfigMaps = &ompb.ResourceListContainer{
		Kind:           kind,
		ApiVersion:     apiVersion,
		Metadata:       &ompb.ResourceListContainer_Metadata{ResourceVersion: resourceVersion},
		ContainerItems: &ompb.ResourceListContainer_ConfigMaps{ConfigMaps: &ompb.ConfigMapList{Items: configMapList}},
	}

	return nil
}

// collectCSIDrivers collects the CSI drivers from the cluster.
func (o *OpenShiftMetrics) collectCSIDrivers(ctx context.Context, payload *ompb.OpenshiftMetricsPayload) error {
	csiDrivers, err := o.K8sClient.StorageV1().CSIDrivers().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	var csiDriverList []*ompb.CsiDriver
	for _, csiDriver := range csiDrivers.Items {
		var fsGroupPolicy *string
		if csiDriver.Spec.FSGroupPolicy != nil {
			policy := string(*csiDriver.Spec.FSGroupPolicy)
			fsGroupPolicy = &policy
		}

		var volumeLifecycleModes []string
		for _, mode := range csiDriver.Spec.VolumeLifecycleModes {
			volumeLifecycleModes = append(volumeLifecycleModes, string(mode))
		}

		csiDriverList = append(csiDriverList, &ompb.CsiDriver{
			Metadata: &ompb.ResourceMetadata{
				Name:              csiDriver.Name,
				Uid:               string(csiDriver.UID),
				ResourceVersion:   csiDriver.ResourceVersion,
				Generation:        csiDriver.Generation,
				CreationTimestamp: tspb.New(csiDriver.CreationTimestamp.Time),
				Labels:            csiDriver.Labels,
				Annotations:       csiDriver.Annotations,
			},
			Spec: &ompb.CsiDriver_Spec{
				AttachRequired:       csiDriver.Spec.AttachRequired,
				PodInfoOnMount:       csiDriver.Spec.PodInfoOnMount,
				VolumeLifecycleModes: volumeLifecycleModes,
				StorageCapacity:      csiDriver.Spec.StorageCapacity,
				FsGroupPolicy:        fsGroupPolicy,
				RequiresRepublish:    csiDriver.Spec.RequiresRepublish,
			},
		})
	}

	payload.CsiDrivers = &ompb.ResourceListContainer{
		Kind:           csiDrivers.Kind,
		ApiVersion:     csiDrivers.APIVersion,
		Metadata:       &ompb.ResourceListContainer_Metadata{ResourceVersion: csiDrivers.ResourceVersion},
		ContainerItems: &ompb.ResourceListContainer_CsiDrivers{CsiDrivers: &ompb.CsiDriverList{Items: csiDriverList}},
	}

	return nil
}

// collectCloudCredentialConfig collects the cloud credential config from the cluster.
func (o *OpenShiftMetrics) collectCloudCredentialConfig(ctx context.Context, payload *ompb.OpenshiftMetricsPayload) error {
	cloudCredentialConfig, err := o.OpenShiftClient.GetCloudCredentialConfig()
	if err != nil {
		log.CtxLogger(ctx).Warnw("Failed to get cloud credential config", "error", err)
		return err
	}
	payload.CloudCredentialConfig = &ompb.CloudCredentialConfig{
		Metadata: &ompb.ResourceMetadata{
			Name:              cloudCredentialConfig.Metadata.Name,
			Uid:               string(cloudCredentialConfig.Metadata.UID),
			ResourceVersion:   cloudCredentialConfig.Metadata.ResourceVersion,
			Generation:        int64(cloudCredentialConfig.Metadata.Generation),
			CreationTimestamp: tspb.New(cloudCredentialConfig.Metadata.CreationTimestamp),
		},
		Spec: &ompb.CloudCredentialConfig_Spec{
			CredentialsMode: cloudCredentialConfig.Spec.CredentialsMode,
		},
	}
	return nil
}

// collectCustomResourceDefinitions collects the custom resource definitions from the cluster.
func (o *OpenShiftMetrics) collectCustomResourceDefinitions(ctx context.Context, payload *ompb.OpenshiftMetricsPayload) error {
	customResourceDefinitions, err := o.APIExtensionsClient.ApiextensionsV1().CustomResourceDefinitions().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	var customResourceDefinitionList []*ompb.CustomResourceDefinition
	for _, customResourceDefinition := range customResourceDefinitions.Items {
		customResourceDefinitionList = append(customResourceDefinitionList, &ompb.CustomResourceDefinition{
			Metadata: &ompb.ResourceMetadata{
				Name: customResourceDefinition.Name,
			},
		})
	}

	payload.CustomResourceDefinitions = &ompb.ResourceListContainer{
		Kind:           customResourceDefinitions.Kind,
		ApiVersion:     customResourceDefinitions.APIVersion,
		Metadata:       &ompb.ResourceListContainer_Metadata{ResourceVersion: customResourceDefinitions.ResourceVersion},
		ContainerItems: &ompb.ResourceListContainer_CustomResourceDefinitions{CustomResourceDefinitions: &ompb.CustomResourceDefinitionList{Items: customResourceDefinitionList}},
	}

	return nil
}

// collectNodes collects the nodes from the cluster.
func (o *OpenShiftMetrics) collectNodes(ctx context.Context, payload *ompb.OpenshiftMetricsPayload) error {
	nodes, err := o.K8sClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	var nodeList []*ompb.Node
	for _, node := range nodes.Items {
		nodeList = append(nodeList, &ompb.Node{
			Metadata: &ompb.ResourceMetadata{
				Name:              node.Name,
				Uid:               string(node.UID),
				ResourceVersion:   node.ResourceVersion,
				Generation:        node.Generation,
				CreationTimestamp: tspb.New(node.CreationTimestamp.Time),
				Labels:            node.Labels,
				Annotations:       node.Annotations,
			},
		})
	}

	payload.Nodes = &ompb.ResourceListContainer{
		Kind:           nodes.Kind,
		ApiVersion:     nodes.APIVersion,
		Metadata:       &ompb.ResourceListContainer_Metadata{ResourceVersion: nodes.ResourceVersion},
		ContainerItems: &ompb.ResourceListContainer_Nodes{Nodes: &ompb.NodeList{Items: nodeList}},
	}

	return nil
}

// collectDaemonSets collects specific daemon sets from the cluster.
func (o *OpenShiftMetrics) collectDaemonSets(ctx context.Context, namespaces []string, payload *ompb.OpenshiftMetricsPayload) error {
	var daemonSetList []*ompb.DaemonSet
	var kind, apiVersion, resourceVersion string

	for _, ns := range namespaces {
		daemonSets, err := o.K8sClient.AppsV1().DaemonSets(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return err
		}

		kind = daemonSets.Kind
		apiVersion = daemonSets.APIVersion
		resourceVersion = daemonSets.ResourceVersion

		for _, ds := range daemonSets.Items {
			if ds.Name != "csi-secrets-store-provider-gcp" {
				continue
			}

			var containers []*ompb.Container
			for _, container := range ds.Spec.Template.Spec.Containers {
				var env []*ompb.Env
				for _, e := range container.Env {
					var valueFrom *ompb.Env_ValueFrom
					if e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil {
						valueFrom = &ompb.Env_ValueFrom{
							SecretKeyRef: &ompb.Env_ValueFrom_SecretKeyRef{
								Name: e.ValueFrom.SecretKeyRef.Name,
							},
						}
					}
					env = append(env, &ompb.Env{
						ValueFrom: valueFrom,
						Name:      e.Name,
						Value:     e.Value,
					})
				}
				containers = append(containers, &ompb.Container{
					Env: env,
				})
			}

			daemonSetList = append(daemonSetList, &ompb.DaemonSet{
				Metadata: &ompb.ResourceMetadata{
					Name:              ds.Name,
					Uid:               string(ds.UID),
					ResourceVersion:   ds.ResourceVersion,
					Generation:        ds.Generation,
					CreationTimestamp: tspb.New(ds.CreationTimestamp.Time),
					Labels:            ds.Labels,
					Annotations:       ds.Annotations,
					Namespace:         ns,
				},
				Spec: &ompb.DaemonSet_Spec{
					PodTemplate: &ompb.PodTemplate{
						Metadata: &ompb.ResourceMetadata{
							Name:              ds.Spec.Template.Name,
							CreationTimestamp: tspb.New(ds.Spec.Template.CreationTimestamp.Time),
							Labels:            ds.Spec.Template.Labels,
							Annotations:       ds.Spec.Template.Annotations,
						},
						Spec: &ompb.PodSpec{
							Containers: containers,
						},
					},
				},
			})
			break
		}
	}

	payload.DaemonSets = &ompb.ResourceListContainer{
		Kind:           kind,
		ApiVersion:     apiVersion,
		Metadata:       &ompb.ResourceListContainer_Metadata{ResourceVersion: resourceVersion},
		ContainerItems: &ompb.ResourceListContainer_DaemonSets{DaemonSets: &ompb.DaemonSetList{Items: daemonSetList}},
	}

	return nil
}

// collectSecretProviderClasses collects SecretProviderClass resources from the cluster.
func (o *OpenShiftMetrics) collectSecretProviderClasses(ctx context.Context, namespaces []string, payload *ompb.OpenshiftMetricsPayload) error {
	var spcList []*ompb.SecretProviderClass
	var kind, apiVersion, resourceVersion string

	gvr := schema.GroupVersionResource{Group: "secrets-store.csi.x-k8s.io", Version: "v1", Resource: "secretproviderclasses"}

	for _, ns := range namespaces {
		list, err := o.DynamicClient.Resource(gvr).Namespace(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			continue
		}

		kind = list.GetKind()
		apiVersion = list.GetAPIVersion()
		resourceVersion = list.GetResourceVersion()

		for _, item := range list.Items {
			var secretObjects []*ompb.SecretProviderClass_SecretObject

			so, found, err := unstructured.NestedFieldNoCopy(item.Object, "spec", "secretObjects")
			if err == nil && found {
				// Use reflection to count elements in the slice, ignoring specific element types
				// (e.g., []interface{} vs []map[string]interface{}). Assuming any one type could break if
				// the underlying type changes.
				val := reflect.ValueOf(so)
				if val.Kind() == reflect.Slice {
					for i := 0; i < val.Len(); i++ {
						secretObjects = append(secretObjects, &ompb.SecretProviderClass_SecretObject{})
					}
				} else {
					log.CtxLogger(ctx).Debugw("Unknown type for secretObjects", "type", fmt.Sprintf("%T", so))
				}
			}

			provider, _, _ := unstructured.NestedString(item.Object, "spec", "provider")

			spcList = append(spcList, &ompb.SecretProviderClass{
				Metadata: &ompb.ResourceMetadata{
					Name:              item.GetName(),
					Uid:               string(item.GetUID()),
					ResourceVersion:   item.GetResourceVersion(),
					Generation:        item.GetGeneration(),
					CreationTimestamp: tspb.New(item.GetCreationTimestamp().Time),
					Labels:            item.GetLabels(),
					Annotations:       item.GetAnnotations(),
					Namespace:         ns,
				},
				Spec: &ompb.SecretProviderClass_Spec{
					SecretObjects: secretObjects,
					Provider:      provider,
				},
			})
		}
	}

	payload.SecretProviderClasses = &ompb.ResourceListContainer{
		Kind:           kind,
		ApiVersion:     apiVersion,
		Metadata:       &ompb.ResourceListContainer_Metadata{ResourceVersion: resourceVersion},
		ContainerItems: &ompb.ResourceListContainer_SecretProviderClasses{SecretProviderClasses: &ompb.SecretProviderClassList{Items: spcList}},
	}

	return nil
}

// collectAPIServers collects APIServer resources from the cluster.
func (o *OpenShiftMetrics) collectAPIServers(ctx context.Context, payload *ompb.OpenshiftMetricsPayload) error {
	var apiServerList []*ompb.APIServer

	resp, err := o.OpenShiftClient.GetAPIServer()
	if err != nil {
		return err
	}

	apiServerList = append(apiServerList, &ompb.APIServer{
		Metadata: &ompb.ResourceMetadata{
			Name: resp.Metadata.Name,
			Uid:  resp.Metadata.UID,
		},
		Spec: &ompb.APIServer_Spec{
			Encryption: &ompb.APIServer_Encryption{
				Type: resp.Spec.Encryption.Type,
			},
		},
	})

	payload.ApiServers = &ompb.ResourceListContainer{
		Kind:           resp.Kind,
		ApiVersion:     resp.APIVersion,
		Metadata:       &ompb.ResourceListContainer_Metadata{ResourceVersion: resp.Metadata.ResourceVersion},
		ContainerItems: &ompb.ResourceListContainer_ApiServers{ApiServers: &ompb.APIServerList{Items: apiServerList}},
	}

	return nil
}

// collectSecretStores collects SecretStore resources from the cluster.
func (o *OpenShiftMetrics) collectSecretStores(ctx context.Context, namespaces []string, payload *ompb.OpenshiftMetricsPayload) error {
	var ssList []*ompb.SecretStore
	var kind, apiVersion, resourceVersion string

	gvr := schema.GroupVersionResource{Group: "external-secrets.io", Version: "v1", Resource: "secretstores"}

	for _, ns := range namespaces {
		list, err := o.DynamicClient.Resource(gvr).Namespace(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			continue
		}

		kind = list.GetKind()
		apiVersion = list.GetAPIVersion()
		resourceVersion = list.GetResourceVersion()

		for _, item := range list.Items {
			var gcpsm *ompb.SecretStore_GcpSm

			spec, found, err := unstructured.NestedMap(item.Object, "spec")
			if err == nil && found {
				if provider, ok := spec["provider"].(map[string]any); ok {
					if _, ok := provider["gcpsm"].(map[string]any); ok {
						gcpsm = &ompb.SecretStore_GcpSm{}
					}
				}
			}

			ssList = append(ssList, &ompb.SecretStore{
				Metadata: &ompb.ResourceMetadata{
					Name:              item.GetName(),
					Uid:               string(item.GetUID()),
					ResourceVersion:   item.GetResourceVersion(),
					Generation:        item.GetGeneration(),
					CreationTimestamp: tspb.New(item.GetCreationTimestamp().Time),
					Labels:            item.GetLabels(),
					Annotations:       item.GetAnnotations(),
					Namespace:         ns,
				},
				Spec: &ompb.SecretStore_Spec{
					Provider: &ompb.SecretStore_Provider{
						Gcpsm: gcpsm,
					},
				},
			})
		}
	}

	payload.SecretStores = &ompb.ResourceListContainer{
		Kind:           kind,
		ApiVersion:     apiVersion,
		Metadata:       &ompb.ResourceListContainer_Metadata{ResourceVersion: resourceVersion},
		ContainerItems: &ompb.ResourceListContainer_SecretStores{SecretStores: &ompb.SecretStoreList{Items: ssList}},
	}

	return nil
}

// collectExternalSecrets collects ExternalSecret resources from the cluster.
func (o *OpenShiftMetrics) collectExternalSecrets(ctx context.Context, namespaces []string, payload *ompb.OpenshiftMetricsPayload) error {
	var esList []*ompb.ExternalSecret
	var kind, apiVersion, resourceVersion string

	gvr := schema.GroupVersionResource{Group: "external-secrets.io", Version: "v1", Resource: "externalsecrets"}

	for _, ns := range namespaces {
		list, err := o.DynamicClient.Resource(gvr).Namespace(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			continue
		}

		kind = list.GetKind()
		apiVersion = list.GetAPIVersion()
		resourceVersion = list.GetResourceVersion()

		for _, item := range list.Items {
			var secretStoreRef *ompb.ExternalSecret_SecretStoreRef

			spec, found, err := unstructured.NestedMap(item.Object, "spec")
			if err == nil && found {
				if ref, ok := spec["secretStoreRef"].(map[string]any); ok {
					name, _ := ref["name"].(string)
					kind, _ := ref["kind"].(string)
					secretStoreRef = &ompb.ExternalSecret_SecretStoreRef{
						Name: name,
						Kind: kind,
					}
				}
			}

			esList = append(esList, &ompb.ExternalSecret{
				Metadata: &ompb.ResourceMetadata{
					Name:              item.GetName(),
					Uid:               string(item.GetUID()),
					ResourceVersion:   item.GetResourceVersion(),
					Generation:        item.GetGeneration(),
					CreationTimestamp: tspb.New(item.GetCreationTimestamp().Time),
					Labels:            item.GetLabels(),
					Annotations:       item.GetAnnotations(),
					Namespace:         ns,
				},
				Spec: &ompb.ExternalSecret_Spec{
					SecretStoreRef: secretStoreRef,
				},
			})
		}
	}

	payload.ExternalSecrets = &ompb.ResourceListContainer{
		Kind:           kind,
		ApiVersion:     apiVersion,
		Metadata:       &ompb.ResourceListContainer_Metadata{ResourceVersion: resourceVersion},
		ContainerItems: &ompb.ResourceListContainer_ExternalSecrets{ExternalSecrets: &ompb.ExternalSecretList{Items: esList}},
	}

	return nil
}
