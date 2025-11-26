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
		logger.Warnw("Failed to collect cluster version data", "error", err)
	}

	namespaces, err := o.collectNamespaceData(ctx, payload)
	if err != nil {
		logger.Warnw("Failed to collect namespace data", "error", err)
	}

	if err := o.collectDeploymentData(ctx, namespaces, payload); err != nil {
		logger.Warnw("Failed to collect deployment data", "error", err)
	}

	if err := o.collectPersistentVolumeClaims(ctx, namespaces, payload); err != nil {
		logger.Warnw("Failed to collect persistent volume claims data", "error", err)
	}

	if err := o.collectStorageClasses(ctx, payload); err != nil {
		logger.Warnw("Failed to collect storage classes data", "error", err)
	}

	if err := o.collectConfigMaps(ctx, namespaces, payload); err != nil {
		logger.Warnw("Failed to collect config maps data", "error", err)
	}

	if err := o.collectCSIDrivers(ctx, payload); err != nil {
		logger.Warnw("Failed to collect CSI drivers data", "error", err)
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
				containers = append(containers, &ompb.Container{
					Env:     env,
					EnvFrom: envFrom,
				})
			}

			for _, container := range deployment.Spec.Template.Spec.InitContainers {
				var env []*ompb.Env
				var envFrom []*ompb.EnvFrom
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
				initContainers = append(initContainers, &ompb.Container{
					Env:     env,
					EnvFrom: envFrom,
				})
			}

			for _, volume := range deployment.Spec.Template.Spec.Volumes {
				var secret *ompb.Volume_Secret
				if volume.Secret != nil {
					secret = &ompb.Volume_Secret{
						SecretName: volume.Secret.SecretName,
					}
				}
				volumes = append(volumes, &ompb.Volume{
					Secret: secret,
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
					Storage: pvc.Spec.Resources.Requests.Storage().String(),
				}
			}

			if pvc.Status.Capacity != nil {
				pvcProto.Status.Capacity = &ompb.PersistentVolumeClaim_Capacity{
					Storage: pvc.Status.Capacity.Storage().String(),
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
