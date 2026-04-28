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

package openshiftmetrics

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/testing/protocmp"
	"github.com/GoogleCloudPlatform/workloadagent/internal/openshiftmetrics/clients/openshift"

	tspb "google.golang.org/protobuf/types/known/timestamppb"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensions "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sjson "k8s.io/apimachinery/pkg/runtime/serializer/json"
	k8sscheme "k8s.io/client-go/kubernetes/scheme"
	ompb "github.com/GoogleCloudPlatform/workloadagent/protos/openshiftmetrics"
)

type fakeTransport struct {
	t   *testing.T
	now time.Time
}

func (f *fakeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	f.t.Logf("fakeTransport received request: %s %s", req.Method, req.URL.String())

	// OpenShift specific endpoints
	if req.URL.Path == "/apis/config.openshift.io/v1/clusterversions" {
		return jsonResponse(`{"apiVersion": "config.openshift.io/v1", "items": [{"spec": {"clusterID": "fake-cluster-id"}}]}`)
	}
	if req.URL.Path == "/apis/operator.openshift.io/v1/cloudcredentials/cluster" {
		return jsonResponse(`{
			"apiVersion": "operator.openshift.io/v1",
			"kind": "CloudCredential",
			"metadata": {"creationTimestamp": "2025-05-14T06:33:06Z", "generation": 1, "name": "cluster", "resourceVersion": "508", "uid": "4f896355-45cd-403a-9953-2b4c9dab04e6"},
			"spec": {"credentialsMode": "Mint"}
		}`)
	}

	serializer := k8sjson.NewSerializerWithOptions(k8sjson.DefaultMetaFactory, k8sscheme.Scheme, k8sscheme.Scheme, k8sjson.SerializerOptions{Yaml: false})
	var obj runtime.Object

	switch req.URL.Path {
	case "/api/v1/namespaces":
		obj = &corev1.NamespaceList{
			Items: []corev1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "default", UID: "ns-uid", ResourceVersion: "1", CreationTimestamp: metav1.NewTime(f.now)}}},
		}
	case "/apis/apps/v1/namespaces/default/deployments":
		obj = &appsv1.DeploymentList{
			Items: []appsv1.Deployment{{
				ObjectMeta: metav1.ObjectMeta{Name: "test-dep", Namespace: "default", UID: "dep-uid", ResourceVersion: "2", CreationTimestamp: metav1.NewTime(f.now)},
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Name:         "main-container",
								Env:          []corev1.EnvVar{{Name: "GOOGLE_APPLICATION_CREDENTIALS", Value: "/var/run/secrets/wif/config.json"}},
								VolumeMounts: []corev1.VolumeMount{{Name: "test-volume", MountPath: "/var/lib/test"}, {Name: "wif-token", MountPath: "/var/run/secrets/wif"}},
							}},
							InitContainers: []corev1.Container{{Name: "init-container", VolumeMounts: []corev1.VolumeMount{{Name: "test-volume2", MountPath: "/usr/lib/test2"}}}},
							Volumes: []corev1.Volume{
								{Name: "test-volume"}, {Name: "test-volume2"},
								{Name: "wif-token", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token"}}}}}},
							},
						},
					},
				},
			}},
		}
	case "/apis/storage.k8s.io/v1/storageclasses":
		obj = &storagev1.StorageClassList{
			Items: []storagev1.StorageClass{{ObjectMeta: metav1.ObjectMeta{Name: "test-sc", UID: "sc-uid", ResourceVersion: "4", CreationTimestamp: metav1.NewTime(f.now)}, Provisioner: "test-provisioner"}},
		}
	case "/api/v1/namespaces/workloadmanager/configmaps":
		obj = &corev1.ConfigMapList{
			Items: []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{Name: "wlm-cluster-environment", Namespace: "workloadmanager", UID: "cm-uid-2", ResourceVersion: "5", CreationTimestamp: metav1.NewTime(f.now)}, Data: map[string]string{"environment": "production"}}},
		}
	case "/api/v1/namespaces/default/configmaps":
    obj = &corev1.ConfigMapList{
        Items: []corev1.ConfigMap{{
            ObjectMeta: metav1.ObjectMeta{Name: "test-cm", Namespace: "default", UID: "cm-uid", ResourceVersion: "5", CreationTimestamp: metav1.NewTime(f.now)},
            Data:       map[string]string{"key": "value"},
        }},
    }
	case "/apis/storage.k8s.io/v1/csidrivers":
		obj = &storagev1.CSIDriverList{
			Items: []storagev1.CSIDriver{{ObjectMeta: metav1.ObjectMeta{Name: "test-csi", UID: "csi-uid", ResourceVersion: "6", CreationTimestamp: metav1.NewTime(f.now)}}},
		}
	case "/apis/apiextensions.k8s.io/v1/customresourcedefinitions":
		obj = &apiextensionsv1.CustomResourceDefinitionList{
			Items: []apiextensionsv1.CustomResourceDefinition{{ObjectMeta: metav1.ObjectMeta{Name: "test-crd", UID: "crd-uid", ResourceVersion: "7", CreationTimestamp: metav1.NewTime(f.now)}}},
		}
	case "/api/v1/nodes":
		obj = &corev1.NodeList{
			Items: []corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "test-node", UID: "node-uid", ResourceVersion: "8", CreationTimestamp: metav1.NewTime(f.now)}}},
		}
	default:
		// Fallback for list requests that shouldn't return data in this test
		if strings.Contains(req.URL.Path, "/deployments") {
			obj = &appsv1.DeploymentList{}
		} else if strings.Contains(req.URL.Path, "/configmaps") {
			obj = &corev1.ConfigMapList{}
		} else {
			return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(bytes.NewReader([]byte{}))}, nil
		}
	}

	buf := new(bytes.Buffer)
	serializer.Encode(obj, buf)
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(buf)}, nil
}

func jsonResponse(body string) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}, nil
}

// --- Main Test Function ---

func TestCollectMetrics(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	nowProto := tspb.New(now)

	// Infrastructure Setup
	ft := &fakeTransport{t: t, now: now}
	cfg := &rest.Config{Host: "http://fake", Transport: ft}
	k8sClient, _ := kubernetes.NewForConfig(cfg)
	ocpClient, _ := openshift.New(cfg)
	apiExtClient, _ := apiextensions.NewForConfig(cfg)

	om := &OpenShiftMetrics{
		K8sClient:           k8sClient,
		OpenShiftClient:     ocpClient,
		APIExtensionsClient: apiExtClient,
	}

	// Single Action: Collect the payload
	payload, err := om.CollectMetrics(ctx, MetricVersioning{PayloadVersion: "1.0", AgentVersion: "2.0"})
	if err != nil {
		t.Fatalf("CollectMetrics() failed: %v", err)
	}

	// Shared Comparison Options
	cmpOpts := []cmp.Option{
		protocmp.Transform(),
		protocmp.IgnoreFields(&tspb.Timestamp{}, protoreflect.Name("nanos")),
	}

	// Table-Driven Definitions
	tests := []struct {
		name string
		got  any
		want any
	}{
		{
			name: "ClusterID",
			got:  payload.GetClusterId(),
			want: "fake-cluster-id",
		},
		{
			name: "Namespaces",
			got:  payload.GetNamespaces().GetNamespaces(),
			want: &ompb.NamespaceList{Items: []*ompb.Namespace{{Metadata: &ompb.ResourceMetadata{Name: "default", Uid: "ns-uid", ResourceVersion: "1", CreationTimestamp: nowProto}}}},
		},
		{
			name: "Deployments",
			got:  payload.GetDeployments().GetDeployments(),
			want: &ompb.DeploymentList{
				Items: []*ompb.Deployment{{
					Metadata: &ompb.ResourceMetadata{Name: "test-dep", Uid: "dep-uid", ResourceVersion: "2", CreationTimestamp: nowProto, Namespace: "default"},
					Spec: &ompb.Deployment_Spec{
						PodTemplate: &ompb.PodTemplate{
							Metadata: &ompb.ResourceMetadata{CreationTimestamp: tspb.New(time.Time{})},
							Spec: &ompb.PodSpec{
								Containers: []*ompb.Container{{
									Env:          []*ompb.Env{{Name: "GOOGLE_APPLICATION_CREDENTIALS", Value: "/var/run/secrets/wif/config.json"}},
									VolumeMounts: []*ompb.VolumeMount{{Name: "test-volume", MountPath: "/var/lib/test"}, {Name: "wif-token", MountPath: "/var/run/secrets/wif"}},
								}},
								InitContainers: []*ompb.Container{{VolumeMounts: []*ompb.VolumeMount{{Name: "test-volume2", MountPath: "/usr/lib/test2"}}}},
								Volumes:        []*ompb.Volume{{Name: "test-volume"}, {Name: "test-volume2"}, {Name: "wif-token", Projected: &ompb.Volume_Projected{Sources: true}}},
								Affinity:       &ompb.Affinity{PodAntiAffinity: &ompb.Affinity_PodAntiAffinity{}},
							},
						},
					},
				}},
			},
		},
		{
			name: "StorageClasses",
			got:  payload.GetStorageClasses().GetStorageClasses(),
			want: &ompb.StorageClassList{Items: []*ompb.StorageClass{{Metadata: &ompb.ResourceMetadata{Name: "test-sc", Uid: "sc-uid", ResourceVersion: "4", CreationTimestamp: nowProto}, Provisioner: "test-provisioner"}}},
		},
		{
			name: "ConfigMaps",
			got:  payload.GetConfigMaps().GetConfigMaps(),
			want: &ompb.ConfigMapList{Items: []*ompb.ConfigMap{{Metadata: &ompb.ResourceMetadata{Name: "wlm-cluster-environment", Uid: "cm-uid-2", ResourceVersion: "5", CreationTimestamp: nowProto, Namespace: "workloadmanager"}, Data: map[string]string{"environment": "production"}}}},
		},
		{
			name: "CSIDrivers",
			got:  payload.GetCsiDrivers().GetCsiDrivers(),
			want: &ompb.CsiDriverList{Items: []*ompb.CsiDriver{{Metadata: &ompb.ResourceMetadata{Name: "test-csi", Uid: "csi-uid", ResourceVersion: "6", CreationTimestamp: nowProto}, Spec: &ompb.CsiDriver_Spec{}}}},
		},
		{
			name: "CloudCredentialConfig",
			got:  payload.GetCloudCredentialConfig(),
			want: &ompb.CloudCredentialConfig{
				Metadata: &ompb.ResourceMetadata{Name: "cluster", Uid: "4f896355-45cd-403a-9953-2b4c9dab04e6", ResourceVersion: "508", CreationTimestamp: tspb.New(time.Date(2025, time.May, 14, 6, 33, 6, 0, time.UTC)), Generation: 1},
				Spec:     &ompb.CloudCredentialConfig_Spec{CredentialsMode: "Mint"},
			},
		},
		{
			name: "CRDs",
			got:  payload.GetCustomResourceDefinitions().GetCustomResourceDefinitions(),
			want: &ompb.CustomResourceDefinitionList{Items: []*ompb.CustomResourceDefinition{{Metadata: &ompb.ResourceMetadata{Name: "test-crd"}}}},
		},
		{
			name: "Nodes",
			got:  payload.GetNodes().GetNodes(),
			want: &ompb.NodeList{Items: []*ompb.Node{{Metadata: &ompb.ResourceMetadata{Name: "test-node", Uid: "node-uid", ResourceVersion: "8", CreationTimestamp: nowProto}}}},
		},
	}

	// Execution Loop
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, tt.got, cmpOpts...); diff != "" {
				t.Errorf("%s diff (-want +got):\n%s", tt.name, diff)
			}
		})
	}
}
