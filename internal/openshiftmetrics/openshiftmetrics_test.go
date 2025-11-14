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
	if req.URL.Path == "/apis/config.openshift.io/v1/clusterversions" {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body: io.NopCloser(bytes.NewReader([]byte(`{
				"apiVersion": "config.openshift.io/v1",
				"items": [{"spec": {"clusterID": "fake-cluster-id"}}]
			}`))),
		}, nil
	}

	serializer := k8sjson.NewSerializerWithOptions(k8sjson.DefaultMetaFactory, k8sscheme.Scheme, k8sscheme.Scheme, k8sjson.SerializerOptions{Yaml: false, Pretty: false, Strict: false})
	var obj runtime.Object
	switch req.URL.Path {
	case "/api/v1/namespaces":
		obj = &corev1.NamespaceList{
			Items: []corev1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "default", UID: "ns-uid", ResourceVersion: "1", CreationTimestamp: metav1.NewTime(f.now)}}},
		}
	case "/apis/apps/v1/namespaces/default/deployments":
		obj = &appsv1.DeploymentList{
			Items: []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Name: "test-dep", Namespace: "default", UID: "dep-uid", ResourceVersion: "2", CreationTimestamp: metav1.NewTime(f.now)}}},
		}
	/*
		Commenting out this test case until version incompatibilities for k8s_io/api is fixed.

		case "/api/v1/namespaces/default/persistentvolumeclaims":
			quantity := resource.MustParse("1Gi")
			obj = &corev1.PersistentVolumeClaimList{
				Items: []corev1.PersistentVolumeClaim{{
					ObjectMeta: metav1.ObjectMeta{Name: "test-pvc", Namespace: "default", UID: "pvc-uid", ResourceVersion: "3", CreationTimestamp: metav1.NewTime(f.now)},
					Spec: corev1.PersistentVolumeClaimSpec{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{corev1.ResourceStorage: quantity},
						},
					},
					Status: corev1.PersistentVolumeClaimStatus{
						Capacity: corev1.ResourceList{corev1.ResourceStorage: quantity},
					},
				}},
			}
	*/
	case "/apis/storage.k8s.io/v1/storageclasses":
		obj = &storagev1.StorageClassList{
			Items: []storagev1.StorageClass{{
				ObjectMeta:  metav1.ObjectMeta{Name: "test-sc", UID: "sc-uid", ResourceVersion: "4", CreationTimestamp: metav1.NewTime(f.now)},
				Provisioner: "test-provisioner",
			}},
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
			Items: []storagev1.CSIDriver{{
				ObjectMeta: metav1.ObjectMeta{Name: "test-csi", UID: "csi-uid", ResourceVersion: "6", CreationTimestamp: metav1.NewTime(f.now)},
			}},
		}
	default:
		if strings.Contains(req.URL.Path, "/deployments") {
			obj = &appsv1.DeploymentList{}
		} else if strings.Contains(req.URL.Path, "/persistentvolumeclaims") {
			obj = &corev1.PersistentVolumeClaimList{}
		} else if strings.Contains(req.URL.Path, "/configmaps") {
			obj = &corev1.ConfigMapList{}
		} else {
			return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(bytes.NewReader([]byte{}))}, nil
		}
	}

	buf := new(bytes.Buffer)
	if err := serializer.Encode(obj, buf); err != nil {
		f.t.Errorf("encoding failed for %s: %v", req.URL.Path, err)
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(buf),
	}, nil
}

func TestCollectMetrics(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	nowProto := tspb.New(now)

	ft := &fakeTransport{t: t, now: now}
	cfg := &rest.Config{
		Host:      "http://fake",
		Transport: ft,
	}

	k8sClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("Failed to create k8s client: %v", err)
	}
	ocpClient, err := openshift.New(cfg)
	if err != nil {
		t.Fatalf("Failed to create openshift client: %v", err)
	}

	om := &OpenShiftMetrics{
		K8sClient:       k8sClient,
		OpenShiftClient: ocpClient,
	}

	payload, err := om.CollectMetrics(ctx, MetricVersioning{PayloadVersion: "1.0", AgentVersion: "2.0"})
	if err != nil {
		t.Fatalf("CollectMetrics() failed: %v", err)
	}

	if payload.GetClusterId() != "fake-cluster-id" {
		t.Errorf("CollectMetrics() clusterID = %v, want fake-cluster-id", payload.GetClusterId())
	}

	cmpOpts := []cmp.Option{
		protocmp.Transform(),
		protocmp.IgnoreFields(&tspb.Timestamp{}, protoreflect.Name("nanos")),
	}

	wantNamespaces := &ompb.NamespaceList{
		Items: []*ompb.Namespace{
			{
				Metadata: &ompb.ResourceMetadata{Name: "default", Uid: "ns-uid", ResourceVersion: "1", CreationTimestamp: nowProto},
			},
		},
	}
	if diff := cmp.Diff(wantNamespaces, payload.GetNamespaces().GetNamespaces(), cmpOpts...); diff != "" {
		t.Errorf("CollectMetrics() namespaces diff (-want +got):\n%s", diff)
	}

	wantDeployments := &ompb.DeploymentList{
		Items: []*ompb.Deployment{
			{
				Metadata: &ompb.ResourceMetadata{Name: "test-dep", Uid: "dep-uid", ResourceVersion: "2", CreationTimestamp: nowProto},
				Spec: &ompb.Deployment_Spec{
					PodTemplate: &ompb.PodTemplate{
						Metadata: &ompb.ResourceMetadata{
							CreationTimestamp: tspb.New(time.Time{}),
						},
						Spec: &ompb.PodSpec{
							Affinity: &ompb.Affinity{
								PodAntiAffinity: &ompb.Affinity_PodAntiAffinity{},
							},
						},
					},
				},
			},
		},
	}
	if diff := cmp.Diff(wantDeployments, payload.GetDeployments().GetDeployments(), cmpOpts...); diff != "" {
		t.Errorf("CollectMetrics() deployments diff (-want +got):\n%s", diff)
	}

	/*
		Commenting out this test case until version incompatibilities for k8s_io/api is fixed.

		wantPvcs := &ompb.PersistentVolumeClaimList{
			Items: []*ompb.PersistentVolumeClaim{
				{
					Metadata: &ompb.ResourceMetadata{Name: "test-pvc", Uid: "pvc-uid", ResourceVersion: "3", CreationTimestamp: nowProto},
					Spec: &ompb.PersistentVolumeClaim_Spec{
						Resources: &ompb.PersistentVolumeClaim_Resources{
							Requests: &ompb.PersistentVolumeClaim_Requests{Storage: "1Gi"},
						},
					},
					Status: &ompb.PersistentVolumeClaim_Status{
						Capacity: &ompb.PersistentVolumeClaim_Capacity{Storage: "1Gi"},
					},
				},
			},
		}
		if diff := cmp.Diff(wantPvcs, payload.GetPersistentVolumeClaims().GetPersistentVolumeClaims(), cmpOpts...); diff != "" {
			t.Errorf("CollectMetrics() persistent volume claims diff (-want +got):\n%s", diff)
		}
	*/

	wantScs := &ompb.StorageClassList{
		Items: []*ompb.StorageClass{
			{
				Metadata:    &ompb.ResourceMetadata{Name: "test-sc", Uid: "sc-uid", ResourceVersion: "4", CreationTimestamp: nowProto},
				Provisioner: "test-provisioner",
			},
		},
	}
	if diff := cmp.Diff(wantScs, payload.GetStorageClasses().GetStorageClasses(), cmpOpts...); diff != "" {
		t.Errorf("CollectMetrics() storage classes diff (-want +got):\n%s", diff)
	}

	wantCms := &ompb.ConfigMapList{
		Items: []*ompb.ConfigMap{
			{
				Metadata: &ompb.ResourceMetadata{Name: "test-cm", Uid: "cm-uid", ResourceVersion: "5", CreationTimestamp: nowProto},
				Data:     map[string]string{"key": "value"},
			},
		},
	}
	if diff := cmp.Diff(wantCms, payload.GetConfigMaps().GetConfigMaps(), cmpOpts...); diff != "" {
		t.Errorf("CollectMetrics() config maps diff (-want +got):\n%s", diff)
	}

	wantCsiDrivers := &ompb.CsiDriverList{
		Items: []*ompb.CsiDriver{
			{
				Metadata: &ompb.ResourceMetadata{Name: "test-csi", Uid: "csi-uid", ResourceVersion: "6", CreationTimestamp: nowProto},
				Spec:     &ompb.CsiDriver_Spec{},
			},
		},
	}
	if diff := cmp.Diff(wantCsiDrivers, payload.GetCsiDrivers().GetCsiDrivers(), cmpOpts...); diff != "" {
		t.Errorf("CollectMetrics() csi drivers diff (-want +got):\n%s", diff)
	}
}
