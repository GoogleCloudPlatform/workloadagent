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

// Package controller contains the logic for reconciling the TelemetryConfig CRD object.

package controller

import (
	"context"
	"fmt"
	"path"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	csoa "github.com/GoogleCloudPlatform/workloadagent/api/v1alpha1"
)

const (
	// typeAvailable represents the status of the Deployment reconciliation
	typeAvailable = "Available"
	// typeDegraded represents the status used when the custom resource is deleted and the finalizer operations are yet to occur.
	typeDegraded            = "Degraded"
	defaultImageRespository = "us-docker.pkg.dev/workload-agent-products/google-cloud-workload-agent/workload-agent"
	//TODO: find a better way to get the package name + service account name this also affects clusterrolebinding
	agentServiceAccountName = "google-cloud-cluster-services-telemetry-agent-workloadagent-service-account"
)

// TelemetryConfigReconciler reconciles a TelemetryConfig object
type TelemetryConfigReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=cluster-services-openshift.cloud.google.com,resources=telemetryconfigs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster-services-openshift.cloud.google.com,resources=telemetryconfigs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cluster-services-openshift.cloud.google.com,resources=telemetryconfigs/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO: Modify the Reconcile function to compare the state specified by
// the Openshift object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.19.0/pkg/reconcile
func (r *TelemetryConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling req", "req", req)

	// Check if CR is still there
	wla := &csoa.TelemetryConfig{}
	err := r.Get(ctx, req.NamespacedName, wla)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// If the custom resource is not found then it usually means that it was deleted or not created
			// In this way, we will stop the reconciliation
			logger.Info("WorkloadAgent resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		// Error reading the object - requeue the request.
		logger.Error(err, "Failed to get WorkloadAgent")
		// Failed to find the telemetry agent, so we can't mark the status as degraded.
		return ctrl.Result{}, err
	}

	if len(wla.Status.Conditions) == 0 {
		meta.SetStatusCondition(&wla.Status.Conditions, metav1.Condition{Type: typeAvailable, Status: metav1.ConditionUnknown, Reason: "Reconciling", Message: "Starting reconciliation"})
		if err = r.Status().Update(ctx, wla); err != nil {
			logger.Error(err, "Failed to update WorkloadAgent status")
			// The call to update the status failed, no point in trying to mark the status as degraded.
			return ctrl.Result{}, err
		}

		// Let's re-fetch the memcached Custom Resource after updating the status
		// so that we have the latest state of the resource on the cluster and we will avoid
		// raising the error "the object has been modified, please apply
		// your changes to the latest version and try again" which would re-trigger the reconciliation
		// if we try to update it again in the following operations
		if err := r.Get(ctx, req.NamespacedName, wla); err != nil {
			logger.Error(err, "Failed to re-fetch WorkloadAgent")
			_ = r.updateStatusToDegraded(ctx, wla, "RefetchFailed", err.Error())
			return ctrl.Result{}, err
		}
	}

	wlaEnabled := wla.Spec.Enabled
	// Check if the deployment already exists, if not create a new one
	found := &appsv1.Deployment{}
	err = r.Get(ctx, types.NamespacedName{Name: wla.Name, Namespace: wla.Namespace}, found)
	if err != nil && apierrors.IsNotFound(err) && wlaEnabled {
		logger.Info("WLA deployment not found, deploying instance.")

		if err := r.validateWLAConfig(ctx, wla); err != nil {
			logger.Error(err, "WorkloadAgent spec is invalid, cannot create instance")
			_ = r.updateStatusToDegraded(ctx, wla, "InvalidSpec", err.Error())
			return ctrl.Result{}, err
		}

		dep := r.deploymentForWLA(wla)

		// Set ownerRef so that deployment will be deleted when the CR is deleted.
		if err := ctrl.SetControllerReference(wla, dep, r.Scheme); err != nil {
			logger.Error(err, "Failed to set controller reference for deployment")
			_ = r.updateStatusToDegraded(ctx, wla, "SetControllerRefFailed", err.Error())
			return ctrl.Result{}, err
		}

		logger.Info(fmt.Sprintf("Deployment spec: %+v", dep))
		logger.Info("Creating a new Deployment",
			"Deployment.Namespace", dep.Namespace, "Deployment.Name", dep.Name)
		if err = r.Create(ctx, dep); err != nil {
			logger.Error(err, "Failed to create new Deployment",
				"Deployment.Namespace", dep.Namespace, "Deployment.Name", dep.Name)
			_ = r.updateStatusToDegraded(ctx, wla, "DeploymentCreationFailed", err.Error())
			return ctrl.Result{}, err
		}
		// Deployment created successfully
		// We will requeue the reconciliation so that we can ensure the state
		// and move forward for the next operations
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}

	if found != nil {
		if !wlaEnabled {
			logger.Info("WLA deployment found but should be disabled, destroying instance.")

			if err = r.Delete(ctx, found); err != nil {
				logger.Error(err, "Failed to delete Deployment",
					"Deployment.Namespace", found.Namespace, "Deployment.Name", found.Name)
				_ = r.updateStatusToDegraded(ctx, wla, "DeploymentDeletionFailed", err.Error())
				return ctrl.Result{}, err
			}

			// Deployment destroyed successfully
			// We will requeue the reconciliation so that we can ensure the state
			// and move forward for the next operations
			return ctrl.Result{RequeueAfter: time.Minute}, nil
		}

		// If the deployment already exists, we always update, and let Kubernetes handle the diff.
		logger.Info("WLA deployment found, updating instance.")

		if err := r.validateWLAConfig(ctx, wla); err != nil {
			logger.Error(err, "WorkloadAgent spec is invalid, cannot update instance")
			_ = r.updateStatusToDegraded(ctx, wla, "InvalidSpec", err.Error())
			return ctrl.Result{}, err
		}

		dep := r.deploymentForWLA(wla)

		if err = r.Update(ctx, dep); err != nil {
			logger.Error(err, "Failed to update Deployment",
				"Deployment.Namespace", dep.Namespace, "Deployment.Name", dep.Name)
			_ = r.updateStatusToDegraded(ctx, wla, "DeploymentUpdateFailed", err.Error())
			return ctrl.Result{}, err
		}

		// Deployment updated successfully
		// We will requeue the reconciliation so that we can ensure the state
		// and move forward for the next operations
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}

	if err != nil {
		logger.Error(err, "Failed to get Deployment")
		_ = r.updateStatusToDegraded(ctx, wla, "DeploymentGetFailed", err.Error())
		// Let's return the error for the reconciliation be re-triggered again
		return ctrl.Result{}, err
	}

	//Perform additional reconciling here as more things are added to the spec.

	logger.Info("Nothing to do or reconciled successfully")
	// The following implementation will update the status
	meta.SetStatusCondition(&wla.Status.Conditions, metav1.Condition{Type: typeAvailable,
		Status: metav1.ConditionTrue, Reason: "Reconciling",
		Message: fmt.Sprintf("Deployment for custom resource (%s) created successfully", wla.Name)})

	if err := r.Status().Update(ctx, wla); err != nil {
		logger.Error(err, "Failed to update WorkloadAgent status")
		// The call to update the status failed, no point in trying to mark the status as degraded.
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *TelemetryConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&csoa.TelemetryConfig{}).
		Complete(r)
}

func (r *TelemetryConfigReconciler) validateWLAConfig(ctx context.Context, wla *csoa.TelemetryConfig) error {
	logger := log.FromContext(ctx)

	if wla.Spec.Enabled {
		if wla.Spec.ImageVersion == "" {
			err := fmt.Errorf("image version is empty")
			logger.Error(err, "Image version is empty")
			return err
		}

	}
	return nil
}

func (r *TelemetryConfigReconciler) deploymentForWLA(wla *csoa.TelemetryConfig) *appsv1.Deployment {
	var expirationSeconds int64
	expirationSeconds = 3600
	labels := map[string]string{
		"app.kubernetes.io/name":       "workloadagent-operator",
		"app.kubernetes.io/version":    "0.0.1",
		"app.kubernetes.io/managed-by": "WorkloadAgentController",
	}

	imageRepository := wla.Spec.ImageRepository
	if imageRepository == "" {
		imageRepository = defaultImageRespository
	}
	imageURL := fmt.Sprintf("%s:%s", imageRepository, wla.Spec.ImageVersion)

	volumes := []corev1.Volume{
		{
			Name: "kube-api-access",
			VolumeSource: corev1.VolumeSource{
				Projected: &corev1.ProjectedVolumeSource{
					Sources: []corev1.VolumeProjection{
						{
							ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
								ExpirationSeconds: &expirationSeconds,
								Path:              "token",
							},
						},
						{
							ConfigMap: &corev1.ConfigMapProjection{
								Items: []corev1.KeyToPath{
									{
										Key:  "ca.crt",
										Path: "ca.crt",
									},
								},
								LocalObjectReference: corev1.LocalObjectReference{
									Name: "kube-root-ca.crt",
								},
							},
						},
					},
				},
			},
		},
		{
			Name: "config-dir",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	}
	volumeMounts := []corev1.VolumeMount{
		{
			Name:      "kube-api-access",
			MountPath: "/var/run/secrets/kubernetes.io/serviceaccount",
			ReadOnly:  true,
		},
		{
			Name:      "config-dir",
			MountPath: "/etc/google-cloud-workload-agent",
		},
	}

	secretName := wla.Spec.ServiceAccountCredentialsSecretName
	if secretName == "" {
		secretName = "google-cloud-cluster-services-telemetry-agent-wif-secret"
	}
	credentialsPath := wla.Spec.ServiceAccountCredentialsPath
	if credentialsPath == "" {
		credentialsPath = "/var/run/secrets/google/service_account.json"
	}

	// 1. Mount the credentials Secret
	credentialsVolume := corev1.Volume{
		Name: "gcp-credentials",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: secretName,
			},
		},
	}
	credentialsVolumeMount := corev1.VolumeMount{
		Name:      "gcp-credentials",
		MountPath: path.Dir(credentialsPath),
		ReadOnly:  true,
	}
	volumes = append(volumes, credentialsVolume)
	volumeMounts = append(volumeMounts, credentialsVolumeMount)

	// 2. Always project the bound token (needed for WIF, harmless for Key)
	tokenAudience := wla.Spec.ServiceAccountTokenAudience
	if tokenAudience == "" {
		tokenAudience = "openshift"
	}
	tokenVolume := corev1.Volume{
		Name: "token",
		VolumeSource: corev1.VolumeSource{
			Projected: &corev1.ProjectedVolumeSource{
				Sources: []corev1.VolumeProjection{
					{
						ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
							ExpirationSeconds: ptr.To(int64(3600)),
							Path:              "token",
							Audience:          tokenAudience,
						},
					},
				},
			},
		},
	}
	tokenVolumeMount := corev1.VolumeMount{
		Name:      "token",
		MountPath: "/var/run/secrets/openshift/serviceaccount",
		ReadOnly:  true,
	}
	volumes = append(volumes, tokenVolume)
	volumeMounts = append(volumeMounts, tokenVolumeMount)

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      wla.Name,
			Namespace: wla.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: ptr.To(true),
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
					},
					Containers: []corev1.Container{
						{
							Image:           imageURL,
							ImagePullPolicy: corev1.PullAlways,
							Name:            "workloadagent",
							SecurityContext: &corev1.SecurityContext{
								AllowPrivilegeEscalation: ptr.To(false),
								Privileged:               ptr.To(false),
								Capabilities: &corev1.Capabilities{
									Drop: []corev1.Capability{"ALL"},
								},
							},
							Env: []corev1.EnvVar{
								{
									Name:  "LOG_LEVEL",
									Value: "DEBUG",
								},
								{
									Name:  "GOOGLE_APPLICATION_CREDENTIALS",
									Value: credentialsPath,
								},
								{
									Name:  "WLA_WORKLOAD",
									Value: "OPENSHIFT",
								},
							},
							VolumeMounts: volumeMounts,
						},
					},
					HostNetwork:        false,
					ServiceAccountName: agentServiceAccountName,
					Volumes:            volumes,
				},
			},
		},
	}
}

func (r *TelemetryConfigReconciler) updateStatusToDegraded(ctx context.Context, wla *csoa.TelemetryConfig, reason, message string) error {
	// The reason must be in UpperCamelCase, otherwise the kube-apiserver will reject the status update.
	meta.SetStatusCondition(&wla.Status.Conditions, metav1.Condition{
		Type:    typeDegraded,
		Status:  metav1.ConditionTrue,
		Reason:  reason,
		Message: message,
	})
	return r.Status().Update(ctx, wla)
}
