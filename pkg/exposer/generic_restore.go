/*
Copyright The Velero Contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package exposer

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/vmware-tanzu/velero/pkg/util/boolptr"
	"github.com/vmware-tanzu/velero/pkg/util/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// GenericRestoreExposer is the interfaces for a generic restore exposer
type GenericRestoreExposer interface {
	// Expose starts the process to a restore expose, the expose process may take long time
	Expose(context.Context, *unstructured.Unstructured, string, string, map[string]string, time.Duration) error

	// GetExposed polls the status of the expose.
	// If the expose is accessible by the current caller, it waits the expose ready and returns the expose result.
	// Otherwise, it returns nil as the expose result without an error.
	GetExposed(context.Context, *unstructured.Unstructured, client.Client, string, time.Duration) (*ExposeResult, error)

	// RebindVolume unexpose the restored PV and rebind it to the target PVC
	RebindVolume(context.Context, *unstructured.Unstructured, string, string, time.Duration) error

	// CleanUp cleans up any objects generated during the restore expose
	CleanUp(context.Context, *unstructured.Unstructured)
}

// NewGenericRestoreExposer create a new instance of generic restore exposer
func NewGenericRestoreExposer(kubeClient kubernetes.Interface, log logrus.FieldLogger) GenericRestoreExposer {
	return &genericRestoreExposer{
		kubeClient: kubeClient,
		log:        log,
	}
}

type genericRestoreExposer struct {
	kubeClient kubernetes.Interface
	log        logrus.FieldLogger
}

func (e *genericRestoreExposer) Expose(ctx context.Context, ownerObject *unstructured.Unstructured, targetPVCName string, sourceNamespace string, hostingPodLabels map[string]string, timeout time.Duration) error {
	curLog := e.log.WithFields(logrus.Fields{
		"owner":            ownerObject.GetName(),
		"target PVC":       targetPVCName,
		"source namespace": sourceNamespace,
	})

	selectedNode, targetPVC, err := kube.WaitPVCConsumed(ctx, e.kubeClient.CoreV1(), targetPVCName, sourceNamespace, e.kubeClient.StorageV1(), timeout)
	if err != nil {
		return errors.Wrapf(err, "error to wait target PVC consumed, %s/%s", sourceNamespace, targetPVCName)
	}

	curLog.WithField("target PVC", targetPVCName).WithField("selected node", selectedNode).Info("Target PVC is consumed")

	restorePod, err := e.createRestorePod(ctx, ownerObject, hostingPodLabels, selectedNode)
	if err != nil {
		return errors.Wrapf(err, "error to create restore pod")
	}

	curLog.WithField("pod name", restorePod.Name).Info("Restore pod is created")

	defer func() {
		if err != nil {
			kube.DeletePodIfAny(ctx, e.kubeClient.CoreV1(), restorePod.Name, restorePod.Namespace, curLog)
		}
	}()

	restorePVC, err := e.createRestorePVC(ctx, ownerObject, targetPVC, selectedNode)
	if err != nil {
		return errors.Wrap(err, "error to create restore pvc")
	}

	curLog.WithField("pvc name", restorePVC.Name).Info("Restore PVC is created")

	defer func() {
		if err != nil {
			kube.DeletePVCIfAny(ctx, e.kubeClient.CoreV1(), restorePVC.Name, restorePVC.Namespace, curLog)
		}
	}()

	return nil
}

func (e *genericRestoreExposer) GetExposed(ctx context.Context, ownerObject *unstructured.Unstructured, nodeClient client.Client, nodeName string, timeout time.Duration) (*ExposeResult, error) {
	restorePodName := ownerObject.GetName()
	restorePVCName := ownerObject.GetName()

	curLog := e.log.WithFields(logrus.Fields{
		"owner": ownerObject.GetName(),
		"node":  nodeName,
	})

	pod := &corev1.Pod{}
	err := nodeClient.Get(ctx, types.NamespacedName{
		Namespace: ownerObject.GetNamespace(),
		Name:      restorePodName,
	}, pod)
	if err != nil {
		if apierrors.IsNotFound(err) {
			curLog.WithField("backup pod", restorePodName).Error("Backup pod is not running in the current node")
			return nil, nil
		} else {
			return nil, errors.Wrapf(err, "error to get backup pod %s", restorePodName)
		}
	}

	curLog.WithField("pod", pod.Name).Infof("Restore pod is in running state in node %s", pod.Spec.NodeName)

	_, err = kube.WaitPVCBound(ctx, e.kubeClient.CoreV1(), e.kubeClient.CoreV1(), restorePVCName, ownerObject.GetNamespace(), timeout)
	if err != nil {
		return nil, errors.Wrapf(err, "error to wait restore PVC bound, %s", restorePVCName)
	}

	curLog.WithField("restore pvc", restorePVCName).Info("Restore PVC is bound")

	return &ExposeResult{ByPod: ExposeByPod{HostingPod: pod, PVC: restorePVCName}}, nil
}

func (e *genericRestoreExposer) CleanUp(ctx context.Context, ownerObject *unstructured.Unstructured) {
	restorePodName := ownerObject.GetName()
	restorePVCName := ownerObject.GetName()

	kube.DeletePodIfAny(ctx, e.kubeClient.CoreV1(), restorePodName, ownerObject.GetNamespace(), e.log)
	kube.DeletePVCIfAny(ctx, e.kubeClient.CoreV1(), restorePVCName, ownerObject.GetNamespace(), e.log)
}

func (e *genericRestoreExposer) RebindVolume(ctx context.Context, ownerObject *unstructured.Unstructured, targetPVCName string, sourceNamespace string, timeout time.Duration) error {
	restorePodName := ownerObject.GetName()
	restorePVCName := ownerObject.GetName()

	curLog := e.log.WithFields(logrus.Fields{
		"owner":            ownerObject.GetName(),
		"target PVC":       targetPVCName,
		"source namespace": sourceNamespace,
	})

	targetPVC, err := e.kubeClient.CoreV1().PersistentVolumeClaims(sourceNamespace).Get(ctx, targetPVCName, metav1.GetOptions{})
	if err != nil {
		return errors.Wrapf(err, "error to get target PVC %s/%s", sourceNamespace, targetPVCName)
	}

	restorePV, err := kube.WaitPVCBound(ctx, e.kubeClient.CoreV1(), e.kubeClient.CoreV1(), restorePVCName, ownerObject.GetNamespace(), timeout)
	if err != nil {
		return errors.Wrapf(err, "error to get PV from restore PVC %s", restorePVCName)
	}

	orgReclaim := restorePV.Spec.PersistentVolumeReclaimPolicy

	curLog.WithField("restore PV", restorePV.Name).Info("Restore PV is retrieved")

	retained, err := kube.SetPVReclaimPolicy(ctx, e.kubeClient.CoreV1(), restorePV, corev1.PersistentVolumeReclaimRetain)
	if err != nil {
		return errors.Wrapf(err, "error to retain PV %s", restorePV.Name)
	}

	curLog.WithField("restore PV", restorePV.Name).WithField("retained", (retained != nil)).Info("Restore PV is retained")

	defer func() {
		if retained != nil {
			curLog.WithField("restore PV", restorePV.Name).Info("Deleting retained PV on error")
			kube.DeletePVIfAny(ctx, e.kubeClient.CoreV1(), retained.Name, curLog)
		}
	}()

	if retained != nil {
		restorePV = retained
	}

	err = kube.EnsureDeletePod(ctx, e.kubeClient.CoreV1(), restorePodName, ownerObject.GetNamespace(), timeout)
	if err != nil {
		return errors.Wrapf(err, "error to delete restore pod %s", restorePodName)
	}

	err = kube.EnsureDeletePVC(ctx, e.kubeClient.CoreV1(), restorePVCName, ownerObject.GetNamespace(), timeout)
	if err != nil {
		return errors.Wrapf(err, "error to delete restore PVC %s", restorePVCName)
	}

	curLog.WithField("restore PVC", restorePVCName).Info("Restore PVC is deleted")

	_, err = kube.RebindPVC(ctx, e.kubeClient.CoreV1(), targetPVC, restorePV.Name)
	if err != nil {
		return errors.Wrapf(err, "error to rebind target PVC %s/%s to %s", targetPVC.Namespace, targetPVC.Name, restorePV.Name)
	}

	curLog.WithField("tartet PVC", fmt.Sprintf("%s/%s", targetPVC.Namespace, targetPVC.Name)).WithField("restore PV", restorePV.Name).Info("Target PVC is rebound to restore PV")

	var matchLabel map[string]string
	if targetPVC.Spec.Selector != nil {
		matchLabel = targetPVC.Spec.Selector.MatchLabels
	}

	restorePV, err = kube.ResetPVBinding(ctx, e.kubeClient.CoreV1(), restorePV, matchLabel)
	if err != nil {
		return errors.Wrapf(err, "error to reset binding info for restore PV %s", restorePV.Name)
	}

	curLog.WithField("restore PV", restorePV.Name).Info("Restore PV is rebound")

	_, err = kube.WaitPVCBound(ctx, e.kubeClient.CoreV1(), e.kubeClient.CoreV1(), targetPVC.Name, targetPVC.Namespace, timeout)
	if err != nil {
		return errors.Wrapf(err, "error to wait target PVC bound, targetPVC %s/%s", targetPVC.Namespace, targetPVC.Name)
	}

	curLog.WithField("target PVC", fmt.Sprintf("%s/%s", targetPVC.Namespace, targetPVC.Name)).Info("Target PVC is ready")

	retained = nil

	_, err = kube.SetPVReclaimPolicy(ctx, e.kubeClient.CoreV1(), restorePV, orgReclaim)
	if err != nil {
		curLog.WithField("restore PV", restorePV.Name).WithError(err).Warn("Restore PV's reclaim policy is not restored")
	} else {
		curLog.WithField("restore PV", restorePV.Name).Info("Restore PV's reclaim policy is restored")
	}

	return nil
}

func (e *genericRestoreExposer) createRestorePod(ctx context.Context, ownerObject *unstructured.Unstructured, label map[string]string, selectedNode string) (*corev1.Pod, error) {
	restorePodName := ownerObject.GetName()
	restorePVCName := ownerObject.GetName()

	var gracePeriod int64 = 0

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      restorePodName,
			Namespace: ownerObject.GetNamespace(),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: ownerObject.GetAPIVersion(),
					Kind:       ownerObject.GetKind(),
					Name:       ownerObject.GetName(),
					UID:        ownerObject.GetUID(),
					Controller: boolptr.True(),
				},
			},
			Labels: label,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:    restorePodName,
					Image:   "alpine:latest",
					Command: []string{"sleep", "infinity"},
					VolumeMounts: []corev1.VolumeMount{{
						Name:      restorePVCName,
						MountPath: "/" + restorePVCName,
					}},
				},
			},
			TerminationGracePeriodSeconds: &gracePeriod,
			Volumes: []corev1.Volume{{
				Name: restorePVCName,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: restorePVCName,
					},
				},
			}},
			NodeName: selectedNode,
		},
	}

	return e.kubeClient.CoreV1().Pods(ownerObject.GetNamespace()).Create(ctx, pod, metav1.CreateOptions{})
}

func (e *genericRestoreExposer) createRestorePVC(ctx context.Context, ownerObject *unstructured.Unstructured, targetPVC *corev1.PersistentVolumeClaim, selectedNode string) (*corev1.PersistentVolumeClaim, error) {
	restorePVCName := ownerObject.GetName()

	pvcObj := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   ownerObject.GetNamespace(),
			Name:        restorePVCName,
			Labels:      targetPVC.Labels,
			Annotations: targetPVC.Annotations,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: ownerObject.GetAPIVersion(),
					Kind:       ownerObject.GetKind(),
					Name:       ownerObject.GetName(),
					UID:        ownerObject.GetUID(),
					Controller: boolptr.True(),
				},
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      targetPVC.Spec.AccessModes,
			StorageClassName: targetPVC.Spec.StorageClassName,
			VolumeMode:       targetPVC.Spec.VolumeMode,
			Resources:        targetPVC.Spec.Resources,
		},
	}

	if selectedNode != "" {
		pvcObj.Annotations = map[string]string{
			kube.KubeAnnSelectedNode: selectedNode,
		}
	}

	return e.kubeClient.CoreV1().PersistentVolumeClaims(pvcObj.Namespace).Create(ctx, pvcObj, metav1.CreateOptions{})
}
