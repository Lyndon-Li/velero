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

package kube

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	corev1api "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
)

func GetVolumeModeFromDataMover(dataMover string) corev1api.PersistentVolumeMode {
	return corev1api.PersistentVolumeFilesystem
}

func ResetPVCDataSource(pvc *corev1api.PersistentVolumeClaim, vsName string) {
	// Restore operation for the PVC will use the volumesnapshot as the data source.
	// So clear out the volume name, which is a ref to the PV
	pvc.Spec.VolumeName = ""
	dataSourceRef := &corev1api.TypedLocalObjectReference{
		APIGroup: &snapshotv1api.SchemeGroupVersion.Group,
		Kind:     "VolumeSnapshot",
		Name:     vsName,
	}
	pvc.Spec.DataSource = dataSourceRef
	pvc.Spec.DataSourceRef = dataSourceRef
}

func DeletePVCIfAny(ctx context.Context, pvcGetter corev1client.PersistentVolumeClaimsGetter, pvcName string, pvcNamespace string, log logrus.FieldLogger) {
	pvc, err := pvcGetter.PersistentVolumeClaims(pvcNamespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.WithError(err).Errorf("Failed to get pvc %s/%s", pvcNamespace, pvcName)
		}

		return
	}

	err = pvcGetter.PersistentVolumeClaims(pvc.Namespace).Delete(ctx, pvc.Name, *&metav1.DeleteOptions{})
	if err != nil {
		log.WithError(err).Errorf("Failed to delete pvc %s/%s", pvc.Namespace, pvc.Name)
	}
}

func WaitPVCBound(ctx context.Context, pvcGetter corev1client.PersistentVolumeClaimsGetter,
	pvGetter corev1client.PersistentVolumesGetter, pvc string, namespace string,
	timeout time.Duration) (*corev1api.PersistentVolumeClaim, *corev1api.PersistentVolume, error) {
	eg, _ := errgroup.WithContext(ctx)
	interval := 5 * time.Second

	var updated *corev1api.PersistentVolumeClaim
	eg.Go(func() error {
		err := wait.PollImmediate(interval, timeout, func() (bool, error) {
			tmpPVC, err := pvcGetter.PersistentVolumeClaims(namespace).Get(ctx, pvc, metav1.GetOptions{})
			if err != nil {
				return false, errors.Wrapf(err, fmt.Sprintf("failed to get pvc %s/%s", namespace, pvc))
			}

			if tmpPVC.Spec.VolumeName == "" {
				return false, nil
			}

			updated = tmpPVC

			return true, nil
		})

		return err
	})

	err := eg.Wait()
	if err != nil {
		return nil, nil, err
	}

	pv, err := pvGetter.PersistentVolumes().Get(ctx, updated.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, err
	}

	return updated, pv, err
}

func RetainPV(ctx context.Context, pvGetter corev1client.PersistentVolumesGetter, pv *corev1api.PersistentVolume) (*corev1api.PersistentVolume, error) {
	if pv.Spec.PersistentVolumeReclaimPolicy == corev1api.PersistentVolumeReclaimRetain {
		return nil, nil
	}

	origBytes, err := json.Marshal(pv)
	if err != nil {
		return nil, errors.Wrap(err, "error marshalling original PV")
	}

	updated := pv.DeepCopy()
	updated.Spec.PersistentVolumeReclaimPolicy = corev1api.PersistentVolumeReclaimRetain

	updatedBytes, err := json.Marshal(updated)
	if err != nil {
		return nil, errors.Wrap(err, "error marshalling updated PV")
	}

	patchBytes, err := jsonpatch.CreateMergePatch(origBytes, updatedBytes)
	if err != nil {
		return nil, errors.Wrap(err, "error creating json merge patch for PV")
	}

	updated, err = pvGetter.PersistentVolumes().Patch(ctx, pv.Name, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return nil, err
	}

	return updated, nil
}

func DeletePVIfAny(ctx context.Context, pvGetter corev1client.PersistentVolumesGetter, pvName string, log logrus.FieldLogger) {
	pv, err := pvGetter.PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.WithError(err).Errorf("Failed to get PV %s", pvName)
		}

		return
	}

	err = pvGetter.PersistentVolumes().Delete(ctx, pv.Name, *&metav1.DeleteOptions{})
	if err != nil {
		log.WithError(err).Errorf("Failed to delete PV %s", pv.Name)
	}
}

func EnsureDeletePVC(ctx context.Context, pvcGetter corev1client.PersistentVolumeClaimsGetter, pvc string, namespace string, timeout time.Duration) error {
	err := pvcGetter.PersistentVolumeClaims(namespace).Delete(ctx, pvc, metav1.DeleteOptions{})
	if err != nil {
		return errors.Wrap(err, "error to delete pvc")
	}

	interval := 1 * time.Second
	err = wait.PollImmediate(interval, timeout, func() (bool, error) {
		_, err := pvcGetter.PersistentVolumeClaims(namespace).Get(ctx, pvc, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}

			return false, errors.Wrapf(err, fmt.Sprintf("failed to get pvc %s", pvc))
		}

		return false, nil
	})

	if err != nil {
		return errors.Wrapf(err, "fail to retrieve pvc info for %s", pvc)
	}

	return nil
}

func EnsureDeletePV(ctx context.Context, pvGetter corev1client.PersistentVolumesGetter, pv *corev1api.PersistentVolume, timeout time.Duration) error {
	if pv == nil {
		return nil
	}

	err := pvGetter.PersistentVolumes().Delete(ctx, pv.Name, metav1.DeleteOptions{})
	if err != nil {
		return errors.Wrap(err, "error to delete pvc")
	}

	interval := 1 * time.Second
	err = wait.PollImmediate(interval, timeout, func() (bool, error) {
		_, err := pvGetter.PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}

			return false, errors.Wrapf(err, fmt.Sprintf("failed to get pvc %s", pv.Name))
		}

		return false, nil
	})

	if err != nil {
		return errors.Wrapf(err, "fail to retrieve pvc info for %s", pv.Name)
	}

	return nil
}
