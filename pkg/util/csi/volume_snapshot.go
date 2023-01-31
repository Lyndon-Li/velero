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

package csi

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	snapshotterClientSet "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	snapshotter "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned/typed/volumesnapshot/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/vmware-tanzu/velero/pkg/util/boolptr"
)

func WaitVolumeSnapshotReady(ctx context.Context, snapshotClient *snapshotterClientSet.Clientset,
	volumeSnapshot string, volumeSnapshotNS string, timeout time.Duration, Log logrus.FieldLogger) (*snapshotv1api.VolumeSnapshot, error) {
	eg, _ := errgroup.WithContext(ctx)
	interval := 5 * time.Second

	var updated *snapshotv1api.VolumeSnapshot
	eg.Go(func() error {
		err := wait.PollImmediate(interval, timeout, func() (bool, error) {
			tmpVS, err := snapshotClient.SnapshotV1().VolumeSnapshots(volumeSnapshotNS).Get(ctx, volumeSnapshot, metav1.GetOptions{})
			if err != nil {
				return false, errors.Wrapf(err, fmt.Sprintf("failed to get volumesnapshot %s/%s", volumeSnapshotNS, volumeSnapshot))
			}
			if tmpVS.Status == nil || tmpVS.Status.BoundVolumeSnapshotContentName == nil || !boolptr.IsSetToTrue(tmpVS.Status.ReadyToUse) || tmpVS.Status.RestoreSize == nil {
				Log.Infof("Waiting for CSI driver to reconcile volumesnapshot %s/%s. Retrying in %ds", volumeSnapshotNS, volumeSnapshot, interval/time.Second)
				return false, nil
			}

			updated = tmpVS
			Log.Debugf("VolumeSnapshot %s/%s turned into ReadyToUse.", volumeSnapshotNS, volumeSnapshot)

			return true, nil
		})

		return err
	})

	err := eg.Wait()

	return updated, err
}

// GetVolumeSnapshotContentForVolumeSnapshot returns the volumesnapshotcontent object associated with the volumesnapshot
func GetVolumeSnapshotContentForVolumeSnapshot(volSnap *snapshotv1api.VolumeSnapshot, snapshotClient snapshotter.SnapshotV1Interface, log logrus.FieldLogger, shouldWait bool) (*snapshotv1api.VolumeSnapshotContent, error) {
	if !shouldWait {
		if volSnap.Status == nil || volSnap.Status.BoundVolumeSnapshotContentName == nil {
			// volumesnapshot hasn't been reconciled and we're not waiting for it.
			return nil, nil
		}
		vsc, err := snapshotClient.VolumeSnapshotContents().Get(context.TODO(), *volSnap.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		if err != nil {
			return nil, errors.Wrap(err, "error getting volume snapshot content from API")
		}
		return vsc, nil
	}

	// We'll wait 10m for the VSC to be reconciled polling every 5s
	// TODO: make this timeout configurable.
	timeout := 10 * time.Minute
	interval := 5 * time.Second
	var snapshotContent *snapshotv1api.VolumeSnapshotContent

	err := wait.PollImmediate(interval, timeout, func() (bool, error) {
		vs, err := snapshotClient.VolumeSnapshots(volSnap.Namespace).Get(context.TODO(), volSnap.Name, metav1.GetOptions{})
		if err != nil {
			return false, errors.Wrapf(err, fmt.Sprintf("failed to get volumesnapshot %s/%s", volSnap.Namespace, volSnap.Name))
		}

		if vs.Status == nil || vs.Status.BoundVolumeSnapshotContentName == nil {
			log.Infof("Waiting for CSI driver to reconcile volumesnapshot %s/%s. Retrying in %ds", volSnap.Namespace, volSnap.Name, interval/time.Second)
			return false, nil
		}

		snapshotContent, err = snapshotClient.VolumeSnapshotContents().Get(context.TODO(), *vs.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		if err != nil {
			return false, errors.Wrapf(err, fmt.Sprintf("failed to get volumesnapshotcontent %s for volumesnapshot %s/%s", *vs.Status.BoundVolumeSnapshotContentName, vs.Namespace, vs.Name))
		}

		// we need to wait for the VolumeSnaphotContent to have a snapshot handle because during restore,
		// we'll use that snapshot handle as the source for the VolumeSnapshotContent so it's statically
		// bound to the existing snapshot.
		if snapshotContent.Status == nil || snapshotContent.Status.SnapshotHandle == nil {
			log.Infof("Waiting for volumesnapshotcontents %s to have snapshot handle. Retrying in %ds", snapshotContent.Name, interval/time.Second)
			if snapshotContent.Status != nil && snapshotContent.Status.Error != nil {
				log.Warnf("Volumesnapshotcontent %s has error: %v", snapshotContent.Name, snapshotContent.Status.Error.Message)
			}
			return false, nil
		}

		return true, nil
	})

	if err != nil {
		if err == wait.ErrWaitTimeout {
			if snapshotContent.Status != nil && snapshotContent.Status.Error != nil {
				log.Errorf("Timed out awaiting reconciliation of volumesnapshot, Volumesnapshotcontent %s has error: %v", snapshotContent.Name, snapshotContent.Status.Error.Message)
			} else {
				log.Errorf("Timed out awaiting reconciliation of volumesnapshot %s/%s", volSnap.Namespace, volSnap.Name)
			}
		}
		return nil, err
	}

	return snapshotContent, nil
}

func RetainVSC(ctx context.Context, snapshotClient *snapshotterClientSet.Clientset,
	vsc *snapshotv1api.VolumeSnapshotContent) (*snapshotv1api.VolumeSnapshotContent, error) {
	if vsc.Spec.DeletionPolicy == snapshotv1api.VolumeSnapshotContentRetain {
		return nil, nil
	}
	origBytes, err := json.Marshal(vsc)
	if err != nil {
		return nil, errors.Wrap(err, "error marshalling original VSC")
	}

	updated := vsc.DeepCopy()
	updated.Spec.DeletionPolicy = snapshotv1api.VolumeSnapshotContentRetain

	updatedBytes, err := json.Marshal(updated)
	if err != nil {
		return nil, errors.Wrap(err, "error marshalling updated VSC")
	}

	patchBytes, err := jsonpatch.CreateMergePatch(origBytes, updatedBytes)
	if err != nil {
		return nil, errors.Wrap(err, "error creating json merge patch for VSC")
	}

	retained, err := snapshotClient.SnapshotV1().VolumeSnapshotContents().Patch(ctx, vsc.Name, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return nil, err
	}

	return retained, nil
}

func DeleteVolumeSnapshotContentIfAny(ctx context.Context, snapshotClient *snapshotterClientSet.Clientset, vscName string, log logrus.FieldLogger) {
	vsc, err := snapshotClient.SnapshotV1().VolumeSnapshotContents().Get(ctx, vscName, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.WithError(err).Errorf("Failed to get volume snapshot content %s", vscName)
		}

		return
	}

	err = snapshotClient.SnapshotV1().VolumeSnapshotContents().Delete(ctx, vsc.Name, *&metav1.DeleteOptions{})
	if err != nil {
		log.WithError(err).Errorf("Failed to delete volume snapshot content %s", vsc.Name)
	}
}

func EnsureDeleteVS(ctx context.Context, snapshotClient *snapshotterClientSet.Clientset,
	vs *snapshotv1api.VolumeSnapshot, timeout time.Duration) error {
	if vs == nil {
		return nil
	}

	err := snapshotClient.SnapshotV1().VolumeSnapshots(vs.Namespace).Delete(ctx, vs.Name, metav1.DeleteOptions{})
	if err != nil {
		return errors.Wrap(err, "error to delete volume snapshot")
	}

	interval := 1 * time.Second
	err = wait.PollImmediate(interval, timeout, func() (bool, error) {
		_, err := snapshotClient.SnapshotV1().VolumeSnapshots(vs.Namespace).Get(ctx, vs.Name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}

			return false, errors.Wrapf(err, fmt.Sprintf("failed to get VolumeSnapshot %s", vs.Name))
		}

		return false, nil
	})

	if err != nil {
		return errors.Wrapf(err, "fail to retrieve VolumeSnapshot info for %s", vs.Name)
	}

	return nil
}

func EnsureDeleteVSC(ctx context.Context, snapshotClient *snapshotterClientSet.Clientset,
	vsc *snapshotv1api.VolumeSnapshotContent, timeout time.Duration) error {
	if vsc == nil {
		return nil
	}

	err := snapshotClient.SnapshotV1().VolumeSnapshotContents().Delete(ctx, vsc.Name, metav1.DeleteOptions{})
	if err != nil {
		return errors.Wrap(err, "error to delete volume snapshotContent")
	}

	interval := 1 * time.Second
	err = wait.PollImmediate(interval, timeout, func() (bool, error) {
		_, err := snapshotClient.SnapshotV1().VolumeSnapshotContents().Get(ctx, vsc.Name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}

			return false, errors.Wrapf(err, fmt.Sprintf("failed to get VolumeSnapshotContent %s", vsc.Name))
		}

		return false, nil
	})

	if err != nil {
		return errors.Wrapf(err, "fail to retrieve VolumeSnapshotContent info for %s", vsc.Name)
	}

	return nil
}

func DeleteVolumeSnapshotIfAny(ctx context.Context, snapshotClient *snapshotterClientSet.Clientset, vsName string, vsNamespace string, log logrus.FieldLogger) {
	vs, err := snapshotClient.SnapshotV1().VolumeSnapshots(vsNamespace).Get(ctx, vsName, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.WithError(err).Errorf("Failed to get volume snapshot %s/%s", vsNamespace, vsName)
		}

		return
	}

	err = snapshotClient.SnapshotV1().VolumeSnapshots(vs.Namespace).Delete(ctx, vs.Name, *&metav1.DeleteOptions{})
	if err != nil {
		log.WithError(err).Errorf("Failed to delete volume snapshot %s/%s", vs.Namespace, vs.Name)
	}
}
