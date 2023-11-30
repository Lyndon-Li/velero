package backup

import (
	"context"

	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/sets"
	kbclient "sigs.k8s.io/controller-runtime/pkg/client"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/features"
	"github.com/vmware-tanzu/velero/pkg/label"
	"github.com/vmware-tanzu/velero/pkg/util/boolptr"
)

// Common function to update the status of CSI snapshots
// returns VolumeSnapshot, VolumeSnapshotContent, VolumeSnapshotClasses referenced
func UpdateBackupCSISnapshotsStatus(client kbclient.Client, globalCRClient kbclient.Client, backup *velerov1api.Backup, backupLog logrus.FieldLogger) (volumeSnapshots []snapshotv1api.VolumeSnapshot, volumeSnapshotContents []snapshotv1api.VolumeSnapshotContent, volumeSnapshotClasses []snapshotv1api.VolumeSnapshotClass) {
	if features.IsEnabled(velerov1api.CSIFeatureFlag) {
		selector := label.NewSelectorForBackup(backup.Name)
		vscList := &snapshotv1api.VolumeSnapshotContentList{}

		vsList := new(snapshotv1api.VolumeSnapshotList)
		err := globalCRClient.List(context.TODO(), vsList, &kbclient.ListOptions{
			LabelSelector: label.NewSelectorForBackup(backup.Name),
		})
		if err != nil {
			backupLog.Error(err)
		}
		volumeSnapshots = append(volumeSnapshots, vsList.Items...)

		if err := client.List(context.Background(), vscList, &kbclient.ListOptions{LabelSelector: selector}); err != nil {
			backupLog.Error(err)
		}
		if len(vscList.Items) >= 0 {
			volumeSnapshotContents = vscList.Items
		}

		vsClassSet := sets.NewString()
		for index := range volumeSnapshotContents {
			// persist the volumesnapshotclasses referenced by vsc
			if volumeSnapshotContents[index].Spec.VolumeSnapshotClassName != nil && !vsClassSet.Has(*volumeSnapshotContents[index].Spec.VolumeSnapshotClassName) {
				vsClass := &snapshotv1api.VolumeSnapshotClass{}
				if err := client.Get(context.TODO(), kbclient.ObjectKey{Name: *volumeSnapshotContents[index].Spec.VolumeSnapshotClassName}, vsClass); err != nil {
					backupLog.Error(err)
				} else {
					vsClassSet.Insert(*volumeSnapshotContents[index].Spec.VolumeSnapshotClassName)
					volumeSnapshotClasses = append(volumeSnapshotClasses, *vsClass)
				}
			}
		}
		backup.Status.CSIVolumeSnapshotsAttempted = len(volumeSnapshots)
		csiVolumeSnapshotsCompleted := 0
		for _, vs := range volumeSnapshots {
			if vs.Status != nil && boolptr.IsSetToTrue(vs.Status.ReadyToUse) {
				csiVolumeSnapshotsCompleted++
			}
		}
		backup.Status.CSIVolumeSnapshotsCompleted = csiVolumeSnapshotsCompleted
	}
	return volumeSnapshots, volumeSnapshotContents, volumeSnapshotClasses
}
