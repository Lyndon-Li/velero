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

package datamover

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/repository"
	"github.com/vmware-tanzu/velero/pkg/util/filesystem"
	"github.com/vmware-tanzu/velero/pkg/util/kube"
)

func GetUploaderType(dataMover string) string {
	if dataMover == "" || dataMover == "velero" {
		return "kopia"
	} else {
		return ""
	}
}

func GetPodVolumeHostPath(ctx context.Context, pod *corev1.Pod, pvc *corev1.PersistentVolumeClaim,
	cli ctrlclient.Client, fs filesystem.Interface, log logrus.FieldLogger) (string, error) {
	logger := log.WithField("pod name", pod.Name).WithField("pod UID", pod.GetUID()).WithField("pvc", pvc.Name)

	volDir, err := kube.GetVolumeDirectory(ctx, logger, pod, pvc.Name, cli)
	if err != nil {
		return "", errors.Wrapf(err, "error getting volume directory name for pvc %s in pod %s", pvc.Name, pod.Name)
	}

	logger.WithField("volDir", volDir).Info("Got volume for backup PVC")

	pathGlob := fmt.Sprintf("/host_pods/%s/volumes/*/%s", string(pod.GetUID()), volDir)
	logger.WithField("pathGlob", pathGlob).Debug("Looking for path matching glob")

	path, err := kube.SinglePathMatch(pathGlob, fs, logger)
	if err != nil {
		return "", errors.Wrapf(err, "error identifying unique volume path on host for pvc %s in pod %s", pvc.Name, pod.Name)
	}

	logger.WithField("path", path).Info("Found path matching glob")

	return path, nil
}

func GetPodMountPath() string {
	return "/var/snapshot_data_mover"
}

// GetSnapshotIdentifier returns the snapshots represented by SnapshotIdentifier for the given SnapshotBackups
func GetSnapshotIdentifier(snapshotBackups *velerov1api.SnapshotBackupList) []repository.SnapshotIdentifier {
	var res []repository.SnapshotIdentifier
	for _, item := range snapshotBackups.Items {
		if item.Status.SnapshotID == "" {
			continue
		}

		res = append(res, repository.SnapshotIdentifier{
			VolumeNamespace:       item.Spec.SourceNamespace,
			BackupStorageLocation: item.Spec.BackupStorageLocation,
			SnapshotID:            item.Status.SnapshotID,
			RepositoryType:        velerov1api.BackupRepositoryTypeKopia,
		})
	}

	return res
}
