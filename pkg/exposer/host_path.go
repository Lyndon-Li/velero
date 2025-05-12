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
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1api "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/vmware-tanzu/velero/pkg/datapath"
	"github.com/vmware-tanzu/velero/pkg/uploader"
	"github.com/vmware-tanzu/velero/pkg/util/filesystem"
	"github.com/vmware-tanzu/velero/pkg/util/kube"
)

var getVolumeDirectory = kube.GetVolumeDirectory
var getVolumeMode = kube.GetVolumeMode
var singlePathMatch = kube.SinglePathMatch

// GetPodVolumeHostPath returns a path that can be accessed from the host for a given volume of a pod
func GetPodVolumeHostPath(ctx context.Context, hostRoot string, pod *corev1api.Pod, volumeName string,
	kubeClient kubernetes.Interface, fs filesystem.Interface, log logrus.FieldLogger) (datapath.AccessPoint, error) {
	logger := log.WithField("pod name", pod.Name).WithField("pod UID", pod.GetUID()).WithField("volume", volumeName)

	volDir, err := getVolumeDirectory(ctx, logger, pod, volumeName, kubeClient)
	if err != nil {
		return datapath.AccessPoint{}, errors.Wrapf(err, "error getting volume directory name for volume %s in pod %s", volumeName, pod.Name)
	}

	logger.WithField("volDir", volDir).Info("Got volume dir")

	volMode, err := getVolumeMode(ctx, logger, pod, volumeName, kubeClient)
	if err != nil {
		return datapath.AccessPoint{}, errors.Wrapf(err, "error getting volume mode for volume %s in pod %s", volumeName, pod.Name)
	}

	volSubDir := "volumes"
	if volMode == uploader.PersistentVolumeBlock {
		volSubDir = "volumeDevices"
	}

	path := ""
	_, err = fs.Stat("/host_pods")
	if os.IsNotExist(err) {
		root := "/var/lib/kubelet/pods"
		if hostRoot != "" {
			root = hostRoot
		}

		path = fmt.Sprintf("%s/%s/%s/kubernetes.io~csi/%s", root, string(pod.GetUID()), volSubDir, volDir)
		logger.WithField("path", path).Info("Using static path for CSI")
	} else if err != nil {
		return datapath.AccessPoint{}, errors.Wrap(err, "error locating host path")
	} else {
		pathGlob := fmt.Sprintf("/host_pods/%s/%s/*/%s", string(pod.GetUID()), volSubDir, volDir)
		logger.WithField("pathGlob", pathGlob).Debug("Looking for path matching glob")

		path, err = singlePathMatch(pathGlob, fs, logger)
		if err != nil {
			return datapath.AccessPoint{}, errors.Wrapf(err, "error identifying unique volume path on host for volume %s in pod %s", volumeName, pod.Name)
		}

		logger.WithField("path", path).Info("Found path matching glob")
	}

	return datapath.AccessPoint{
		ByPath:  path,
		VolMode: volMode,
	}, nil
}

// GetPodVolumeHostPathForCSI returns a path that can be accessed from the host for a given CSI volume of a pod
func GetPodVolumeHostPathForCSI(ctx context.Context, hostRoot string, pod *corev1api.Pod, volumeName string,
	kubeClient kubernetes.Interface, log logrus.FieldLogger) (datapath.AccessPoint, error) {
	logger := log.WithField("pod name", pod.Name).WithField("pod UID", pod.GetUID()).WithField("volume", volumeName)

	volDir, err := getVolumeDirectory(ctx, logger, pod, volumeName, kubeClient)
	if err != nil {
		return datapath.AccessPoint{}, errors.Wrapf(err, "error getting volume directory name for volume %s in pod %s", volumeName, pod.Name)
	}

	logger.WithField("volDir", volDir).Info("Got volume dir")

	volMode, err := getVolumeMode(ctx, logger, pod, volumeName, kubeClient)
	if err != nil {
		return datapath.AccessPoint{}, errors.Wrapf(err, "error getting volume mode for volume %s in pod %s", volumeName, pod.Name)
	}

	volSubDir := "volumes"
	if volMode == uploader.PersistentVolumeBlock {
		volSubDir = "volumeDevices"
	}

	root := "/var/lib/kubelet/pods"
	if hostRoot != "" {
		root = hostRoot
	}

	path := fmt.Sprintf("%s/%s/%s/kubernetes.io~csi/%s", root, string(pod.GetUID()), volSubDir, volDir)
	logger.WithField("path", path).Info("Static path for CSI")

	return datapath.AccessPoint{
		ByPath:  path,
		VolMode: volMode,
	}, nil
}
