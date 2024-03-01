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

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/vmware-tanzu/velero/pkg/nodeagent"
)

type inheritedPodInfo struct {
	image          string
	serviceAccount string
	env            []v1.EnvVar
	volumeMounts   []v1.VolumeMount
	volumes        []v1.Volume
	logLevel       string
	logFormat      string
}

func getInheritedPodInfo(ctx context.Context, client kubernetes.Interface, veleroNamespace string) (inheritedPodInfo, error) {
	podInfo := inheritedPodInfo{}

	podSpec, err := nodeagent.GetPodSpec(ctx, client, veleroNamespace)
	if err != nil {
		return podInfo, errors.Wrap(err, "error to get node-agent pod template")
	}

	if len(podSpec.Containers) != 1 {
		return podInfo, errors.Wrap(err, "unexpected pod template from node-agent")
	}

	podInfo.image = podSpec.Containers[0].Image
	podInfo.serviceAccount = podSpec.ServiceAccountName

	podInfo.env = podSpec.Containers[0].Env
	podInfo.volumeMounts = podSpec.Containers[0].VolumeMounts
	podInfo.volumes = podSpec.Volumes

	command := podSpec.Containers[0].Command
	for i, flag := range command {
		if flag == "log-format" {
			podInfo.logFormat = command[i+1]
		} else if flag == "log-level" {
			podInfo.logLevel = command[i+1]
		}
	}

	return podInfo, nil
}
