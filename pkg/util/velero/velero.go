/*
Copyright the Velero contributors.

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

package velero

import (
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
)

// GetNodeSelectorFromVeleroServer get the node selector from the Velero server deployment
func GetNodeSelectorFromVeleroServer(deployment *appsv1.Deployment) map[string]string {
	return deployment.Spec.Template.Spec.NodeSelector
}

// GetTolerationsFromVeleroServer get the tolerations from the Velero server deployment
func GetTolerationsFromVeleroServer(deployment *appsv1.Deployment) []v1.Toleration {
	return deployment.Spec.Template.Spec.Tolerations
}

// GetAffinityFromVeleroServer get the affinity from the Velero server deployment
func GetAffinityFromVeleroServer(deployment *appsv1.Deployment) *v1.Affinity {
	return deployment.Spec.Template.Spec.Affinity
}

// GetEnvVarsFromVeleroServer get the environment variables from the Velero server deployment
func GetEnvVarsFromVeleroServer(deployment *appsv1.Deployment) []v1.EnvVar {
	for _, container := range deployment.Spec.Template.Spec.Containers {
		// We only have one container in the Velero server deployment
		return container.Env
	}
	return nil
}

// GetVolumeMountsFromVeleroServer get the volume mounts from the Velero server deployment
func GetVolumeMountsFromVeleroServer(deployment *appsv1.Deployment) []v1.VolumeMount {
	for _, container := range deployment.Spec.Template.Spec.Containers {
		// We only have one container in the Velero server deployment
		return container.VolumeMounts
	}
	return nil
}

// GetVolumesFromVeleroServer get the volumes from the Velero server deployment
func GetVolumesFromVeleroServer(deployment *appsv1.Deployment) []v1.Volume {
	return deployment.Spec.Template.Spec.Volumes
}

// GetServiceAccountFromVeleroServer get the service account from the Velero server deployment
func GetServiceAccountFromVeleroServer(deployment *appsv1.Deployment) string {
	return deployment.Spec.Template.Spec.ServiceAccountName
}

// getVeleroServerImage get the image of the Velero server deployment
func GetVeleroServerImage(deployment *appsv1.Deployment) string {
	return deployment.Spec.Template.Spec.Containers[0].Image
}

// GetEnvVarsFromNodeAgent get the environment variables from the node-agent daemonset
func GetEnvVarsFromNodeAgent(ds *appsv1.DaemonSet) []v1.EnvVar {
	for _, container := range ds.Spec.Template.Spec.Containers {
		// We only have one container in the node-agent daemonset
		return container.Env
	}
	return nil
}

// GetMountsFromNodeAgent get the volume mounts from the node-agent daemonset
func GetMountsFromNodeAgent(ds *appsv1.DaemonSet) []v1.VolumeMount {
	for _, container := range ds.Spec.Template.Spec.Containers {
		// We only have one container in the node-agent daemonset
		return container.VolumeMounts
	}
	return nil
}

// GetVolumesFromNodeAgent get the volumes from the node-agent daemonset
func GetVolumesFromNodeAgent(ds *appsv1.DaemonSet) []v1.Volume {
	return ds.Spec.Template.Spec.Volumes
}

// GetServiceAccountFromNodeAgent get the service account from the node-agent daemonset
func GetServiceAccountFromNodeAgent(ds *appsv1.DaemonSet) string {
	return ds.Spec.Template.Spec.ServiceAccountName
}

// GetNodeAgentImage get the image of the node-agent daemonset
func GetNodeAgentImage(ds *appsv1.DaemonSet) string {
	return ds.Spec.Template.Spec.Containers[0].Image
}
