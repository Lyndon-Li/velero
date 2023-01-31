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

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1api "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
)

// IsPodRunning does a well-rounded check to make sure the specified pod is running stably.
// If not, return the error found
func IsPodRunning(pod *corev1api.Pod) error {
	if pod.Spec.NodeName == "" {
		return errors.Errorf("pod is not scheduled, name=%s, namespace=%s, phase=%s", pod.Name, pod.Namespace, pod.Status.Phase)
	}

	if pod.Status.Phase != corev1api.PodRunning {
		return errors.Errorf("pod is not running, name=%s, namespace=%s, phase=%s", pod.Name, pod.Namespace, pod.Status.Phase)
	}

	if pod.DeletionTimestamp != nil {
		return errors.Errorf("pod is being terminated, name=%s, namespace=%s, phase=%s", pod.Name, pod.Namespace, pod.Status.Phase)
	}

	return nil
}

func DeletePodIfAny(ctx context.Context, podGetter corev1client.PodsGetter, podName string, podNamespace string, log logrus.FieldLogger) {
	pod, err := podGetter.Pods(podNamespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.WithError(err).Errorf("Failed to get volume snapshot %s/%s", podNamespace, podName)
		}

		return
	}

	err = podGetter.Pods(pod.Namespace).Delete(ctx, pod.Name, *&metav1.DeleteOptions{})
	if err != nil {
		log.WithError(err).Errorf("Failed to delete pod %s/%s", pod.Namespace, pod.Name)
	}
}
