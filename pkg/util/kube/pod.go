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
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1api "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
)

// IsPodRunning does a well-rounded check to make sure the specified pod is running stably.
// If not, return the error found
func IsPodRunning(pod *corev1api.Pod) error {
	return isPodScheduledInStatus(pod, func(pod *corev1api.Pod) error {
		if pod.Status.Phase != corev1api.PodRunning {
			return errors.New("pod is not running")
		}

		return nil
	})
}

// IsPodScheduled does a well-rounded check to make sure the specified pod has been scheduled into a node and in a stable status.
// If not, return the error found
func IsPodScheduled(pod *corev1api.Pod) error {
	return isPodScheduledInStatus(pod, func(pod *corev1api.Pod) error {
		if pod.Status.Phase != corev1api.PodRunning && pod.Status.Phase != corev1api.PodPending {
			return errors.New("pod is not running or pending")
		}

		return nil
	})
}

func isPodScheduledInStatus(pod *corev1api.Pod, statusCheckFunc func(*corev1api.Pod) error) error {
	if pod == nil {
		return errors.New("invalid input pod")
	}

	if pod.Spec.NodeName == "" {
		return errors.Errorf("pod is not scheduled, name=%s, namespace=%s, phase=%s", pod.Name, pod.Namespace, pod.Status.Phase)
	}

	if err := statusCheckFunc(pod); err != nil {
		return errors.Wrapf(err, "pod is not in the expected status, name=%s, namespace=%s, phase=%s", pod.Name, pod.Namespace, pod.Status.Phase)
	}

	if pod.DeletionTimestamp != nil {
		return errors.Errorf("pod is being terminated, name=%s, namespace=%s, phase=%s", pod.Name, pod.Namespace, pod.Status.Phase)
	}

	return nil
}

// DeletePodIfAny deletes a pod by name if it exists, and log an error when the deletion fails
func DeletePodIfAny(ctx context.Context, podGetter corev1client.CoreV1Interface, podName string, podNamespace string, log logrus.FieldLogger) {
	err := podGetter.Pods(podNamespace).Delete(ctx, podName, metav1.DeleteOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.WithError(err).Debugf("Abort deleting pod, it doesn't exist %s/%s", podNamespace, podName)
		} else {
			log.WithError(err).Errorf("Failed to delete pod %s/%s", podNamespace, podName)
		}
	}
}

// EnsureDeletePod asserts the existence of a pod by name, deletes it and waits for its disappearance and returns errors on any failure
func EnsureDeletePod(ctx context.Context, podGetter corev1client.CoreV1Interface, pod string, namespace string, timeout time.Duration) error {
	err := podGetter.Pods(namespace).Delete(ctx, pod, metav1.DeleteOptions{})
	if err != nil {
		return errors.Wrapf(err, "error to delete pod %s", pod)
	}

	err = wait.PollImmediate(waitInternal, timeout, func() (bool, error) {
		_, err := podGetter.Pods(namespace).Get(ctx, pod, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}

			return false, errors.Wrapf(err, "error to get pod %s", pod)
		}

		return false, nil
	})

	if err != nil {
		return errors.Wrapf(err, "error to assure pod is deleted, %s", pod)
	}

	return nil
}

// IsPodUnrecoverable checks if the pod is in an abnormal state and could not be recovered
// It could not cover all the cases but we could add more cases in the future
func IsPodUnrecoverable(pod *corev1api.Pod, log logrus.FieldLogger) (bool, string) {
	// Check the Status field
	message := ""
	for _, containerStatus := range pod.Status.ContainerStatuses {
		// If the container's image state is ImagePullBackOff, it indicates an image pull failure
		if containerStatus.State.Waiting != nil && (containerStatus.State.Waiting.Reason == "ImagePullBackOff" || containerStatus.State.Waiting.Reason == "ErrImageNeverPull") {
			return true, fmt.Sprintf("Container %s in Pod %s/%s is in pull image failed with reason %s", containerStatus.Name, pod.Namespace, pod.Name, containerStatus.State.Waiting.Reason)
		}

		if containerStatus.State.Terminated != nil {
			message += containerStatus.State.Terminated.Message + "/"
		}
	}

	// Check the Phase field
	if pod.Status.Phase == corev1api.PodFailed || pod.Status.Phase == corev1api.PodUnknown {
		return true, fmt.Sprintf("Pod is in abnormal state [%s], message [%s]", pod.Status.Phase, message)
	}

	return false, ""
}

func GetPodTerminateMessage(pod *corev1api.Pod, container string) string {
	message := ""
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.Name == container {
			if containerStatus.State.Terminated != nil {
				message = containerStatus.State.Terminated.Message
			}
			break
		}
	}

	return message
}

func CollectPodLogs(ctx context.Context, podGetter corev1client.CoreV1Interface, pod string, namespace string, container string, includePrevious bool, output io.Writer) error {
	logIndicator := fmt.Sprintf("***************************begin pod logs[%s/%s]***************************\n", pod, container)

	logOptions := &corev1api.PodLogOptions{
		Container: container,
	}

	request := podGetter.Pods(namespace).GetLogs(pod, logOptions)
	input, err := request.Stream(ctx)
	if err != nil {
		logIndicator += fmt.Sprintf("No present log retrieved, err: %v\n", err)
		input = nil
	}

	if _, err := output.Write([]byte(logIndicator)); err != nil {
		return errors.Wrap(err, "error to write begin pod log indicator")
	}

	if input != nil {
		if _, err := io.Copy(output, input); err != nil {
			return errors.Wrap(err, "error to copy input")
		}
	}

	if includePrevious {
		logIndicator = "***************************previous logs***************************\n"

		logOptions.Previous = true
		request = podGetter.Pods(namespace).GetLogs(pod, logOptions)
		input, err = request.Stream(ctx)
		if err != nil {
			logIndicator += fmt.Sprintf("No previous log retrieved, err: %v\n", err)
			input = nil
		}

		if _, err := output.Write([]byte(logIndicator)); err != nil {
			return errors.Wrap(err, "error to write previous pod log indicator")
		}

		if input != nil {
			if _, err := io.Copy(output, input); err != nil {
				return errors.Wrap(err, "error to copy input for previous log")
			}
		}
	}

	logIndicator = fmt.Sprintf("***************************end pod logs[%s/%s]***************************\n", pod, container)
	if _, err := output.Write([]byte(logIndicator)); err != nil {
		return errors.Wrap(err, "error to write end pod log indicator")
	}

	return nil
}
