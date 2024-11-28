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
	corev1api "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	NodeOSLinux   = "linux"
	NodeOSWindows = "windows"
	NodeOSLabel   = "kubernetes.io/os"
)

func IsLinuxNode(ctx context.Context, nodeName string, client client.Client) error {
	node := &corev1api.Node{}
	if err := client.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
		return errors.Wrapf(err, "error getting node %s", nodeName)
	}

	if os, found := node.Labels[NodeOSLabel]; !found {
		return errors.Errorf("no os type label for node %s", nodeName)
	} else if os != NodeOSLinux {
		return errors.Errorf("os type %s for node %s is not linux", os, nodeName)
	} else {
		return nil
	}
}

func GetNodeOS(ctx context.Context, nodeName string, nodeClient corev1client.CoreV1Interface) (string, error) {
	node, err := nodeClient.Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})
	if err != nil {
		return "", errors.Wrapf(err, "error getting node %s", nodeName)
	}

	if os, found := node.Labels[NodeOSLabel]; found {
		return os, nil
	} else {
		return "", nil
	}
}
