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
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	corev1api "k8s.io/api/core/v1"
	clientTesting "k8s.io/client-go/testing"
)

func TestRestoreExpose(t *testing.T) {
	restore := &velerov1.Restore{
		TypeMeta: metav1.TypeMeta{
			APIVersion: velerov1.SchemeGroupVersion.String(),
			Kind:       "Restore",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: velerov1.DefaultNamespace,
			Name:      "fake-restore",
			UID:       "fake-uid",
		},
	}

	targetPVCObj := &corev1api.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "fake-ns",
			Name:      "fake-target-pvc",
		},
	}

	targetPVCObjBound := &corev1api.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "fake-ns",
			Name:      "fake-target-pvc",
		},
		Spec: corev1api.PersistentVolumeClaimSpec{
			VolumeName: "fake-pv",
		},
	}

	daemonSet := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "velero",
			Name:      "node-agent",
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "DaemonSet",
			APIVersion: appsv1.SchemeGroupVersion.String(),
		},
		Spec: appsv1.DaemonSetSpec{},
	}

	tests := []struct {
		name            string
		kubeClientObj   []runtime.Object
		ownerRestore    *velerov1.Restore
		targetPVCName   string
		sourceNamespace string
		kubeReactors    []reactor
		err             string
	}{
		{
			name:            "wait target pvc consumed fail",
			targetPVCName:   "fake-target-pvc",
			sourceNamespace: "fake-ns",
			ownerRestore:    restore,
			err:             "error to wait target PVC consumed, fake-ns/fake-target-pvc: error to wait for PVC: error to get pvc fake-ns/fake-target-pvc: persistentvolumeclaims \"fake-target-pvc\" not found",
		},
		{
			name:            "target pvc is already bound",
			targetPVCName:   "fake-target-pvc",
			sourceNamespace: "fake-ns",
			ownerRestore:    restore,
			kubeClientObj: []runtime.Object{
				targetPVCObjBound,
			},
			err: "Target PVC fake-ns/fake-target-pvc has already been bound, abort",
		},
		{
			name:            "create restore pod fail",
			targetPVCName:   "fake-target-pvc",
			sourceNamespace: "fake-ns",
			ownerRestore:    restore,
			kubeClientObj: []runtime.Object{
				targetPVCObj,
				daemonSet,
			},
			kubeReactors: []reactor{
				{
					verb:     "create",
					resource: "pods",
					reactorFunc: func(action clientTesting.Action) (handled bool, ret runtime.Object, err error) {
						return true, nil, errors.New("fake-create-error")
					},
				},
			},
			err: "error to create restore pod: fake-create-error",
		},
		{
			name:            "create restore pvc fail",
			targetPVCName:   "fake-target-pvc",
			sourceNamespace: "fake-ns",
			ownerRestore:    restore,
			kubeClientObj: []runtime.Object{
				targetPVCObj,
				daemonSet,
			},
			kubeReactors: []reactor{
				{
					verb:     "create",
					resource: "persistentvolumeclaims",
					reactorFunc: func(action clientTesting.Action) (handled bool, ret runtime.Object, err error) {
						return true, nil, errors.New("fake-create-error")
					},
				},
			},
			err: "error to create restore pvc: fake-create-error",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fakeKubeClient := fake.NewSimpleClientset(test.kubeClientObj...)

			for _, reactor := range test.kubeReactors {
				fakeKubeClient.Fake.PrependReactor(reactor.verb, reactor.resource, reactor.reactorFunc)
			}

			exposer := genericRestoreExposer{
				kubeClient: fakeKubeClient,
				log:        velerotest.NewLogger(),
			}

			var ownerObject corev1api.ObjectReference
			if test.ownerRestore != nil {
				ownerObject = corev1api.ObjectReference{
					Kind:       test.ownerRestore.Kind,
					Namespace:  test.ownerRestore.Namespace,
					Name:       test.ownerRestore.Name,
					UID:        test.ownerRestore.UID,
					APIVersion: test.ownerRestore.APIVersion,
				}
			}

			err := exposer.Expose(context.Background(), ownerObject, test.targetPVCName, test.sourceNamespace, map[string]string{}, time.Millisecond)
			assert.EqualError(t, err, test.err)
		})
	}
}

func TestRebindVolume(t *testing.T) {
	restore := &velerov1.Restore{
		TypeMeta: metav1.TypeMeta{
			APIVersion: velerov1.SchemeGroupVersion.String(),
			Kind:       "Restore",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: velerov1.DefaultNamespace,
			Name:      "fake-restore",
			UID:       "fake-uid",
		},
	}

	targetPVCObj := &corev1api.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "fake-ns",
			Name:      "fake-target-pvc",
		},
	}

	restorePVCObj := &corev1api.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: velerov1.DefaultNamespace,
			Name:      "fake-restore",
		},
		Spec: corev1api.PersistentVolumeClaimSpec{
			VolumeName: "fake-restore-pv",
		},
	}

	restorePVObj := &corev1api.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "fake-restore-pv",
		},
		Spec: corev1api.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1api.PersistentVolumeReclaimDelete,
		},
	}

	restorePod := &corev1api.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: velerov1.DefaultNamespace,
			Name:      "fake-restore",
		},
	}

	hookCount := 0

	tests := []struct {
		name            string
		kubeClientObj   []runtime.Object
		ownerRestore    *velerov1.Restore
		targetPVCName   string
		sourceNamespace string
		kubeReactors    []reactor
		err             string
	}{
		{
			name:            "get target pvc fail",
			targetPVCName:   "fake-target-pvc",
			sourceNamespace: "fake-ns",
			ownerRestore:    restore,
			err:             "error to get target PVC fake-ns/fake-target-pvc: persistentvolumeclaims \"fake-target-pvc\" not found",
		},
		{
			name:            "wait restore pvc bound fail",
			targetPVCName:   "fake-target-pvc",
			sourceNamespace: "fake-ns",
			ownerRestore:    restore,
			kubeClientObj: []runtime.Object{
				targetPVCObj,
			},
			err: "error to get PV from restore PVC fake-restore: error to wait for rediness of PVC: error to get pvc velero/fake-restore: persistentvolumeclaims \"fake-restore\" not found",
		},
		{
			name:            "retain target pv fail",
			targetPVCName:   "fake-target-pvc",
			sourceNamespace: "fake-ns",
			ownerRestore:    restore,
			kubeClientObj: []runtime.Object{
				targetPVCObj,
				restorePVCObj,
				restorePVObj,
			},
			kubeReactors: []reactor{
				{
					verb:     "patch",
					resource: "persistentvolumes",
					reactorFunc: func(action clientTesting.Action) (handled bool, ret runtime.Object, err error) {
						return true, nil, errors.New("fake-patch-error")
					},
				},
			},
			err: "error to retain PV fake-restore-pv: error patching PV: fake-patch-error",
		},
		{
			name:            "delete restore pod fail",
			targetPVCName:   "fake-target-pvc",
			sourceNamespace: "fake-ns",
			ownerRestore:    restore,
			kubeClientObj: []runtime.Object{
				targetPVCObj,
				restorePVCObj,
				restorePVObj,
				restorePod,
			},
			kubeReactors: []reactor{
				{
					verb:     "delete",
					resource: "pods",
					reactorFunc: func(action clientTesting.Action) (handled bool, ret runtime.Object, err error) {
						return true, nil, errors.New("fake-delete-error")
					},
				},
			},
			err: "error to delete restore pod fake-restore: error to delete pod fake-restore: fake-delete-error",
		},
		{
			name:            "delete restore pvc fail",
			targetPVCName:   "fake-target-pvc",
			sourceNamespace: "fake-ns",
			ownerRestore:    restore,
			kubeClientObj: []runtime.Object{
				targetPVCObj,
				restorePVCObj,
				restorePVObj,
				restorePod,
			},
			kubeReactors: []reactor{
				{
					verb:     "delete",
					resource: "persistentvolumeclaims",
					reactorFunc: func(action clientTesting.Action) (handled bool, ret runtime.Object, err error) {
						return true, nil, errors.New("fake-delete-error")
					},
				},
			},
			err: "error to delete restore PVC fake-restore: error to delete pvc fake-restore: fake-delete-error",
		},
		{
			name:            "rebind target pvc fail",
			targetPVCName:   "fake-target-pvc",
			sourceNamespace: "fake-ns",
			ownerRestore:    restore,
			kubeClientObj: []runtime.Object{
				targetPVCObj,
				restorePVCObj,
				restorePVObj,
				restorePod,
			},
			kubeReactors: []reactor{
				{
					verb:     "patch",
					resource: "persistentvolumeclaims",
					reactorFunc: func(action clientTesting.Action) (handled bool, ret runtime.Object, err error) {
						return true, nil, errors.New("fake-patch-error")
					},
				},
			},
			err: "error to rebind target PVC fake-ns/fake-target-pvc to fake-restore-pv: error patching PVC: fake-patch-error",
		},
		{
			name:            "reset pv binding fail",
			targetPVCName:   "fake-target-pvc",
			sourceNamespace: "fake-ns",
			ownerRestore:    restore,
			kubeClientObj: []runtime.Object{
				targetPVCObj,
				restorePVCObj,
				restorePVObj,
				restorePod,
			},
			kubeReactors: []reactor{
				{
					verb:     "patch",
					resource: "persistentvolumes",
					reactorFunc: func(action clientTesting.Action) (handled bool, ret runtime.Object, err error) {
						if hookCount == 0 {
							hookCount++
							return false, nil, nil
						} else {
							return true, nil, errors.New("fake-patch-error")
						}
					},
				},
			},
			err: "error to reset binding info for restore PV fake-restore-pv: error patching PV: fake-patch-error",
		},
		{
			name:            "wait restore PV bound fail",
			targetPVCName:   "fake-target-pvc",
			sourceNamespace: "fake-ns",
			ownerRestore:    restore,
			kubeClientObj: []runtime.Object{
				targetPVCObj,
				restorePVCObj,
				restorePVObj,
				restorePod,
			},
			err: "error to wait restore PV bound, restore PV fake-restore-pv: error to wait for bound of PV: context deadline exceeded",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fakeKubeClient := fake.NewSimpleClientset(test.kubeClientObj...)

			for _, reactor := range test.kubeReactors {
				fakeKubeClient.Fake.PrependReactor(reactor.verb, reactor.resource, reactor.reactorFunc)
			}

			exposer := genericRestoreExposer{
				kubeClient: fakeKubeClient,
				log:        velerotest.NewLogger(),
			}

			var ownerObject corev1api.ObjectReference
			if test.ownerRestore != nil {
				ownerObject = corev1api.ObjectReference{
					Kind:       test.ownerRestore.Kind,
					Namespace:  test.ownerRestore.Namespace,
					Name:       test.ownerRestore.Name,
					UID:        test.ownerRestore.UID,
					APIVersion: test.ownerRestore.APIVersion,
				}
			}

			hookCount = 0

			err := exposer.RebindVolume(context.Background(), ownerObject, test.targetPVCName, test.sourceNamespace, time.Millisecond)
			assert.EqualError(t, err, test.err)
		})
	}
}

func TestRestorePeekExpose(t *testing.T) {
	restore := &velerov1.Restore{
		TypeMeta: metav1.TypeMeta{
			APIVersion: velerov1.SchemeGroupVersion.String(),
			Kind:       "Restore",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: velerov1.DefaultNamespace,
			Name:      "fake-restore",
			UID:       "fake-uid",
		},
	}

	restorePodUrecoverable := &corev1api.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: restore.Namespace,
			Name:      restore.Name,
		},
		Status: corev1api.PodStatus{
			Phase: corev1api.PodFailed,
		},
	}

	restorePod := &corev1api.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: restore.Namespace,
			Name:      restore.Name,
		},
	}

	tests := []struct {
		name          string
		kubeClientObj []runtime.Object
		ownerRestore  *velerov1.Restore
		err           string
	}{
		{
			name:         "restore pod is not found",
			ownerRestore: restore,
		},
		{
			name:         "pod is unrecoverable",
			ownerRestore: restore,
			kubeClientObj: []runtime.Object{
				restorePodUrecoverable,
			},
			err: "Pod is in abnormal state Failed",
		},
		{
			name:         "succeed",
			ownerRestore: restore,
			kubeClientObj: []runtime.Object{
				restorePod,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fakeKubeClient := fake.NewSimpleClientset(test.kubeClientObj...)

			exposer := genericRestoreExposer{
				kubeClient: fakeKubeClient,
				log:        velerotest.NewLogger(),
			}

			var ownerObject corev1api.ObjectReference
			if test.ownerRestore != nil {
				ownerObject = corev1api.ObjectReference{
					Kind:       test.ownerRestore.Kind,
					Namespace:  test.ownerRestore.Namespace,
					Name:       test.ownerRestore.Name,
					UID:        test.ownerRestore.UID,
					APIVersion: test.ownerRestore.APIVersion,
				}
			}

			err := exposer.PeekExposed(context.Background(), ownerObject)
			if test.err == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, test.err)
			}
		})
	}
}

func TestRestoreDiagnoseExpose(t *testing.T) {
	restore := &velerov1.Restore{
		TypeMeta: metav1.TypeMeta{
			APIVersion: velerov1.SchemeGroupVersion.String(),
			Kind:       "Restore",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: velerov1.DefaultNamespace,
			Name:      "fake-restore",
			UID:       "fake-uid",
		},
	}

	restorePodUrecoverable := &corev1api.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: restore.Namespace,
			Name:      restore.Name,
		},
		Status: corev1api.PodStatus{
			Phase: corev1api.PodFailed,
		},
	}

	restorePodUnSchedulable := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: restore.Namespace,
			Name:      restore.Name,
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{
				{
					Type:    corev1.PodScheduled,
					Reason:  "Unschedulable",
					Message: "fake-unschedulable-message",
				},
			},
		},
	}

	restorePodRunning := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: restore.Namespace,
			Name:      restore.Name,
		},

		Spec: corev1.PodSpec{
			NodeName: "node-1",
		},

		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}

	restorePod := &corev1api.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: restore.Namespace,
			Name:      restore.Name,
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodSucceeded,
		},
	}

	tests := []struct {
		name          string
		kubeClientObj []runtime.Object
		ownerRestore  *velerov1.Restore
		expectedMsg   string
	}{
		{
			name:         "failed to peek backup pod",
			ownerRestore: restore,
			expectedMsg:  fmt.Sprintf("error to peek restore pod %s, err: pods \"fake-restore\" not found", restore.Name),
		},
		{
			name:         "pod is unrecoverable",
			ownerRestore: restore,
			kubeClientObj: []runtime.Object{
				restorePodUrecoverable,
			},
			expectedMsg: "Pod is in abnormal state Failed",
		},
		{
			name:         "pod is unschedulable",
			ownerRestore: restore,
			kubeClientObj: []runtime.Object{
				restorePodUnSchedulable,
			},
			expectedMsg: "Pod is unschedulable: fake-unschedulable-message",
		},
		{
			name:         "node-agent is not running",
			ownerRestore: restore,
			kubeClientObj: []runtime.Object{
				restorePodRunning,
			},
			expectedMsg: "node-agent is not running in node node-1",
		},
		{
			name:         "no info",
			ownerRestore: restore,
			kubeClientObj: []runtime.Object{
				restorePod,
			},
			expectedMsg: fmt.Sprintf("unkonwn condition for restore pod %s in phase %s", restore.Name, corev1.PodSucceeded),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fakeKubeClient := fake.NewSimpleClientset(test.kubeClientObj...)

			exposer := genericRestoreExposer{
				kubeClient: fakeKubeClient,
				log:        velerotest.NewLogger(),
			}

			var ownerObject corev1api.ObjectReference
			if test.ownerRestore != nil {
				ownerObject = corev1api.ObjectReference{
					Kind:       test.ownerRestore.Kind,
					Namespace:  test.ownerRestore.Namespace,
					Name:       test.ownerRestore.Name,
					UID:        test.ownerRestore.UID,
					APIVersion: test.ownerRestore.APIVersion,
				}
			}

			msg := exposer.DiagnoseExpose(context.Background(), ownerObject)
			assert.Equal(t, test.expectedMsg, msg)
		})
	}
}
