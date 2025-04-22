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

package controller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	corev1api "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	clocks "k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/vmware-tanzu/velero/internal/credentials"
	veleroapishared "github.com/vmware-tanzu/velero/pkg/apis/velero/shared"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/datapath"
	"github.com/vmware-tanzu/velero/pkg/exposer"
	"github.com/vmware-tanzu/velero/pkg/nodeagent"
	"github.com/vmware-tanzu/velero/pkg/repository"
	"github.com/vmware-tanzu/velero/pkg/restorehelper"
	"github.com/vmware-tanzu/velero/pkg/uploader"
	"github.com/vmware-tanzu/velero/pkg/util"
	"github.com/vmware-tanzu/velero/pkg/util/boolptr"
	"github.com/vmware-tanzu/velero/pkg/util/filesystem"
	"github.com/vmware-tanzu/velero/pkg/util/kube"
)

func NewPodVolumeRestoreReconciler(client client.Client, mgr manager.Manager, kubeClient kubernetes.Interface, dataPathMgr *datapath.Manager,
	ensurer *repository.Ensurer, credentialGetter *credentials.CredentialGetter, nodeName string, podResources corev1.ResourceRequirements,
	logger logrus.FieldLogger) *PodVolumeRestoreReconciler {
	return &PodVolumeRestoreReconciler{
		client:            client,
		mgr:               mgr,
		kubeClient:        kubeClient,
		logger:            logger.WithField("controller", "PodVolumeRestore"),
		repositoryEnsurer: ensurer,
		credentialGetter:  credentialGetter,
		nodeName:          nodeName,
		fileSystem:        filesystem.NewFileSystem(),
		clock:             &clocks.RealClock{},
		podResources:      podResources,
		dataPathMgr:       dataPathMgr,
	}
}

type PodVolumeRestoreReconciler struct {
	client            client.Client
	mgr               manager.Manager
	kubeClient        kubernetes.Interface
	logger            logrus.FieldLogger
	repositoryEnsurer *repository.Ensurer
	credentialGetter  *credentials.CredentialGetter
	nodeName          string
	fileSystem        filesystem.Interface
	clock             clocks.WithTickerAndDelayedExecution
	podResources      corev1.ResourceRequirements
	exposer           exposer.PodVolumeExposer
	dataPathMgr       *datapath.Manager
}

// +kubebuilder:rbac:groups=velero.io,resources=podvolumerestores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=velero.io,resources=podvolumerestores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get
// +kubebuilder:rbac:groups="",resources=persistentvolumerclaims,verbs=get

func (c *PodVolumeRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := c.logger.WithField("PodVolumeRestore", req.NamespacedName.String())

	pvr := &velerov1api.PodVolumeRestore{}
	if err := c.client.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: req.Name}, pvr); err != nil {
		if apierrors.IsNotFound(err) {
			log.Warn("PodVolumeRestore not found, skip")
			return ctrl.Result{}, nil
		}
		log.WithError(err).Error("Unable to get the PodVolumeRestore")
		return ctrl.Result{}, err
	}
	log = log.WithField("pod", fmt.Sprintf("%s/%s", pvr.Spec.Pod.Namespace, pvr.Spec.Pod.Name))
	if len(pvr.OwnerReferences) == 1 {
		log = log.WithField("restore", fmt.Sprintf("%s/%s", pvr.Namespace, pvr.OwnerReferences[0].Name))
	}

	shouldProcess, pod, err := c.shouldProcess(ctx, log, pvr)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !shouldProcess {
		return ctrl.Result{}, nil
	}

	initContainerIndex := getInitContainerIndex(pod)
	if initContainerIndex > 0 {
		log.Warnf(`Init containers before the %s container may cause issues
		          if they interfere with volumes being restored: %s index %d`, restorehelper.WaitInitContainer, restorehelper.WaitInitContainer, initContainerIndex)
	}

	// Logic for clear resources when pvb been deleted
	if pvr.DeletionTimestamp.IsZero() { // add finalizer for all cr at beginning
		if !isPVRInFinalState(pvr) && !controllerutil.ContainsFinalizer(pvr, PodVolumeFinalizer) {
			if err := PathPVR(ctx, c.client, pvr, func(pvb *velerov1api.PodVolumeRestore) {
				controllerutil.AddFinalizer(pvb, PodVolumeFinalizer)
			}); err != nil {
				log.Errorf("failed to add finalizer with error %s for %s/%s", err.Error(), pvr.Namespace, pvr.Name)
				return ctrl.Result{}, err
			}
		}
	} else if controllerutil.ContainsFinalizer(pvr, PodVolumeFinalizer) && !pvr.Spec.Cancel && !isPVRInFinalState(pvr) {
		// when delete cr we need to clear up internal resources created by Velero, here we use the cancel mechanism
		// to help clear up resources instead of clear them directly in case of some conflict with Expose action
		log.Warnf("Cancel pvb under phase %s because it is being deleted", pvr.Status.Phase)

		if err := PathPVR(ctx, c.client, pvr, func(pvb *velerov1api.PodVolumeRestore) {
			pvr.Spec.Cancel = true
			pvr.Status.Message = "Cancel pvb because it is being deleted"
		}); err != nil {
			log.Errorf("failed to set cancel flag with error %s for %s/%s", err.Error(), pvr.Namespace, pvr.Name)
			return ctrl.Result{}, err
		}
	}

	if pvr.Status.Phase == "" || pvr.Status.Phase == velerov1api.PodVolumeRestorePhaseNew {
		log.Info("Restore starting")

		exposeParam, err := c.setupExposeParam(pvr)
		if err != nil {
			return c.errorOut(ctx, pvr, err, "failed to set exposer parameters", log)
		}

		if err := c.exposer.Expose(ctx, getPVROwnerObject(pvr), exposeParam); err != nil {
			if err := c.client.Get(ctx, req.NamespacedName, pvr); err != nil {
				if !apierrors.IsNotFound(err) {
					return ctrl.Result{}, errors.Wrap(err, "getting pvr")
				}
			}
			if isPVRInFinalState(pvr) {
				log.Warnf("expose pvr with err %v but it may caused by clean up resources in cancel action", err)
				c.exposer.CleanUp(ctx, getPVROwnerObject(pvr))
				return ctrl.Result{}, nil
			} else {
				return c.errorOut(ctx, pvr, err, "error to expose pvr", log)
			}
		}

		log.Info("PVR is exposed")

		return ctrl.Result{}, nil
	} else if pvr.Status.Phase == velerov1api.PodVolumeRestorePhasePrepared {
		log.Info("PVR is prepared")

		if pvr.Spec.Cancel {
			c.OnDataPathCancelled(ctx, pvr.GetNamespace(), pvr.GetName())
			return ctrl.Result{}, nil
		}

		asyncBR := c.dataPathMgr.GetAsyncBR(pvr.Name)
		if asyncBR != nil {
			log.Info("Cancellable data path is already started")
			return ctrl.Result{}, nil
		}

		res, err := c.exposer.GetExposed(ctx, getPVROwnerObject(pvr), c.client, c.nodeName)
		if err != nil {
			return c.errorOut(ctx, pvr, err, "exposed pvr is not ready", log)
		} else if res == nil {
			log.Debug("Get empty exposer")
			return ctrl.Result{}, nil
		}

		log.Info("Exposed pvr is ready and creating data path routine")

		callbacks := datapath.Callbacks{
			OnCompleted: c.OnDataPathCompleted,
			OnFailed:    c.OnDataPathFailed,
			OnCancelled: c.OnDataPathCancelled,
			OnProgress:  c.OnDataPathProgress,
		}

		asyncBR, err = c.dataPathMgr.CreateMicroServiceBRWatcher(ctx, c.client, c.kubeClient, c.mgr, datapath.TaskTypeBackup,
			pvr.Name, pvr.Namespace, res.ByPod.HostingPod.Name, res.ByPod.HostingContainer, pvr.Name, callbacks, false, log)
		if err != nil {
			if err == datapath.ConcurrentLimitExceed {
				log.Info("Data path instance is concurrent limited requeue later")
				return ctrl.Result{Requeue: true, RequeueAfter: time.Second * 5}, nil
			} else {
				return c.errorOut(ctx, pvr, err, "error to create data path", log)
			}
		}

		if err := c.initCancelableDataPath(ctx, asyncBR, res, log); err != nil {
			log.WithError(err).Errorf("Failed to init cancelable data path for %s", pvr.Name)

			c.closeDataPath(ctx, pvr.Name)
			return c.errorOut(ctx, pvr, err, "error initializing data path", log)
		}

		// Update status to InProgress
		original := pvr.DeepCopy()
		pvr.Status.Phase = velerov1api.PodVolumeRestorePhaseInProgress
		pvr.Status.StartTimestamp = &metav1.Time{Time: c.clock.Now()}
		if err := c.client.Patch(ctx, pvr, client.MergeFrom(original)); err != nil {
			log.WithError(err).Warnf("Failed to update pvb %s to InProgress, will data path close and retry", pvr.Name)

			c.closeDataPath(ctx, pvr.Name)
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second * 5}, nil
		}

		log.Info("pvr is marked as in progress")

		if err := c.startCancelableDataPath(asyncBR, pvr, res, log); err != nil {
			log.WithError(err).Errorf("Failed to start cancelable data path for %s", pvr.Name)
			c.closeDataPath(ctx, pvr.Name)

			return c.errorOut(ctx, pvr, err, "error starting data path", log)
		}

		return ctrl.Result{}, nil
	} else if pvr.Status.Phase == velerov1api.PodVolumeRestorePhaseInProgress {
		log.Info("pvr is in progress")
		if pvr.Spec.Cancel {
			log.Info("pvr is being canceled")

			asyncBR := c.dataPathMgr.GetAsyncBR(pvr.Name)
			if asyncBR == nil {
				c.OnDataPathCancelled(ctx, pvr.GetNamespace(), pvr.GetName())
				return ctrl.Result{}, nil
			}

			// Update status to Canceling
			original := pvr.DeepCopy()
			pvr.Status.Phase = velerov1api.PodVolumeRestorePhaseCanceling
			if err := c.client.Patch(ctx, pvr, client.MergeFrom(original)); err != nil {
				log.WithError(err).Error("error updating pvr into canceling status")
				return ctrl.Result{}, err
			}
			asyncBR.Cancel()
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, nil
	} else {
		if err := PathPVR(ctx, c.client, pvr, func(pvr *velerov1api.PodVolumeRestore) {
			controllerutil.RemoveFinalizer(pvr, PodVolumeFinalizer)
		}); err != nil {
			log.WithError(err).Error("error to remove finalizer")
		}

		return ctrl.Result{}, nil
	}
}

func (c *PodVolumeRestoreReconciler) initCancelableDataPath(ctx context.Context, asyncBR datapath.AsyncBR, res *exposer.ExposeResult, log logrus.FieldLogger) error {
	log.Info("Init cancelable pvr")

	if err := asyncBR.Init(ctx, nil); err != nil {
		return errors.Wrap(err, "error initializing asyncBR")
	}

	log.Infof("async data path init for pod %s, volume %s", res.ByPod.HostingPod.Name, res.ByPod.VolumeName)

	return nil
}

func (c *PodVolumeRestoreReconciler) startCancelableDataPath(asyncBR datapath.AsyncBR, pvr *velerov1api.PodVolumeRestore, res *exposer.ExposeResult, log logrus.FieldLogger) error {
	log.Info("Start cancelable pvr")

	if err := asyncBR.StartBackup(datapath.AccessPoint{
		ByPath: res.ByPod.VolumeName,
	}, pvr.Spec.UploaderSettings, nil); err != nil {
		return errors.Wrapf(err, "error starting async backup for pod %s, volume %s", res.ByPod.HostingPod.Name, res.ByPod.VolumeName)
	}

	log.Infof("Async backup started for pod %s, volume %s", res.ByPod.HostingPod.Name, res.ByPod.VolumeName)
	return nil
}

func (c *PodVolumeRestoreReconciler) errorOut(ctx context.Context, pvr *velerov1api.PodVolumeRestore, err error, msg string, log logrus.FieldLogger) (ctrl.Result, error) {
	c.exposer.CleanUp(ctx, getPVROwnerObject(pvr))

	_ = UpdatePVRStatusToFailed(ctx, c.client, pvr, errors.WithMessage(err, msg).Error(), c.clock.Now(), log)

	return ctrl.Result{}, err
}

func UpdatePVRStatusToFailed(ctx context.Context, c client.Client, pvb *velerov1api.PodVolumeRestore, errString string, time time.Time, log logrus.FieldLogger) error {
	original := pvb.DeepCopy()
	pvb.Status.Phase = velerov1api.PodVolumeRestorePhaseFailed
	pvb.Status.Message = errString
	pvb.Status.CompletionTimestamp = &metav1.Time{Time: time}

	err := c.Patch(ctx, pvb, client.MergeFrom(original))
	if err != nil {
		log.WithError(err).Error("error updating PodVolumeRestore status")
	}

	return err
}

func (c *PodVolumeRestoreReconciler) shouldProcess(ctx context.Context, log logrus.FieldLogger, pvr *velerov1api.PodVolumeRestore) (bool, *corev1api.Pod, error) {
	if !isPVRNew(pvr) {
		log.Debug("PodVolumeRestore is not new, skip")
		return false, nil, nil
	}

	// we filter the pods during the initialization of cache, if we can get a pod here, the pod must be in the same node with the controller
	// so we don't need to compare the node anymore
	pod := &corev1api.Pod{}
	if err := c.client.Get(ctx, types.NamespacedName{Namespace: pvr.Spec.Pod.Namespace, Name: pvr.Spec.Pod.Name}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			log.WithError(err).Debug("Pod not found on this node, skip")
			return false, nil, nil
		}
		log.WithError(err).Error("Unable to get pod")
		return false, nil, err
	}

	if !isInitContainerRunning(pod) {
		log.Debug("Pod is not running restore-wait init container, skip")
		return false, nil, nil
	}

	return true, pod, nil
}

func (c *PodVolumeRestoreReconciler) closeDataPath(ctx context.Context, pvrName string) {
	asyncBR := c.dataPathMgr.GetAsyncBR(pvrName)
	if asyncBR != nil {
		asyncBR.Close(ctx)
	}

	c.dataPathMgr.RemoveAsyncBR(pvrName)
}

func (c *PodVolumeRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// The pod may not being scheduled at the point when its PVRs are initially reconciled.
	// By watching the pods, we can trigger the PVR reconciliation again once the pod is finally scheduled on the node.
	return ctrl.NewControllerManagedBy(mgr).
		For(&velerov1api.PodVolumeRestore{}).
		Watches(&corev1api.Pod{}, handler.EnqueueRequestsFromMapFunc(c.findVolumeRestoresForPod)).
		Watches(&v1.Pod{}, kube.EnqueueRequestsFromMapUpdateFunc(c.findPVRForPod),
			builder.WithPredicates(predicate.Funcs{
				UpdateFunc: func(ue event.UpdateEvent) bool {
					newObj := ue.ObjectNew.(*v1.Pod)

					if _, ok := newObj.Labels[velerov1api.PVRLabel]; !ok {
						return false
					}

					if newObj.Spec.NodeName == "" {
						return false
					}

					return true
				},
				CreateFunc: func(event.CreateEvent) bool {
					return false
				},
				DeleteFunc: func(de event.DeleteEvent) bool {
					return false
				},
				GenericFunc: func(ge event.GenericEvent) bool {
					return false
				},
			})).
		Complete(c)
}

func (c *PodVolumeRestoreReconciler) findVolumeRestoresForPod(ctx context.Context, pod client.Object) []reconcile.Request {
	list := &velerov1api.PodVolumeRestoreList{}
	options := &client.ListOptions{
		LabelSelector: labels.Set(map[string]string{
			velerov1api.PodUIDLabel: string(pod.GetUID()),
		}).AsSelector(),
	}
	if err := c.client.List(context.TODO(), list, options); err != nil {
		c.logger.WithField("pod", fmt.Sprintf("%s/%s", pod.GetNamespace(), pod.GetName())).WithError(err).
			Error("unable to list PodVolumeRestores")
		return []reconcile.Request{}
	}
	requests := make([]reconcile.Request, len(list.Items))
	for i, item := range list.Items {
		requests[i] = reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: item.GetNamespace(),
				Name:      item.GetName(),
			},
		}
	}
	return requests
}

func (c *PodVolumeRestoreReconciler) findPVRForPod(ctx context.Context, podObj client.Object) []reconcile.Request {
	pod := podObj.(*v1.Pod)
	pvr, err := findPVRByPod(c.client, *pod)

	log := c.logger.WithField("pod", pod.Name)
	if err != nil {
		log.WithError(err).Error("unable to get pvr")
		return []reconcile.Request{}
	} else if pvr == nil {
		log.Error("get empty pvr")
		return []reconcile.Request{}
	}
	log = log.WithFields(logrus.Fields{
		"pvr": pvr.Name,
	})

	if !isPVRNew(pvr) {
		return []reconcile.Request{}
	}

	if pod.Status.Phase == v1.PodRunning {
		log.Info("Preparing pvr")
		// we don't expect anyone else update the CR during the Prepare process
		err := PathPVR(context.Background(), c.client, pvr, c.preparePVR)
		if err != nil {
			log.WithError(err).Warn("failed to update pvr, prepare will halt for this pvr")
			return []reconcile.Request{}
		}
	} else if unrecoverable, reason := kube.IsPodUnrecoverable(pod, log); unrecoverable {
		if !pvr.Spec.Cancel {
			if err := PathPVR(context.Background(), c.client, pvr, func(pvr *velerov1api.PodVolumeRestore) {
				pvr.Spec.Cancel = true
				pvr.Status.Message = fmt.Sprintf("Cancel pvr because the exposing pod %s/%s is in abnormal status for reason %s", pod.Namespace, pod.Name, reason)
			}); err != nil {
				log.WithError(err).Warn("failed to cancel pvr, and it will wait for prepare timeout")
				return []reconcile.Request{}
			}
		}

		log.Infof("Exposed pod is in abnormal status(reason %s) and pvr is marked as cancel", reason)
	} else {
		return []reconcile.Request{}
	}

	request := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: pvr.Namespace,
			Name:      pvr.Name,
		},
	}
	return []reconcile.Request{request}
}

func (c *PodVolumeRestoreReconciler) preparePVR(ssb *velerov1api.PodVolumeRestore) {
	ssb.Status.Phase = velerov1api.PodVolumeRestorePhasePrepared
}

func isPVRNew(pvr *velerov1api.PodVolumeRestore) bool {
	return pvr.Status.Phase == "" || pvr.Status.Phase == velerov1api.PodVolumeRestorePhaseNew
}

func isInitContainerRunning(pod *corev1api.Pod) bool {
	// Pod volume wait container can be anywhere in the list of init containers, but must be running.
	i := getInitContainerIndex(pod)
	return i >= 0 &&
		len(pod.Status.InitContainerStatuses)-1 >= i &&
		pod.Status.InitContainerStatuses[i].State.Running != nil
}

func getInitContainerIndex(pod *corev1api.Pod) int {
	// Pod volume wait container can be anywhere in the list of init containers so locate it.
	for i, initContainer := range pod.Spec.InitContainers {
		if initContainer.Name == restorehelper.WaitInitContainer {
			return i
		}
	}

	return -1
}

func (c *PodVolumeRestoreReconciler) OnDataPathCompleted(ctx context.Context, namespace string, pvrName string, result datapath.Result) {
	defer c.dataPathMgr.RemoveAsyncBR(pvrName)

	log := c.logger.WithField("pvr", pvrName)

	log.WithField("PVR", pvrName).Info("Async fs restore data path completed")

	var pvr velerov1api.PodVolumeRestore
	if err := c.client.Get(ctx, types.NamespacedName{Name: pvrName, Namespace: namespace}, &pvr); err != nil {
		log.WithError(err).Warn("Failed to get PVR on completion")
		return
	}

	log.Info("Cleaning up exposed environment")
	c.exposer.CleanUp(ctx, getPVROwnerObject(&pvr))

	volumePath := result.Restore.Target.ByPath
	if volumePath == "" {
		_, _ = c.errorOut(ctx, &pvr, errors.New("path is empty"), "invalid restore target", log)
		return
	}

	// Remove the .velero directory from the restored volume (it may contain done files from previous restores
	// of this volume, which we don't want to carry over). If this fails for any reason, log and continue, since
	// this is non-essential cleanup (the done files are named based on restore UID and the init container looks
	// for the one specific to the restore being executed).
	if err := os.RemoveAll(filepath.Join(volumePath, ".velero")); err != nil {
		log.WithError(err).Warnf("error removing .velero directory from directory %s", volumePath)
	}

	var restoreUID types.UID
	for _, owner := range pvr.OwnerReferences {
		if boolptr.IsSetToTrue(owner.Controller) {
			restoreUID = owner.UID
			break
		}
	}

	// Create the .velero directory within the volume dir so we can write a done file
	// for this restore.
	if err := os.MkdirAll(filepath.Join(volumePath, ".velero"), 0755); err != nil {
		_, _ = c.errorOut(ctx, &pvr, err, "error creating .velero directory for done file", log)
		return
	}

	// Write a done file with name=<restore-uid> into the just-created .velero dir
	// within the volume. The velero init container on the pod is waiting
	// for this file to exist in each restored volume before completing.
	if err := os.WriteFile(filepath.Join(volumePath, ".velero", string(restoreUID)), nil, 0644); err != nil { //nolint:gosec // Internal usage. No need to check.
		_, _ = c.errorOut(ctx, &pvr, err, "error writing done file", log)
		return
	}

	if err := PathPVR(ctx, c.client, &pvr, func(pvr *velerov1api.PodVolumeRestore) {
		pvr.Status.Phase = velerov1api.PodVolumeRestorePhaseCompleted
		pvr.Status.CompletionTimestamp = &metav1.Time{Time: c.clock.Now()}
	}); err != nil {
		log.WithError(err).Error("error updating PodVolumeRestore status")
	}

	log.Info("Restore completed")
}

func (c *PodVolumeRestoreReconciler) OnDataPathFailed(ctx context.Context, namespace string, pvrName string, err error) {
	defer c.dataPathMgr.RemoveAsyncBR(pvrName)

	log := c.logger.WithField("pvr", pvrName)

	log.WithError(err).Error("Async fs restore data path failed")

	var pvr velerov1api.PodVolumeRestore
	if getErr := c.client.Get(ctx, types.NamespacedName{Name: pvrName, Namespace: namespace}, &pvr); getErr != nil {
		log.WithError(getErr).Warn("Failed to get PVR on failure")
	} else {
		_, _ = c.errorOut(ctx, &pvr, err, "data path restore failed", log)
	}
}

func (c *PodVolumeRestoreReconciler) OnDataPathCancelled(ctx context.Context, namespace string, pvrName string) {
	defer c.dataPathMgr.RemoveAsyncBR(pvrName)

	log := c.logger.WithField("pvr", pvrName)

	log.Warn("Async fs restore data path canceled")

	var pvr velerov1api.PodVolumeRestore
	if getErr := c.client.Get(ctx, types.NamespacedName{Name: pvrName, Namespace: namespace}, &pvr); getErr != nil {
		log.WithError(getErr).Warn("Failed to get PVR on cancel")
	} else {
		// cleans up any objects generated during the snapshot expose
		c.exposer.CleanUp(ctx, getPVROwnerObject(&pvr))

		if err := PathPVR(ctx, c.client, &pvr, func(pvr *velerov1api.PodVolumeRestore) {
			pvr.Status.Phase = velerov1api.PodVolumeRestorePhaseCanceled
			if pvr.Status.StartTimestamp.IsZero() {
				pvr.Status.StartTimestamp = &metav1.Time{Time: c.clock.Now()}
			}
			pvr.Status.CompletionTimestamp = &metav1.Time{Time: c.clock.Now()}
		}); err != nil {
			log.WithError(err).Error("error updating pvb status on cancel")
		}
	}
}

func (c *PodVolumeRestoreReconciler) OnDataPathProgress(ctx context.Context, namespace string, pvrName string, progress *uploader.Progress) {
	log := c.logger.WithField("pvr", pvrName)

	var pvr velerov1api.PodVolumeRestore
	if err := c.client.Get(ctx, types.NamespacedName{Name: pvrName, Namespace: namespace}, &pvr); err != nil {
		log.WithError(err).Warn("Failed to get PVB on progress")
		return
	}

	original := pvr.DeepCopy()
	pvr.Status.Progress = veleroapishared.DataMoveOperationProgress{TotalBytes: progress.TotalBytes, BytesDone: progress.BytesDone}

	if err := c.client.Patch(ctx, &pvr, client.MergeFrom(original)); err != nil {
		log.WithError(err).Error("Failed to update progress")
	}
}

func (c *PodVolumeRestoreReconciler) setupExposeParam(pvr *velerov1api.PodVolumeRestore) (exposer.PodVolumeExposeParam, error) {
	log := c.logger.WithField("pvr", pvr.Name)

	hostingPodLabels := map[string]string{velerov1api.PVRLabel: pvr.Name}
	for _, k := range util.ThirdPartyLabels {
		if v, err := nodeagent.GetLabelValue(context.Background(), c.kubeClient, pvr.Namespace, k, ""); err != nil {
			if err != nodeagent.ErrNodeAgentLabelNotFound {
				log.WithError(err).Warnf("Failed to check node-agent label, skip adding host pod label %s", k)
			}
		} else {
			hostingPodLabels[k] = v
		}
	}

	hostingPodAnnotation := map[string]string{}
	for _, k := range util.ThirdPartyAnnotations {
		if v, err := nodeagent.GetAnnotationValue(context.Background(), c.kubeClient, pvr.Namespace, k, ""); err != nil {
			if err != nodeagent.ErrNodeAgentAnnotationNotFound {
				log.WithError(err).Warnf("Failed to check node-agent annotation, skip adding host pod annotation %s", k)
			}
		} else {
			hostingPodAnnotation[k] = v
		}
	}

	return exposer.PodVolumeExposeParam{
		Type:                  exposer.PodVolumeExposeTypeRestore,
		ClientNamespace:       pvr.Spec.Pod.Namespace,
		ClientPodName:         pvr.Spec.Pod.Name,
		HostingPodLabels:      hostingPodLabels,
		HostingPodAnnotations: hostingPodAnnotation,
		Resources:             c.podResources,
	}, nil
}

func getPVROwnerObject(pvr *velerov1api.PodVolumeRestore) corev1.ObjectReference {
	return corev1.ObjectReference{
		Kind:       pvr.Kind,
		Namespace:  pvr.Namespace,
		Name:       pvr.Name,
		UID:        pvr.UID,
		APIVersion: pvr.APIVersion,
	}
}

func findPVRByPod(client client.Client, pod corev1.Pod) (*velerov1api.PodVolumeRestore, error) {
	if label, exist := pod.Labels[velerov1api.PVRLabel]; exist {
		pvr := &velerov1api.PodVolumeRestore{}
		err := client.Get(context.Background(), types.NamespacedName{
			Namespace: pod.Namespace,
			Name:      label,
		}, pvr)

		if err != nil {
			return nil, errors.Wrapf(err, "error to find pvr by pod %s/%s", pod.Namespace, pod.Name)
		}
		return pvr, nil
	}
	return nil, nil
}

func isPVRInFinalState(pvr *velerov1api.PodVolumeRestore) bool {
	return pvr.Status.Phase == velerov1api.PodVolumeRestorePhaseFailed ||
		pvr.Status.Phase == velerov1api.PodVolumeRestorePhaseCanceled ||
		pvr.Status.Phase == velerov1api.PodVolumeRestorePhaseCompleted
}

func PathPVR(ctx context.Context, cli client.Client, pvr *velerov1api.PodVolumeRestore, updateFunc func(*velerov1api.PodVolumeRestore)) error {
	original := pvr.DeepCopy()
	updateFunc(pvr)

	return cli.Patch(ctx, pvr, client.MergeFrom(original))
}

var funcResumeCancellablePVR = (*PodVolumeRestoreReconciler).resumeCancellableDataPath

func (c *PodVolumeRestoreReconciler) AttemptPVBResume(ctx context.Context, logger *logrus.Entry, ns string) error {
	pvrs := &velerov1api.PodVolumeRestoreList{}
	if err := c.client.List(ctx, pvrs, &client.ListOptions{Namespace: ns, FieldSelector: fields.OneTermEqualSelector("spec.node", c.nodeName)}); err != nil {
		c.logger.WithError(errors.WithStack(err)).Error("failed to list pvrs")
		return errors.Wrapf(err, "error to list pvrs")
	}

	for i := range pvrs.Items {
		pvr := &pvrs.Items[i]
		if pvr.Status.Phase != velerov1api.PodVolumeRestorePhaseInProgress {
			continue
		}

		err := funcResumeCancellablePVR(c, ctx, pvr, logger)
		if err == nil {
			logger.WithField("pvr", pvr.Name).WithField("current node", c.nodeName).Info("Completed to resume in progress pvr")
			continue
		}

		logger.WithField("pvr", pvr.GetName()).WithError(err).Warn("Failed to resume data path for pvr, have to cancel it")

		resumeErr := err
		err = PathPVR(ctx, c.client, pvr, func(pvr *velerov1api.PodVolumeRestore) {
			pvr.Spec.Cancel = true
			pvr.Status.Message = fmt.Sprintf("Resume InProgress pvr failed with error %v, mark it as cancel", resumeErr)
		})
		if err != nil {
			logger.WithField("pvr", pvr.GetName()).WithError(errors.WithStack(err)).Error("Failed to trigger pvr cancel")
		}
	}

	return nil
}

func (c *PodVolumeRestoreReconciler) resumeCancellableDataPath(ctx context.Context, pvr *velerov1api.PodVolumeRestore, log logrus.FieldLogger) error {
	log.Info("Resume cancelable pvr")

	res, err := c.exposer.GetExposed(ctx, getPVROwnerObject(pvr), c.client, c.nodeName)
	if err != nil {
		return errors.Wrapf(err, "error to get exposed pvr %s", pvr.Name)
	}

	if res == nil {
		return errors.Errorf("expose info missed for pvr %s", pvr.Name)
	}

	callbacks := datapath.Callbacks{
		OnCompleted: c.OnDataPathCompleted,
		OnFailed:    c.OnDataPathFailed,
		OnCancelled: c.OnDataPathCancelled,
		OnProgress:  c.OnDataPathProgress,
	}

	asyncBR, err := c.dataPathMgr.CreateMicroServiceBRWatcher(ctx, c.client, c.kubeClient, c.mgr, datapath.TaskTypeBackup, pvr.Name, pvr.Namespace, res.ByPod.HostingPod.Name, res.ByPod.HostingContainer, pvr.Name, callbacks, true, log)
	if err != nil {
		return errors.Wrapf(err, "error to create asyncBR watcher for pvr %s", pvr.Name)
	}

	resumeComplete := false
	defer func() {
		if !resumeComplete {
			c.closeDataPath(ctx, pvr.Name)
		}
	}()

	if err := asyncBR.Init(ctx, nil); err != nil {
		return errors.Wrapf(err, "error to init asyncBR watcher pvr pvb %s", pvr.Name)
	}

	if err := asyncBR.StartBackup(datapath.AccessPoint{
		ByPath: res.ByPod.VolumeName,
	}, pvr.Spec.UploaderSettings, nil); err != nil {
		return errors.Wrapf(err, "error to resume asyncBR watcher for pvr %s", pvr.Name)
	}

	resumeComplete = true

	log.Infof("asyncBR is resumed for pvr %s", pvr.Name)

	return nil
}
