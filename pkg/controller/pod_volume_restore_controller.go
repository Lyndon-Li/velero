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
	"strings"
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
	"k8s.io/apimachinery/pkg/util/wait"
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

	veleroapishared "github.com/vmware-tanzu/velero/pkg/apis/velero/shared"
	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/constant"
	"github.com/vmware-tanzu/velero/pkg/datapath"
	"github.com/vmware-tanzu/velero/pkg/exposer"
	"github.com/vmware-tanzu/velero/pkg/nodeagent"
	"github.com/vmware-tanzu/velero/pkg/restorehelper"
	"github.com/vmware-tanzu/velero/pkg/uploader"
	"github.com/vmware-tanzu/velero/pkg/util"
	"github.com/vmware-tanzu/velero/pkg/util/kube"
)

func NewPodVolumeRestoreReconciler(client client.Client, mgr manager.Manager, kubeClient kubernetes.Interface, dataPathMgr *datapath.Manager,
	nodeName string, preparingTimeout time.Duration, resourceTimeout time.Duration, podResources corev1.ResourceRequirements,
	logger logrus.FieldLogger) *PodVolumeRestoreReconciler {
	return &PodVolumeRestoreReconciler{
		client:           client,
		mgr:              mgr,
		kubeClient:       kubeClient,
		logger:           logger.WithField("controller", "PodVolumeRestore"),
		nodeName:         nodeName,
		clock:            &clocks.RealClock{},
		podResources:     podResources,
		dataPathMgr:      dataPathMgr,
		preparingTimeout: preparingTimeout,
		resourceTimeout:  resourceTimeout,
		exposer:          exposer.NewPodVolumeExposer(kubeClient, logger),
		cancelledPVR:     make(map[string]time.Time),
	}
}

type PodVolumeRestoreReconciler struct {
	client           client.Client
	mgr              manager.Manager
	kubeClient       kubernetes.Interface
	logger           logrus.FieldLogger
	nodeName         string
	clock            clocks.WithTickerAndDelayedExecution
	podResources     corev1.ResourceRequirements
	exposer          exposer.PodVolumeExposer
	dataPathMgr      *datapath.Manager
	preparingTimeout time.Duration
	resourceTimeout  time.Duration
	cancelledPVR     map[string]time.Time
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

	if pvr.Spec.UploaderType == uploader.ResticType {
		return ctrl.Result{}, nil
	}

	log = log.WithField("pod", fmt.Sprintf("%s/%s", pvr.Spec.Pod.Namespace, pvr.Spec.Pod.Name))
	if len(pvr.OwnerReferences) == 1 {
		log = log.WithField("restore", fmt.Sprintf("%s/%s", pvr.Namespace, pvr.OwnerReferences[0].Name))
	}

	// Logic for clear resources when pvr been deleted
	if !isPVRInFinalState(pvr) {
		if !controllerutil.ContainsFinalizer(pvr, PodVolumeFinalizer) {
			if err := UpdatePVRWithRetry(ctx, c.client, req.NamespacedName, log, func(pvr *velerov1api.PodVolumeRestore) bool {
				if controllerutil.ContainsFinalizer(pvr, PodVolumeFinalizer) {
					return false
				}

				controllerutil.AddFinalizer(pvr, PodVolumeFinalizer)

				return true
			}); err != nil {
				log.WithError(err).Errorf("failed to add finalizer for pvr %s/%s", pvr.Namespace, pvr.Name)
				return ctrl.Result{}, err
			}

			return ctrl.Result{}, nil
		}

		if !pvr.DeletionTimestamp.IsZero() {
			if !pvr.Spec.Cancel {
				log.Warnf("Cancel pvr under phase %s because it is being deleted", pvr.Status.Phase)

				if err := UpdatePVRWithRetry(ctx, c.client, req.NamespacedName, log, func(pvr *velerov1api.PodVolumeRestore) bool {
					if pvr.Spec.Cancel {
						return false
					}

					pvr.Spec.Cancel = true
					pvr.Status.Message = "Cancel pvr because it is being deleted"

					return true
				}); err != nil {
					log.WithError(err).Errorf("failed to set cancel flag for pvr %s/%s", pvr.Namespace, pvr.Name)
					return ctrl.Result{}, err
				}

				return ctrl.Result{}, nil
			}
		}
	} else {
		delete(c.cancelledPVR, pvr.Name)

		if controllerutil.ContainsFinalizer(pvr, PodVolumeFinalizer) {
			if err := UpdatePVRWithRetry(ctx, c.client, req.NamespacedName, log, func(pvr *velerov1api.PodVolumeRestore) bool {
				if !controllerutil.ContainsFinalizer(pvr, PodVolumeFinalizer) {
					return false
				}

				controllerutil.RemoveFinalizer(pvr, PodVolumeFinalizer)

				return true
			}); err != nil {
				log.WithError(err).Error("error to remove finalizer")
				return ctrl.Result{}, err
			}

			return ctrl.Result{}, nil
		}
	}

	if pvr.Spec.Cancel {
		if spotted, found := c.cancelledPVR[pvr.Name]; !found {
			c.cancelledPVR[pvr.Name] = c.clock.Now()
		} else {
			delay := cancelDelayOthers
			if pvr.Status.Phase == velerov1api.PodVolumeRestorePhaseInProgress {
				delay = cancelDelayInProgress
			}

			if time.Since(spotted) > delay {
				log.Infof("pvr %s is canceled in Phase %s but not handled in rasonable time", pvr.GetName(), pvr.Status.Phase)
				if c.tryCancelPodVolumeRestore(ctx, pvr, "") {
					delete(c.cancelledPVR, pvr.Name)
				}

				return ctrl.Result{}, nil
			}
		}
	}

	if pvr.Status.Phase == "" || pvr.Status.Phase == velerov1api.PodVolumeRestorePhaseNew {
		if pvr.Spec.Cancel {
			log.Infof("pvr %s is canceled in Phase %s", pvr.GetName(), pvr.Status.Phase)
			c.tryCancelPodVolumeRestore(ctx, pvr, "")

			return ctrl.Result{}, nil
		}

		shouldProcess, pod, err := shouldProcess(ctx, c.client, log, pvr)
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

		log.Info("Accepting PVR")

		if accepted, err := c.acceptPodVolumeRestore(ctx, pvr); err != nil {
			return c.errorOut(ctx, pvr, err, "error to accept pvr", log)
		} else if !accepted {
			log.Debug("pvr is not accepted")
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second * 5}, nil
		}

		log.Info("Exposing PVR")

		exposeParam, err := c.setupExposeParam(pvr)
		if err != nil {
			return c.errorOut(ctx, pvr, err, "failed to set exposer parameters", log)
		}

		if err := c.exposer.Expose(ctx, getPVROwnerObject(pvr), exposeParam); err != nil {
			return c.errorOut(ctx, pvr, err, "error to expose pvr", log)
		}

		log.Info("pvr is exposed")

		return ctrl.Result{}, nil
	} else if pvr.Status.Phase == velerov1api.PodVolumeRestorePhaseAccepted {
		if peekErr := c.exposer.PeekExposed(ctx, getPVROwnerObject(pvr)); peekErr != nil {
			log.Errorf("Cancel PVR %s/%s because of expose error %s", pvr.Namespace, pvr.Name, peekErr)
			c.tryCancelPodVolumeRestore(ctx, pvr, fmt.Sprintf("found a pvr %s/%s with expose error: %s. mark it as cancel", pvr.Namespace, pvr.Name, peekErr))
		} else if pvr.Status.AcceptedTimestamp != nil {
			if time.Since(pvr.Status.AcceptedTimestamp.Time) >= c.preparingTimeout {
				c.onPrepareTimeout(ctx, pvr)
			}
		}

		return ctrl.Result{}, nil
	} else if pvr.Status.Phase == velerov1api.PodVolumeRestorePhasePrepared {
		log.Infof("PVR is prepared and should be processed by %s (%s)", pvr.Status.Node, c.nodeName)

		if pvr.Status.Node != c.nodeName {
			return ctrl.Result{}, nil
		}

		if pvr.Spec.Cancel {
			log.Info("Prepared pvr is being cancelled")
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

		asyncBR, err = c.dataPathMgr.CreateMicroServiceBRWatcher(ctx, c.client, c.kubeClient, c.mgr, datapath.TaskTypeRestore,
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

		terminated := false
		if err := UpdatePVRWithRetry(ctx, c.client, types.NamespacedName{Namespace: pvr.Namespace, Name: pvr.Name}, log, func(pvr *velerov1api.PodVolumeRestore) bool {
			if isPVRInFinalState(pvr) {
				terminated = true
				return false
			}

			pvr.Status.Phase = velerov1api.PodVolumeRestorePhaseInProgress
			pvr.Status.StartTimestamp = &metav1.Time{Time: c.clock.Now()}

			return true
		}); err != nil {
			log.WithError(err).Warnf("Failed to update pvr %s to InProgress, will data path close and retry", pvr.Name)

			c.closeDataPath(ctx, pvr.Name)
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second * 5}, nil
		}

		if terminated {
			log.Warnf("pvr %s is terminated during transition from prepared", pvr.Name)
			c.closeDataPath(ctx, pvr.Name)
			return ctrl.Result{}, nil
		}

		log.Info("pvr is marked as in progress")

		if err := c.startCancelableDataPath(asyncBR, pvr, res, log); err != nil {
			log.WithError(err).Errorf("Failed to start cancelable data path for %s", pvr.Name)
			c.closeDataPath(ctx, pvr.Name)

			return c.errorOut(ctx, pvr, err, "error starting data path", log)
		}

		return ctrl.Result{}, nil
	} else if pvr.Status.Phase == velerov1api.PodVolumeRestorePhaseInProgress {
		if pvr.Spec.Cancel {
			if pvr.Status.Node != c.nodeName {
				return ctrl.Result{}, nil
			}

			log.Info("pvr is being canceled")

			asyncBR := c.dataPathMgr.GetAsyncBR(pvr.Name)
			if asyncBR == nil {
				c.OnDataPathCancelled(ctx, pvr.GetNamespace(), pvr.GetName())
				return ctrl.Result{}, nil
			}

			// Update status to Canceling
			if err := UpdatePVRWithRetry(ctx, c.client, types.NamespacedName{Namespace: pvr.Namespace, Name: pvr.Name}, log, func(pvr *velerov1api.PodVolumeRestore) bool {
				if isPVRInFinalState(pvr) {
					log.Warnf("pvr %s is terminated, abort setting it to cancelling", pvr.Name)
					return false
				}

				pvr.Status.Phase = velerov1api.PodVolumeRestorePhaseCanceling
				return true
			}); err != nil {
				log.WithError(err).Error("error updating pvr into canceling status")
				return ctrl.Result{}, err
			}

			asyncBR.Cancel()
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
}

func (r *PodVolumeRestoreReconciler) acceptPodVolumeRestore(ctx context.Context, pvr *velerov1api.PodVolumeRestore) (bool, error) {
	updated := pvr.DeepCopy()

	updateFunc := func(pvr *velerov1api.PodVolumeRestore) {
		pvr.Status.AcceptedTimestamp = &metav1.Time{Time: r.clock.Now()}
		pvr.Status.Phase = velerov1api.PodVolumeRestorePhaseAccepted
		pvr.Status.Node = r.nodeName
	}

	succeeded, err := r.exclusiveUpdatePodVolumeRestore(ctx, updated, updateFunc)

	if err != nil {
		return false, err
	}

	if succeeded {
		updateFunc(pvr) // If update success, it's need to update pvr values in memory
		r.logger.WithField("pvr", pvr.Name).Infof("This pvr has been accepted by %s", r.nodeName)
		return true, nil
	}

	r.logger.WithField("pvr", pvr.Name).Info("This pvr is not successfully accepted")
	return false, nil
}

func (c *PodVolumeRestoreReconciler) tryCancelPodVolumeRestore(ctx context.Context, pvr *velerov1api.PodVolumeRestore, message string) bool {
	log := c.logger.WithField("pvr", pvr.Name)
	succeeded, err := c.exclusiveUpdatePodVolumeRestore(ctx, pvr, func(pvr *velerov1api.PodVolumeRestore) {
		pvr.Status.Phase = velerov1api.PodVolumeRestorePhaseCanceled
		if pvr.Status.StartTimestamp.IsZero() {
			pvr.Status.StartTimestamp = &metav1.Time{Time: c.clock.Now()}
		}
		pvr.Status.CompletionTimestamp = &metav1.Time{Time: c.clock.Now()}

		if message != "" {
			pvr.Status.Message = message
		}
	})

	if err != nil {
		log.WithError(err).Error("error updating pvr status")
		return false
	} else if !succeeded {
		log.Warn("conflict in updating pvr status and will try it again later")
		return false
	}

	c.exposer.CleanUp(ctx, getPVROwnerObject(pvr))

	log.Warn("pvr is canceled")

	return true
}

func (c *PodVolumeRestoreReconciler) exclusiveUpdatePodVolumeRestore(ctx context.Context, pvr *velerov1api.PodVolumeRestore,
	updateFunc func(*velerov1api.PodVolumeRestore)) (bool, error) {
	updateFunc(pvr)

	err := c.client.Update(ctx, pvr)
	if err == nil {
		return true, nil
	}

	// warn we won't rollback pvr values in memory when error
	if apierrors.IsConflict(err) {
		return false, nil
	} else {
		return false, err
	}
}

func (c *PodVolumeRestoreReconciler) onPrepareTimeout(ctx context.Context, pvr *velerov1api.PodVolumeRestore) {
	log := c.logger.WithField("PVR", pvr.Name)

	log.Info("Timeout happened for preparing PVR")

	succeeded, err := c.exclusiveUpdatePodVolumeRestore(ctx, pvr, func(pvr *velerov1api.PodVolumeRestore) {
		pvr.Status.Phase = velerov1api.PodVolumeRestorePhaseFailed
		pvr.Status.Message = "timeout on preparing pvr"
	})

	if err != nil {
		log.WithError(err).Warn("Failed to update pvr")
		return
	}

	if !succeeded {
		log.Warn("pvr has been updated by others")
		return
	}

	diags := strings.Split(c.exposer.DiagnoseExpose(ctx, getPVROwnerObject(pvr)), "\n")
	for _, diag := range diags {
		log.Warnf("[Diagnose PVR expose]%s", diag)
	}

	c.exposer.CleanUp(ctx, getPVROwnerObject(pvr))

	log.Info("PVR has been cleaned up")
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

	return ctrl.Result{}, UpdatePVRStatusToFailed(ctx, c.client, pvr, err, msg, c.clock.Now(), log)
}

func UpdatePVRStatusToFailed(ctx context.Context, c client.Client, pvr *velerov1api.PodVolumeRestore, err error, msg string, time time.Time, log logrus.FieldLogger) error {
	log.Info("update pvr status to Failed")

	if patchErr := UpdatePVRWithRetry(context.Background(), c, types.NamespacedName{Namespace: pvr.Namespace, Name: pvr.Name}, log,
		func(pvr *velerov1api.PodVolumeRestore) bool {
			if isPVRInFinalState(pvr) {
				return false
			}

			pvr.Status.Phase = velerov1api.PodVolumeRestorePhaseFailed
			pvr.Status.Message = errors.WithMessage(err, msg).Error()
			pvr.Status.CompletionTimestamp = &metav1.Time{Time: time}

			return true
		}); patchErr != nil {
		log.WithError(patchErr).Warn("error updating pvr status")
	}

	return err
}

func shouldProcess(ctx context.Context, client client.Client, log logrus.FieldLogger, pvr *velerov1api.PodVolumeRestore) (bool, *corev1api.Pod, error) {
	if !isPVRNew(pvr) {
		log.Debug("PodVolumeRestore is not new, skip")
		return false, nil, nil
	}

	// we filter the pods during the initialization of cache, if we can get a pod here, the pod must be in the same node with the controller
	// so we don't need to compare the node anymore
	pod := &corev1api.Pod{}
	if err := client.Get(ctx, types.NamespacedName{Namespace: pvr.Spec.Pod.Namespace, Name: pvr.Spec.Pod.Name}, pod); err != nil {
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
	gp := kube.NewGenericEventPredicate(func(object client.Object) bool {
		pvr := object.(*velerov1api.PodVolumeRestore)
		if pvr.Spec.UploaderType == uploader.ResticType {
			return false
		}

		if pvr.Status.Phase == velerov1api.PodVolumeRestorePhaseAccepted {
			return true
		}

		if pvr.Spec.Cancel && !isPVRInFinalState(pvr) {
			return true
		}

		if isPVRInFinalState(pvr) && !pvr.DeletionTimestamp.IsZero() {
			return true
		}

		return false
	})

	s := kube.NewPeriodicalEnqueueSource(c.logger.WithField("controller", constant.ControllerPodVolumeRestore), c.client, &velerov1api.PodVolumeRestoreList{}, preparingMonitorFrequency, kube.PeriodicalEnqueueSourceOption{
		Predicates: []predicate.Predicate{gp},
	})

	pred := kube.NewAllEventPredicate(func(obj client.Object) bool {
		pvr := obj.(*velerov1.PodVolumeRestore)
		return (pvr.Spec.UploaderType != uploader.ResticType)
	})

	return ctrl.NewControllerManagedBy(mgr).
		For(&velerov1api.PodVolumeRestore{}, builder.WithPredicates(kube.SpecChangePredicate{}, pred)).
		WatchesRawSource(s).
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
	return findVolumeRestoresForPodHelper(c.client, pod, c.logger)
}

func findVolumeRestoresForPodHelper(cli client.Client, pod client.Object, logger logrus.FieldLogger) []reconcile.Request {
	list := &velerov1api.PodVolumeRestoreList{}
	options := &client.ListOptions{
		LabelSelector: labels.Set(map[string]string{
			velerov1api.PodUIDLabel: string(pod.GetUID()),
		}).AsSelector(),
	}
	if err := cli.List(context.TODO(), list, options); err != nil {
		logger.WithField("pod", fmt.Sprintf("%s/%s", pod.GetNamespace(), pod.GetName())).WithError(err).
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

	if pvr.Status.Phase != velerov1api.PodVolumeRestorePhaseAccepted {
		return []reconcile.Request{}
	}

	if pod.Status.Phase == v1.PodRunning {
		log.Info("Preparing pvr")

		if err = UpdatePVRWithRetry(context.Background(), c.client, types.NamespacedName{Namespace: pvr.Namespace, Name: pvr.Name}, log,
			func(pvr *velerov1api.PodVolumeRestore) bool {
				if isPVRInFinalState(pvr) {
					log.Warnf("pvr %s is terminated, abort setting it to prepared", pvr.Name)
					return false
				}

				pvr.Status.Phase = velerov1api.PodVolumeRestorePhasePrepared
				return true
			}); err != nil {
			log.WithError(err).Warn("failed to update pvr, prepare will halt for this pvr")
			return []reconcile.Request{}
		}
	} else if unrecoverable, reason := kube.IsPodUnrecoverable(pod, log); unrecoverable {
		err := UpdatePVRWithRetry(context.Background(), c.client, types.NamespacedName{Namespace: pvr.Namespace, Name: pvr.Name}, log,
			func(pvr *velerov1api.PodVolumeRestore) bool {
				if pvr.Spec.Cancel {
					return false
				}

				pvr.Spec.Cancel = true
				pvr.Status.Message = fmt.Sprintf("Cancel pvr because the exposing pod %s/%s is in abnormal status for reason %s", pod.Namespace, pod.Name, reason)

				return true
			})

		if err != nil {
			log.WithError(err).Warn("failed to cancel pvr, and it will wait for prepare timeout")
			return []reconcile.Request{}
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

	log.WithField("PVR", pvrName).WithField("result", result.Restore).Info("Async fs restore data path completed")

	var pvr velerov1api.PodVolumeRestore
	if err := c.client.Get(ctx, types.NamespacedName{Name: pvrName, Namespace: namespace}, &pvr); err != nil {
		log.WithError(err).Warn("Failed to get PVR on completion")
		return
	}

	log.Info("Cleaning up exposed environment")
	c.exposer.CleanUp(ctx, getPVROwnerObject(&pvr))

	if err := UpdatePVRWithRetry(ctx, c.client, types.NamespacedName{Namespace: pvr.Namespace, Name: pvr.Name}, log, func(pvr *velerov1api.PodVolumeRestore) bool {
		if isPVRInFinalState(pvr) {
			return false
		}

		pvr.Status.Phase = velerov1api.PodVolumeRestorePhaseCompleted
		pvr.Status.CompletionTimestamp = &metav1.Time{Time: c.clock.Now()}

		return true
	}); err != nil {
		log.WithError(err).Error("error updating PodVolumeRestore status")
	} else {
		log.Info("Restore completed")
	}
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
		return
	}
	// cleans up any objects generated during the snapshot expose
	c.exposer.CleanUp(ctx, getPVROwnerObject(&pvr))

	if err := UpdatePVRWithRetry(ctx, c.client, types.NamespacedName{Namespace: pvr.Namespace, Name: pvr.Name}, log, func(pvr *velerov1api.PodVolumeRestore) bool {
		if isPVRInFinalState(pvr) {
			return false
		}

		pvr.Status.Phase = velerov1api.PodVolumeRestorePhaseCanceled
		if pvr.Status.StartTimestamp.IsZero() {
			pvr.Status.StartTimestamp = &metav1.Time{Time: c.clock.Now()}
		}
		pvr.Status.CompletionTimestamp = &metav1.Time{Time: c.clock.Now()}

		return true
	}); err != nil {
		log.WithError(err).Error("error updating pvr status on cancel")
	} else {
		delete(c.cancelledPVR, pvr.Name)
	}
}

func (c *PodVolumeRestoreReconciler) OnDataPathProgress(ctx context.Context, namespace string, pvrName string, progress *uploader.Progress) {
	log := c.logger.WithField("pvr", pvrName)

	if err := UpdatePVRWithRetry(ctx, c.client, types.NamespacedName{Namespace: namespace, Name: pvrName}, log, func(pvr *velerov1api.PodVolumeRestore) bool {
		pvr.Status.Progress = veleroapishared.DataMoveOperationProgress{TotalBytes: progress.TotalBytes, BytesDone: progress.BytesDone}
		return true
	}); err != nil {
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
		ClientPodVolume:       pvr.Spec.Volume,
		HostingPodLabels:      hostingPodLabels,
		HostingPodAnnotations: hostingPodAnnotation,
		OperationTimeout:      c.resourceTimeout,
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

func UpdatePVRWithRetry(ctx context.Context, client client.Client, namespacedName types.NamespacedName, log logrus.FieldLogger, updateFunc func(*velerov1api.PodVolumeRestore) bool) error {
	return wait.PollUntilContextCancel(ctx, time.Millisecond*100, true, func(ctx context.Context) (bool, error) {
		pvr := &velerov1api.PodVolumeRestore{}
		if err := client.Get(ctx, namespacedName, pvr); err != nil {
			return false, errors.Wrap(err, "getting pvr")
		}

		if updateFunc(pvr) {
			err := client.Update(ctx, pvr)
			if err != nil {
				if apierrors.IsConflict(err) {
					log.Warnf("failed to update pvr for %s/%s and will retry it", pvr.Namespace, pvr.Name)
					return false, nil
				} else {
					return false, errors.Wrapf(err, "error updating pvr with error %s/%s", pvr.Namespace, pvr.Name)
				}
			}
		}

		return true, nil
	})
}

var funcResumeCancellablePVR = (*PodVolumeRestoreReconciler).resumeCancellableDataPath

func (c *PodVolumeRestoreReconciler) AttemptPVRResume(ctx context.Context, logger *logrus.Entry, ns string) error {
	pvrs := &velerov1api.PodVolumeRestoreList{}
	if err := c.client.List(ctx, pvrs, &client.ListOptions{Namespace: ns, FieldSelector: fields.OneTermEqualSelector("spec.node", c.nodeName)}); err != nil {
		c.logger.WithError(errors.WithStack(err)).Error("failed to list pvrs")
		return errors.Wrapf(err, "error to list pvrs")
	}

	for i := range pvrs.Items {
		pvr := &pvrs.Items[i]
		if pvr.Status.Phase != velerov1api.PodVolumeRestorePhaseInProgress {
			if pvr.Status.Node != c.nodeName {
				logger.WithField("pvr", pvr.Name).WithField("current node", c.nodeName).Infof("pvr should be resumed by another node %s", pvr.Status.Node)
				continue
			}

			err := funcResumeCancellablePVR(c, ctx, pvr, logger)
			if err == nil {
				logger.WithField("pvr", pvr.Name).WithField("current node", c.nodeName).Info("Completed to resume in progress pvr")
				continue
			}

			logger.WithField("pvr", pvr.GetName()).WithError(err).Warn("Failed to resume data path for pvr, have to cancel it")

			resumeErr := err
			err = UpdatePVRWithRetry(ctx, c.client, types.NamespacedName{Namespace: pvr.Namespace, Name: pvr.Name}, logger.WithField("pvr", pvr.Name),
				func(pvr *velerov1api.PodVolumeRestore) bool {
					if pvr.Spec.Cancel {
						return false
					}

					pvr.Spec.Cancel = true
					pvr.Status.Message = fmt.Sprintf("Resume InProgress pvr failed with error %v, mark it as cancel", resumeErr)

					return true
				})
			if err != nil {
				logger.WithField("pvr", pvr.GetName()).WithError(errors.WithStack(err)).Error("Failed to trigger pvr cancel")
			}
		} else {
			logger.WithField("pvr", pvr.GetName()).Infof("find a pvr with status %s", pvr.Status.Phase)
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

	asyncBR, err := c.dataPathMgr.CreateMicroServiceBRWatcher(ctx, c.client, c.kubeClient, c.mgr, datapath.TaskTypeRestore, pvr.Name, pvr.Namespace, res.ByPod.HostingPod.Name, res.ByPod.HostingContainer, pvr.Name, callbacks, true, log)
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
		return errors.Wrapf(err, "error to init asyncBR watcher pvr %s", pvr.Name)
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
