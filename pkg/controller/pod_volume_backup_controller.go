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
	corev1api "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	clocks "k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	veleroapishared "github.com/vmware-tanzu/velero/pkg/apis/velero/shared"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/constant"
	"github.com/vmware-tanzu/velero/pkg/datapath"
	"github.com/vmware-tanzu/velero/pkg/exposer"
	"github.com/vmware-tanzu/velero/pkg/metrics"
	"github.com/vmware-tanzu/velero/pkg/nodeagent"
	"github.com/vmware-tanzu/velero/pkg/uploader"
	"github.com/vmware-tanzu/velero/pkg/util"
	"github.com/vmware-tanzu/velero/pkg/util/kube"
)

const (
	pVBRRequestor      = "pod-volume-backup-restore"
	PodVolumeFinalizer = "velero.io/pod-volume-finalizer"
)

// NewPodVolumeBackupReconciler creates the PodVolumeBackupReconciler instance
func NewPodVolumeBackupReconciler(client client.Client, mgr manager.Manager, kubeClient kubernetes.Interface, dataPathMgr *datapath.Manager,
	nodeName string, preparingTimeout time.Duration, resourceTimeout time.Duration, podResources corev1api.ResourceRequirements,
	metrics *metrics.ServerMetrics, logger logrus.FieldLogger) *PodVolumeBackupReconciler {
	return &PodVolumeBackupReconciler{
		client:           client,
		mgr:              mgr,
		kubeClient:       kubeClient,
		logger:           logger.WithField("controller", "PodVolumeBackup"),
		nodeName:         nodeName,
		clock:            &clocks.RealClock{},
		metrics:          metrics,
		podResources:     podResources,
		dataPathMgr:      dataPathMgr,
		preparingTimeout: preparingTimeout,
		resourceTimeout:  resourceTimeout,
		exposer:          exposer.NewPodVolumeExposer(kubeClient, logger),
		cancelledPVB:     make(map[string]time.Time),
	}
}

// PodVolumeBackupReconciler reconciles a PodVolumeBackup object
type PodVolumeBackupReconciler struct {
	client           client.Client
	mgr              manager.Manager
	kubeClient       kubernetes.Interface
	clock            clocks.WithTickerAndDelayedExecution
	exposer          exposer.PodVolumeExposer
	metrics          *metrics.ServerMetrics
	nodeName         string
	logger           logrus.FieldLogger
	podResources     corev1api.ResourceRequirements
	dataPathMgr      *datapath.Manager
	preparingTimeout time.Duration
	resourceTimeout  time.Duration
	cancelledPVB     map[string]time.Time
}

// +kubebuilder:rbac:groups=velero.io,resources=podvolumebackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=velero.io,resources=podvolumebackups/status,verbs=get;update;patch

func (r *PodVolumeBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.logger.WithFields(logrus.Fields{
		"controller":      "podvolumebackup",
		"podvolumebackup": req.NamespacedName,
	})

	var pvb = &velerov1api.PodVolumeBackup{}
	if err := r.client.Get(ctx, req.NamespacedName, pvb); err != nil {
		if apierrors.IsNotFound(err) {
			log.Debug("Unable to find PodVolumeBackup")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, errors.Wrap(err, "getting PodVolumeBackup")
	}
	if len(pvb.OwnerReferences) == 1 {
		log = log.WithField(
			"backup",
			fmt.Sprintf("%s/%s", req.Namespace, pvb.OwnerReferences[0].Name),
		)
	}

	if !isPVBInFinalState(pvb) {
		if !controllerutil.ContainsFinalizer(pvb, PodVolumeFinalizer) {
			if err := UpdatePVBWithRetry(ctx, r.client, req.NamespacedName, log, func(pvb *velerov1api.PodVolumeBackup) bool {
				if controllerutil.ContainsFinalizer(pvb, PodVolumeFinalizer) {
					return false
				}

				controllerutil.AddFinalizer(pvb, PodVolumeFinalizer)

				return true
			}); err != nil {
				log.WithError(err).Errorf("failed to add finalizer for pvb %s/%s", pvb.Namespace, pvb.Name)
				return ctrl.Result{}, err
			}

			return ctrl.Result{}, nil
		}

		if !pvb.DeletionTimestamp.IsZero() {
			if !pvb.Spec.Cancel {
				log.Warnf("Cancel pvb under phase %s because it is being deleted", pvb.Status.Phase)

				if err := UpdatePVBWithRetry(ctx, r.client, req.NamespacedName, log, func(pvb *velerov1api.PodVolumeBackup) bool {
					if pvb.Spec.Cancel {
						return false
					}

					pvb.Spec.Cancel = true
					pvb.Status.Message = "Cancel pvb because it is being deleted"

					return true
				}); err != nil {
					log.WithError(err).Errorf("failed to set cancel flag for pvb %s/%s", pvb.Namespace, pvb.Name)
					return ctrl.Result{}, err
				}

				return ctrl.Result{}, nil
			}
		}
	} else {
		delete(r.cancelledPVB, pvb.Name)

		if controllerutil.ContainsFinalizer(pvb, PodVolumeFinalizer) {
			if err := UpdatePVBWithRetry(ctx, r.client, req.NamespacedName, log, func(pvb *velerov1api.PodVolumeBackup) bool {
				if !controllerutil.ContainsFinalizer(pvb, PodVolumeFinalizer) {
					return false
				}

				controllerutil.RemoveFinalizer(pvb, PodVolumeFinalizer)

				return true
			}); err != nil {
				log.WithError(err).Error("error to remove finalizer")
				return ctrl.Result{}, err
			}

			return ctrl.Result{}, nil
		}
	}

	if pvb.Spec.Cancel {
		if spotted, found := r.cancelledPVB[pvb.Name]; !found {
			r.cancelledPVB[pvb.Name] = r.clock.Now()
		} else {
			delay := cancelDelayOthers
			if pvb.Status.Phase == velerov1api.PodVolumeBackupPhaseInProgress {
				delay = cancelDelayInProgress
			}

			if time.Since(spotted) > delay {
				log.Infof("pvb %s is canceled in Phase %s but not handled in rasonable time", pvb.GetName(), pvb.Status.Phase)
				if r.tryCancelPodVolumeBackup(ctx, pvb, "") {
					delete(r.cancelledPVB, pvb.Name)
				}

				return ctrl.Result{}, nil
			}
		}
	}

	if pvb.Status.Phase == "" || pvb.Status.Phase == velerov1api.PodVolumeBackupPhaseNew {
		if pvb.Spec.Cancel {
			log.Infof("pvb %s is canceled in Phase %s", pvb.GetName(), pvb.Status.Phase)
			r.tryCancelPodVolumeBackup(ctx, pvb, "")

			return ctrl.Result{}, nil
		}

		// Only process items for this node.
		if pvb.Spec.Node != r.nodeName {
			return ctrl.Result{}, nil
		}

		log.Info("Accepting pvb")

		if accepted, err := r.acceptPodVolumeBackup(ctx, pvb); err != nil {
			return ctrl.Result{}, errors.Wrapf(err, "error accepting pvb %s", pvb.Name)
		} else if !accepted {
			log.Debug("pvb is not accepted")
			return ctrl.Result{Requeue: true}, nil
		}

		log.Info("Exposing pvb")

		exposeParam, err := r.setupExposeParam(pvb)
		if err != nil {
			return r.errorOut(ctx, pvb, err, "failed to set exposer parameters", log)
		}

		if err := r.exposer.Expose(ctx, getPVBOwnerObject(pvb), exposeParam); err != nil {
			return r.errorOut(ctx, pvb, err, "error to expose pvb", log)
		}

		log.Info("pvb is exposed")

		return ctrl.Result{}, nil
	} else if pvb.Status.Phase == velerov1api.PodVolumeBackupPhaseAccepted {
		if peekErr := r.exposer.PeekExposed(ctx, getPVBOwnerObject(pvb)); peekErr != nil {
			log.Errorf("Cancel PVB %s/%s because of expose error %s", pvb.Namespace, pvb.Name, peekErr)
			r.tryCancelPodVolumeBackup(ctx, pvb, fmt.Sprintf("found a pvb %s/%s with expose error: %s. mark it as cancel", pvb.Namespace, pvb.Name, peekErr))
		} else if pvb.Status.AcceptedTimestamp != nil {
			if time.Since(pvb.Status.AcceptedTimestamp.Time) >= r.preparingTimeout {
				r.onPrepareTimeout(ctx, pvb)
			}
		}

		return ctrl.Result{}, nil
	} else if pvb.Status.Phase == velerov1api.PodVolumeBackupPhasePrepared {
		log.Infof("pvb is prepared and should be processed by %s (%s)", pvb.Spec.Node, r.nodeName)

		if pvb.Spec.Node != r.nodeName {
			return ctrl.Result{}, nil
		}

		if pvb.Spec.Cancel {
			log.Info("Prepared pvb is being cancelled")
			r.OnDataPathCancelled(ctx, pvb.GetNamespace(), pvb.GetName())
			return ctrl.Result{}, nil
		}

		asyncBR := r.dataPathMgr.GetAsyncBR(pvb.Name)
		if asyncBR != nil {
			log.Info("Cancellable data path is already started")
			return ctrl.Result{}, nil
		}

		res, err := r.exposer.GetExposed(ctx, getPVBOwnerObject(pvb), r.client, r.nodeName)
		if err != nil {
			return r.errorOut(ctx, pvb, err, "exposed pvb is not ready", log)
		} else if res == nil {
			return r.errorOut(ctx, pvb, errors.New("no expose result is available for the current node"), "exposed snapshot is not ready", log)
		}

		log.Info("Exposed pvb is ready and creating data path routine")

		callbacks := datapath.Callbacks{
			OnCompleted: r.OnDataPathCompleted,
			OnFailed:    r.OnDataPathFailed,
			OnCancelled: r.OnDataPathCancelled,
			OnProgress:  r.OnDataPathProgress,
		}

		asyncBR, err = r.dataPathMgr.CreateMicroServiceBRWatcher(ctx, r.client, r.kubeClient, r.mgr, datapath.TaskTypeBackup,
			pvb.Name, pvb.Namespace, res.ByPod.HostingPod.Name, res.ByPod.HostingContainer, pvb.Name, callbacks, false, log)
		if err != nil {
			if err == datapath.ConcurrentLimitExceed {
				log.Info("Data path instance is concurrent limited requeue later")
				return ctrl.Result{Requeue: true, RequeueAfter: time.Second * 5}, nil
			} else {
				return r.errorOut(ctx, pvb, err, "error to create data path", log)
			}
		}

		r.metrics.RegisterPodVolumeBackupEnqueue(r.nodeName)

		if err := r.initCancelableDataPath(ctx, asyncBR, res, log); err != nil {
			log.WithError(err).Errorf("Failed to init cancelable data path for %s", pvb.Name)

			r.closeDataPath(ctx, pvb.Name)
			return r.errorOut(ctx, pvb, err, "error initializing data path", log)
		}

		terminated := false
		if err := UpdatePVBWithRetry(ctx, r.client, types.NamespacedName{Namespace: pvb.Namespace, Name: pvb.Name}, log, func(pvb *velerov1api.PodVolumeBackup) bool {
			if isPVBInFinalState(pvb) {
				terminated = true
				return false
			}

			pvb.Status.Phase = velerov1api.PodVolumeBackupPhaseInProgress
			pvb.Status.StartTimestamp = &metav1.Time{Time: r.clock.Now()}

			return true
		}); err != nil {
			log.WithError(err).Warnf("Failed to update pvb %s to InProgress, will data path close and retry", pvb.Name)

			r.closeDataPath(ctx, pvb.Name)
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second * 5}, nil
		}

		if terminated {
			log.Warnf("pvb %s is terminated during transition from prepared", pvb.Name)
			r.closeDataPath(ctx, pvb.Name)
			return ctrl.Result{}, nil
		}

		log.Info("PVB is marked as in progress")

		if err := r.startCancelableDataPath(asyncBR, pvb, res, log); err != nil {
			log.WithError(err).Errorf("Failed to start cancelable data path for %s", pvb.Name)
			r.closeDataPath(ctx, pvb.Name)

			return r.errorOut(ctx, pvb, err, "error starting data path", log)
		}

		return ctrl.Result{}, nil
	} else if pvb.Status.Phase == velerov1api.PodVolumeBackupPhaseInProgress {
		if pvb.Spec.Cancel {
			if pvb.Spec.Node != r.nodeName {
				return ctrl.Result{}, nil
			}

			log.Info("In progress PVB is being canceled")

			asyncBR := r.dataPathMgr.GetAsyncBR(pvb.Name)
			if asyncBR == nil {
				r.OnDataPathCancelled(ctx, pvb.GetNamespace(), pvb.GetName())
				return ctrl.Result{}, nil
			}

			// Update status to Canceling
			if err := UpdatePVBWithRetry(ctx, r.client, types.NamespacedName{Namespace: pvb.Namespace, Name: pvb.Name}, log, func(pvb *velerov1api.PodVolumeBackup) bool {
				if isPVBInFinalState(pvb) {
					log.Warnf("pvb %s is terminated, abort setting it to cancelling", pvb.Name)
					return false
				}

				pvb.Status.Phase = velerov1api.PodVolumeBackupPhaseCanceling
				return true
			}); err != nil {
				log.WithError(err).Error("error updating pvb into canceling status")
				return ctrl.Result{}, err
			}

			asyncBR.Cancel()
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
}

func (r *PodVolumeBackupReconciler) acceptPodVolumeBackup(ctx context.Context, pvb *velerov1api.PodVolumeBackup) (bool, error) {
	r.logger.Infof("Accepting pvb %s", pvb.Name)

	updated := pvb.DeepCopy()

	updateFunc := func(pvb *velerov1api.PodVolumeBackup) {
		pvb.Status.AcceptedTimestamp = &metav1.Time{Time: r.clock.Now()}
		pvb.Status.Phase = velerov1api.PodVolumeBackupPhaseAccepted
	}

	succeeded, err := r.exclusiveUpdatePodVolumeBackup(ctx, updated, updateFunc)

	if err != nil {
		return false, err
	}

	if succeeded {
		updateFunc(pvb) // If update success, it's need to update pvb values in memory
		r.logger.WithField("pvb", pvb.Name).Infof("This pvb has been accepted by %s", r.nodeName)
		return true, nil
	}

	r.logger.WithField("pvb", pvb.Name).Info("This pvb is not successfully accepted")
	return false, nil
}

func (r *PodVolumeBackupReconciler) tryCancelPodVolumeBackup(ctx context.Context, pvb *velerov1api.PodVolumeBackup, message string) bool {
	log := r.logger.WithField("pvb", pvb.Name)
	succeeded, err := r.exclusiveUpdatePodVolumeBackup(ctx, pvb, func(pvb *velerov1api.PodVolumeBackup) {
		pvb.Status.Phase = velerov1api.PodVolumeBackupPhaseCanceled
		if pvb.Status.StartTimestamp.IsZero() {
			pvb.Status.StartTimestamp = &metav1.Time{Time: r.clock.Now()}
		}
		pvb.Status.CompletionTimestamp = &metav1.Time{Time: r.clock.Now()}

		if message != "" {
			pvb.Status.Message = message
		}
	})

	if err != nil {
		log.WithError(err).Error("error updating pvb status")
		return false
	} else if !succeeded {
		log.Warn("conflict in updating pvb status and will try it again later")
		return false
	}

	r.exposer.CleanUp(ctx, getPVBOwnerObject(pvb))

	log.Warn("pvb is canceled")

	return true
}

func (r *PodVolumeBackupReconciler) exclusiveUpdatePodVolumeBackup(ctx context.Context, pvb *velerov1api.PodVolumeBackup,
	updateFunc func(*velerov1api.PodVolumeBackup)) (bool, error) {
	updateFunc(pvb)

	err := r.client.Update(ctx, pvb)
	if err == nil {
		return true, nil
	}

	if apierrors.IsConflict(err) {
		return false, nil
	} else {
		return false, err
	}
}

func (r *PodVolumeBackupReconciler) onPrepareTimeout(ctx context.Context, pvb *velerov1api.PodVolumeBackup) {
	log := r.logger.WithField("pvb", pvb.Name)

	log.Info("Timeout happened for preparing pvb")

	succeeded, err := r.exclusiveUpdatePodVolumeBackup(ctx, pvb, func(pvb *velerov1api.PodVolumeBackup) {
		pvb.Status.Phase = velerov1api.PodVolumeBackupPhaseFailed
		pvb.Status.Message = "timeout on preparing pvb"
	})

	if err != nil {
		log.WithError(err).Warn("Failed to update pvb")
		return
	}

	if !succeeded {
		log.Warn("pvb has been updated by others")
		return
	}

	diags := strings.Split(r.exposer.DiagnoseExpose(ctx, getPVBOwnerObject(pvb)), "\n")
	for _, diag := range diags {
		log.Warnf("[Diagnose pvb expose]%s", diag)
	}

	r.exposer.CleanUp(ctx, getPVBOwnerObject(pvb))

	log.Info("pvb has been cleaned up")
}

func (r *PodVolumeBackupReconciler) initCancelableDataPath(ctx context.Context, asyncBR datapath.AsyncBR, res *exposer.ExposeResult, log logrus.FieldLogger) error {
	log.Info("Init cancelable pvb")

	if err := asyncBR.Init(ctx, nil); err != nil {
		return errors.Wrap(err, "error initializing asyncBR")
	}

	log.Infof("async data path init for pod %s, volume %s", res.ByPod.HostingPod.Name, res.ByPod.VolumeName)

	return nil
}

func (r *PodVolumeBackupReconciler) startCancelableDataPath(asyncBR datapath.AsyncBR, pvb *velerov1api.PodVolumeBackup, res *exposer.ExposeResult, log logrus.FieldLogger) error {
	log.Info("Start cancelable pvb")

	if err := asyncBR.StartBackup(datapath.AccessPoint{
		ByPath: res.ByPod.VolumeName,
	}, pvb.Spec.UploaderSettings, nil); err != nil {
		return errors.Wrapf(err, "error starting async backup for pod %s, volume %s", res.ByPod.HostingPod.Name, res.ByPod.VolumeName)
	}

	log.Infof("Async backup started for pod %s, volume %s", res.ByPod.HostingPod.Name, res.ByPod.VolumeName)
	return nil
}

func (r *PodVolumeBackupReconciler) OnDataPathCompleted(ctx context.Context, namespace string, pvbName string, result datapath.Result) {
	defer r.dataPathMgr.RemoveAsyncBR(pvbName)

	log := r.logger.WithField("pvb", pvbName)

	log.WithField("PVB", pvbName).Info("Async fs backup data path completed")

	pvb := &velerov1api.PodVolumeBackup{}
	if err := r.client.Get(ctx, types.NamespacedName{Name: pvbName, Namespace: namespace}, pvb); err != nil {
		log.WithError(err).Warn("Failed to get PVB on completion")
		return
	}

	log.Info("Cleaning up exposed environment")
	r.exposer.CleanUp(ctx, getPVBOwnerObject(pvb))

	// Update status to Completed with path & snapshot ID.
	if err := UpdatePVBWithRetry(ctx, r.client, types.NamespacedName{Namespace: pvb.Namespace, Name: pvb.Name}, log, func(pvb *velerov1api.PodVolumeBackup) bool {
		if isPVBInFinalState(pvb) {
			return false
		}

		pvb.Status.Path = result.Backup.Source.ByPath
		pvb.Status.Phase = velerov1api.PodVolumeBackupPhaseCompleted
		pvb.Status.SnapshotID = result.Backup.SnapshotID
		pvb.Status.CompletionTimestamp = &metav1.Time{Time: r.clock.Now()}
		if result.Backup.EmptySnapshot {
			pvb.Status.Message = "volume was empty so no snapshot was taken"
		}

		return true
	}); err != nil {
		log.WithError(err).Error("error updating PodVolumeBackup status")
	} else {
		latencyDuration := pvb.Status.CompletionTimestamp.Time.Sub(pvb.Status.StartTimestamp.Time)
		latencySeconds := float64(latencyDuration / time.Second)
		backupName := fmt.Sprintf("%s/%s", pvb.Namespace, pvb.OwnerReferences[0].Name)
		generateOpName := fmt.Sprintf("%s-%s-%s-%s-backup", pvb.Name, pvb.Spec.BackupStorageLocation, pvb.Spec.Pod.Namespace, pvb.Spec.UploaderType)
		r.metrics.ObservePodVolumeOpLatency(r.nodeName, pvb.Name, generateOpName, backupName, latencySeconds)
		r.metrics.RegisterPodVolumeOpLatencyGauge(r.nodeName, pvb.Name, generateOpName, backupName, latencySeconds)
		r.metrics.RegisterPodVolumeBackupDequeue(r.nodeName)

		log.Info("PodVolumeBackup completed")
	}
}

func (r *PodVolumeBackupReconciler) OnDataPathFailed(ctx context.Context, namespace, pvbName string, err error) {
	defer r.dataPathMgr.RemoveAsyncBR(pvbName)

	log := r.logger.WithField("pvb", pvbName)

	log.WithError(err).Error("Async fs backup data path failed")

	var pvb velerov1api.PodVolumeBackup
	if getErr := r.client.Get(ctx, types.NamespacedName{Name: pvbName, Namespace: namespace}, &pvb); getErr != nil {
		log.WithError(getErr).Warn("Failed to get PVB on failure")
	} else {
		_, _ = r.errorOut(ctx, &pvb, err, "data path backup failed", log)
	}
}

func (r *PodVolumeBackupReconciler) OnDataPathCancelled(ctx context.Context, namespace string, pvbName string) {
	defer r.dataPathMgr.RemoveAsyncBR(pvbName)

	log := r.logger.WithField("pvb", pvbName)

	log.Warn("Async fs backup data path canceled")

	var pvb velerov1api.PodVolumeBackup
	if getErr := r.client.Get(ctx, types.NamespacedName{Name: pvbName, Namespace: namespace}, &pvb); getErr != nil {
		log.WithError(getErr).Warn("Failed to get PVB on cancel")
		return
	}
	// cleans up any objects generated during the snapshot expose
	r.exposer.CleanUp(ctx, getPVBOwnerObject(&pvb))

	if err := UpdatePVBWithRetry(ctx, r.client, types.NamespacedName{Namespace: pvb.Namespace, Name: pvb.Name}, log, func(pvb *velerov1api.PodVolumeBackup) bool {
		if isPVBInFinalState(pvb) {
			return false
		}

		pvb.Status.Phase = velerov1api.PodVolumeBackupPhaseCanceled
		if pvb.Status.StartTimestamp.IsZero() {
			pvb.Status.StartTimestamp = &metav1.Time{Time: r.clock.Now()}
		}
		pvb.Status.CompletionTimestamp = &metav1.Time{Time: r.clock.Now()}

		return true
	}); err != nil {
		log.WithError(err).Error("error updating pvb status on cancel")
	} else {
		delete(r.cancelledPVB, pvb.Name)
	}
}

func (r *PodVolumeBackupReconciler) OnDataPathProgress(ctx context.Context, namespace string, pvbName string, progress *uploader.Progress) {
	log := r.logger.WithField("pvb", pvbName)

	if err := UpdatePVBWithRetry(ctx, r.client, types.NamespacedName{Namespace: namespace, Name: pvbName}, log, func(pvb *velerov1api.PodVolumeBackup) bool {
		pvb.Status.Progress = veleroapishared.DataMoveOperationProgress{TotalBytes: progress.TotalBytes, BytesDone: progress.BytesDone}
		return true
	}); err != nil {
		log.WithError(err).Error("Failed to update progress")
	}
}

// SetupWithManager registers the PVB controller.
func (r *PodVolumeBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	gp := kube.NewGenericEventPredicate(func(object client.Object) bool {
		pvb := object.(*velerov1api.PodVolumeBackup)
		if pvb.Status.Phase == velerov1api.PodVolumeBackupPhaseAccepted {
			return true
		}

		if pvb.Spec.Cancel && !isPVBInFinalState(pvb) {
			return true
		}

		if isPVBInFinalState(pvb) && !pvb.DeletionTimestamp.IsZero() {
			return true
		}

		return false
	})

	s := kube.NewPeriodicalEnqueueSource(r.logger.WithField("controller", constant.ControllerPodVolumeBackup), r.client, &velerov1api.PodVolumeBackupList{}, preparingMonitorFrequency, kube.PeriodicalEnqueueSourceOption{
		Predicates: []predicate.Predicate{gp},
	})

	return ctrl.NewControllerManagedBy(mgr).
		For(&velerov1api.PodVolumeBackup{}).
		WatchesRawSource(s).
		Watches(&corev1api.Pod{}, kube.EnqueueRequestsFromMapUpdateFunc(r.findPVBForPod),
			builder.WithPredicates(predicate.Funcs{
				UpdateFunc: func(ue event.UpdateEvent) bool {
					newObj := ue.ObjectNew.(*corev1api.Pod)

					if _, ok := newObj.Labels[velerov1api.PVBLabel]; !ok {
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
		Complete(r)
}

func (r *PodVolumeBackupReconciler) findPVBForPod(ctx context.Context, podObj client.Object) []reconcile.Request {
	pod := podObj.(*corev1api.Pod)
	pvb, err := findPVBByPod(r.client, *pod)

	log := r.logger.WithField("pod", pod.Name)
	if err != nil {
		log.WithError(err).Error("unable to get pvb")
		return []reconcile.Request{}
	} else if pvb == nil {
		log.Error("get empty pvb")
		return []reconcile.Request{}
	}
	log = log.WithFields(logrus.Fields{
		"pvb": pvb.Name,
	})

	if pvb.Status.Phase != velerov1api.PodVolumeBackupPhaseAccepted {
		return []reconcile.Request{}
	}

	if pod.Status.Phase == corev1api.PodRunning {
		log.Info("Preparing pvb")

		if err = UpdatePVBWithRetry(context.Background(), r.client, types.NamespacedName{Namespace: pvb.Namespace, Name: pvb.Name}, log,
			func(pvb *velerov1api.PodVolumeBackup) bool {
				if isPVBInFinalState(pvb) {
					log.Warnf("pvb %s is terminated, abort setting it to prepared", pvb.Name)
					return false
				}

				pvb.Status.Phase = velerov1api.PodVolumeBackupPhasePrepared
				return true
			}); err != nil {
			log.WithError(err).Warn("failed to update pvb, prepare will halt for this pvb")
			return []reconcile.Request{}
		}
	} else if unrecoverable, reason := kube.IsPodUnrecoverable(pod, log); unrecoverable {
		err := UpdatePVBWithRetry(context.Background(), r.client, types.NamespacedName{Namespace: pvb.Namespace, Name: pvb.Name}, log,
			func(pvb *velerov1api.PodVolumeBackup) bool {
				if pvb.Spec.Cancel {
					return false
				}

				pvb.Spec.Cancel = true
				pvb.Status.Message = fmt.Sprintf("Cancel pvb because the exposing pod %s/%s is in abnormal status for reason %s", pod.Namespace, pod.Name, reason)

				return true
			})

		if err != nil {
			log.WithError(err).Warn("failed to cancel pvb, and it will wait for prepare timeout")
			return []reconcile.Request{}
		}
		log.Infof("Exposed pod is in abnormal status(reason %s) and pvb is marked as cancel", reason)
	} else {
		return []reconcile.Request{}
	}

	request := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: pvb.Namespace,
			Name:      pvb.Name,
		},
	}
	return []reconcile.Request{request}
}

func (r *PodVolumeBackupReconciler) errorOut(ctx context.Context, pvb *velerov1api.PodVolumeBackup, err error, msg string, log logrus.FieldLogger) (ctrl.Result, error) {
	r.exposer.CleanUp(ctx, getPVBOwnerObject(pvb))

	_ = UpdatePVBStatusToFailed(ctx, r.client, pvb, err, msg, r.clock.Now(), log)

	return ctrl.Result{}, err
}

func UpdatePVBStatusToFailed(ctx context.Context, c client.Client, pvb *velerov1api.PodVolumeBackup, errOut error, msg string, time time.Time, log logrus.FieldLogger) error {
	log.Info("update pvb status to Failed")

	if patchErr := UpdatePVBWithRetry(context.Background(), c, types.NamespacedName{Namespace: pvb.Namespace, Name: pvb.Name}, log,
		func(pvb *velerov1api.PodVolumeBackup) bool {
			if isPVBInFinalState(pvb) {
				return false
			}

			pvb.Status.Phase = velerov1api.PodVolumeBackupPhaseFailed
			pvb.Status.CompletionTimestamp = &metav1.Time{Time: time}
			if dataPathError, ok := errOut.(datapath.DataPathError); ok {
				pvb.Status.SnapshotID = dataPathError.GetSnapshotID()
			}
			if len(strings.TrimSpace(msg)) == 0 {
				pvb.Status.Message = errOut.Error()
			} else {
				pvb.Status.Message = errors.WithMessage(errOut, msg).Error()
			}

			return true
		}); patchErr != nil {
		log.WithError(patchErr).Warn("error updating pvb status")
	}

	return errOut
}

func (r *PodVolumeBackupReconciler) closeDataPath(ctx context.Context, pvbName string) {
	asyncBR := r.dataPathMgr.GetAsyncBR(pvbName)
	if asyncBR != nil {
		asyncBR.Close(ctx)
	}

	r.dataPathMgr.RemoveAsyncBR(pvbName)
}

func (r *PodVolumeBackupReconciler) setupExposeParam(pvb *velerov1api.PodVolumeBackup) (exposer.PodVolumeExposeParam, error) {
	log := r.logger.WithField("pvb", pvb.Name)

	hostingPodLabels := map[string]string{velerov1api.PVBLabel: pvb.Name}
	for _, k := range util.ThirdPartyLabels {
		if v, err := nodeagent.GetLabelValue(context.Background(), r.kubeClient, pvb.Namespace, k, ""); err != nil {
			if err != nodeagent.ErrNodeAgentLabelNotFound {
				log.WithError(err).Warnf("Failed to check node-agent label, skip adding host pod label %s", k)
			}
		} else {
			hostingPodLabels[k] = v
		}
	}

	hostingPodAnnotation := map[string]string{}
	for _, k := range util.ThirdPartyAnnotations {
		if v, err := nodeagent.GetAnnotationValue(context.Background(), r.kubeClient, pvb.Namespace, k, ""); err != nil {
			if err != nodeagent.ErrNodeAgentAnnotationNotFound {
				log.WithError(err).Warnf("Failed to check node-agent annotation, skip adding host pod annotation %s", k)
			}
		} else {
			hostingPodAnnotation[k] = v
		}
	}

	return exposer.PodVolumeExposeParam{
		Type:                  exposer.PodVolumeExposeTypeBackup,
		ClientNamespace:       pvb.Spec.Pod.Namespace,
		ClientPodName:         pvb.Spec.Pod.Name,
		ClientPodVolume:       pvb.Spec.Volume,
		HostingPodLabels:      hostingPodLabels,
		HostingPodAnnotations: hostingPodAnnotation,
		OperationTimeout:      r.resourceTimeout,
		Resources:             r.podResources,
	}, nil
}

func getPVBOwnerObject(pvb *velerov1api.PodVolumeBackup) corev1api.ObjectReference {
	return corev1api.ObjectReference{
		Kind:       pvb.Kind,
		Namespace:  pvb.Namespace,
		Name:       pvb.Name,
		UID:        pvb.UID,
		APIVersion: pvb.APIVersion,
	}
}

func findPVBByPod(client client.Client, pod corev1api.Pod) (*velerov1api.PodVolumeBackup, error) {
	if label, exist := pod.Labels[velerov1api.PVBLabel]; exist {
		pvb := &velerov1api.PodVolumeBackup{}
		err := client.Get(context.Background(), types.NamespacedName{
			Namespace: pod.Namespace,
			Name:      label,
		}, pvb)

		if err != nil {
			return nil, errors.Wrapf(err, "error to find pvb by pod %s/%s", pod.Namespace, pod.Name)
		}
		return pvb, nil
	}
	return nil, nil
}

func isPVBInFinalState(pvb *velerov1api.PodVolumeBackup) bool {
	return pvb.Status.Phase == velerov1api.PodVolumeBackupPhaseFailed ||
		pvb.Status.Phase == velerov1api.PodVolumeBackupPhaseCanceled ||
		pvb.Status.Phase == velerov1api.PodVolumeBackupPhaseCompleted
}

func UpdatePVBWithRetry(ctx context.Context, client client.Client, namespacedName types.NamespacedName, log logrus.FieldLogger, updateFunc func(*velerov1api.PodVolumeBackup) bool) error {
	return wait.PollUntilContextCancel(ctx, time.Millisecond*100, true, func(ctx context.Context) (bool, error) {
		pvb := &velerov1api.PodVolumeBackup{}
		if err := client.Get(ctx, namespacedName, pvb); err != nil {
			return false, errors.Wrap(err, "getting pvb")
		}

		if updateFunc(pvb) {
			err := client.Update(ctx, pvb)
			if err != nil {
				if apierrors.IsConflict(err) {
					log.Warnf("failed to update pvb for %s/%s and will retry it", pvb.Namespace, pvb.Name)
					return false, nil
				} else {
					return false, errors.Wrapf(err, "error updating pvb with error %s/%s", pvb.Namespace, pvb.Name)
				}
			}
		}

		return true, nil
	})
}

var funcResumeCancellablePVB = (*PodVolumeBackupReconciler).resumeCancellableDataPath

func (r *PodVolumeBackupReconciler) AttemptPVBResume(ctx context.Context, logger *logrus.Entry, ns string) error {
	pvbs := &velerov1api.PodVolumeBackupList{}
	if err := r.client.List(ctx, pvbs, &client.ListOptions{Namespace: ns, FieldSelector: fields.OneTermEqualSelector("spec.node", r.nodeName)}); err != nil {
		r.logger.WithError(errors.WithStack(err)).Error("failed to list pvbs")
		return errors.Wrapf(err, "error to list pvbs")
	}

	for i := range pvbs.Items {
		pvb := &pvbs.Items[i]
		if pvb.Status.Phase == velerov1api.PodVolumeBackupPhaseInProgress {
			if pvb.Spec.Node != r.nodeName {
				logger.WithField("pvb", pvb.Name).WithField("current node", r.nodeName).Infof("pvb should be resumed by another node %s", pvb.Spec.Node)
				continue
			}

			err := funcResumeCancellablePVB(r, ctx, pvb, logger)
			if err == nil {
				logger.WithField("pvb", pvb.Name).WithField("current node", r.nodeName).Info("Completed to resume in progress pvb")
				continue
			}

			logger.WithField("pvb", pvb.GetName()).WithError(err).Warn("Failed to resume data path for pvb, have to cancel it")

			resumeErr := err
			err = UpdatePVBWithRetry(ctx, r.client, types.NamespacedName{Namespace: pvb.Namespace, Name: pvb.Name}, logger.WithField("pvb", pvb.Name),
				func(pvb *velerov1api.PodVolumeBackup) bool {
					if pvb.Spec.Cancel {
						return false
					}

					pvb.Spec.Cancel = true
					pvb.Status.Message = fmt.Sprintf("Resume InProgress pvb failed with error %v, mark it as cancel", resumeErr)

					return true
				})
			if err != nil {
				logger.WithField("pvb", pvb.GetName()).WithError(errors.WithStack(err)).Error("Failed to trigger pvb cancel")
			}
		} else {
			logger.WithField("pvb", pvb.GetName()).Infof("find a pvb with status %s", pvb.Status.Phase)
		}
	}

	return nil
}

func (r *PodVolumeBackupReconciler) resumeCancellableDataPath(ctx context.Context, pvb *velerov1api.PodVolumeBackup, log logrus.FieldLogger) error {
	log.Info("Resume cancelable pvb")

	res, err := r.exposer.GetExposed(ctx, getPVBOwnerObject(pvb), r.client, r.nodeName)
	if err != nil {
		return errors.Wrapf(err, "error to get exposed pvb %s", pvb.Name)
	}

	if res == nil {
		return errors.Errorf("expose info missed for pvb %s", pvb.Name)
	}

	callbacks := datapath.Callbacks{
		OnCompleted: r.OnDataPathCompleted,
		OnFailed:    r.OnDataPathFailed,
		OnCancelled: r.OnDataPathCancelled,
		OnProgress:  r.OnDataPathProgress,
	}

	asyncBR, err := r.dataPathMgr.CreateMicroServiceBRWatcher(ctx, r.client, r.kubeClient, r.mgr, datapath.TaskTypeBackup, pvb.Name, pvb.Namespace, res.ByPod.HostingPod.Name, res.ByPod.HostingContainer, pvb.Name, callbacks, true, log)
	if err != nil {
		return errors.Wrapf(err, "error to create asyncBR watcher for pvb %s", pvb.Name)
	}

	resumeComplete := false
	defer func() {
		if !resumeComplete {
			r.closeDataPath(ctx, pvb.Name)
		}
	}()

	if err := asyncBR.Init(ctx, nil); err != nil {
		return errors.Wrapf(err, "error to init asyncBR watcher pvb pvb %s", pvb.Name)
	}

	if err := asyncBR.StartBackup(datapath.AccessPoint{
		ByPath: res.ByPod.VolumeName,
	}, pvb.Spec.UploaderSettings, nil); err != nil {
		return errors.Wrapf(err, "error to resume asyncBR watcher for pvb %s", pvb.Name)
	}

	resumeComplete = true

	log.Infof("asyncBR is resumed for pvb %s", pvb.Name)

	return nil
}
