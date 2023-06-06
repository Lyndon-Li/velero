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
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	corev1api "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/clock"
	clocks "k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/vmware-tanzu/velero/internal/credentials"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/repository"
	repokey "github.com/vmware-tanzu/velero/pkg/repository/keys"
	"github.com/vmware-tanzu/velero/pkg/uploader"
	"github.com/vmware-tanzu/velero/pkg/uploader/provider"
	"github.com/vmware-tanzu/velero/pkg/util/boolptr"
	"github.com/vmware-tanzu/velero/pkg/util/datamover"
	"github.com/vmware-tanzu/velero/pkg/util/filesystem"
	"github.com/vmware-tanzu/velero/pkg/util/kube"
)

func NewSnapshotRestoreReconciler(logger logrus.FieldLogger, client client.Client, kubeClient kubernetes.Interface,
	repoEnsurer *repository.RepositoryEnsurer, credentialGetter *credentials.CredentialGetter, nodeName string) *SnapshotRestoreReconciler {
	return &SnapshotRestoreReconciler{
		Client:            client,
		kubeClient:        kubeClient,
		logger:            logger.WithField("controller", "SnapshotRestore"),
		credentialGetter:  credentialGetter,
		fileSystem:        filesystem.NewFileSystem(),
		clock:             &clock.RealClock{},
		nodeName:          nodeName,
		repositoryEnsurer: repoEnsurer,
		DataPathTracker:   make(map[string]DataPathContext),
	}
}

type SnapshotRestoreReconciler struct {
	client.Client
	kubeClient          kubernetes.Interface
	logger              logrus.FieldLogger
	credentialGetter    *credentials.CredentialGetter
	fileSystem          filesystem.Interface
	clock               clocks.WithTickerAndDelayedExecution
	nodeName            string
	repositoryEnsurer   *repository.RepositoryEnsurer
	DataPathTrackerLock sync.Mutex
	DataPathTracker     map[string]DataPathContext
}

type SnapshotRestoreProgressUpdater struct {
	SnapshotRestore *velerov1api.SnapshotRestore
	Log             logrus.FieldLogger
	Ctx             context.Context
	Cli             client.Client
}

// +kubebuilder:rbac:groups=velero.io,resources=snapshotrestore,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=velero.io,resources=snapshotrestore/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get
// +kubebuilder:rbac:groups="",resources=persistentvolumerclaims,verbs=get

func (s *SnapshotRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := s.logger.WithField("SnapshotRestore", req.NamespacedName.String())

	ssr := &velerov1api.SnapshotRestore{}

	if err := s.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: req.Name}, ssr); err != nil {
		if apierrors.IsNotFound(err) {
			log.Warn("SnapshotRestore not found, skip")
			return ctrl.Result{}, nil
		}
		log.WithError(err).Error("Unable to get the SnapshotRestore")
		return ctrl.Result{}, err
	}

	if ssr.Spec.DataMover != "" {
		log.WithField("data mover", ssr.Spec.DataMover).Info("Snapshot restore is not to be processed by velero")
		return ctrl.Result{}, nil
	}

	if ssr.Status.Phase == "" || ssr.Status.Phase == velerov1api.SnapshotRestorePhaseNew {
		log.Info("Snapshot restore starting")

		if _, err := s.getTargetPVC(ctx, ssr); err != nil {
			return ctrl.Result{Requeue: true}, nil
		}

		accepted, err := s.acceptSnapshotRestore(ctx, ssr)
		if err != nil {
			return s.errorOut(ctx, ssr, err, "error to accept the snapshot restore", log)
		}

		if !accepted {
			return ctrl.Result{}, nil
		}

		log.Info("Snapshot restore is accepted")

		selectedNode, targetPVC, err := kube.WaitPVCMounted(ctx, s.kubeClient.CoreV1(), ssr.Spec.TargetVolume.PVC, ssr.Spec.TargetVolume.Namespace, s.kubeClient.StorageV1(), ssr.Spec.OperationTimeout.Duration)
		if err != nil {
			return s.errorOut(ctx, ssr, err, "error to wait target PVC mounted, %s/%s", log)
		}

		log.WithField("selected node", selectedNode).Info("Target PVC is mounted")

		if err := s.createRestorePod(ctx, ssr, selectedNode, log); err != nil {
			return s.errorOut(ctx, ssr, err, "error creating restore pod", log)
		}

		log.Info("Snapshot restore pod is created")

		if err := s.createRestorePVC(ctx, ssr, selectedNode, targetPVC, log); err != nil {
			return s.errorOut(ctx, ssr, err, "error to create restore pvc", log)
		}

		log.Info("Restore pvc is created")

		return ctrl.Result{}, nil
	} else if ssr.Status.Phase == velerov1api.SnapshotRestorePhasePrepared {
		s.DataPathTrackerLock.Lock()
		_, found := s.DataPathTracker[ssr.Name]
		s.DataPathTrackerLock.Unlock()
		if found {
			log.Info("Cancellable data path is already started")
			return ctrl.Result{}, nil
		}

		path, err := s.waitRestorePVCExposed(ctx, ssr, log)
		if err != nil {
			return s.errorOut(ctx, ssr, err, "restore PVC is not ready", log)
		} else if path == "" {
			return ctrl.Result{}, nil
		}

		log.Info("Restore PVC is ready")

		original := ssr.DeepCopy()
		ssr.Status.Phase = velerov1api.SnapshotRestorePhaseInProgress
		ssr.Status.StartTimestamp = &metav1.Time{Time: s.clock.Now()}
		if err := s.Patch(ctx, ssr, client.MergeFrom(original)); err != nil {
			log.WithError(err).Error("Unable to update status to in progress")
			return ctrl.Result{}, err
		}

		log.Info("SnapshotRestore is marked as in progress")

		s.runCancelableDataPath(ctx, ssr, path, log)

		return ctrl.Result{}, nil
	} else if ssr.Status.Phase == velerov1api.SnapshotRestorePhaseInProgress && ssr.Spec.Cancel {
		s.DataPathTrackerLock.Lock()
		dataPathContext, found := s.DataPathTracker[ssr.Name]
		s.DataPathTrackerLock.Unlock()
		if !found {
			return ctrl.Result{}, nil
		}

		log.Info("Snapshot restore is being canceled")

		// Update status to Canceling.
		original := ssr.DeepCopy()
		ssr.Status.Phase = velerov1api.SnapshotRestorePhaseCanceling
		if err := s.Client.Patch(ctx, ssr, client.MergeFrom(original)); err != nil {
			log.WithError(err).Error("error updating SnapshotRestore status")
			return ctrl.Result{}, err
		}

		dataPathContext.cancelRoutine()

		return ctrl.Result{}, nil
	} else {
		log.WithField("phase", ssr.Status.Phase).Info("Snapshot restore is not in the expected phase")
		return ctrl.Result{}, nil
	}
}

func (s *SnapshotRestoreReconciler) runCancelableDataPath(ctx context.Context, ssr *velerov1api.SnapshotRestore, path string, log logrus.FieldLogger) {
	cancelCtx, cancel := context.WithCancel(ctx)

	log.Info("Creating data path routine")

	go func() {
		defer func() {
			s.DataPathTrackerLock.Lock()
			delete(s.DataPathTracker, ssr.Name)
			s.DataPathTrackerLock.Unlock()

			cancel()
		}()

		backupLocation := &velerov1api.BackupStorageLocation{}
		if err := s.Get(ctx, client.ObjectKey{
			Namespace: ssr.Namespace,
			Name:      ssr.Spec.BackupStorageLocation,
		}, backupLocation); err != nil {
			s.errorOut(ctx, ssr, err, "error getting backup storage location", log)
			return
		}

		backupRepo, err := s.repositoryEnsurer.EnsureRepo(ctx, ssr.Namespace, ssr.Spec.SourceNamespace, ssr.Spec.BackupStorageLocation, datamover.GetUploaderType(ssr.Spec.DataMover))
		if err != nil {
			s.errorOut(ctx, ssr, err, "error ensure backup repository", log)
			return
		}

		uploaderProv, err := provider.NewUploaderProvider(ctx, s.Client, datamover.GetUploaderType(ssr.Spec.DataMover),
			"", backupLocation, backupRepo, s.credentialGetter, repokey.RepoKeySelector(), log)
		if err != nil {
			s.errorOut(ctx, ssr, err, "error creating uploader", log)
			return
		}

		defer func() {
			if err := uploaderProv.Close(ctx); err != nil {
				log.Errorf("failed to close uploader provider with error %v", err)
			}
		}()

		err = uploaderProv.RunRestore(cancelCtx, ssr.Spec.SnapshotID, path, s.NewSnapshotBackupProgressUpdater(ssr, log, ctx))
		if err != nil && err != provider.ErrorCanceled {
			s.errorOut(ctx, ssr, err, fmt.Sprintf("running restore, err=%v", err), log)
			return
		}

		if err == nil {
			if err := s.rebindRestoreVolume(ctx, ssr, log); err != nil {
				s.errorOut(ctx, ssr, err, "error rebind restore volume", log)
				return
			}
		}

		log.Info("Cleaning up exposed environment")
		s.cleanUpExposeEnv(ctx, ssr, log)

		original := ssr.DeepCopy()

		if err == provider.ErrorCanceled {
			ssr.Status.Phase = velerov1api.SnapshotRestorePhaseCanceled
			ssr.Status.CompletionTimestamp = &metav1.Time{Time: s.clock.Now()}
		} else {
			ssr.Status.Phase = velerov1api.SnapshotRestorePhaseCompleted
			ssr.Status.CompletionTimestamp = &metav1.Time{Time: s.clock.Now()}
		}

		if err := s.Patch(ctx, ssr.DeepCopy(), client.MergeFrom(original)); err != nil {
			log.WithError(err).Error("Unable to update status")
		} else {
			log.Infof("SnapshotRestore is marked as %s", ssr.Status.Phase)
		}
	}()

	s.DataPathTrackerLock.Lock()
	s.DataPathTracker[ssr.Name] = DataPathContext{cancelRoutine: cancel}
	s.DataPathTrackerLock.Unlock()
}

func (s *SnapshotRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&velerov1api.SnapshotRestore{}).
		Watches(&source.Kind{Type: &corev1.Pod{}}, kube.EnqueueRequestsFromMapUpdateFunc(s.findSnapshotRestoreForPod),
			builder.WithPredicates(predicate.Funcs{
				UpdateFunc: func(ue event.UpdateEvent) bool {
					newObj := ue.ObjectNew.(*corev1.Pod)

					if _, ok := newObj.Labels[velerov1api.SnapshotRestoreLabel]; !ok {
						return false
					}

					if newObj.Status.Phase != v1.PodRunning {
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
		Complete(s)
}

func (s *SnapshotRestoreReconciler) findSnapshotRestoreForPod(podObj client.Object) []reconcile.Request {
	pod := podObj.(*corev1.Pod)

	ssr := &velerov1api.SnapshotRestore{}
	err := s.Client.Get(context.Background(), types.NamespacedName{
		Namespace: pod.Namespace,
		Name:      pod.Labels[velerov1api.SnapshotRestoreLabel],
	}, ssr)

	if err != nil {
		s.logger.WithField("Restore pod", pod.Name).WithError(err).Error("unable to get SnapshotRestore")
		return []reconcile.Request{}
	}

	if ssr.Status.Phase != velerov1api.SnapshotRestorePhaseAccepted {
		return []reconcile.Request{}
	}

	requests := make([]reconcile.Request, 1)

	s.logger.WithField("Restore pod", pod.Name).Infof("Preparing SnapshotRestore %s", ssr.Name)
	s.patchSnapshotRestore(context.Background(), ssr, prepareSnapshotRestore)

	requests[0] = reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: ssr.Namespace,
			Name:      ssr.Name,
		},
	}

	return requests
}

func (s *SnapshotRestoreReconciler) patchSnapshotRestore(ctx context.Context, req *velerov1api.SnapshotRestore, mutate func(*velerov1api.SnapshotRestore)) error {
	original := req.DeepCopy()
	mutate(req)
	if err := s.Client.Patch(ctx, req, client.MergeFrom(original)); err != nil {
		return errors.Wrap(err, "error patching SnapshotRestore")
	}

	return nil
}

func prepareSnapshotRestore(ssb *velerov1api.SnapshotRestore) {
	ssb.Status.Phase = velerov1api.SnapshotRestorePhasePrepared
}

func (s *SnapshotRestoreReconciler) errorOut(ctx context.Context, ssr *velerov1api.SnapshotRestore, err error, msg string, log logrus.FieldLogger) (ctrl.Result, error) {
	s.cleanUpExposeEnv(ctx, ssr, log)

	return ctrl.Result{}, s.updateStatusToFailed(ctx, ssr, err, msg, log)
}

func (s *SnapshotRestoreReconciler) updateStatusToFailed(ctx context.Context, ssr *velerov1api.SnapshotRestore, err error, msg string, log logrus.FieldLogger) error {
	log.Infof("updateStatusToFailed %v", ssr.Status.Phase)
	original := ssr.DeepCopy()
	ssr.Status.Phase = velerov1api.SnapshotRestorePhaseFailed
	ssr.Status.Message = errors.WithMessage(err, msg).Error()
	ssr.Status.CompletionTimestamp = &metav1.Time{Time: s.clock.Now()}

	if err = s.Client.Patch(ctx, ssr, client.MergeFrom(original)); err != nil {
		log.WithError(err).Error("error updating SnapshotRestore status")
		return err
	}

	return nil
}

func (s *SnapshotRestoreReconciler) NewSnapshotBackupProgressUpdater(ssr *velerov1api.SnapshotRestore, log logrus.FieldLogger, ctx context.Context) *SnapshotRestoreProgressUpdater {
	return &SnapshotRestoreProgressUpdater{ssr, log, ctx, s.Client}
}

// UpdateProgress which implement ProgressUpdater interface to update pvr progress status
func (s *SnapshotRestoreProgressUpdater) UpdateProgress(p *uploader.UploaderProgress) {
	restoreVCName := s.SnapshotRestore.Name
	original := s.SnapshotRestore.DeepCopy()
	s.SnapshotRestore.Status.Progress = velerov1api.DataMoveOperationProgress{TotalBytes: p.TotalBytes, BytesDone: p.BytesDone}
	if s.Cli == nil {
		s.Log.Errorf("failed to update snapshot %s restore progress with uninitailize client", restoreVCName)
		return
	}
	if err := s.Cli.Patch(s.Ctx, s.SnapshotRestore, client.MergeFrom(original)); err != nil {
		s.Log.Errorf("update restore snapshot %s  progress with %v", restoreVCName, err)
	}
}

func (r *SnapshotRestoreReconciler) acceptSnapshotRestore(ctx context.Context, ssr *velerov1api.SnapshotRestore) (bool, error) {
	updated := ssr.DeepCopy()
	updated.Status.Phase = velerov1api.SnapshotRestorePhaseAccepted

	r.logger.Infof("Accepting snapshot restore %s", ssr.Name)

	time.Sleep(2 * time.Second)

	err := r.Client.Update(ctx, updated)
	if err == nil {
		return true, nil
	} else if apierrors.IsConflict(err) {
		r.logger.WithField("SnapshotRestore", ssr.Name).Error("This snapshot restore has been accepted by others")
		return false, nil
	} else {
		return false, err
	}
}

func (s *SnapshotRestoreReconciler) createRestorePod(ctx context.Context, ssr *velerov1api.SnapshotRestore, selectedNode string, log logrus.FieldLogger) error {
	restorePodName := ssr.Name
	restorePVCName := ssr.Name

	var gracePeriod int64 = 0

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      restorePodName,
			Namespace: ssr.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: velerov1api.SchemeGroupVersion.String(),
					Kind:       "SnapshotRestore",
					Name:       ssr.Name,
					UID:        ssr.UID,
					Controller: boolptr.True(),
				},
			},
			Labels: map[string]string{
				velerov1api.SnapshotRestoreLabel: ssr.Name,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:    restorePodName,
					Image:   "alpine:latest",
					Command: []string{"sleep", "infinity"},
					VolumeMounts: []corev1.VolumeMount{{
						Name:      restorePVCName,
						MountPath: "/" + restorePVCName,
					}},
				},
			},
			TerminationGracePeriodSeconds: &gracePeriod,
			Volumes: []corev1.Volume{{
				Name: restorePVCName,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: restorePVCName,
					},
				},
			}},
			NodeName: selectedNode,
		},
	}

	return s.Client.Create(ctx, pod, &client.CreateOptions{})
}

func (s *SnapshotRestoreReconciler) getTargetPVC(ctx context.Context, ssr *velerov1api.SnapshotRestore) (*v1.PersistentVolumeClaim, error) {
	return s.kubeClient.CoreV1().PersistentVolumeClaims(ssr.Spec.TargetVolume.Namespace).Get(ctx, ssr.Spec.TargetVolume.PVC, metav1.GetOptions{})
}

func (s *SnapshotRestoreReconciler) createRestorePVC(ctx context.Context, ssr *velerov1api.SnapshotRestore, selectedNode string, targetPVC *v1.PersistentVolumeClaim, log logrus.FieldLogger) error {
	restorePVCName := ssr.Name

	pvcObj := &corev1api.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   ssr.Namespace,
			Name:        restorePVCName,
			Labels:      targetPVC.Labels,
			Annotations: targetPVC.Annotations,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: velerov1api.SchemeGroupVersion.String(),
					Kind:       "SnapshotRestore",
					Name:       ssr.Name,
					UID:        ssr.UID,
					Controller: boolptr.True(),
				},
			},
		},
		Spec: corev1api.PersistentVolumeClaimSpec{
			AccessModes:      targetPVC.Spec.AccessModes,
			StorageClassName: targetPVC.Spec.StorageClassName,
			VolumeMode:       targetPVC.Spec.VolumeMode,
			Resources:        targetPVC.Spec.Resources,
		},
	}

	if selectedNode != "" {
		pvcObj.Annotations = map[string]string{
			kube.KubeAnnSelectedNode: selectedNode,
		}
	}

	restorePVC, err := s.kubeClient.CoreV1().PersistentVolumeClaims(pvcObj.Namespace).Create(ctx, pvcObj, metav1.CreateOptions{})
	if err != nil {
		return errors.Wrap(err, "error to create restore pvc")
	}

	log.WithField("restorePVC", restorePVC.Name).Infof("Restore PVC is created")

	defer func() {
		if err != nil {
			kube.DeletePVCIfAny(ctx, s.kubeClient.CoreV1(), restorePVC.Name, restorePVC.Namespace, log)
		}
	}()

	return nil
}

func (s *SnapshotRestoreReconciler) waitRestorePVCExposed(ctx context.Context, ssr *velerov1api.SnapshotRestore, log logrus.FieldLogger) (string, error) {
	restorePodName := ssr.Name
	restorePVCName := ssr.Name

	pod := &corev1.Pod{}
	err := s.Client.Get(ctx, types.NamespacedName{
		Namespace: ssr.Namespace,
		Name:      restorePodName,
	}, pod)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.WithField("restore pod", restorePodName).Infof("Restore pod is not running in the current node %s", s.nodeName)
			return "", nil
		} else {
			return "", errors.Wrapf(err, "error to get restore pod %s", restorePodName)
		}
	}

	log.WithField("pod", pod.Name).Infof("Restore pod is in running state in node %s", s.nodeName)

	pvc := &corev1.PersistentVolumeClaim{}
	pollInterval := 2 * time.Second
	if err := wait.PollImmediate(pollInterval, ssr.Spec.OperationTimeout.Duration, func() (done bool, err error) {
		if err := s.Client.Get(ctx, types.NamespacedName{
			Namespace: ssr.Namespace,
			Name:      restorePVCName,
		}, pvc); err != nil {
			return false, err
		} else {
			return pvc.Status.Phase == v1.ClaimBound, nil
		}
	}); err != nil {
		if err == wait.ErrWaitTimeout {
			log.WithField("restore pvc", restorePVCName).Error("restore pvc is not bound till timeout")
			return "", nil
		} else {
			return "", errors.Wrapf(err, "error to get restore PVC %s", restorePVCName)
		}
	}

	log.WithField("restore pvc", restorePVCName).Info("Restore PVC is bound")

	path, err := datamover.GetPodVolumeHostPath(ctx, pod, pvc, s.Client, s.fileSystem, log)
	if err != nil {
		return "", errors.Wrap(err, "error to get restore pvc host path")
	}

	log.WithField("restore pod", pod.Name).WithField("restore pvc", pvc.Name).Infof("Got restore PVC host path %s", path)

	return path, nil
}

func (s *SnapshotRestoreReconciler) cleanUpExposeEnv(ctx context.Context, ssr *velerov1api.SnapshotRestore, log logrus.FieldLogger) {
	restorePodName := ssr.Name
	restorePVCName := ssr.Name

	kube.DeletePodIfAny(ctx, s.kubeClient.CoreV1(), restorePodName, ssr.Namespace, log)
	kube.DeletePVCIfAny(ctx, s.kubeClient.CoreV1(), restorePVCName, ssr.Namespace, log)
}

func (s *SnapshotRestoreReconciler) rebindRestoreVolume(ctx context.Context, ssr *velerov1api.SnapshotRestore, log logrus.FieldLogger) error {
	restorePodName := ssr.Name
	restorePVCName := ssr.Name

	targetPVC, err := s.getTargetPVC(ctx, ssr)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("Failed to get target PVC"))
	}

	_, restorePV, err := kube.WaitPVCBound(ctx, s.kubeClient.CoreV1(), s.kubeClient.CoreV1(), restorePVCName, ssr.Namespace, ssr.Spec.OperationTimeout.Duration)
	if err != nil {
		return errors.Wrapf(err, fmt.Sprintf("Failed to get PV from restore PVC %s", restorePVCName))
	}

	orgReclaim := restorePV.Spec.PersistentVolumeReclaimPolicy

	log.WithField("restore PV", restorePV.Name).Info("Restore PV is retrieved")

	retained, err := kube.RetainPV(ctx, s.kubeClient.CoreV1(), restorePV)
	if err != nil {
		return errors.Wrapf(err, fmt.Sprintf("Failed to retain PV %s", restorePV.Name))
	}

	log.WithField("restore PV", restorePV.Name).WithField("retained", (retained != nil)).Info("Restore PV is retained")

	defer func() {
		if retained != nil {
			log.WithField("restore PV", retained.Name).Info("Deleting retained PV on error")
			kube.DeletePVIfAny(ctx, s.kubeClient.CoreV1(), retained.Name, log)
		}
	}()

	if retained != nil {
		restorePV = retained
	}

	err = kube.EnsureDeletePod(ctx, s.kubeClient.CoreV1(), restorePodName, ssr.Namespace, ssr.Spec.OperationTimeout.Duration)
	if err != nil {
		return errors.Wrapf(err, fmt.Sprintf("Failed to delete restore pod %s", restorePodName))
	}

	err = kube.EnsureDeletePVC(ctx, s.kubeClient.CoreV1(), restorePVCName, ssr.Namespace, ssr.Spec.OperationTimeout.Duration)
	if err != nil {
		return errors.Wrapf(err, fmt.Sprintf("Failed to delete restore PVC %s", restorePVCName))
	}

	log.WithField("restore PVC", restorePVCName).Info("Restore PVC is deleted")

	_, err = kube.RebindPVC(ctx, s.kubeClient.CoreV1(), targetPVC, restorePV.Name)
	if err != nil {
		return errors.Wrapf(err, fmt.Sprintf("Failed to rebind target PVC %s/%s to %s", targetPVC.Namespace, targetPVC.Name, restorePV.Name))
	}

	log.WithField("tartet PVC", fmt.Sprintf("%s/%s", targetPVC.Namespace, targetPVC.Name)).WithField("restore PV", restorePV.Name).Info("Target PVC is rebound to restore PV")

	var matchLabel map[string]string
	if targetPVC.Spec.Selector != nil {
		matchLabel = targetPVC.Spec.Selector.MatchLabels
	}

	restorePVName := restorePV.Name
	restorePV, err = kube.RebindPV(ctx, s.kubeClient.CoreV1(), restorePV, matchLabel)
	if err != nil {
		return errors.Wrapf(err, fmt.Sprintf("Failed to rebind restore PV %s", restorePVName))
	}

	log.WithField("restore PV", restorePV.Name).Info("Restore PV is rebound")

	restorePV, err = kube.WaitPVBound(ctx, s.kubeClient.CoreV1(), restorePV.Name, targetPVC.Name, targetPVC.Namespace, ssr.Spec.OperationTimeout.Duration)
	if err != nil {
		return errors.Wrapf(err, "error to wait restore PV bound, restore PV %s", restorePVName)
	}

	log.WithField("restore PV", restorePV.Name).Info("Restore PV is ready")

	retained = nil

	err = kube.SetPVReclaimPolicy(ctx, s.kubeClient.CoreV1(), restorePV, orgReclaim)
	if err != nil {
		log.WithField("restore PV", restorePV.Name).WithError(err).Warn("Restore PV's reclaim policy is not restored")
	} else {
		log.WithField("restore PV", restorePV.Name).Info("Restore PV's reclaim policy is restored")
	}

	return nil
}

func createWorkloadPV(ctx context.Context, kubeClient kubernetes.Interface, restorePV *corev1api.PersistentVolume, name string) (*corev1api.PersistentVolume, error) {
	updated := restorePV.DeepCopy()

	annotations := updated.GetAnnotations()
	delete(updated.Annotations, kube.KubeAnnBindCompleted)
	delete(updated.Annotations, kube.KubeAnnBoundByController)
	updated.SetAnnotations(annotations)

	updated.Spec.ClaimRef = nil
	updated.Name = name
	updated.ResourceVersion = ""
	updated.UID = ""

	created, err := kubeClient.CoreV1().PersistentVolumes().Create(ctx, updated, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}

	return created, nil
}
