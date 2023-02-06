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
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	corev1api "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/vmware-tanzu/velero/internal/credentials"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/datamover"
	"github.com/vmware-tanzu/velero/pkg/repository"
	repokey "github.com/vmware-tanzu/velero/pkg/repository/keys"
	"github.com/vmware-tanzu/velero/pkg/uploader"
	"github.com/vmware-tanzu/velero/pkg/uploader/provider"
	"github.com/vmware-tanzu/velero/pkg/util/boolptr"
	"github.com/vmware-tanzu/velero/pkg/util/filesystem"
	"github.com/vmware-tanzu/velero/pkg/util/kube"
)

func NewSnapshotRestoreReconciler(logger logrus.FieldLogger, client client.Client, kubeClient kubernetes.Interface, credentialGetter *credentials.CredentialGetter, nodeName string) *SnapshotRestoreReconciler {
	return &SnapshotRestoreReconciler{
		Client:            client,
		kubeClient:        kubeClient,
		logger:            logger.WithField("controller", "SnapshotRestore"),
		credentialGetter:  credentialGetter,
		fileSystem:        filesystem.NewFileSystem(),
		clock:             &clock.RealClock{},
		nodeName:          nodeName,
		repositoryEnsurer: repository.NewRepositoryEnsurer(client, logger),
	}
}

type SnapshotRestoreReconciler struct {
	client.Client
	kubeClient        kubernetes.Interface
	logger            logrus.FieldLogger
	credentialGetter  *credentials.CredentialGetter
	fileSystem        filesystem.Interface
	clock             clock.Clock
	nodeName          string
	repositoryEnsurer *repository.RepositoryEnsurer
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

	switch ssr.Status.Phase {
	case "", velerov1api.SnapshotRestorePhaseNew, velerov1api.SnapshotRestorePhasePrepared:
		// Only process new items.
	default:
		log.Debug("Snapshot restore is not new or Prepared, not processing")
		return ctrl.Result{}, nil
	}

	log.Infof("Snapshot restore starting with phase %v", ssr.Status.Phase)

	if ssr.Status.Phase == "" || ssr.Status.Phase == velerov1api.SnapshotRestorePhaseNew {

		if err := s.createRestorePod(ctx, ssr, log); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return s.errorOut(ctx, ssr, err, "error creating restore pod", log)
			} else {
				return ctrl.Result{}, nil
			}
		}

		log.Info("Snapshot restore pod is created")

		if err := s.createRestorePVC(ctx, ssr, log); err != nil {
			return s.errorOut(ctx, ssr, err, "error to create restore pvc", log)
		}

		log.Info("Restore pvc is created")

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

	if err := s.processRestore(ctx, ssr, path, log); err != nil {
		log.WithError(err).Error("Unable to process the SnapshotRestore")
		return s.errorOut(ctx, ssr, err, fmt.Sprintf("running restore, stderr=%v", err), log)
	}

	err = s.rebindRestoreVolume(ctx, ssr, log)
	if err != nil {
		return s.errorOut(ctx, ssr, err, "error rebind restore volume", log)
	}

	log.Info("Cleaning up exposed environment")
	s.cleanUpExposeEnv(ctx, ssr, log)

	original = ssr.DeepCopy()
	ssr.Status.Phase = velerov1api.SnapshotRestorePhaseCompleted
	ssr.Status.CompletionTimestamp = &metav1.Time{Time: s.clock.Now()}
	if err := s.Patch(ctx, ssr.DeepCopy(), client.MergeFrom(original)); err != nil {
		log.WithError(err).Error("Unable to update status to completed")
		return ctrl.Result{}, err
	}
	log.Info("Restore completed")
	return ctrl.Result{}, nil

}

func (s *SnapshotRestoreReconciler) processRestore(ctx context.Context, req *velerov1api.SnapshotRestore, path string, log logrus.FieldLogger) error {
	backupLocation := &velerov1api.BackupStorageLocation{}
	if err := s.Get(ctx, client.ObjectKey{
		Namespace: req.Namespace,
		Name:      req.Spec.BackupStorageLocation,
	}, backupLocation); err != nil {
		return errors.Wrap(err, "error getting backup storage location")
	}

	backupRepo, err := s.repositoryEnsurer.EnsureRepo(ctx, req.Namespace, req.Spec.SourceNamespace, req.Spec.BackupStorageLocation, datamover.GetUploaderType(req.Spec.DataMover))
	if err != nil {
		return errors.Wrap(err, "error ensure backup repository")
	}

	uploaderProv, err := provider.NewUploaderProvider(ctx, s.Client, datamover.GetUploaderType(req.Spec.DataMover),
		"", backupLocation, backupRepo, s.credentialGetter, repokey.RepoKeySelector(), log)
	if err != nil {
		return errors.Wrap(err, "error creating uploader")
	}

	defer func() {
		if err := uploaderProv.Close(ctx); err != nil {
			log.Errorf("failed to close uploader provider with error %v", err)
		}
	}()

	if err = uploaderProv.RunRestore(ctx, req.Spec.SnapshotID, path, s.NewSnapshotBackupProgressUpdater(req, log, ctx)); err != nil {
		return errors.Wrapf(err, "error running restore err=%v", err)
	}

	return nil
}

func (s *SnapshotRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&velerov1api.SnapshotRestore{}).
		Watches(&source.Kind{Type: &corev1.Pod{}}, handler.EnqueueRequestsFromMapFunc(s.findSnapshotRestoreForPod),
			builder.WithPredicates(predicate.Funcs{
				UpdateFunc: func(ue event.UpdateEvent) bool {
					newObj := ue.ObjectNew.(*corev1.Pod)

					if _, ok := newObj.Labels[velerov1api.SnapshotRestoreLabel]; !ok {
						return false
					}

					s.logger.WithField("pod", newObj.Name).Info("Pod is a snapshot restore")

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

	if ssr.Status.Phase != "" && ssr.Status.Phase != velerov1api.SnapshotRestorePhaseNew {
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

	return s.updateStatusToFailed(ctx, ssr, err, msg, log)
}

func (s *SnapshotRestoreReconciler) updateStatusToFailed(ctx context.Context, ssr *velerov1api.SnapshotRestore, err error, msg string, log logrus.FieldLogger) (ctrl.Result, error) {
	log.Infof("updateStatusToFailed %v", ssr.Status.Phase)
	original := ssr.DeepCopy()
	ssr.Status.Phase = velerov1api.SnapshotRestorePhaseFailed
	ssr.Status.Message = errors.WithMessage(err, msg).Error()
	ssr.Status.CompletionTimestamp = &metav1.Time{Time: s.clock.Now()}

	if err = s.Client.Patch(ctx, ssr, client.MergeFrom(original)); err != nil {
		log.WithError(err).Error("error updating SnapshotRestore status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (s *SnapshotRestoreReconciler) NewSnapshotBackupProgressUpdater(ssr *velerov1api.SnapshotRestore, log logrus.FieldLogger, ctx context.Context) *SnapshotRestoreProgressUpdater {
	return &SnapshotRestoreProgressUpdater{ssr, log, ctx, s.Client}
}

//UpdateProgress which implement ProgressUpdater interface to update pvr progress status
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

func (s *SnapshotRestoreReconciler) createRestorePod(ctx context.Context, ssr *velerov1api.SnapshotRestore, log logrus.FieldLogger) error {
	restorePodName := ssr.Name
	restorePVCName := ssr.Name

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
					Image:   "gcr.io/velero-gcp/busybox",
					Command: []string{"sleep", "infinity"},
					VolumeMounts: []corev1.VolumeMount{{
						Name:      restorePVCName,
						MountPath: "/" + restorePVCName,
					}},
				},
			},
			Volumes: []corev1.Volume{{
				Name: restorePVCName,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: restorePVCName,
					},
				},
			}},
		},
	}

	return s.Client.Create(ctx, pod, &client.CreateOptions{})
}

func (s *SnapshotRestoreReconciler) createRestorePVC(ctx context.Context, ssr *velerov1api.SnapshotRestore, log logrus.FieldLogger) error {
	restorePVCName := ssr.Name

	volumeMode := kube.GetVolumeModeFromDataMover(ssr.Spec.DataMover)

	pvcObj := &corev1api.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ssr.Namespace,
			Name:      restorePVCName,
		},
		Spec: corev1api.PersistentVolumeClaimSpec{
			AccessModes: []corev1api.PersistentVolumeAccessMode{
				corev1api.ReadWriteOnce,
			},
			StorageClassName: &ssr.Spec.TargetVolume.StorageClass,
			VolumeMode:       &volumeMode,
			Resources:        ssr.Spec.TargetVolume.Resources,
		},
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
			log.WithField("restore pod", restorePodName).Errorf("Restore pod is not running in the current node %s", s.nodeName)
			return "", nil
		} else {
			return "", errors.Wrapf(err, "error to get restore pod %s", restorePodName)
		}
	}

	log.WithField("pod", pod.Name).Infof("Restore pod is in running state in node %s", s.nodeName)

	pvc := &corev1.PersistentVolumeClaim{}
	pollInterval := 2 * time.Second
	if err := wait.PollImmediate(pollInterval, ssr.Spec.TargetVolume.OperationTimeout.Duration, func() (done bool, err error) {
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

	_, restorePV, err := kube.WaitPVCBound(ctx, s.kubeClient.CoreV1(), s.kubeClient.CoreV1(), restorePVCName, ssr.Namespace, ssr.Spec.TargetVolume.OperationTimeout.Duration)
	if err != nil {
		return errors.Wrapf(err, fmt.Sprintf("Failed to get PV from restore PVC %s", restorePVCName))
	}

	log.WithField("restore PV", restorePV.Name).Info("Restore PV is retrieved")

	retained, err := kube.RetainPV(ctx, s.kubeClient.CoreV1(), restorePV)
	if err != nil {
		return errors.Wrapf(err, fmt.Sprintf("Failed to retain PV %s", restorePV.Name))
	}

	log.WithField("restore PV", restorePV.Name).WithField("retained", (retained != nil)).Info("Restore PV is retained")

	defer func() {
		if retained != nil {
			log.WithField("restore PV", restorePV.Name).Info("Rollback PV binding policy")
			kube.DeletePVIfAny(ctx, s.kubeClient.CoreV1(), retained.Name, log)
		}
	}()

	err = kube.EnsureDeletePod(ctx, s.kubeClient.CoreV1(), restorePodName, ssr.Namespace, ssr.Spec.TargetVolume.OperationTimeout.Duration)
	if err != nil {
		return errors.Wrapf(err, fmt.Sprintf("Failed to delete restore pod %s", restorePodName))
	}

	err = kube.EnsureDeletePVC(ctx, s.kubeClient.CoreV1(), restorePVCName, ssr.Namespace, ssr.Spec.TargetVolume.OperationTimeout.Duration)
	if err != nil {
		return errors.Wrapf(err, fmt.Sprintf("Failed to delete restore PVC %s", restorePVCName))
	}

	log.WithField("restore PVC", restorePVCName).Info("Restore PVC is deleted")

	err = kube.EnsureDeletePV(ctx, s.kubeClient.CoreV1(), restorePV.Name, ssr.Spec.TargetVolume.OperationTimeout.Duration)
	if err != nil {
		return errors.Wrapf(err, fmt.Sprintf("Failed to delete restore PV %s", restorePV.Name))
	}

	log.WithField("restore PV", restorePV.Name).Info("Restore PV is deleted")

	retained = nil

	workloadPV, err := createWorkloadPV(ctx, s.kubeClient, restorePV, ssr.Spec.TargetVolume.PV)
	if err != nil {
		return errors.Wrapf(err, fmt.Sprintf("Failed to create workload PV from restore PV %s", restorePV.Name))
	}

	log.WithField("workload PV", workloadPV.Name).WithField("restore PV", restorePV.Name).Info("Workload PV is created")

	return nil
}

func createWorkloadPV(ctx context.Context, kubeClient kubernetes.Interface, restorePV *corev1api.PersistentVolume, name string) (*corev1api.PersistentVolume, error) {
	updated := restorePV.DeepCopy()
	delete(updated.Annotations, kube.KubeAnnBindCompleted)
	delete(updated.Annotations, kube.KubeAnnBoundByController)
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
