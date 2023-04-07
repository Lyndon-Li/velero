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
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
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
	"github.com/vmware-tanzu/velero/pkg/metrics"
	"github.com/vmware-tanzu/velero/pkg/repository"
	repokey "github.com/vmware-tanzu/velero/pkg/repository/keys"
	"github.com/vmware-tanzu/velero/pkg/uploader"
	"github.com/vmware-tanzu/velero/pkg/uploader/provider"
	"github.com/vmware-tanzu/velero/pkg/util/boolptr"
	"github.com/vmware-tanzu/velero/pkg/util/csi"
	"github.com/vmware-tanzu/velero/pkg/util/datamover"
	"github.com/vmware-tanzu/velero/pkg/util/filesystem"
	"github.com/vmware-tanzu/velero/pkg/util/kube"

	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	snapshotterClientSet "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
)

// SnapshotBackupReconciler reconciles a Snapshotbackup object
type SnapshotBackupReconciler struct {
	Scheme              *runtime.Scheme
	Client              client.Client
	kubeClient          kubernetes.Interface
	csiSnapshotClient   *snapshotterClientSet.Clientset
	RepoEnsurer         *repository.RepositoryEnsurer
	Clock               clocks.WithTickerAndDelayedExecution
	Metrics             *metrics.ServerMetrics
	CredentialGetter    *credentials.CredentialGetter
	NodeName            string
	FileSystem          filesystem.Interface
	Log                 logrus.FieldLogger
	RepositoryEnsurer   *repository.RepositoryEnsurer
	DataPathTrackerLock sync.Mutex
	DataPathTracker     map[string]DataPathContext
}

type DataPathContext struct {
	cancelRoutine context.CancelFunc
}

type SnapshotBackupProgressUpdater struct {
	SnapshotBackup *velerov1api.SnapshotBackup
	Log            logrus.FieldLogger
	Ctx            context.Context
	Cli            client.Client
}

type snapshotExposeResult struct {
	csiExpose cSISnapshotExposeResult
}

type cSISnapshotExposeResult struct {
	path string
}

func NewSnapshotBackupReconciler(scheme *runtime.Scheme, client client.Client, kubeClient kubernetes.Interface,
	csiSnapshotClient *snapshotterClientSet.Clientset, repoEnsurer *repository.RepositoryEnsurer, clock clocks.WithTickerAndDelayedExecution,
	metrics *metrics.ServerMetrics, cred *credentials.CredentialGetter, nodeName string, fs filesystem.Interface, log logrus.FieldLogger) *SnapshotBackupReconciler {
	return &SnapshotBackupReconciler{
		Scheme:            scheme,
		Client:            client,
		kubeClient:        kubeClient,
		csiSnapshotClient: csiSnapshotClient,
		Clock:             clock,
		Metrics:           metrics,
		CredentialGetter:  cred,
		NodeName:          nodeName,
		FileSystem:        fs,
		Log:               log,
		RepositoryEnsurer: repoEnsurer,
		DataPathTracker:   make(map[string]DataPathContext),
	}
}

// +kubebuilder:rbac:groups=velero.io,resources=snapshotbackup,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=velero.io,resources=snapshotbackup/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get
// +kubebuilder:rbac:groups="",resources=persistentvolumerclaims,verbs=get
func (s *SnapshotBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := s.Log.WithFields(logrus.Fields{
		"controller":     "snapshotbackup",
		"snapshotbackup": req.NamespacedName,
	})

	var ssb velerov1api.SnapshotBackup
	if err := s.Client.Get(ctx, req.NamespacedName, &ssb); err != nil {
		if apierrors.IsNotFound(err) {
			log.Debug("Unable to find SnapshotBackup")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, errors.Wrap(err, "getting SnapshotBackup")
	}

	if ssb.Spec.DataMover != "" {
		log.WithField("data mover", ssb.Spec.DataMover).Info("Snapshot backup is not to be processed by velero")
		return ctrl.Result{}, nil
	}

	if ssb.Status.Phase == "" || ssb.Status.Phase == velerov1api.SnapshotBackupPhaseNew {
		log.Info("Snapshot backup starting")

		accepted, err := s.acceptSnapshotBackup(ctx, &ssb)
		if err != nil {
			return s.errorOut(ctx, &ssb, err, "error to accept the snapshot backup", log)
		}

		if !accepted {
			return ctrl.Result{}, nil
		}

		log.Info("Snapshot backup is accepted")

		if err := s.exposeSnapshot(ctx, &ssb, log); err != nil {
			return s.errorOut(ctx, &ssb, err, "error to expose snapshot", log)
		}

		log.Info("Snapshot is exposed")

		return ctrl.Result{}, nil
	} else if ssb.Status.Phase == velerov1api.SnapshotBackupPhasePrepared {
		log.Info("Snapshot backup is prepared")

		s.DataPathTrackerLock.Lock()
		_, found := s.DataPathTracker[ssb.Name]
		s.DataPathTrackerLock.Unlock()
		if found {
			log.Info("Cancellable data path is already started")
			return ctrl.Result{}, nil
		}

		sser, err := s.waitSnapshotExposed(ctx, &ssb, log)
		if err != nil {
			return s.errorOut(ctx, &ssb, err, "exposed snapshot is not ready", log)
		} else if sser == nil {
			return ctrl.Result{}, nil
		}

		log.Info("Exposed snapshot is ready")

		// Update status to InProgress.
		original := ssb.DeepCopy()
		ssb.Status.Phase = velerov1api.SnapshotBackupPhaseInProgress
		ssb.Status.StartTimestamp = &metav1.Time{Time: s.Clock.Now()}
		if err := s.Client.Patch(ctx, &ssb, client.MergeFrom(original)); err != nil {
			log.WithError(err).Error("error updating SnapshotBackup status")
			return ctrl.Result{}, err
		}

		log.Info("SnapshotBackup is marked as in progress")

		s.runCancelableDataPath(ctx, &ssb, sser, log)

		return ctrl.Result{}, nil
	} else if ssb.Status.Phase == velerov1api.SnapshotBackupPhaseInProgress && ssb.Spec.Cancel {
		s.DataPathTrackerLock.Lock()
		dataPathContext, found := s.DataPathTracker[ssb.Name]
		s.DataPathTrackerLock.Unlock()
		if !found {
			return ctrl.Result{}, nil
		}

		log.Info("Snapshot backup is being canceled")

		// Update status to Canceling.
		original := ssb.DeepCopy()
		ssb.Status.Phase = velerov1api.SnapshotBackupPhaseCanceling
		if err := s.Client.Patch(ctx, &ssb, client.MergeFrom(original)); err != nil {
			log.WithError(err).Error("error updating SnapshotBackup status")
			return ctrl.Result{}, err
		}

		dataPathContext.cancelRoutine()

		return ctrl.Result{}, nil
	} else {
		log.WithField("phase", ssb.Status.Phase).Info("Snapshot backup is not in the expected phase")
		return ctrl.Result{}, nil
	}
}

func (s *SnapshotBackupReconciler) runCancelableDataPath(ctx context.Context, ssb *velerov1api.SnapshotBackup, sser *snapshotExposeResult, log logrus.FieldLogger) {
	cancelCtx, cancel := context.WithCancel(ctx)

	log.Info("Creating data path routine")

	go func() {
		defer func() {
			s.DataPathTrackerLock.Lock()
			delete(s.DataPathTracker, ssb.Name)
			s.DataPathTrackerLock.Unlock()

			cancel()
		}()

		backupLocation := &velerov1api.BackupStorageLocation{}
		if err := s.Client.Get(ctx, client.ObjectKey{
			Namespace: ssb.Namespace,
			Name:      ssb.Spec.BackupStorageLocation,
		}, backupLocation); err != nil {
			s.errorOut(ctx, ssb, err, "error getting backup storage location", log)
			return
		}

		backupRepo, err := s.RepositoryEnsurer.EnsureRepo(ctx, ssb.Namespace, ssb.Spec.SourceNamespace, ssb.Spec.BackupStorageLocation, datamover.GetUploaderType(ssb.Spec.DataMover))
		if err != nil {
			s.errorOut(ctx, ssb, err, "error ensure backup repository", log)
			return
		}

		var uploaderProv provider.Provider
		uploaderProv, err = NewUploaderProviderFunc(ctx, s.Client, datamover.GetUploaderType(ssb.Spec.DataMover), "",
			backupLocation, backupRepo, s.CredentialGetter, repokey.RepoKeySelector(), log)
		if err != nil {
			s.errorOut(ctx, ssb, err, "error creating uploader", log)
			return
		}

		// If this is a PVC, look for the most recent completed pod volume backup for it and get
		// its snapshot ID to do new backup based on it. Without this,
		// if the pod using the PVC (and therefore the directory path under /host_pods/) has
		// changed since the PVC's last backup, for backup, it will not be able to identify a suitable
		// parent snapshot to use, and will have to do a full rescan of the contents of the PVC.
		var parentSnapshotID string
		if pvcUID, ok := ssb.Labels[velerov1api.PVCUIDLabel]; ok {
			parentSnapshotID = s.getParentSnapshot(ctx, log, pvcUID, ssb)
			if parentSnapshotID == "" {
				log.Info("No parent snapshot found for PVC, not based on parent snapshot for this backup")
			} else {
				log.WithField("parentSnapshotID", parentSnapshotID).Info("Based on parent snapshot for this backup")
			}
		}

		defer func() {
			if uploaderProv != nil {
				if err := uploaderProv.Close(ctx); err != nil {
					log.Errorf("failed to close uploader provider with error %v", err)
				}
			}
		}()

		snapshotID, emptySnapshot, err := uploaderProv.RunBackup(cancelCtx, sser.csiExpose.path, nil, parentSnapshotID, s.NewSnapshotBackupProgressUpdater(ssb, log, ctx))
		if err != nil && err != provider.ErrorCanceled {
			s.errorOut(ctx, ssb, err, fmt.Sprintf("running backup, stderr=%v", err), log)
			return
		}

		log.Info("cleaning up exposed snapshot")
		if err := s.cleanUpSnapshot(ctx, ssb, log); err != nil {
			log.WithError(err).Error("Failed to clean up exposed snapshot")
		}

		original := ssb.DeepCopy()

		if err == provider.ErrorCanceled {
			ssb.Status.Phase = velerov1api.SnapshotBackupPhaseCanceled
			ssb.Status.CompletionTimestamp = &metav1.Time{Time: s.Clock.Now()}
		} else {
			// Update status to Completed with path & snapshot ID.
			ssb.Status.Path = sser.csiExpose.path
			ssb.Status.Phase = velerov1api.SnapshotBackupPhaseCompleted
			ssb.Status.SnapshotID = snapshotID
			ssb.Status.CompletionTimestamp = &metav1.Time{Time: s.Clock.Now()}

			if emptySnapshot {
				ssb.Status.Message = "volume was empty so no snapshot was taken"
			}
		}

		if err := s.Client.Patch(ctx, ssb, client.MergeFrom(original)); err != nil {
			log.WithError(err).Error("error updating SnapshoteBackup status")
		} else {
			log.Infof("SnapshotBackup is marked as %s", ssb.Status.Phase)
		}
	}()

	s.DataPathTrackerLock.Lock()
	s.DataPathTracker[ssb.Name] = DataPathContext{cancelRoutine: cancel}
	s.DataPathTrackerLock.Unlock()
}

// SetupWithManager registers the SnapshotBackup controller.
func (r *SnapshotBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&velerov1api.SnapshotBackup{}).
		Watches(&source.Kind{Type: &corev1.Pod{}}, kube.EnqueueRequestsFromMapUpdateFunc(r.findSnapshotBackupForPod),
			builder.WithPredicates(predicate.Funcs{
				UpdateFunc: func(ue event.UpdateEvent) bool {
					newObj := ue.ObjectNew.(*corev1.Pod)

					if _, ok := newObj.Labels[velerov1api.SnapshotBackupLabel]; !ok {
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
		Complete(r)
}

func (r *SnapshotBackupReconciler) findSnapshotBackupForPod(podObj client.Object) []reconcile.Request {
	pod := podObj.(*corev1.Pod)

	ssb := &velerov1api.SnapshotBackup{}
	err := r.Client.Get(context.Background(), types.NamespacedName{
		Namespace: pod.Namespace,
		Name:      pod.Labels[velerov1api.SnapshotBackupLabel],
	}, ssb)

	if err != nil {
		r.Log.WithField("Backup pod", pod.Name).WithError(err).Error("unable to get SnapshotBackup")
		return []reconcile.Request{}
	}

	if ssb.Status.Phase != velerov1api.SnapshotBackupPhaseAccepted {
		return []reconcile.Request{}
	}

	requests := make([]reconcile.Request, 1)

	r.Log.WithField("Backup pod", pod.Name).Infof("Preparing SnapshotBackup %s", ssb.Name)
	r.patchSnapshotBackup(context.Background(), ssb, prepareSnapshotBackup)

	requests[0] = reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: ssb.Namespace,
			Name:      ssb.Name,
		},
	}

	return requests
}

func (r *SnapshotBackupReconciler) patchSnapshotBackup(ctx context.Context, req *velerov1api.SnapshotBackup, mutate func(*velerov1api.SnapshotBackup)) error {
	original := req.DeepCopy()
	mutate(req)
	if err := r.Client.Patch(ctx, req, client.MergeFrom(original)); err != nil {
		return errors.Wrap(err, "error patching SnapshotBackup")
	}

	return nil
}

func prepareSnapshotBackup(ssb *velerov1api.SnapshotBackup) {
	ssb.Status.Phase = velerov1api.SnapshotBackupPhasePrepared
}

// getParentSnapshot finds the most recent completed PodVolumeBackup for the
// specified PVC and returns its snapshot ID. Any errors encountered are
// logged but not returned since they do not prevent a backup from proceeding.
func (r *SnapshotBackupReconciler) getParentSnapshot(ctx context.Context, log logrus.FieldLogger, pvcUID string, ssb *velerov1api.SnapshotBackup) string {
	log = log.WithField("pvcUID", pvcUID)
	log.Infof("Looking for most recent completed SnapshotBackup for this PVC")

	listOpts := &client.ListOptions{
		Namespace: ssb.Namespace,
	}
	matchingLabels := client.MatchingLabels(map[string]string{velerov1api.PVCUIDLabel: pvcUID})
	matchingLabels.ApplyToList(listOpts)

	var ssbList velerov1api.SnapshotBackupList
	if err := r.Client.List(ctx, &ssbList, listOpts); err != nil {
		log.WithError(errors.WithStack(err)).Error("getting list of SnapshotBackups for this PVC")
	}

	// Go through all the podvolumebackups for the PVC and look for the most
	// recent completed one to use as the parent.
	var mostRecentSSB velerov1api.SnapshotBackup
	for _, ssbItem := range ssbList.Items {
		if datamover.GetUploaderType(ssbItem.Spec.DataMover) != datamover.GetUploaderType(ssb.Spec.DataMover) {
			continue
		}
		if ssbItem.Status.Phase != velerov1api.SnapshotBackupPhaseCompleted {
			continue
		}

		if ssbItem.Spec.BackupStorageLocation != ssb.Spec.BackupStorageLocation {
			// Check the backup storage location is the same as spec in order to
			// support backup to multiple backup-locations. Otherwise, there exists
			// a case that backup volume snapshot to the second location would
			// failed, since the founded parent ID is only valid for the first
			// backup location, not the second backup location. Also, the second
			// backup should not use the first backup parent ID since its for the
			// first backup location only.
			continue
		}

		if mostRecentSSB.Status == (velerov1api.SnapshotBackupStatus{}) || ssb.Status.StartTimestamp.After(mostRecentSSB.Status.StartTimestamp.Time) {
			mostRecentSSB = ssbItem
		}
	}

	if mostRecentSSB.Status == (velerov1api.SnapshotBackupStatus{}) {
		log.Info("No completed SnapshotBackup found for PVC")
		return ""
	}

	log.WithFields(map[string]interface{}{
		"parentSnapshotBackup": mostRecentSSB.Name,
		"parentSnapshotID":     mostRecentSSB.Status.SnapshotID,
	}).Info("Found most recent completed SnapshotBackup for PVC")

	return mostRecentSSB.Status.SnapshotID
}

func (s *SnapshotBackupReconciler) errorOut(ctx context.Context, ssb *velerov1api.SnapshotBackup, err error, msg string, log logrus.FieldLogger) (ctrl.Result, error) {
	if delErr := s.cleanUpSnapshot(ctx, ssb, log); delErr != nil {
		log.WithError(delErr).Error("Failed to clean up exposed snapshot")
	}

	return ctrl.Result{}, s.updateStatusToFailed(ctx, ssb, err, msg, log)
}

func (s *SnapshotBackupReconciler) updateStatusToFailed(ctx context.Context, ssb *velerov1api.SnapshotBackup, err error, msg string, log logrus.FieldLogger) error {
	original := ssb.DeepCopy()
	ssb.Status.Phase = velerov1api.SnapshotBackupPhaseFailed
	ssb.Status.Message = errors.WithMessage(err, msg).Error()
	if ssb.Status.StartTimestamp.IsZero() {
		ssb.Status.StartTimestamp = &metav1.Time{Time: s.Clock.Now()}
	}

	ssb.Status.CompletionTimestamp = &metav1.Time{Time: s.Clock.Now()}
	if err = s.Client.Patch(ctx, ssb, client.MergeFrom(original)); err != nil {
		log.WithError(err).Error("error updating SnapshotBackup status")
		return err
	}

	return nil
}

func (s *SnapshotBackupReconciler) NewSnapshotBackupProgressUpdater(ssb *velerov1api.SnapshotBackup, log logrus.FieldLogger, ctx context.Context) *SnapshotBackupProgressUpdater {
	return &SnapshotBackupProgressUpdater{ssb, log, ctx, s.Client}
}

// UpdateProgress which implement ProgressUpdater interface to update snapshot backup progress status
func (s *SnapshotBackupProgressUpdater) UpdateProgress(p *uploader.UploaderProgress) {
	original := s.SnapshotBackup.DeepCopy()
	backupPVCName := s.SnapshotBackup.Name
	s.SnapshotBackup.Status.Progress = velerov1api.DataMoveOperationProgress{TotalBytes: p.TotalBytes, BytesDone: p.BytesDone}
	if s.Cli == nil {
		s.Log.Errorf("failed to update snapshot %s backup progress with uninitailize client", backupPVCName)
		return
	}
	if err := s.Cli.Patch(s.Ctx, s.SnapshotBackup, client.MergeFrom(original)); err != nil {
		s.Log.Errorf("update backup snapshot %s  progress with %v", backupPVCName, err)
	}
}

func (r *SnapshotBackupReconciler) acceptSnapshotBackup(ctx context.Context, ssb *velerov1api.SnapshotBackup) (bool, error) {
	updated := ssb.DeepCopy()
	updated.Status.Phase = velerov1api.SnapshotBackupPhaseAccepted

	r.Log.Infof("Accepting snapshot backup %s", ssb.Name)

	time.Sleep(2 * time.Second)

	err := r.Client.Update(ctx, updated)
	if err == nil {
		return true, nil
	} else if apierrors.IsConflict(err) {
		r.Log.WithField("SnapshotBackup", ssb.Name).Info("This snapshot backup has been accepted by others")
		return false, nil
	} else {
		return false, err
	}
}

func (r *SnapshotBackupReconciler) exposeSnapshot(ctx context.Context, ssb *velerov1api.SnapshotBackup, log logrus.FieldLogger) error {
	switch ssb.Spec.SnapshotType {
	case velerov1api.SnapshotTypeCSI:
		return r.exposeCSISnapshot(ctx, ssb, log)
	default:
		return errors.Errorf("unsupported snapshot type %s", ssb.Spec.SnapshotType)
	}
}

func (r *SnapshotBackupReconciler) waitSnapshotExposed(ctx context.Context, ssb *velerov1api.SnapshotBackup,
	log logrus.FieldLogger) (*snapshotExposeResult, error) {
	switch ssb.Spec.SnapshotType {
	case velerov1api.SnapshotTypeCSI:
		return r.waitCSISnapshotExposed(ctx, ssb, log)
	default:
		return nil, errors.Errorf("unsupported snapshot type %s", ssb.Spec.SnapshotType)
	}
}

func (r *SnapshotBackupReconciler) cleanUpSnapshot(ctx context.Context, ssb *velerov1api.SnapshotBackup, log logrus.FieldLogger) error {
	switch ssb.Spec.SnapshotType {
	case velerov1api.SnapshotTypeCSI:
		return r.cleanUpCSISnapshot(ctx, ssb, log)
	default:
		return errors.Errorf("unsupported snapshot type %s", ssb.Spec.SnapshotType)
	}
}

func (r *SnapshotBackupReconciler) waitCSISnapshotExposed(ctx context.Context, ssb *velerov1api.SnapshotBackup,
	log logrus.FieldLogger) (*snapshotExposeResult, error) {
	backupPodName := ssb.Name
	backupPVCName := ssb.Name

	pod := &corev1.Pod{}
	err := r.Client.Get(ctx, types.NamespacedName{
		Namespace: ssb.Namespace,
		Name:      backupPodName,
	}, pod)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.WithField("backup pod", backupPodName).Errorf("Backup pod is not running in the current node %s", r.NodeName)
			return nil, nil
		} else {
			return nil, errors.Wrapf(err, "error to get backup pod %s", backupPodName)
		}
	}

	log.WithField("pod", pod.Name).Infof("Backup pod is in running state in node %s", r.NodeName)

	pvc := &corev1.PersistentVolumeClaim{}
	pollInterval := 2 * time.Second
	err = wait.PollImmediate(pollInterval, ssb.Spec.OperationTimeout.Duration, func() (done bool, err error) {
		if err := r.Client.Get(ctx, types.NamespacedName{
			Namespace: ssb.Namespace,
			Name:      backupPVCName,
		}, pvc); err != nil {
			return false, err
		} else {
			return pvc.Status.Phase == v1.ClaimBound, nil
		}
	})

	if err != nil {
		if err == wait.ErrWaitTimeout {
			log.WithField("backup pvc", backupPVCName).Error("backup pvc is not bound till timeout")
			return nil, nil
		} else {
			return nil, errors.Wrapf(err, "error to get backup PVC %s", backupPVCName)
		}
	}

	log.WithField("backup pvc", backupPVCName).Info("Backup PVC is bound")

	sser := snapshotExposeResult{}

	sser.csiExpose.path, err = datamover.GetPodVolumeHostPath(ctx, pod, pvc, r.Client, r.FileSystem, log)
	if err != nil {
		return nil, errors.Wrap(err, "error to get backup pvc host path")
	}

	log.WithField("backup pod", pod.Name).WithField("backup pvc", pvc.Name).Infof("Got backup PVC host path %s", sser.csiExpose.path)

	return &sser, nil
}

func (r *SnapshotBackupReconciler) cleanUpCSISnapshot(ctx context.Context, ssb *velerov1api.SnapshotBackup, log logrus.FieldLogger) error {
	backupPodName := ssb.Name
	backupPVCName := ssb.Name
	backupVSName := ssb.Name

	kube.DeletePodIfAny(ctx, r.kubeClient.CoreV1(), backupPodName, ssb.Namespace, log)
	kube.DeletePVCIfAny(ctx, r.kubeClient.CoreV1(), backupPVCName, ssb.Namespace, log)
	csi.DeleteVolumeSnapshotIfAny(ctx, r.csiSnapshotClient, backupVSName, ssb.Namespace, log)
	csi.DeleteVolumeSnapshotIfAny(ctx, r.csiSnapshotClient, ssb.Spec.CSISnapshot.VolumeSnapshot, ssb.Spec.SourceNamespace, log)

	return nil
}

func (r *SnapshotBackupReconciler) exposeCSISnapshot(ctx context.Context, ssb *velerov1api.SnapshotBackup, log logrus.FieldLogger) error {
	curLog := log.WithFields(logrus.Fields{
		"snapshot backup": ssb.Name,
	})

	curLog.Info("Exposing CSI snapshot")

	backupVCName := ssb.Name

	volumeSnapshot, err := csi.WaitVolumeSnapshotReady(ctx, r.csiSnapshotClient, ssb.Spec.CSISnapshot.VolumeSnapshot, ssb.Spec.SourceNamespace, ssb.Spec.OperationTimeout.Duration, log)
	if err != nil {
		return errors.Wrapf(err, "error wait volume snapshot ready")
	}

	curLog.Info("Volumesnapshot is ready")

	volumeMode := kube.GetVolumeModeFromDataMover(ssb.Spec.DataMover)

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: volumeSnapshot.Namespace,
			Name:      backupVCName,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: velerov1api.SchemeGroupVersion.String(),
					Kind:       "SnapshotBackup",
					Name:       ssb.Name,
					UID:        ssb.UID,
					Controller: boolptr.True(),
				},
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			StorageClassName: &ssb.Spec.CSISnapshot.StorageClass,
			VolumeMode:       &volumeMode,

			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: *volumeSnapshot.Status.RestoreSize,
				},
			},
		},
	}

	vsc, err := csi.GetVolumeSnapshotContentForVolumeSnapshot(volumeSnapshot, r.csiSnapshotClient.SnapshotV1(), log, false)
	if err != nil {
		return errors.Wrap(err, "error to get volume snapshot content")
	}

	curLog.WithField("vsc name", vsc.Name).WithField("vs name", volumeSnapshot.Name).Infof("Got VSC from VS in namespace %s", volumeSnapshot.Namespace)

	retained, err := csi.RetainVSC(ctx, r.csiSnapshotClient, vsc)
	if err != nil {
		return errors.Wrap(err, "error to retain volume snapshot content")
	}

	curLog.WithField("vsc name", vsc.Name).WithField("retained", (retained != nil)).Info("Finished to retain VSC")

	defer func() {
		if retained != nil {
			csi.DeleteVolumeSnapshotContentIfAny(ctx, r.csiSnapshotClient, retained.Name, log)
		}
	}()

	err = csi.EnsureDeleteVS(ctx, r.csiSnapshotClient, volumeSnapshot, ssb.Spec.OperationTimeout.Duration)
	if err != nil {
		return errors.Wrap(err, "error to delete volume snapshot")
	}

	curLog.WithField("vs name", volumeSnapshot.Name).Infof("VS is deleted in namespace %s", volumeSnapshot.Namespace)

	err = csi.EnsureDeleteVSC(ctx, r.csiSnapshotClient, vsc, ssb.Spec.OperationTimeout.Duration)
	if err != nil {
		return errors.Wrap(err, "error to delete volume snapshot")
	}

	curLog.WithField("vsc name", vsc.Name).Infof("VSC is deleted")
	retained = nil

	backupVS, err := r.createBackupVS(ctx, volumeSnapshot, ssb, vsc.Name)
	if err != nil {
		return errors.Wrap(err, "error to create backup volume snapshot")
	}

	curLog.WithField("vs name", backupVS.Name).Infof("Backup VS is created from %s/%s", volumeSnapshot.Namespace, volumeSnapshot.Name)

	defer func() {
		if err != nil {
			csi.DeleteVolumeSnapshotIfAny(ctx, r.csiSnapshotClient, backupVS.Name, backupVS.Namespace, log)
		}
	}()

	backupVSC, err := r.createBackupVSC(ctx, ssb, vsc, backupVS)
	if err != nil {
		return errors.Wrap(err, "error to create backup volume snapshot content")
	}

	curLog.WithField("vsc name", backupVSC.Name).Info("Backup VSC is created from %s", vsc.Name)

	kube.ResetPVCDataSource(pvc, backupVS.Name)

	backupPVC, err := r.createBackupPVC(ctx, ssb, pvc)
	if err != nil {
		return errors.Wrap(err, "error to create backup pvc")
	}

	curLog.WithField("pvc name", backupPVC.Name).Info("Backup PVC is created")

	defer func() {
		if err != nil {
			kube.DeletePVCIfAny(ctx, r.kubeClient.CoreV1(), backupPVC.Name, backupPVC.Namespace, log)
		}
	}()

	backupPod, err := r.createBackupPod(ctx, ssb, backupPVC)
	if err != nil {
		return errors.Wrap(err, "error to create backup pod")
	}

	curLog.WithField("pod name", backupPod.Name).Info("Backup pod is created")

	defer func() {
		if err != nil {
			kube.DeletePodIfAny(ctx, r.kubeClient.CoreV1(), backupPod.Name, backupPod.Namespace, log)
		}
	}()

	return nil
}

func (r *SnapshotBackupReconciler) createBackupVS(ctx context.Context, snapshotVS *snapshotv1api.VolumeSnapshot, ssb *velerov1api.SnapshotBackup, vscName string) (*snapshotv1api.VolumeSnapshot, error) {
	backupVSName := ssb.Name
	backupVSCName := ssb.Name

	copied := &snapshotv1api.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backupVSName,
			Namespace: ssb.Namespace,
			Labels: map[string]string{
				velerov1api.SnapshotBackupLabel: ssb.Name,
			},
			// Don't add ownerReference to SnapshotBackup.
			// The backupPVC should be deleted before backupVS, otherwise, the deletion of backupVS will fail since
			// backupPVC has its dataSource referring to it
		},
		Spec: snapshotv1api.VolumeSnapshotSpec{
			Source: snapshotv1api.VolumeSnapshotSource{
				VolumeSnapshotContentName: &backupVSCName,
			},
			VolumeSnapshotClassName: snapshotVS.Spec.VolumeSnapshotClassName,
		},
	}

	created, err := r.csiSnapshotClient.SnapshotV1().VolumeSnapshots(copied.Namespace).Create(ctx, copied, metav1.CreateOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "error to create backup volume snapshot")
	}

	return created, nil
}

func (r *SnapshotBackupReconciler) createBackupVSC(ctx context.Context, ssb *velerov1api.SnapshotBackup,
	snapshotVSC *snapshotv1api.VolumeSnapshotContent, vs *snapshotv1api.VolumeSnapshot) (*snapshotv1api.VolumeSnapshotContent, error) {
	backupVSCName := ssb.Name

	copied := &snapshotv1api.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{
			Name: backupVSCName,
		},
		Spec: snapshotv1api.VolumeSnapshotContentSpec{
			VolumeSnapshotRef: corev1.ObjectReference{
				Name:            vs.Name,
				Namespace:       vs.Namespace,
				UID:             vs.UID,
				ResourceVersion: vs.ResourceVersion,
			},
			Source: snapshotv1api.VolumeSnapshotContentSource{
				SnapshotHandle: snapshotVSC.Status.SnapshotHandle,
			},
			DeletionPolicy:          snapshotVSC.Spec.DeletionPolicy,
			Driver:                  snapshotVSC.Spec.Driver,
			VolumeSnapshotClassName: snapshotVSC.Spec.VolumeSnapshotClassName,
		},
	}

	created, err := r.csiSnapshotClient.SnapshotV1().VolumeSnapshotContents().Create(ctx, copied, metav1.CreateOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "error to create backup volume snapshot content")
	}

	return created, nil
}

func (r *SnapshotBackupReconciler) createBackupPVC(ctx context.Context, ssb *velerov1api.SnapshotBackup, pvcTemplate *corev1.PersistentVolumeClaim) (*corev1.PersistentVolumeClaim, error) {
	copied := pvcTemplate.DeepCopy()
	copied.Namespace = ssb.Namespace

	pvc, err := r.kubeClient.CoreV1().PersistentVolumeClaims(copied.Namespace).Create(ctx, copied, metav1.CreateOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "error to create backup pvc")
	}

	return pvc, err
}

func (r *SnapshotBackupReconciler) createBackupPod(ctx context.Context, ssb *velerov1api.SnapshotBackup, backupPVC *corev1.PersistentVolumeClaim) (*corev1.Pod, error) {
	podName := ssb.Name

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: ssb.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: velerov1api.SchemeGroupVersion.String(),
					Kind:       "SnapshotBackup",
					Name:       ssb.Name,
					UID:        ssb.UID,
					Controller: boolptr.True(),
				},
			},
			Labels: map[string]string{
				velerov1api.SnapshotBackupLabel: ssb.Name,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:    podName,
					Image:   "gcr.io/velero-gcp/busybox",
					Command: []string{"sleep", "infinity"},
					VolumeMounts: []corev1.VolumeMount{{
						Name:      backupPVC.Name,
						MountPath: "/" + backupPVC.Name,
					}},
				},
			},
			Volumes: []corev1.Volume{{
				Name: backupPVC.Name,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: backupPVC.Name,
					},
				},
			}},
		},
	}

	updated, err := r.kubeClient.CoreV1().Pods(ssb.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "error to create backup pod")
	}

	return updated, nil
}
