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

	switch ssr.Status.Phase {
	case "", velerov1api.SnapshotRestorePhaseNew, velerov1api.SnapshotRestorePhasePrepared:
		// Only process new items.
	default:
		log.Debug("Snapshot restore is not new or Prepared, not processing")
		return ctrl.Result{}, nil
	}

	log.Info("Snapshot restore starting with phase %v", ssr.Status.Phase)

	if ssr.Status.Phase == "" || ssr.Status.Phase == velerov1api.SnapshotRestorePhaseNew {
		//Create pod and mount pvc
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ssr.Spec.RestorePvc,
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
						Name:    ssr.Spec.RestorePvc,
						Image:   "gcr.io/velero-gcp/busybox",
						Command: []string{"sleep", "infinity"},
						VolumeMounts: []corev1.VolumeMount{{
							Name:      ssr.Spec.RestorePvc,
							MountPath: "/" + ssr.Spec.RestorePvc,
						}},
					},
				},
				Volumes: []corev1.Volume{{
					Name: ssr.Spec.RestorePvc,
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: ssr.Spec.RestorePvc,
						},
					},
				}},
			},
		}

		if err := s.Client.Create(ctx, pod, &client.CreateOptions{}); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return s.updateStatusToFailed(ctx, ssr, err, fmt.Sprintf("error creating pod %s in namespace %s", pod.Name, pod.Namespace), log)
			}
		}

		log.WithField("pod", pod.Name).Info("Snapshot restore pod is created")

		return ctrl.Result{}, nil
	}

	pod := &corev1.Pod{}
	err := s.Client.Get(ctx, types.NamespacedName{
		Namespace: ssr.Namespace,
		Name:      ssr.Spec.RestorePvc,
	}, pod)
	if err != nil {
		return ctrl.Result{}, nil
	}

	log.WithField("pod", pod.Name).Infof("Restore pod is in running state in node %s", pod.Spec.NodeName)

	pvc := &corev1.PersistentVolumeClaim{}
	if err := wait.PollImmediate(PollInterval, PollTimeout, func() (done bool, err error) {
		if err := s.Client.Get(ctx, types.NamespacedName{
			Namespace: ssr.Namespace,
			Name:      ssr.Spec.RestorePvc,
		}, pvc); err != nil {
			return false, err
		} else {
			return pvc.Status.Phase == v1.ClaimBound, nil
		}
	}); err != nil {
		log.WithField("restore pvc", ssr.Spec.RestorePvc).Debug("restore pvc is not bound")
		return ctrl.Result{}, err
	}

	log.WithField("pod", pod.Name).Info("Restore PVC is ready")

	original := ssr.DeepCopy()
	ssr.Status.Phase = velerov1api.SnapshotRestorePhaseInProgress
	ssr.Status.StartTimestamp = &metav1.Time{Time: s.clock.Now()}
	if err := s.Patch(ctx, ssr, client.MergeFrom(original)); err != nil {
		log.WithError(err).Error("Unable to update status to in progress")
		return ctrl.Result{}, err
	}

	log.Info("SnapshotRestore is marked as in progress")

	deletePod := true

	defer func() {
		if deletePod {
			log.Infof("deleting pod %s in namespace %s on failure", pod.Name, pod.Namespace)
			var gracePeriodSeconds int64 = 0
			if delErr := s.Client.Delete(ctx, pod, &client.DeleteOptions{GracePeriodSeconds: &gracePeriodSeconds}); delErr != nil {
				s.updateStatusToFailed(ctx, ssr, delErr, fmt.Sprintf("error delete pod %s in namespace %s", pod.Name, pod.Namespace), log)
			}
		}
	}()

	if err := s.processRestore(ctx, ssr, pod, log); err != nil {
		log.WithError(err).Error("Unable to process the SnapshotRestore")
		return s.updateStatusToFailed(ctx, ssr, err, fmt.Sprintf("running restore, stderr=%v", err), log)
	}

	deletePod = false
	log.Infof("deleting pod %s in namespace %s", pod.Name, pod.Namespace)
	var gracePeriodSeconds int64 = 0
	if err := s.Client.Delete(ctx, pod, &client.DeleteOptions{GracePeriodSeconds: &gracePeriodSeconds}); err != nil {
		return s.updateStatusToFailed(ctx, ssr, err, fmt.Sprintf("error delete pod %s in namespace %s", pod.Name, pod.Namespace), log)
	}

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

func (s *SnapshotRestoreReconciler) processRestore(ctx context.Context, req *velerov1api.SnapshotRestore, pod *corev1api.Pod, log logrus.FieldLogger) error {
	volumeDir, err := kube.GetVolumeDirectory(ctx, log, pod, req.Spec.RestorePvc, s.Client)
	if err != nil {
		return errors.Wrap(err, "error getting volume directory name")
	}

	// Get the full path of the new volume's directory as mounted in the daemonset pod, which
	// will look like: /host_pods/<new-pod-uid>/volumes/<volume-plugin-name>/<volume-dir>
	volumePath, err := kube.SinglePathMatch(
		fmt.Sprintf("/host_pods/%s/volumes/*/%s", string(pod.UID), volumeDir),
		s.fileSystem, log)
	if err != nil {
		return errors.Wrap(err, "error identifying path of volume")
	}

	backupLocation := &velerov1api.BackupStorageLocation{}
	if err := s.Get(ctx, client.ObjectKey{
		Namespace: req.Namespace,
		Name:      req.Spec.BackupStorageLocation,
	}, backupLocation); err != nil {
		return errors.Wrap(err, "error getting backup storage location")
	}

	backupRepo, err := s.repositoryEnsurer.EnsureRepo(ctx, req.Namespace, req.Spec.SourceNamespace, req.Spec.BackupStorageLocation, req.Spec.UploaderType)
	if err != nil {
		return errors.Wrap(err, "error ensure backup repository")
	}

	uploaderProv, err := provider.NewUploaderProvider(ctx, s.Client, req.Spec.UploaderType,
		req.Spec.RepoIdentifier, backupLocation, backupRepo, s.credentialGetter, repokey.RepoKeySelector(), log)
	if err != nil {
		return errors.Wrap(err, "error creating uploader")
	}

	defer func() {
		if err := uploaderProv.Close(ctx); err != nil {
			log.Errorf("failed to close uploader provider with error %v", err)
		}
	}()

	if err = uploaderProv.RunRestore(ctx, req.Spec.SnapshotID, volumePath, s.NewSnapshotBackupProgressUpdater(req, log, ctx)); err != nil {
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
	original := s.SnapshotRestore.DeepCopy()
	s.SnapshotRestore.Status.Progress = velerov1api.DataMoveOperationProgress{TotalBytes: p.TotalBytes, BytesDone: p.BytesDone}
	if s.Cli == nil {
		s.Log.Errorf("failed to update snapshot %s restore progress with uninitailize client", s.SnapshotRestore.Spec.RestorePvc)
		return
	}
	if err := s.Cli.Patch(s.Ctx, s.SnapshotRestore, client.MergeFrom(original)); err != nil {
		s.Log.Errorf("update restore snapshot %s  progress with %v", s.SnapshotRestore.Spec.RestorePvc, err)
	}
}
