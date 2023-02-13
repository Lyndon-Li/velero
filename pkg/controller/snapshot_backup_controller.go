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
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/datamover"
	"github.com/vmware-tanzu/velero/pkg/label"
	"github.com/vmware-tanzu/velero/pkg/metrics"
	"github.com/vmware-tanzu/velero/pkg/util/boolptr"
	"github.com/vmware-tanzu/velero/pkg/util/csi"
	"github.com/vmware-tanzu/velero/pkg/util/kube"

	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	snapshotterClientSet "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	batchv1api "k8s.io/api/batch/v1"
)

// SnapshotBackupReconciler reconciles a Snapshotbackup object
type SnapshotBackupReconciler struct {
	Scheme            *runtime.Scheme
	Client            client.Client
	kubeClient        kubernetes.Interface
	csiSnapshotClient *snapshotterClientSet.Clientset
	Clock             clock.Clock
	Metrics           *metrics.ServerMetrics
	NodeName          string
	VeleroImage       string
	Log               logrus.FieldLogger
}

func NewSnapshotBackupReconciler(scheme *runtime.Scheme, client client.Client, kubeClient kubernetes.Interface, csiSnapshotClient *snapshotterClientSet.Clientset, clock clock.Clock, metrics *metrics.ServerMetrics, nodeName string, image string, log logrus.FieldLogger) *SnapshotBackupReconciler {
	return &SnapshotBackupReconciler{scheme, client, kubeClient, csiSnapshotClient, clock, metrics, nodeName, image, log}
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

		if _, err := s.createBackupJob(ctx, &ssb); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return s.errorOut(ctx, &ssb, err, "error to create backup job", log)
			} else {
				return ctrl.Result{}, nil
			}
		}

		log.Info("Backup job is created")

		if err := s.exposeSnapshot(ctx, &ssb, log); err != nil {
			return s.errorOut(ctx, &ssb, err, "error to expose snapshot", log)
		}

		log.Info("Snapshot is exposed")

		return ctrl.Result{}, nil
	} else if ssb.Status.Phase == velerov1api.SnapshotBackupPhasePrepared {
		log.Info("Snapshot backup is prepared")

		_, _, err := s.waitBackupJob(ctx, &ssb, log)
		if err != nil {
			return s.errorOut(ctx, &ssb, err, "backup job is not ready", log)
		}

		exposed, err := s.waitSnapshotExposed(ctx, &ssb, log)
		if err != nil {
			return s.errorOut(ctx, &ssb, err, "exposed snapshot is not ready", log)
		} else if !exposed {
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

		return ctrl.Result{}, nil
	} else if ssb.Status.Phase == velerov1api.SnapshotBackupPhaseInProgress && ssb.Spec.Cancel {
		log.Info("Snapshot backup is being canceled")

		// Update status to Canceling.
		original := ssb.DeepCopy()
		ssb.Status.Phase = velerov1api.SnapshotBackupPhaseCanceling
		if err := s.Client.Patch(ctx, &ssb, client.MergeFrom(original)); err != nil {
			log.WithError(err).Error("error updating SnapshotBackup status")
			return ctrl.Result{}, err
		}

		if err := s.cancelSnapshotBackup(ctx, &ssb); err != nil {
			log.WithError(err).Error("error canceling SnapshotBackup")
			return ctrl.Result{}, err
		}

		// Update status to Canceled.
		original = ssb.DeepCopy()
		ssb.Status.Phase = velerov1api.SnapshotBackupPhaseCanceled
		if err := s.Client.Patch(ctx, &ssb, client.MergeFrom(original)); err != nil {
			log.WithError(err).Error("error updating SnapshotBackup status")
			return ctrl.Result{}, err
		}

		return ctrl.Result{}, nil
	} else if ssb.Status.Phase == velerov1api.SnapshotBackupPhaseDataPathExit {
		log.Info("Snapshot backup data path exits")

		//ssbOutput, err := s.getBackupResult(ctx, &ssb, log)
		err := s.checkBackupResult(ctx, &ssb, log)
		if err != nil {
			return s.errorOut(ctx, &ssb, err, "error check snapshot backup result", log)
		}
		// if err != nil {
		// 	return s.errorOut(ctx, &ssb, err, "error get snapshot backup result", log)
		// } else if ssbOutput.ExitCode != 0 {
		// 	return s.errorOut(ctx, &ssb, err, fmt.Sprintf("running backup, stderr=%v", ssbOutput.Message), log)
		// }

		log.Info("cleaning up exposed snapshot")
		if delErr := s.cleanUpSnapshot(ctx, &ssb, log); delErr != nil {
			return s.errorOut(ctx, &ssb, err, "error clean up exposed snapshot", log)
		}

		// Update status to Completed with path & snapshot ID.
		original := ssb.DeepCopy()
		ssb.Status.Phase = velerov1api.SnapshotBackupPhaseCompleted
		// ssb.Status.SnapshotID = ssbOutput.SnapshotID
		ssb.Status.CompletionTimestamp = &metav1.Time{Time: s.Clock.Now()}

		if err = s.Client.Patch(ctx, &ssb, client.MergeFrom(original)); err != nil {
			log.WithError(err).Error("error updating PodVolumeBackup status")
			return ctrl.Result{}, err
		}
		log.Info("snapshot backup completed")
		return ctrl.Result{}, nil
	} else {
		log.WithField("phase", ssb.Status.Phase).Info("Snapshot backup is not in the expected phase")
		return ctrl.Result{}, nil
	}
}

// SetupWithManager registers the SnapshotBackup controller.
func (r *SnapshotBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&velerov1api.SnapshotBackup{}).
		Watches(&source.Kind{Type: &corev1.Pod{}}, handler.EnqueueRequestsFromMapFunc(r.findSnapshotBackupForPod),
			builder.WithPredicates(predicate.Funcs{
				UpdateFunc: func(ue event.UpdateEvent) bool {
					newObj := ue.ObjectNew.(*corev1.Pod)

					if _, ok := newObj.Labels[velerov1api.SnapshotBackupLabel]; !ok {
						return false
					}

					r.Log.WithField("pod", newObj.Name).Info("Pod is a snapshot backup")

					if newObj.Spec.NodeName == "" {
						return false
					}

					if newObj.Status.Phase == v1.PodPending {
						return false
					}

					if newObj.Status.Phase == v1.PodUnknown {
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

	if pod.Status.Phase == corev1.PodRunning {
		if ssb.Status.Phase != "" && ssb.Status.Phase != velerov1api.SnapshotBackupPhaseNew {
			return []reconcile.Request{}
		}

		r.Log.WithField("Backup pod", pod.Name).Infof("Preparing SnapshotBackup %s", ssb.Name)
		r.patchSnapshotBackup(context.Background(), ssb, prepareSnapshotBackup)
	} else {
		if ssb.Status.Phase != velerov1api.SnapshotBackupPhaseInProgress {
			return []reconcile.Request{}
		}

		r.Log.WithField("Backup pod", pod.Name).Infof("Data path exits for SnapshotBackup %s", ssb.Name)
		r.patchSnapshotBackup(context.Background(), ssb, snapshotBackupDataPathExit)
	}

	requests := make([]reconcile.Request, 1)
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

func snapshotBackupDataPathExit(ssb *velerov1api.SnapshotBackup) {
	ssb.Status.Phase = velerov1api.SnapshotBackupPhaseDataPathExit
}

func (s *SnapshotBackupReconciler) errorOut(ctx context.Context, ssb *velerov1api.SnapshotBackup, err error, msg string, log logrus.FieldLogger) (ctrl.Result, error) {
	s.cleanUpSnapshot(ctx, ssb, log)

	return s.updateStatusToFailed(ctx, ssb, err, msg, log)
}

func (s *SnapshotBackupReconciler) updateStatusToFailed(ctx context.Context, ssb *velerov1api.SnapshotBackup, err error, msg string, log logrus.FieldLogger) (ctrl.Result, error) {
	original := ssb.DeepCopy()
	ssb.Status.Phase = velerov1api.SnapshotBackupPhaseFailed
	ssb.Status.Message = errors.WithMessage(err, msg).Error()
	if ssb.Status.StartTimestamp.IsZero() {
		ssb.Status.StartTimestamp = &metav1.Time{Time: s.Clock.Now()}
	}
	ssb.Status.CompletionTimestamp = &metav1.Time{Time: s.Clock.Now()}
	if err = s.Client.Patch(ctx, ssb, client.MergeFrom(original)); err != nil {
		log.WithError(err).Error("error updating SnapshotBackup status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *SnapshotBackupReconciler) createBackupPod(ctx context.Context, ssb *velerov1api.SnapshotBackup) (*corev1.Pod, error) {
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
					Image:   r.VeleroImage,
					Command: []string{"dm", "snapshot-backup", ssb.Name},
				},
			},
		},
	}

	switch ssb.Spec.SnapshotType {
	case velerov1api.SnapshotTypeCSI:
		r.buildCSIBackupPodSpec(&pod.Spec, ssb)
	}

	updated, err := r.kubeClient.CoreV1().Pods(ssb.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "error to create backup pod")
	}

	return updated, nil
}

func (r *SnapshotBackupReconciler) getBackupPod(ctx context.Context, ssb *velerov1api.SnapshotBackup, log logrus.FieldLogger) (*corev1.Pod, error) {
	backupPodName := ssb.Name

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

	return pod, nil
}

func (r *SnapshotBackupReconciler) createBackupJob(ctx context.Context, ssb *velerov1api.SnapshotBackup) (*batchv1api.Job, error) {
	backupJobName := ssb.Name
	var parallelism int32 = 1
	var backupoffLimit int32 = 0

	job := &batchv1api.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backupJobName,
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
		Spec: batchv1api.JobSpec{
			Parallelism:  &parallelism,
			BackoffLimit: &backupoffLimit,
			Selector:     &metav1.LabelSelector{MatchLabels: map[string]string{velerov1api.SnapshotBackupLabel: ssb.Name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{velerov1api.SnapshotBackupLabel: ssb.Name},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    backupJobName,
							Image:   r.VeleroImage,
							Command: []string{"dm", "snapshot-backup", ssb.Name},
						},
					},
				},
			},
		},
	}

	switch ssb.Spec.SnapshotType {
	case velerov1api.SnapshotTypeCSI:
		r.buildCSIBackupPodSpec(&job.Spec.Template.Spec, ssb)
	}

	updated, err := r.kubeClient.BatchV1().Jobs(ssb.Namespace).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "error to create backup job")
	}

	return updated, nil
}

func (r *SnapshotBackupReconciler) buildCSIBackupPodSpec(podSpec *v1.PodSpec, ssb *velerov1api.SnapshotBackup) {
	backupPVCName := ssb.Name

	podSpec.Containers[0].VolumeMounts = append(podSpec.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name:      backupPVCName,
		MountPath: datamover.GetPodMountPath(),
	})

	podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
		Name: backupPVCName,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: backupPVCName,
			},
		},
	})
}

func (r *SnapshotBackupReconciler) waitBackupJob(ctx context.Context, ssb *velerov1api.SnapshotBackup,
	log logrus.FieldLogger) (*batchv1api.Job, *corev1.Pod, error) {
	backupJobName := ssb.Name

	pod, err := r.getBackupPod(ctx, ssb, log)
	if pod == nil {
		return nil, nil, err
	}

	job := &batchv1api.Job{}
	r.Client.Get(ctx, types.NamespacedName{
		Namespace: ssb.Namespace,
		Name:      backupJobName,
	}, job)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "error to get backup pod %s", backupJobName)
	}

	log.WithField("job", job.Name).Infof("Backup job is ready")

	return job, pod, nil
}

func (r *SnapshotBackupReconciler) checkBackupResult(ctx context.Context, ssb *velerov1api.SnapshotBackup, log logrus.FieldLogger) error {
	pod, err := r.getBackupPod(ctx, ssb, log)
	if pod == nil {
		return errors.Wrap(err, "error to get backup pod")
	}

	if pod.Status.Phase == corev1.PodFailed {
		return errors.Errorf("backup failed with message %s", pod.Status.Message)
	}

	if pod.Status.ContainerStatuses[0].State.Terminated == nil {
		return errors.New("backup pod doesn't seem to complete")
	}

	return nil
}

func (r *SnapshotBackupReconciler) getBackupResult(ctx context.Context, ssb *velerov1api.SnapshotBackup, log logrus.FieldLogger) (*datamover.SnapshotBackupOutput, error) {
	pod, err := r.getBackupPod(ctx, ssb, log)
	if pod == nil {
		return nil, errors.Wrap(err, "error to get backup pod")
	}

	ssbOutput := &datamover.SnapshotBackupOutput{}

	if pod.Status.Phase == corev1.PodFailed {
		ssbOutput.ExitCode = -1
		ssbOutput.Message = fmt.Sprintf("Backup pod failed with message %s", pod.Status.Message)
	} else {
		if pod.Status.ContainerStatuses[0].State.Terminated == nil {
			ssbOutput.ExitCode = -1
			ssbOutput.Message = "Backup pod doesn't seem to complete"
		} else {
			json.Unmarshal([]byte(pod.Status.ContainerStatuses[0].State.Terminated.Message), ssbOutput)
		}
	}

	return ssbOutput, nil
}

func (r *SnapshotBackupReconciler) exposeSnapshot(ctx context.Context, ssb *velerov1api.SnapshotBackup, log logrus.FieldLogger) error {
	switch ssb.Spec.SnapshotType {
	case velerov1api.SnapshotTypeCSI:
		return r.exposeCSISnapshot(ctx, ssb, log)
	default:
		return errors.Errorf("unsupported snapshot type %s", ssb.Spec.SnapshotType)
	}
}

func (r *SnapshotBackupReconciler) waitSnapshotExposed(ctx context.Context, ssb *velerov1api.SnapshotBackup, log logrus.FieldLogger) (bool, error) {
	switch ssb.Spec.SnapshotType {
	case velerov1api.SnapshotTypeCSI:
		return r.waitCSISnapshotExposed(ctx, ssb, log)
	default:
		return false, errors.Errorf("unsupported snapshot type %s", ssb.Spec.SnapshotType)
	}
}

func (r *SnapshotBackupReconciler) cleanUpSnapshot(ctx context.Context, ssb *velerov1api.SnapshotBackup, log logrus.FieldLogger) error {
	backupPodName := ssb.Name
	kube.DeletePodIfAny(ctx, r.kubeClient.CoreV1(), backupPodName, ssb.Namespace, log)

	switch ssb.Spec.SnapshotType {
	case velerov1api.SnapshotTypeCSI:
		return r.cleanUpCSISnapshot(ctx, ssb, log)
	default:
		return errors.Errorf("unsupported snapshot type %s", ssb.Spec.SnapshotType)
	}
}

func (r *SnapshotBackupReconciler) cancelSnapshotBackup(ctx context.Context, ssb *velerov1api.SnapshotBackup) error {
	backupPodName := ssb.Name
	err := kube.EnsureDeletePod(ctx, r.kubeClient.CoreV1(), backupPodName, ssb.Namespace, ssb.Spec.CSISnapshot.CSISnapshotTimeout.Duration)
	if apierrors.IsNotFound(err) {
		return nil
	} else {
		return err
	}
}

func (r *SnapshotBackupReconciler) waitCSISnapshotExposed(ctx context.Context, ssb *velerov1api.SnapshotBackup, log logrus.FieldLogger) (bool, error) {
	backupPVCName := ssb.Name

	pvc := &corev1.PersistentVolumeClaim{}
	pollInterval := 2 * time.Second
	err := wait.PollImmediate(pollInterval, ssb.Spec.CSISnapshot.CSISnapshotTimeout.Duration, func() (done bool, err error) {
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
			return false, nil
		} else {
			return false, errors.Wrapf(err, "error to get backup PVC %s", backupPVCName)
		}
	}

	log.WithField("backup pvc", backupPVCName).Info("Backup PVC is bound")

	return true, nil
}

func (r *SnapshotBackupReconciler) cleanUpCSISnapshot(ctx context.Context, ssb *velerov1api.SnapshotBackup, log logrus.FieldLogger) error {
	backupPVCName := ssb.Name
	backupVSName := ssb.Name

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

	volumeSnapshot, err := csi.WaitVolumeSnapshotReady(ctx, r.csiSnapshotClient, ssb.Spec.CSISnapshot.VolumeSnapshot, ssb.Spec.SourceNamespace, ssb.Spec.CSISnapshot.CSISnapshotTimeout.Duration, log)
	if err != nil {
		return errors.Wrapf(err, "error wait volume snapshot ready")
	}

	curLog.Info("Volumesnapshot is ready")

	volumeMode := kube.GetVolumeModeFromDataMover(ssb.Spec.DataMover)

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: volumeSnapshot.Namespace,
			Name:      backupVCName,
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

	err = csi.EnsureDeleteVS(ctx, r.csiSnapshotClient, volumeSnapshot, ssb.Spec.CSISnapshot.CSISnapshotTimeout.Duration)
	if err != nil {
		return errors.Wrap(err, "error to delete volume snapshot")
	}

	curLog.WithField("vs name", volumeSnapshot.Name).Infof("VS is deleted in namespace %s", volumeSnapshot.Namespace)

	err = csi.EnsureDeleteVSC(ctx, r.csiSnapshotClient, vsc, ssb.Spec.CSISnapshot.CSISnapshotTimeout.Duration)
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
				velerov1api.BackupNameLabel: label.GetValidName(ssb.Spec.BackupName),
			},
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
