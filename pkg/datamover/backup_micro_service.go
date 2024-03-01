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

package datamover

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/vmware-tanzu/velero/internal/credentials"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velerov2alpha1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"
	"github.com/vmware-tanzu/velero/pkg/datapath"
	"github.com/vmware-tanzu/velero/pkg/repository"
	"github.com/vmware-tanzu/velero/pkg/uploader"
	"github.com/vmware-tanzu/velero/pkg/util/kube"
)

// BackupMicroService process data mover backups inside the backup pod
type BackupMicroService struct {
	client           client.Client
	kubeClient       kubernetes.Interface
	repoEnsurer      *repository.Ensurer
	credentialGetter *credentials.CredentialGetter
	logger           logrus.FieldLogger
	dataPathMgr      *datapath.Manager
	eventRecorder    *kube.EventRecorder

	dataUpload *velerov2alpha1api.DataUpload
	thisPod    *corev1.Pod

	resultSignal chan dataPathResult
	startSignal  chan struct{}
}

type dataPathResult struct {
	err    error
	result string
}

func NewBackupMicroService(ctx context.Context, client client.Client, kubeClient kubernetes.Interface, dataUpload *velerov2alpha1api.DataUpload,
	thisPod *corev1.Pod, dataPathMgr *datapath.Manager, repoEnsurer *repository.Ensurer, cred *credentials.CredentialGetter, log logrus.FieldLogger) *BackupMicroService {
	return &BackupMicroService{
		client:           client,
		kubeClient:       kubeClient,
		credentialGetter: cred,
		logger:           log,
		repoEnsurer:      repoEnsurer,
		dataPathMgr:      dataPathMgr,
		dataUpload:       dataUpload,
		eventRecorder:    kube.NewEventRecorder(kubeClient, dataUpload.Name, thisPod.Spec.NodeName),
		thisPod:          thisPod,
		resultSignal:     make(chan dataPathResult),
	}
}

// +kubebuilder:rbac:groups=velero.io,resources=datauploads,verbs=get;list;watch
// +kubebuilder:rbac:groups=velero.io,resources=datauploads/status,verbs=get
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list

func (r *BackupMicroService) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.logger.WithFields(logrus.Fields{
		"controller": "dataupload",
		"dataupload": req.NamespacedName,
	})

	log.Infof("Reconcile %s", req.Name)
	du := &velerov2alpha1api.DataUpload{}
	if err := r.client.Get(ctx, req.NamespacedName, du); err != nil {
		if apierrors.IsNotFound(err) {
			log.Debug("Unable to find DataUpload")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, errors.Wrap(err, "getting DataUpload")
	}

	if du.Spec.Cancel {
		log.Info("Data upload is being canceled")

		r.eventRecorder.Event(du, false, "Cancelling", "Cancelling for data upload %s", du.Name)

		fsBackup := r.dataPathMgr.GetAsyncBR(du.Name)
		if fsBackup == nil {
			r.OnDataUploadCancelled(ctx, du.GetNamespace(), du.GetName())
		} else {
			fsBackup.Cancel()
		}

		return ctrl.Result{}, nil
	} else {
		log.Info("Data upload turns to InProgress")
		r.startSignal <- struct{}{}
	}

	return ctrl.Result{}, nil
}

func (r *BackupMicroService) RunCancelableDataUpload(ctx context.Context) (string, error) {
	log := r.logger.WithFields(logrus.Fields{
		"dataupload": r.dataUpload.Name,
	})

	select {
	case <-r.startSignal:
		break
	case <-time.After(time.Minute * 2):
		log.Error("Timeout waiting for start signal")
		return "", errors.New("timeout waiting for start signal")
	}

	log.Info("Run cancelable dataUpload")

	du := r.dataUpload

	path, err := kube.GetPodVolumePath(r.thisPod, 0)
	if err != nil {
		return "", errors.New("error getting path for pod volume")
	}

	callbacks := datapath.Callbacks{
		OnCompleted: r.OnDataUploadCompleted,
		OnFailed:    r.OnDataUploadFailed,
		OnCancelled: r.OnDataUploadCancelled,
		OnProgress:  r.OnDataUploadProgress,
	}

	fsBackup, err := r.dataPathMgr.CreateFileSystemBR(du.Name, uploader.DataUploadDownloadRequestor, ctx, r.client, du.Namespace, callbacks, log)
	if err != nil {
		return "", errors.Wrap(err, "error to create data path")
	}

	log.WithField("path", path).Debug("Found volume path")
	if err := fsBackup.Init(ctx, du.Spec.BackupStorageLocation, du.Spec.SourceNamespace, GetUploaderType(du.Spec.DataMover),
		velerov1api.BackupRepositoryTypeKopia, "", r.repoEnsurer, r.credentialGetter); err != nil {
		return "", errors.Wrap(err, "error to initialize data path")
	}
	log.WithField("path", path).Info("fs init")

	tags := map[string]string{
		velerov1api.AsyncOperationIDLabel: du.Labels[velerov1api.AsyncOperationIDLabel],
	}

	if err := fsBackup.StartBackup(datapath.AccessPoint{ByPath: path}, fmt.Sprintf("%s/%s", du.Spec.SourceNamespace, du.Spec.SourcePVC), "", false, tags, du.Spec.DataMoverConfig); err != nil {
		return "", errors.Wrap(err, "error starting data path backup")
	}

	log.WithField("path", path).Info("Async fs backup data path started")

	result := ""
	select {
	case <-ctx.Done():
		err = errors.New("timed out waiting for fs backup to complete")
		break
	case res := <-r.resultSignal:
		err = res.err
		result = res.result
		break
	}

	if err != nil {
		log.WithField("path", path).WithError(err).Error("Async fs backup was not completed")
	}

	return result, err
}

func (r *BackupMicroService) OnDataUploadCompleted(ctx context.Context, namespace string, duName string, result datapath.Result) {
	defer r.closeDataPath(ctx, duName)

	log := r.logger.WithField("dataupload", duName)

	backupBytes, err := json.Marshal(result.Backup)
	if err != nil {
		log.WithError(err).Errorf("Failed to marshal backup result %v", result.Backup)
		r.resultSignal <- dataPathResult{
			err: errors.Wrapf(err, "Failed to marshal backup result %v", result.Backup),
		}
		return
	}

	r.eventRecorder.Event(r.thisPod, false, datapath.EventReasonCompleted, string(backupBytes))
	r.resultSignal <- dataPathResult{
		result: string(backupBytes),
	}

	log.Info("Async fs backup data path completed")
}

func (r *BackupMicroService) OnDataUploadFailed(ctx context.Context, namespace string, duName string, err error) {
	defer r.closeDataPath(ctx, duName)

	log := r.logger.WithField("dataupload", duName)
	log.WithError(err).Error("Async fs backup data path failed")

	r.eventRecorder.Event(r.thisPod, false, datapath.EventReasonFailed, "Data path for data upload %s failed, error %v", r.dataUpload.Name, err)
	r.resultSignal <- dataPathResult{
		err: errors.Wrapf(err, "Data path for data upload %s failed", r.dataUpload.Name),
	}
}

func (r *BackupMicroService) OnDataUploadCancelled(ctx context.Context, namespace string, duName string) {
	defer r.closeDataPath(ctx, duName)

	log := r.logger.WithField("dataupload", duName)
	log.Warn("Async fs backup data path canceled")

	r.eventRecorder.Event(r.thisPod, false, datapath.EventReasonCancelled, "Data path for data upload %s cancelled", duName)
	r.resultSignal <- dataPathResult{
		err: errors.New(datapath.ErrCancelled),
	}
}

func (r *BackupMicroService) OnDataUploadProgress(ctx context.Context, namespace string, duName string, progress *uploader.Progress) {
	log := r.logger.WithFields(logrus.Fields{
		"dataupload": duName,
	})

	progressBytes, err := json.Marshal(progress)
	if err != nil {
		log.WithError(err).Errorf("Failed to marshal progress %v", progress)
		return
	}

	r.eventRecorder.Event(r.thisPod, false, datapath.EventReasonProgress, string(progressBytes))
}

func (r *BackupMicroService) closeDataPath(ctx context.Context, duName string) {
	fsBackup := r.dataPathMgr.GetAsyncBR(duName)
	if fsBackup != nil {
		fsBackup.Close(ctx)
	}

	r.dataPathMgr.RemoveAsyncBR(duName)
}

// SetupWithManager registers the BackupMicroService controller.
func (r *BackupMicroService) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&velerov2alpha1api.DataUpload{},
			builder.WithPredicates(predicate.Funcs{
				UpdateFunc: func(ue event.UpdateEvent) bool {
					oldObj := ue.ObjectOld.(*velerov2alpha1api.DataUpload)
					newObj := ue.ObjectNew.(*velerov2alpha1api.DataUpload)

					if newObj.Name != r.dataUpload.Name {
						return false
					}

					if newObj.Status.Phase != velerov2alpha1api.DataUploadPhaseInProgress {
						return false
					}

					if oldObj.Status.Phase == velerov2alpha1api.DataUploadPhasePrepared {
						return true
					}

					if newObj.Spec.Cancel && !oldObj.Spec.Cancel {
						return true
					}

					return false
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
