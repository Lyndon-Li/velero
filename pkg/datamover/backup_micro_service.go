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
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/internal/credentials"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velerov2alpha1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"
	"github.com/vmware-tanzu/velero/pkg/datapath"
	"github.com/vmware-tanzu/velero/pkg/repository"
	"github.com/vmware-tanzu/velero/pkg/uploader"
	"github.com/vmware-tanzu/velero/pkg/util/kube"
	cachetool "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
)

// BackupMicroService process data mover backups inside the backup pod
type BackupMicroService struct {
	ctx              context.Context
	client           client.Client
	kubeClient       kubernetes.Interface
	repoEnsurer      *repository.Ensurer
	credentialGetter *credentials.CredentialGetter
	logger           logrus.FieldLogger
	dataPathMgr      *datapath.Manager
	eventRecorder    *kube.EventRecorder

	dataUploadName string
	thisPod        *corev1.Pod

	resultSignal chan dataPathResult
	startSignal  chan *velerov2alpha1api.DataUpload
}

type dataPathResult struct {
	err    error
	result string
}

func NewBackupMicroService(ctx context.Context, client client.Client, kubeClient kubernetes.Interface, dataUploadName string,
	thisPod *corev1.Pod, dataPathMgr *datapath.Manager, repoEnsurer *repository.Ensurer, cred *credentials.CredentialGetter, log logrus.FieldLogger) *BackupMicroService {
	return &BackupMicroService{
		ctx:              ctx,
		client:           client,
		kubeClient:       kubeClient,
		credentialGetter: cred,
		logger:           log,
		repoEnsurer:      repoEnsurer,
		dataPathMgr:      dataPathMgr,
		dataUploadName:   dataUploadName,
		eventRecorder:    kube.NewEventRecorder(kubeClient, dataUploadName, thisPod.Spec.NodeName),
		thisPod:          thisPod,
		resultSignal:     make(chan dataPathResult),
		//startSignal:      make(chan *velerov2alpha1api.DataUpload),
	}
}

func (r *BackupMicroService) RunCancelableDataUpload(ctx context.Context) (string, error) {
	log := r.logger.WithFields(logrus.Fields{
		"dataupload": r.dataUploadName,
	})

	var du *velerov2alpha1api.DataUpload

	select {
	case recv := <-r.startSignal:
		du = recv
		break
	case <-time.After(time.Minute * 2):
		log.Error("Timeout waiting for start signal")
		return "", errors.New("timeout waiting for start signal")
	}

	log.Info("Run cancelable dataUpload")

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
	} else {
		//r.eventRecorder.Event(r.thisPod, false, datapath.EventReasonCompleted, string(backupBytes))
		r.resultSignal <- dataPathResult{
			result: string(backupBytes),
		}
	}

	log.Info("Async fs backup data path completed")
}

func (r *BackupMicroService) OnDataUploadFailed(ctx context.Context, namespace string, duName string, err error) {
	defer r.closeDataPath(ctx, duName)

	log := r.logger.WithField("dataupload", duName)
	log.WithError(err).Error("Async fs backup data path failed")

	//r.eventRecorder.Event(r.thisPod, false, datapath.EventReasonFailed, "Data path for data upload %s failed, error %v", r.dataUploadName, err)
	r.resultSignal <- dataPathResult{
		err: errors.Wrapf(err, "Data path for data upload %s failed", r.dataUploadName),
	}
}

func (r *BackupMicroService) OnDataUploadCancelled(ctx context.Context, namespace string, duName string) {
	defer r.closeDataPath(ctx, duName)

	log := r.logger.WithField("dataupload", duName)
	log.Warn("Async fs backup data path canceled")

	//r.eventRecorder.Event(r.thisPod, false, datapath.EventReasonCancelled, "Data path for data upload %s cancelled", duName)
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

// SetupWatcher start to watch the DataUpload.
func (r *BackupMicroService) SetupWatcher(ctx context.Context, duInformer cache.Informer) {
	duInformer.AddEventHandler(
		cachetool.ResourceEventHandlerFuncs{
			UpdateFunc: func(oldObj interface{}, newObj interface{}) {
				oldDu := oldObj.(*velerov2alpha1api.DataUpload)
				newDu := newObj.(*velerov2alpha1api.DataUpload)

				if newDu.Name != r.dataUploadName {
					return
				}

				if newDu.Status.Phase != velerov2alpha1api.DataUploadPhaseInProgress {
					return
				}

				if oldDu.Status.Phase == velerov2alpha1api.DataUploadPhasePrepared {
					r.logger.WithField("DataUpload", newDu.Name).Info("Data upload turns to InProgress")
					r.startSignal <- newDu
				}

				if newDu.Spec.Cancel && !oldDu.Spec.Cancel {
					r.cancelDataUpload(newDu)
				}
			},
		},
	)
}

func (r *BackupMicroService) cancelDataUpload(du *velerov2alpha1api.DataUpload) {
	r.logger.WithField("DataUpload", du.Name).Info("Data upload is being canceled")

	r.eventRecorder.Event(du, false, "Cancelling", "Cancelling for data upload %s", du.Name)

	fsBackup := r.dataPathMgr.GetAsyncBR(du.Name)
	if fsBackup == nil {
		r.OnDataUploadCancelled(r.ctx, du.GetNamespace(), du.GetName())
	} else {
		fsBackup.Cancel()
	}
}
