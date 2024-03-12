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
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/internal/credentials"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velerov2alpha1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"
	"github.com/vmware-tanzu/velero/pkg/datapath"
	"github.com/vmware-tanzu/velero/pkg/exposer"
	"github.com/vmware-tanzu/velero/pkg/repository"
	"github.com/vmware-tanzu/velero/pkg/uploader"
	"github.com/vmware-tanzu/velero/pkg/util/kube"

	cachetool "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
)

// RestoreMicroService process data mover restores inside the restore pod
type RestoreMicroService struct {
	ctx              context.Context
	client           client.Client
	kubeClient       kubernetes.Interface
	repoEnsurer      *repository.Ensurer
	credentialGetter *credentials.CredentialGetter
	logger           logrus.FieldLogger
	dataPathMgr      *datapath.Manager
	eventRecorder    *kube.EventRecorder

	namespace        string
	dataDownloadName string
	thisPod          *corev1.Pod
	thisContainer    string
	thisVolume       string

	resultSignal chan dataPathResult
}

func NewRestoreMicroService(ctx context.Context, client client.Client, kubeClient kubernetes.Interface, dataDownloadName string, namespace string,
	thisPod *corev1.Pod, thisContainer string, thisVolume string, dataPathMgr *datapath.Manager, repoEnsurer *repository.Ensurer, cred *credentials.CredentialGetter, log logrus.FieldLogger) *RestoreMicroService {
	return &RestoreMicroService{
		ctx:              ctx,
		client:           client,
		kubeClient:       kubeClient,
		credentialGetter: cred,
		logger:           log,
		repoEnsurer:      repoEnsurer,
		dataPathMgr:      dataPathMgr,
		namespace:        namespace,
		dataDownloadName: dataDownloadName,
		eventRecorder:    kube.NewEventRecorder(kubeClient, client.Scheme(), dataDownloadName, thisPod.Spec.NodeName),
		thisPod:          thisPod,
		thisContainer:    thisContainer,
		thisVolume:       thisVolume,
		resultSignal:     make(chan dataPathResult),
	}
}

func (r *RestoreMicroService) RunCancelableDataDownload(ctx context.Context) (string, error) {
	log := r.logger.WithFields(logrus.Fields{
		"datadownload": r.dataDownloadName,
	})

	dd := &velerov2alpha1api.DataDownload{}
	err := wait.PollImmediateWithContext(ctx, 500*time.Millisecond, time.Minute*2, func(ctx context.Context) (bool, error) {
		err := r.client.Get(ctx, types.NamespacedName{
			Namespace: r.namespace,
			Name:      r.dataDownloadName,
		}, dd)
		if apierrors.IsNotFound(err) {
			return false, nil
		}

		if err != nil {
			return true, errors.Wrapf(err, "error to get dd %s", r.dataDownloadName)
		}

		if dd.Status.Phase == velerov2alpha1api.DataDownloadPhaseInProgress {
			return true, nil
		} else {
			return false, nil
		}
	})
	if err != nil {
		log.WithError(err).Error("Failed to wait dd")
		return "", errors.Wrap(err, "error waiting for dd")
	}

	log.Info("Run cancelable dataDownload")

	callbacks := datapath.Callbacks{
		OnCompleted: r.OnDataDownloadCompleted,
		OnFailed:    r.OnDataDownloadFailed,
		OnCancelled: r.OnDataDownloadCancelled,
		OnProgress:  r.OnDataDownloadProgress,
	}

	fsRestore, err := r.dataPathMgr.CreateFileSystemBR(dd.Name, uploader.DataUploadDownloadRequestor, ctx, r.client, dd.Namespace, callbacks, log)
	if err != nil {
		return "", errors.Wrap(err, "error to create data path")
	}

	log.Debug("Found volume path")
	if err := fsRestore.Init(ctx, &exposer.ExposeResult{ByPod: exposer.ExposeByPod{
		HostingPod:       r.thisPod,
		HostingContainer: r.thisContainer,
		VolumeName:       r.thisVolume,
	}}, &datapath.FSBRInitParam{
		BSLName:           dd.Spec.BackupStorageLocation,
		SourceNamespace:   dd.Spec.SourceNamespace,
		UploaderType:      GetUploaderType(dd.Spec.DataMover),
		RepositoryType:    velerov1api.BackupRepositoryTypeKopia,
		RepositoryEnsurer: r.repoEnsurer,
		CredentialGetter:  r.credentialGetter,
	}); err != nil {
		return "", errors.Wrap(err, "error to initialize data path")
	}
	log.Info("fs init")

	if err := fsRestore.StartRestore(dd.Spec.SnapshotID, dd.Spec.DataMoverConfig); err != nil {
		return "", errors.Wrap(err, "error starting data path restore")
	}

	log.Info("Async fs restore data path started")

	result := ""
	select {
	case <-ctx.Done():
		err = errors.New("timed out waiting for fs restore to complete")
		break
	case res := <-r.resultSignal:
		err = res.err
		result = res.result
		break
	}

	if err != nil {
		log.WithError(err).Error("Async fs restore was not completed")
	}

	return result, err
}

func (r *RestoreMicroService) Shutdown() {
	r.eventRecorder.Shutdown()
}

func (r *RestoreMicroService) OnDataDownloadCompleted(ctx context.Context, namespace string, ddName string, result datapath.Result) {
	defer r.closeDataPath(ctx, ddName)

	log := r.logger.WithField("datadownload", ddName)

	restoreBytes, err := json.Marshal(result.Restore)
	if err != nil {
		log.WithError(err).Errorf("Failed to marshal restore result %v", result.Restore)
		r.resultSignal <- dataPathResult{
			err: errors.Wrapf(err, "Failed to marshal restore result %v", result.Restore),
		}
	} else {
		r.eventRecorder.Event(r.thisPod, false, datapath.EventReasonCompleted, string(restoreBytes))
		r.resultSignal <- dataPathResult{
			result: string(restoreBytes),
		}
	}

	log.Info("Async fs restore data path completed")
}

func (r *RestoreMicroService) OnDataDownloadFailed(ctx context.Context, namespace string, ddName string, err error) {
	defer r.closeDataPath(ctx, ddName)

	log := r.logger.WithField("datadownload", ddName)
	log.WithError(err).Error("Async fs restore data path failed")

	r.eventRecorder.Event(r.thisPod, false, datapath.EventReasonFailed, "Data path for data download %s failed, error %v", r.dataDownloadName, err)
	r.resultSignal <- dataPathResult{
		err: errors.Wrapf(err, "Data path for data download %s failed", r.dataDownloadName),
	}
}

func (r *RestoreMicroService) OnDataDownloadCancelled(ctx context.Context, namespace string, ddName string) {
	defer r.closeDataPath(ctx, ddName)

	log := r.logger.WithField("datadownload", ddName)
	log.Warn("Async fs restore data path canceled")

	r.eventRecorder.Event(r.thisPod, false, datapath.EventReasonCancelled, "Data path for data download %s cancelled", ddName)
	r.resultSignal <- dataPathResult{
		err: errors.Errorf("Data path for data download %s cancelled", r.dataDownloadName),
	}
}

func (r *RestoreMicroService) OnDataDownloadProgress(ctx context.Context, namespace string, ddName string, progress *uploader.Progress) {
	log := r.logger.WithFields(logrus.Fields{
		"datadownload": ddName,
	})

	progressBytes, err := json.Marshal(progress)
	if err != nil {
		log.WithError(err).Errorf("Failed to marshal progress %v", progress)
		return
	}

	r.eventRecorder.Event(r.thisPod, false, datapath.EventReasonProgress, string(progressBytes))
}

func (r *RestoreMicroService) closeDataPath(ctx context.Context, ddName string) {
	fsRestore := r.dataPathMgr.GetAsyncBR(ddName)
	if fsRestore != nil {
		fsRestore.Close(ctx)
	}

	r.dataPathMgr.RemoveAsyncBR(ddName)
}

// SetupWatcher start to watch the DataDownload.
func (r *RestoreMicroService) SetupWatcher(ctx context.Context, ddInformer cache.Informer) {
	ddInformer.AddEventHandler(
		cachetool.ResourceEventHandlerFuncs{
			UpdateFunc: func(oldObj interface{}, newObj interface{}) {
				oldDd := oldObj.(*velerov2alpha1api.DataDownload)
				newDd := newObj.(*velerov2alpha1api.DataDownload)

				if newDd.Name != r.dataDownloadName {
					return
				}

				if newDd.Status.Phase != velerov2alpha1api.DataDownloadPhaseInProgress {
					return
				}

				if newDd.Spec.Cancel && !oldDd.Spec.Cancel {
					r.cancelDataDownload(newDd)
				}
			},
		},
	)
}

func (r *RestoreMicroService) cancelDataDownload(dd *velerov2alpha1api.DataDownload) {
	r.logger.WithField("DataDownload", dd.Name).Info("Data download is being canceled")

	r.eventRecorder.Event(dd, false, "Cancelling", "Cancelling for data download %s", dd.Name)

	fsBackup := r.dataPathMgr.GetAsyncBR(dd.Name)
	if fsBackup == nil {
		r.OnDataDownloadCancelled(r.ctx, dd.GetNamespace(), dd.GetName())
	} else {
		fsBackup.Cancel()
	}
}
