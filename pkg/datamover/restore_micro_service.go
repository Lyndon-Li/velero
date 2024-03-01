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

// RestoreMicroService process data mover restores inside the restore pod
type RestoreMicroService struct {
	client           client.Client
	kubeClient       kubernetes.Interface
	repoEnsurer      *repository.Ensurer
	credentialGetter *credentials.CredentialGetter
	logger           logrus.FieldLogger
	dataPathMgr      *datapath.Manager
	eventRecorder    *kube.EventRecorder

	dataDownload *velerov2alpha1api.DataDownload
	thisPod      *corev1.Pod

	resultSignal chan dataPathResult
	startSignal  chan struct{}
}

func NewRestoreMicroService(ctx context.Context, client client.Client, kubeClient kubernetes.Interface, dataDownload *velerov2alpha1api.DataDownload,
	thisPod *corev1.Pod, dataPathMgr *datapath.Manager, repoEnsurer *repository.Ensurer, cred *credentials.CredentialGetter, log logrus.FieldLogger) *RestoreMicroService {
	return &RestoreMicroService{
		client:           client,
		kubeClient:       kubeClient,
		credentialGetter: cred,
		logger:           log,
		repoEnsurer:      repoEnsurer,
		dataPathMgr:      dataPathMgr,
		dataDownload:     dataDownload,
		eventRecorder:    kube.NewEventRecorder(kubeClient, dataDownload.Name, thisPod.Spec.NodeName),
		thisPod:          thisPod,
		resultSignal:     make(chan dataPathResult),
	}
}

// +kubebuilder:rbac:groups=velero.io,resources=datadownloads,verbs=get;list;watch
// +kubebuilder:rbac:groups=velero.io,resources=datadownload/status,verbs=get
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list

func (r *RestoreMicroService) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.logger.WithFields(logrus.Fields{
		"controller":   "datadownload",
		"datadownload": req.NamespacedName,
	})

	log.Infof("Reconcile %s", req.Name)
	dd := &velerov2alpha1api.DataDownload{}
	if err := r.client.Get(ctx, req.NamespacedName, dd); err != nil {
		if apierrors.IsNotFound(err) {
			log.Debug("Unable to find DataDownload")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, errors.Wrap(err, "getting DataDownload")
	}

	if dd.Spec.Cancel {
		log.Info("Data download is being canceled")

		r.eventRecorder.Event(dd, false, "Cancelling", "Cancelling for data download %s", dd.Name)

		fsRestore := r.dataPathMgr.GetAsyncBR(dd.Name)
		if fsRestore == nil {
			r.OnDataDownloadCancelled(ctx, dd.GetNamespace(), dd.GetName())
		} else {
			fsRestore.Cancel()
		}

		return ctrl.Result{}, nil
	} else {
		log.Info("Data download turns to InProgress")
		r.startSignal <- struct{}{}
	}

	return ctrl.Result{}, nil
}

func (r *RestoreMicroService) RunCancelableDataDownload(ctx context.Context) (string, error) {
	log := r.logger.WithFields(logrus.Fields{
		"datadownload": r.dataDownload.Name,
	})

	select {
	case <-r.startSignal:
		break
	case <-time.After(time.Minute * 2):
		log.Error("Timeout waiting for start signal")
		return "", errors.New("timeout waiting for start signal")
	}

	log.Info("Run cancelable dataDownload")

	dd := r.dataDownload

	path, err := kube.GetPodVolumePath(r.thisPod, 0)
	if err != nil {
		return "", errors.New("error getting path for pod volume")
	}

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

	log.WithField("path", path).Debug("Found volume path")
	if err := fsRestore.Init(ctx, dd.Spec.BackupStorageLocation, dd.Spec.SourceNamespace, GetUploaderType(dd.Spec.DataMover),
		velerov1api.BackupRepositoryTypeKopia, "", r.repoEnsurer, r.credentialGetter); err != nil {
		return "", errors.Wrap(err, "error to initialize data path")
	}
	log.WithField("path", path).Info("fs init")

	if err := fsRestore.StartRestore(dd.Spec.SnapshotID, datapath.AccessPoint{ByPath: path}, dd.Spec.DataMoverConfig); err != nil {
		return "", errors.Wrap(err, "error starting data path restore")
	}

	log.WithField("path", path).Info("Async fs restore data path started")

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
		log.WithField("path", path).WithError(err).Error("Async fs restore was not completed")
	}

	return result, err
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
		return
	}

	r.eventRecorder.Event(r.thisPod, false, datapath.EventReasonCompleted, string(restoreBytes))
	r.resultSignal <- dataPathResult{
		result: string(restoreBytes),
	}

	log.Info("Async fs restore data path completed")
}

func (r *RestoreMicroService) OnDataDownloadFailed(ctx context.Context, namespace string, ddName string, err error) {
	defer r.closeDataPath(ctx, ddName)

	log := r.logger.WithField("datadownload", ddName)
	log.WithError(err).Error("Async fs restore data path failed")

	r.eventRecorder.Event(r.thisPod, false, datapath.EventReasonFailed, "Data path for data download %s failed, error %v", r.dataDownload.Name, err)
	r.resultSignal <- dataPathResult{
		err: errors.Wrapf(err, "Data path for data download %s failed", r.dataDownload.Name),
	}
}

func (r *RestoreMicroService) OnDataDownloadCancelled(ctx context.Context, namespace string, ddName string) {
	defer r.closeDataPath(ctx, ddName)

	log := r.logger.WithField("datadownload", ddName)
	log.Warn("Async fs restore data path canceled")

	r.eventRecorder.Event(r.thisPod, false, datapath.EventReasonCancelled, "Data path for data download %s cancelled", ddName)
	r.resultSignal <- dataPathResult{
		err: errors.Errorf("Data path for data download %s cancelled", r.dataDownload.Name),
	}
}

func (r *RestoreMicroService) OnDataDownloadProgress(ctx context.Context, namespace string, duName string, progress *uploader.Progress) {
	log := r.logger.WithFields(logrus.Fields{
		"datadownload": duName,
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

// SetupWithManager registers the RestoreMicroService controller.
func (r *RestoreMicroService) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&velerov2alpha1api.DataDownload{},
			builder.WithPredicates(predicate.Funcs{
				UpdateFunc: func(ue event.UpdateEvent) bool {
					oldObj := ue.ObjectOld.(*velerov2alpha1api.DataDownload)
					newObj := ue.ObjectNew.(*velerov2alpha1api.DataDownload)

					if newObj.Name != r.dataDownload.Name {
						return false
					}

					if newObj.Status.Phase != velerov2alpha1api.DataDownloadPhaseInProgress {
						return false
					}

					if oldObj.Status.Phase == velerov2alpha1api.DataDownloadPhasePrepared {
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
