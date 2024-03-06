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

package datapath

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/internal/credentials"
	"github.com/vmware-tanzu/velero/pkg/repository"
	"github.com/vmware-tanzu/velero/pkg/uploader"
	"github.com/vmware-tanzu/velero/pkg/util/kube"

	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/vmware-tanzu/velero/pkg/util/logging"
)

const (
	TaskTypeBackup  = "backup"
	TaskTypeRestore = "restore"

	ErrCancelled = "data path is cancelled"

	EventReasonCompleted = "Completed"
	EventReasonFailed    = "Failed"
	EventReasonCancelled = "Cancelled"
	EventReasonProgress  = "Progress"
)

type microServiceBR struct {
	ctx        context.Context
	cancel     context.CancelFunc
	log        logrus.FieldLogger
	client     client.Client
	kubeClient kubernetes.Interface
	mgr        manager.Manager
	namespace  string
	callbacks  Callbacks
	taskName   string
	taskType   string
	eventCh    chan *v1.Event
	podCh      chan *v1.Pod
}

func newMicroServiceBR(client client.Client, kubeClient kubernetes.Interface, mgr manager.Manager, taskType string, taskName string, namespace string, callbacks Callbacks, log logrus.FieldLogger) AsyncBR {
	ms := &microServiceBR{
		mgr:        mgr,
		client:     client,
		kubeClient: kubeClient,
		namespace:  namespace,
		callbacks:  callbacks,
		taskType:   taskType,
		taskName:   taskName,
		log:        log,
	}

	return ms
}

func (ms *microServiceBR) Init(ctx context.Context, bslName string, sourceNamespace string, uploaderType string, repositoryType string,
	repoIdentifier string, repositoryEnsurer *repository.Ensurer, credentialGetter *credentials.CredentialGetter) error {

	ms.ctx, ms.cancel = context.WithCancel(ctx)

	thisPod := &v1.Pod{}
	if err := ms.client.Get(ms.ctx, types.NamespacedName{
		Namespace: ms.namespace,
		Name:      ms.taskName,
	}, thisPod); err != nil {
		return errors.Wrap(err, "error getting job pod")
	}

	eventInformer, err := ms.mgr.GetCache().GetInformer(ms.ctx, &v1.Event{})
	if err != nil {
		return errors.Wrap(err, "error getting event informer")
	}

	podInformer, err := ms.mgr.GetCache().GetInformer(ms.ctx, &v1.Pod{})
	if err != nil {
		return errors.Wrap(err, "error getting pod informer")
	}

	ref := v1.ObjectReference{
		Kind:       thisPod.Kind,
		Namespace:  thisPod.Namespace,
		Name:       thisPod.Name,
		UID:        thisPod.UID,
		APIVersion: thisPod.APIVersion,
	}

	eventInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				evt := obj.(*v1.Event)
				if evt.InvolvedObject != ref {
					return
				}

				ms.eventCh <- evt
			},
			UpdateFunc: func(_, obj interface{}) {
				evt := obj.(*v1.Event)
				if evt.InvolvedObject != ref {
					return
				}

				ms.eventCh <- evt
			},
		},
	)

	podInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(_, obj interface{}) {
				pod := obj.(*v1.Pod)
				if pod.UID != thisPod.UID {
					return
				}

				if pod.Status.Phase == v1.PodSucceeded || pod.Status.Phase == v1.PodFailed {
					ms.podCh <- pod
				}
			},
		},
	)

	ms.log.WithFields(
		logrus.Fields{
			"taskType": ms.taskType,
			"taskName": ms.taskName,
			"thisPod":  thisPod.Name,
		}).Info("MicroServiceBR is initialized")

	return nil
}

func (ms *microServiceBR) Close(ctx context.Context) {
	if ms.cancel != nil {
		ms.cancel()
		ms.cancel = nil
	}

	ms.log.WithField("taskType", ms.taskType).WithField("taskName", ms.taskName).Info("MicroServiceBR is closed")
}

func (ms *microServiceBR) StartBackup(source AccessPoint, realSource string, parentSnapshot string, forceFull bool, tags map[string]string, uploaderConfig map[string]string) error {
	ms.startWatch()
	return nil
}

func (ms *microServiceBR) StartRestore(snapshotID string, target AccessPoint, uploaderConfigs map[string]string) error {
	ms.startWatch()
	return nil
}

func (ms *microServiceBR) startWatch() {
	go func() {
		ms.log.Info("Start watching backup pod")

		var lastPod *v1.Pod

	watchLoop:
		for {
			select {
			case <-ms.ctx.Done():
				ms.log.Warn("micro service BR quits without waiting job")
				break watchLoop
			case pod := <-ms.podCh:
				lastPod = pod
				break watchLoop
			case evt := <-ms.eventCh:
				if evt.Reason == EventReasonProgress {
					ms.callbacks.OnProgress(ms.ctx, ms.namespace, ms.taskName, getProgressFromMessage(evt.Message, ms.log))
				} else {
					ms.log.Infof("Received event for data mover %s.[reason %s, message %s]", ms.taskName, evt.Reason, evt.Message)
				}
			}
		}

		ms.log.Info("Finish waiting backup pod, got lastPod %v", lastPod != nil)

		previousLog := false
		if lastPod != nil && lastPod.Status.Phase == v1.PodFailed && lastPod.Status.Message != ErrCancelled {
			previousLog = true
		}

		ms.log.Info("Recording data mover logs, include previous %v", previousLog)

		if err := ms.redirectDataMoverLogs(previousLog); err != nil {
			ms.log.WithError(err).Warn("Failed to collect data mover logs")
		}

		if lastPod == nil {
			return
		}

	terminateLoop:
		for {
			select {
			case <-time.After(time.Second * 2):
				ms.log.Warn("Failed to wait quit event after pod terminates, get result from pod")

				if lastPod.Status.Phase == v1.PodSucceeded {
					ms.callbacks.OnCompleted(ms.ctx, ms.namespace, ms.taskName, getResultFromMessage(ms.taskType, lastPod.Status.Message, ms.log))
				} else if lastPod.Status.Message == ErrCancelled {
					ms.callbacks.OnCancelled(ms.ctx, ms.namespace, ms.taskName)
				} else {
					ms.callbacks.OnFailed(ms.ctx, ms.namespace, ms.taskName, errors.New(lastPod.Status.Message))
				}

				ms.log.Infof("Complete callback on pod termination message %s", lastPod.Status.Message)
				break terminateLoop
			case evt := <-ms.eventCh:
				terminiated := true
				switch evt.Reason {
				case EventReasonCompleted:
					ms.callbacks.OnCompleted(ms.ctx, ms.namespace, ms.taskName, getResultFromMessage(ms.taskType, evt.Message, ms.log))
				case EventReasonFailed:
					ms.callbacks.OnFailed(ms.ctx, ms.namespace, ms.taskName, errors.New(evt.Message))
				case EventReasonCancelled:
					ms.callbacks.OnCancelled(ms.ctx, ms.namespace, ms.taskName)
				default:
					terminiated = false
				}

				if terminiated {
					ms.log.Infof("Complete callback on event message %s", evt.Message)
					break terminateLoop
				}
			}
		}
	}()
}

func getResultFromMessage(taskType string, message string, logger logrus.FieldLogger) Result {
	if taskType == TaskTypeBackup {
		backupResult := BackupResult{}
		err := json.Unmarshal([]byte(message), &backupResult)
		if err != nil {
			logger.WithError(err).Errorf("Failed to unmarshal result message %s", message)
		}

		return Result{Backup: backupResult}
	} else {
		restoreResult := RestoreResult{}
		err := json.Unmarshal([]byte(message), &restoreResult)
		if err != nil {
			logger.WithError(err).Errorf("Failed to unmarshal result message %s", message)
		}

		return Result{Restore: restoreResult}
	}
}

func getProgressFromMessage(message string, logger logrus.FieldLogger) *uploader.Progress {
	progress := &uploader.Progress{}
	err := json.Unmarshal([]byte(message), progress)
	if err != nil {
		logger.WithError(err).Debugf("Failed to unmarshal progress message %s", message)
	}

	return progress
}

func (ms *microServiceBR) Cancel() {
	ms.log.WithField("taskType", ms.taskType).WithField("taskName", ms.taskName).Info("MicroServiceBR is canceled")
}

func (ms *microServiceBR) redirectDataMoverLogs(includePrevious bool) error {
	ms.log.Infof("Starting to collect data mover pod log for %s", ms.taskName)

	logFileName := os.TempDir()

	logFile, err := os.CreateTemp(logFileName, "")
	if err != nil {
		return errors.Wrapf(err, "error to create temp file for data mover pod log under %s", logFileName)
	}

	defer logFile.Close()

	logFileName = filepath.Join(logFileName, logFile.Name())

	ms.log.Info("Created log file %s", logFileName)

	err = kube.CollectPodLogs(ms.ctx, ms.kubeClient.CoreV1(), ms.taskName, ms.namespace, ms.taskName, includePrevious, logFile)
	if err != nil {
		return errors.Wrapf(err, "error to collect logs to %s for data mover pod %s", logFileName, ms.taskName)
	}

	ms.log.Info("Redirecting to log file %s", logFileName)

	logger := ms.log.WithField(logging.LogSourceKey, logFileName)
	logger.Logln(logging.ListeningLevel, logging.ListeningMessage)

	ms.log.Infof("Completed to collect data mover pod log for %s", ms.taskName)

	return nil
}
