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

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/pkg/exposer"
	"github.com/vmware-tanzu/velero/pkg/uploader"
	"github.com/vmware-tanzu/velero/pkg/util/kube"

	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/vmware-tanzu/velero/pkg/util/logging"
)

const (
	TaskTypeBackup  = "backup"
	TaskTypeRestore = "restore"

	ErrCancelled = "data path is cancelled"

	//EventReasonCompleted = "Completed"
	//EventReasonFailed    = "Failed"
	//EventReasonCancelled = "Cancelled"
	EventReasonProgress = "Progress"
)

type microServiceBRWatcher struct {
	ctx           context.Context
	cancel        context.CancelFunc
	log           logrus.FieldLogger
	client        client.Client
	kubeClient    kubernetes.Interface
	mgr           manager.Manager
	namespace     string
	callbacks     Callbacks
	taskName      string
	taskType      string
	thisPod       string
	thisContainer string
	thisVolume    string
	eventCh       chan *v1.Event
	podCh         chan *v1.Pod
}

func newMicroServiceBRWatcher(client client.Client, kubeClient kubernetes.Interface, mgr manager.Manager, taskType string, taskName string, namespace string, callbacks Callbacks, log logrus.FieldLogger) AsyncBR {
	ms := &microServiceBRWatcher{
		mgr:        mgr,
		client:     client,
		kubeClient: kubeClient,
		namespace:  namespace,
		callbacks:  callbacks,
		taskType:   taskType,
		taskName:   taskName,
		eventCh:    make(chan *v1.Event),
		podCh:      make(chan *v1.Pod),
		log:        log,
	}

	return ms
}

func (ms *microServiceBRWatcher) Init(ctx context.Context, res *exposer.ExposeResult, param interface{}) error {
	ms.thisPod = res.ByPod.HostingPod.Name
	ms.thisContainer = res.ByPod.HostingContainer
	ms.thisVolume = res.ByPod.VolumeName

	ms.ctx, ms.cancel = context.WithCancel(ctx)

	thisPod := &v1.Pod{}
	if err := ms.client.Get(ms.ctx, types.NamespacedName{
		Namespace: ms.namespace,
		Name:      ms.thisPod,
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

	eventInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				evt := obj.(*v1.Event)

				ms.log.Infof("Received adding event %s/%s, message %s for object %v, count %v, type %s, reason %s, report controller %s, report instance %s", evt.Namespace, evt.Name,
					evt.Message, evt.InvolvedObject, evt.Count, evt.Type, evt.Reason, evt.ReportingController, evt.ReportingInstance)

				if evt.InvolvedObject.UID != thisPod.UID {
					return
				}

				ms.log.Infof("Pushed adding event %s/%s, message %s for object %v", evt.Namespace, evt.Name, evt.Message, evt.InvolvedObject)

				ms.eventCh <- evt
			},
			UpdateFunc: func(_, obj interface{}) {
				evt := obj.(*v1.Event)

				ms.log.Infof("Received updating event %s/%s, message %s for object %v", evt.Namespace, evt.Name, evt.Message, evt.InvolvedObject)

				if evt.InvolvedObject.UID != thisPod.UID {
					return
				}

				ms.log.Infof("Pushed updating event %s/%s, message %s for object %v", evt.Namespace, evt.Name, evt.Message, evt.InvolvedObject)

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

func (ms *microServiceBRWatcher) Close(ctx context.Context) {
	if ms.cancel != nil {
		ms.cancel()
		ms.cancel = nil
	}

	ms.log.WithField("taskType", ms.taskType).WithField("taskName", ms.taskName).Info("MicroServiceBR is closed")
}

func (ms *microServiceBRWatcher) StartBackup(uploaderConfig map[string]string, param interface{}) error {
	ms.startWatch()
	return nil
}

func (ms *microServiceBRWatcher) StartRestore(snapshotID string, uploaderConfigs map[string]string) error {
	ms.startWatch()
	return nil
}

func (ms *microServiceBRWatcher) startWatch() {
	go func() {
		ms.log.Info("Start watching backup pod")

		var lastPod *v1.Pod

	watchLoop:
		for {
			select {
			case <-ms.ctx.Done():
				ms.log.Warn("Micro service BR quits without waiting job")
				break watchLoop
			case pod := <-ms.podCh:
				lastPod = pod
				break watchLoop
			case evt := <-ms.eventCh:
				if evt.Reason == EventReasonProgress {
					ms.callbacks.OnProgress(ms.ctx, ms.namespace, ms.taskName, getProgressFromMessage(evt.Message, ms.log))
				} else {
					ms.log.Debugf("Received event for data mover %s.[reason %s, message %s]", ms.taskName, evt.Reason, evt.Message)
				}
			}
		}

		terminateMessage := kube.GetPodTerminateMessage(lastPod, ms.thisContainer)

		previousLog := false
		if lastPod != nil && lastPod.Status.Phase == v1.PodFailed && terminateMessage != ErrCancelled {
			previousLog = true
		}

		ms.log.Infof("Recording data mover logs, include previous %v", previousLog)

		if err := ms.redirectDataMoverLogs(previousLog); err != nil {
			ms.log.WithError(err).Warn("Failed to collect data mover logs")
		}

		if lastPod == nil {
			ms.log.Warn("Data mover watch loop is cancelled")
			return
		}

		ms.log.Infof("Finish waiting backup pod, got lastPod, phase %s, message %s", lastPod.Status.Phase, terminateMessage)

		if lastPod.Status.Phase == v1.PodSucceeded {
			ms.callbacks.OnCompleted(ms.ctx, ms.namespace, ms.taskName, getResultFromMessage(ms.taskType, terminateMessage, ms.log))
		} else {
			if terminateMessage == ErrCancelled {
				ms.callbacks.OnCancelled(ms.ctx, ms.namespace, ms.taskName)
			} else {
				ms.callbacks.OnFailed(ms.ctx, ms.namespace, ms.taskName, errors.New(terminateMessage))
			}
		}

		ms.log.Info("Complete callback on pod termination")
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

func (ms *microServiceBRWatcher) Cancel() {
	ms.log.WithField("taskType", ms.taskType).WithField("taskName", ms.taskName).Info("MicroServiceBR is canceled")
}

func (ms *microServiceBRWatcher) redirectDataMoverLogs(includePrevious bool) error {
	ms.log.Infof("Starting to collect data mover pod log for %s", ms.thisPod)

	logFileDir := os.TempDir()
	logFile, err := os.CreateTemp(logFileDir, "")
	if err != nil {
		return errors.Wrapf(err, "error to create temp file for data mover pod log under %s", logFileDir)
	}

	defer logFile.Close()

	logFileName := logFile.Name()
	ms.log.Infof("Created log file %s", logFileName)

	err = kube.CollectPodLogs(ms.ctx, ms.kubeClient.CoreV1(), ms.thisPod, ms.namespace, ms.thisContainer, includePrevious, logFile)
	if err != nil {
		return errors.Wrapf(err, "error to collect logs to %s for data mover pod %s", logFileName, ms.thisPod)
	}

	logFile.Close()

	ms.log.Infof("Redirecting to log file %s", logFileName)

	logger := ms.log.WithField(logging.LogSourceKey, logFileName)
	logger.Logln(logging.ListeningLevel, logging.ListeningMessage)

	ms.log.Infof("Completed to collect data mover pod log for %s", ms.thisPod)

	return nil
}
