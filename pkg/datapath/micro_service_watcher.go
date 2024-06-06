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
	"time"

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

	EventReasonStarted   = "Data-Path-Started"
	EventReasonCompleted = "Data-Path-Completed"
	EventReasonFailed    = "Data-Path-Failed"
	EventReasonCancelled = "Data-Path-Cancelled"
	EventReasonProgress  = "Data-Path-Progress"
)

type microServiceBRWatcher struct {
	ctx                 context.Context
	cancel              context.CancelFunc
	log                 logrus.FieldLogger
	client              client.Client
	kubeClient          kubernetes.Interface
	mgr                 manager.Manager
	namespace           string
	callbacks           Callbacks
	taskName            string
	taskType            string
	thisPod             string
	thisContainer       string
	thisVolume          string
	eventCh             chan *v1.Event
	podCh               chan *v1.Pod
	startedFromEvent    bool
	terminatedFromEvent bool
}

func newMicroServiceBRWatcher(client client.Client, kubeClient kubernetes.Interface, mgr manager.Manager, taskType string, taskName string, namespace string, res *exposer.ExposeResult, callbacks Callbacks, log logrus.FieldLogger) AsyncBR {
	ms := &microServiceBRWatcher{
		mgr:           mgr,
		client:        client,
		kubeClient:    kubeClient,
		namespace:     namespace,
		callbacks:     callbacks,
		taskType:      taskType,
		taskName:      taskName,
		thisPod:       res.ByPod.HostingPod.Name,
		thisContainer: res.ByPod.HostingContainer,
		thisVolume:    res.ByPod.VolumeName,
		eventCh:       make(chan *v1.Event, 10),
		podCh:         make(chan *v1.Pod, 2),
		log:           log,
	}

	return ms
}

func (ms *microServiceBRWatcher) Init(ctx context.Context, param interface{}) error {
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
				if evt.InvolvedObject.UID != thisPod.UID {
					return
				}

				ms.log.Infof("Pushed adding event %s/%s, message %s for object %v", evt.Namespace, evt.Name, evt.Message, evt.InvolvedObject)

				ms.eventCh <- evt
			},
			UpdateFunc: func(_, obj interface{}) {
				evt := obj.(*v1.Event)
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
	if err := ms.reEnsureThisPod(); err != nil {
		return err
	}

	ms.startWatch()

	return nil
}

func (ms *microServiceBRWatcher) StartRestore(snapshotID string, uploaderConfigs map[string]string) error {
	if err := ms.reEnsureThisPod(); err != nil {
		return err
	}

	ms.startWatch()

	return nil
}

func (ms *microServiceBRWatcher) reEnsureThisPod() error {
	thisPod := &v1.Pod{}
	if err := ms.client.Get(ms.ctx, types.NamespacedName{
		Namespace: ms.namespace,
		Name:      ms.thisPod,
	}, thisPod); err != nil {
		return errors.Wrapf(err, "error getting this pod %s", ms.thisPod)
	}

	if thisPod.Status.Phase == v1.PodSucceeded || thisPod.Status.Phase == v1.PodFailed {
		ms.podCh <- thisPod
		ms.log.WithField("this pod", ms.thisPod).Infof("This pod comes to terminital status %s before watch start", thisPod.Status.Phase)
	}

	return nil
}

func (ms *microServiceBRWatcher) startWatch() {
	go func() {
		ms.log.Info("Start watching data path pod")

		var lastPod *v1.Pod

	watchLoop:
		for {
			select {
			case <-ms.ctx.Done():
				break watchLoop
			case pod := <-ms.podCh:
				lastPod = pod
				break watchLoop
			case evt := <-ms.eventCh:
				ms.onEvent(evt)
			}
		}

		//ms.log.Infof("Watch loop ends, got lastPod %v", lastPod != nil)

		if lastPod == nil {
			ms.log.Warn("Data path pod watch loop is cancelled")
			return
		}

	epilogLoop:
		for ms.startedFromEvent && !ms.terminatedFromEvent {
			select {
			case <-time.After(time.Minute):
				break epilogLoop
			case evt := <-ms.eventCh:
				ms.onEvent(evt)
			}
		}

		if ms.startedFromEvent && !ms.terminatedFromEvent {
			ms.log.Warn("Data path pod started but termination event is not received")
		}

		terminateMessage := kube.GetPodTerminateMessage(lastPod, ms.thisContainer)

		ms.log.Info("Recording data path pod logs")

		if err := ms.redirectDataMoverLogs(); err != nil {
			ms.log.WithError(err).Warn("Failed to collect data mover logs")
		}

		ms.log.Infof("Finish waiting data path pod, got lastPod, phase %s, message %s", lastPod.Status.Phase, terminateMessage)

		if lastPod.Status.Phase == v1.PodSucceeded {
			ms.callbacks.OnCompleted(ms.ctx, ms.namespace, ms.taskName, getResultFromMessage(ms.taskType, terminateMessage, ms.log))
		} else {
			if terminateMessage == ErrCancelled {
				ms.callbacks.OnCancelled(ms.ctx, ms.namespace, ms.taskName)
			} else {
				ms.callbacks.OnFailed(ms.ctx, ms.namespace, ms.taskName, errors.New(terminateMessage))
			}
		}

		ms.log.Info("Complete callback on data path pod termination")
	}()
}

func (ms *microServiceBRWatcher) onEvent(evt *v1.Event) {
	switch evt.Reason {
	case EventReasonStarted:
		ms.startedFromEvent = true
		ms.log.Infof("Received data path start message %s", evt.Message)
	case EventReasonProgress:
		ms.callbacks.OnProgress(ms.ctx, ms.namespace, ms.taskName, getProgressFromMessage(evt.Message, ms.log))
	case EventReasonCompleted:
		ms.log.Infof("Received data path completed message %v", getResultFromMessage(ms.taskType, evt.Message, ms.log))
		ms.terminatedFromEvent = true
	case EventReasonCancelled:
		ms.log.Infof("Received data path cancelled message %s", evt.Message)
		ms.terminatedFromEvent = true
	case EventReasonFailed:
		ms.log.Infof("Received data path failed message %s", evt.Message)
		ms.terminatedFromEvent = true
	default:
		ms.log.Debugf("Received event for data mover %s.[reason %s, message %s]", ms.taskName, evt.Reason, evt.Message)
	}
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

func (ms *microServiceBRWatcher) redirectDataMoverLogs() error {
	ms.log.Infof("Starting to collect data mover pod log for %s", ms.thisPod)

	logFileDir := os.TempDir()
	logFile, err := os.CreateTemp(logFileDir, "")
	if err != nil {
		return errors.Wrapf(err, "error to create temp file for data mover pod log under %s", logFileDir)
	}

	defer logFile.Close()

	logFileName := logFile.Name()
	ms.log.Infof("Created log file %s", logFileName)

	err = kube.CollectPodLogs(ms.ctx, ms.kubeClient.CoreV1(), ms.thisPod, ms.namespace, ms.thisContainer, false, logFile)
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
