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
package kube

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
)

type EventRecorder struct {
	recorder record.EventRecorder
}

func NewEventRecorder(kubeClient kubernetes.Interface, scheme *runtime.Scheme, eventSource string, eventNode string) *EventRecorder {
	res := EventRecorder{}

	eventBroadcaster := record.NewBroadcasterWithCorrelatorOptions(record.CorrelatorOptions{
		MaxEvents: 1,
		MessageFunc: func(event *v1.Event) string {
			return event.Message
		},
	})

	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	res.recorder = eventBroadcaster.NewRecorder(scheme, v1.EventSource{
		Component: eventSource,
		Host:      eventNode,
	})

	return &res
}

func (er *EventRecorder) Event(object runtime.Object, warning bool, reason string, messagefmt string, a ...any) {
	eventType := v1.EventTypeNormal
	if warning {
		eventType = v1.EventTypeWarning
	}

	message := fmt.Sprintf(messagefmt, a...)

	er.recorder.Event(object, eventType, reason, message)
}
