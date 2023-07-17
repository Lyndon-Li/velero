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

package shared

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DataMoveOperationProgress represents the progress of a
// data movement operation

// +k8s:deepcopy-gen=true
type DataMoveOperationProgress struct {
	// StartTime records the time a data movement was started.
	// The server's time is used for StartTime
	// +optional
	// +nullable
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompleteTime records the time a data movement was completed.
	// The server's time is used for CompleteTime
	// +optional
	// +nullable
	CompleteTime *metav1.Time `json:"completeTime,omitempty"`

	// TotalBytes records the total bytes of data for a data movement
	// +optional
	TotalBytes int64 `json:"totalBytes,omitempty"`

	// BytesDone records the completed bytes of data for a data movement
	// +optional
	BytesDone int64 `json:"bytesDone,omitempty"`
}
