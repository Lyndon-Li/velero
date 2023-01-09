/*
Copyright the Velero contributors.

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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SnapshotRestoreSpec is the specification for a SnapshotRestore.
type SnapshotRestoreSpec struct {
	// RestorePvc is the name of the PVC for volume to be restored
	RestorePvc string `json:"restorePvc"`

	// BackupStorageLocation is the name of the backup storage location
	// where the backup repository is stored.
	BackupStorageLocation string `json:"backupStorageLocation"`

	// RepoIdentifier is the backup repository identifier.
	RepoIdentifier string `json:"repoIdentifier"`

	// UploaderType is the type of the uploader to handle the data transfer.
	// +kubebuilder:validation:Enum=kopia;restic;""
	// +optional
	UploaderType string `json:"uploaderType"`

	// SnapshotID is the ID of the volume snapshot to be restored.
	SnapshotID string `json:"snapshotID"`

	// SourceNamespace is the original namespace for namaspace mapping.
	SourceNamespace string `json:"sourceNamespace"`
}

// SnapshotRestorePhase represents the lifecycle phase of a SnapshotRestore.
// +kubebuilder:validation:Enum=New;Prepared;InProgress;Completed;Failed
type SnapshotRestorePhase string

const (
	SnapshotRestorePhaseNew        SnapshotRestorePhase = "New"
	SnapshotRestorePhasePrepared   SnapshotRestorePhase = "Prepared"
	SnapshotRestorePhaseInProgress SnapshotRestorePhase = "InProgress"
	SnapshotRestorePhaseCompleted  SnapshotRestorePhase = "Completed"
	SnapshotRestorePhaseFailed     SnapshotRestorePhase = "Failed"
)

// SnapshotRestoreStatus is the current status of a SnapshotRestore.
type SnapshotRestoreStatus struct {
	// Phase is the current state of theSnapshotRestore.
	// +optional
	Phase SnapshotRestorePhase `json:"phase,omitempty"`

	// Message is a message about the snapshot restore's status.
	// +optional
	Message string `json:"message,omitempty"`

	// StartTimestamp records the time a restore was started.
	// The server's time is used for StartTimestamps
	// +optional
	// +nullable
	StartTimestamp *metav1.Time `json:"startTimestamp,omitempty"`

	// CompletionTimestamp records the time a restore was completed.
	// Completion time is recorded even on failed restores.
	// The server's time is used for CompletionTimestamps
	// +optional
	// +nullable
	CompletionTimestamp *metav1.Time `json:"completionTimestamp,omitempty"`

	// Progress holds the total number of bytes of the snapshot and the current
	// number of restored bytes. This can be used to display progress information
	// about the restore operation.
	// +optional
	Progress DataMoveOperationProgress `json:"progress,omitempty"`
}

// TODO(2.0) After converting all resources to use the runtime-controller client, the genclient and k8s:deepcopy markers will no longer be needed and should be removed.
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:generate=true
// +kubebuilder:object:root=true
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Uploader Type",type="string",JSONPath=".spec.uploaderType",description="The type of the uploader to handle data transfer"
// +kubebuilder:printcolumn:name="Volume",type="string",JSONPath=".spec.volume",description="Name of the volume to be restored"
// +kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.phase",description="Snapshot Restore status such as New/InProgress"
// +kubebuilder:printcolumn:name="TotalBytes",type="integer",format="int64",JSONPath=".status.progress.totalBytes",description="Snapshot Restore status such as New/InProgress"
// +kubebuilder:printcolumn:name="BytesDone",type="integer",format="int64",JSONPath=".status.progress.bytesDone",description="Snapshot Restore status such as New/InProgress"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

type SnapshotRestore struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +optional
	Spec SnapshotRestoreSpec `json:"spec,omitempty"`

	// +optional
	Status SnapshotRestoreStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:generate=true
// +kubebuilder:object:root=true

// SnapshotRestoreList is a list of SnapshotRestores.
type SnapshotRestoreList struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []SnapshotRestore `json:"items"`
}
