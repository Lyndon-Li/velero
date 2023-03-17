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

// SnapshotBackupSpec is the specification for a SnapshotBackup.
type SnapshotBackupSpec struct {
	// SnapshotType is the type of the snapshot to be backed up.
	SnapshotType SnapshotType `json:"snapshotType"`

	// If SnapshotType is CSI, CSISnapshot provides the information of the CSI snapshot.
	// +optional
	// +nullable
	CSISnapshot *CSISnapshotSpec `json:"csiSnapshot"`

	// SourcePVC is the name of the PVC which the snapshot is taken for.
	SourcePVC string `json:"sourcePVC"`

	// DataMover specifies the data mover to be used by the backup.
	// If DataMover is "" or "velero", the built-in data mover will be used.
	// +optional
	DataMover string `json:"datamover,omitempty"`

	// BackupStorageLocation is the name of the backup storage location
	// where the backup repository is stored.
	BackupStorageLocation string `json:"backupStorageLocation"`

	// SourceNamespace is the original namespace where the volume is backed up from.
	// It is the same namespace for SourcePVC and CSI namespaced objects.
	SourceNamespace string `json:"sourceNamespace"`

	// DataMoverConfig is for data-mover-specific configuration fields.
	// +optional
	DataMoverConfig map[string]string `json:"dataMoverConfig,omitempty"`

	// Cancel indicates request to cancel the ongoing snapshot backup. It can be set
	// when the snapshot backup is in InProgress phase
	Cancel bool `json:"cancel,omitempty"`

	// OperationTimeout specifies the time used to wait internal operations,
	// before returning error as timeout.
	OperationTimeout metav1.Duration `json:"operationTimeout"`
}

type SnapshotType string

const (
	SnapshotTypeCSI SnapshotType = "CSI"
)

// CSISnapshotSpec is the specification for a CSI snapshot.
type CSISnapshotSpec struct {
	// VolumeSnapshot is the name of the volume snapshot to be backed up
	VolumeSnapshot string `json:"volumeSnapshot"`

	// StorageClass is the name of the storage class of the PVC that the volume snapshot is created from
	StorageClass string `json:"storageClass"`
}

// SnapshotBackupPhase represents the lifecycle phase of a SnapshotBackup.
// +kubebuilder:validation:Enum=New;Accepted;Prepared;InProgress;Canceling;Canceled;Completed;Failed
type SnapshotBackupPhase string

const (
	SnapshotBackupPhaseNew        SnapshotBackupPhase = "New"
	SnapshotBackupPhaseAccepted   SnapshotBackupPhase = "Accepted"
	SnapshotBackupPhasePrepared   SnapshotBackupPhase = "Prepared"
	SnapshotBackupPhaseInProgress SnapshotBackupPhase = "InProgress"
	SnapshotBackupPhaseCanceling  SnapshotBackupPhase = "Canceling"
	SnapshotBackupPhaseCanceled   SnapshotBackupPhase = "Canceled"
	SnapshotBackupPhaseCompleted  SnapshotBackupPhase = "Completed"
	SnapshotBackupPhaseFailed     SnapshotBackupPhase = "Failed"
)

// SnapshotBackupStatus is the current status of a SnapshotBackup.
type SnapshotBackupStatus struct {
	// Phase is the current state of the SnapshotBackup.
	// +optional
	Phase SnapshotBackupPhase `json:"phase,omitempty"`

	// Path is the full path of the snapshot volume being backed up.
	// +optional
	Path string `json:"path,omitempty"`

	// SnapshotID is the identifier for the snapshot in the backup repository.
	// +optional
	SnapshotID string `json:"snapshotID,omitempty"`

	// Message is a message about the snapshot backup's status.
	// +optional
	Message string `json:"message,omitempty"`

	// StartTimestamp records the time a backup was started.
	// Separate from CreationTimestamp, since that value changes
	// on restores.
	// The server's time is used for StartTimestamps
	// +optional
	// +nullable
	StartTimestamp *metav1.Time `json:"startTimestamp,omitempty"`

	// CompletionTimestamp records the time a backup was completed.
	// Completion time is recorded even on failed backups.
	// Completion time is recorded before uploading the backup object.
	// The server's time is used for CompletionTimestamps
	// +optional
	// +nullable
	CompletionTimestamp *metav1.Time `json:"completionTimestamp,omitempty"`

	// Progress holds the total number of bytes of the volume and the current
	// number of backed up bytes. This can be used to display progress information
	// about the backup operation.
	// +optional
	Progress DataMoveOperationProgress `json:"progress,omitempty"`
}

// TODO(2.0) After converting all resources to use the runttime-controller client,
// the genclient and k8s:deepcopy markers will no longer be needed and should be removed.
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.phase",description="SnapshotBackup status such as New/InProgress"
// +kubebuilder:printcolumn:name="Created",type="date",JSONPath=".status.startTimestamp",description="Time when this SnapshotBackup was started"
// +kubebuilder:printcolumn:name="Data Mover",type="string",JSONPath=".spec.dataMover",description="Name of the data mover"
// +kubebuilder:printcolumn:name="Bytes Done",type="integer",format="int64",JSONPath=".status.progress.bytesDone",description="Completed bytes"
// +kubebuilder:printcolumn:name="Bytes Total",type="integer",format="int64",JSONPath=".status.progress.totalBytes",description="Total bytes"
// +kubebuilder:printcolumn:name="Storage Location",type="string",JSONPath=".spec.backupStorageLocation",description="Name of the Backup Storage Location where this backup should be stored"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:object:root=true
// +kubebuilder:object:generate=true

type SnapshotBackup struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +optional
	Spec SnapshotBackupSpec `json:"spec,omitempty"`

	// +optional
	Status SnapshotBackupStatus `json:"status,omitempty"`
}

// TODO(2.0) After converting all resources to use the runtime-controller client,
// the k8s:deepcopy marker will no longer be needed and should be removed.
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:rbac:groups=velero.io,resources=snapshotbackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=velero.io,resources=snapshotbackups/status,verbs=get;update;patch

// SnapshotBackupList is a list of SnapshotBackups.
type SnapshotBackupList struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []SnapshotBackup `json:"items"`
}
