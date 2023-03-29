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
	// TargetVolume is the information of the target PVC and PV.
	TargetVolume TargetVolumeSpec `json:"targetVolume"`

	// BackupStorageLocation is the name of the backup storage location
	// where the backup repository is stored.
	BackupStorageLocation string `json:"backupStorageLocation"`

	// DataMover specifies the data mover to be used by the backup.
	// If DataMover is "" or "velero", the built-in data mover will be used.
	// +optional
	DataMover string `json:"datamover,omitempty"`

	// SnapshotID is the ID of the Velero backup snapshot to be restored from.
	SnapshotID string `json:"snapshotID"`

	// SourceNamespace is the original namespace where the volume is backed up from.
	// It may be different from SourcePVC's namespace if namespace is remapped during restore.
	SourceNamespace string `json:"sourceNamespace"`

	// DataMoverConfig is for data-mover-specific configuration fields.
	// +optional
	DataMoverConfig map[string]string `json:"dataMoverConfig,omitempty"`

	// Cancel indicates request to cancel the ongoing snapshot restore. It can be set
	// when the snapshot restore is in InProgress phase
	Cancel bool `json:"cancel,omitempty"`

	// OperationTimeout specifies the time used to wait internal operations,
	// before returning error as timeout.
	OperationTimeout metav1.Duration `json:"operationTimeout"`
}

// TargetPVCSpec is the specification for a target PVC.
type TargetVolumeSpec struct {
	// PVC is the name of the target PVC that is created by Velero restore
	PVC string `json:"pvc"`

	// PV is the name of the target PV that is created by Velero restore
	PV string `json:"pv"`

	// Namespace is the target namespace
	Namespace string `json:"namespace"`
}

// SnapshotRestorePhase represents the lifecycle phase of a SnapshotRestore.
// +kubebuilder:validation:Enum=New;Accepted;Prepared;InProgress;Canceling;Canceled;Completed;Failed
type SnapshotRestorePhase string

const (
	SnapshotRestorePhaseNew        SnapshotRestorePhase = "New"
	SnapshotRestorePhaseAccepted   SnapshotRestorePhase = "Accepted"
	SnapshotRestorePhasePrepared   SnapshotRestorePhase = "Prepared"
	SnapshotRestorePhaseInProgress SnapshotRestorePhase = "InProgress"
	SnapshotRestorePhaseCanceling  SnapshotRestorePhase = "Canceling"
	SnapshotRestorePhaseCanceled   SnapshotRestorePhase = "Canceled"
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
// +kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.phase",description="SnapshotRestore status such as New/InProgress"
// +kubebuilder:printcolumn:name="Started",type="date",JSONPath=".status.startTimestamp",description="Time duration since this SnapshotRestore was started"
// +kubebuilder:printcolumn:name="Bytes Done",type="integer",format="int64",JSONPath=".status.progress.bytesDone",description="Completed bytes"
// +kubebuilder:printcolumn:name="Total Bytes",type="integer",format="int64",JSONPath=".status.progress.totalBytes",description="Total bytes"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp",description="Time duration since this SnapshotRestore was created"

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
