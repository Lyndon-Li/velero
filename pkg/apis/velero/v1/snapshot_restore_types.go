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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SnapshotRestoreSpec is the specification for a SnapshotRestore.
type SnapshotRestoreSpec struct {
	// TargetVolume is the information of the target PVC and PV.
	TargetVolume TargetVolumeSpec `json:"targetVolume"`

	// RestoreName is the name of the restore which owns this snapshot restore.
	RestoreName string `json:"restoreName"`

	// BackupName is the name of the backup for this snapshot restore.
	BackupName string `json:"backupName"`

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
}

// TargetPVCSpec is the specification for a target PVC.
type TargetVolumeSpec struct {
	// PVC is the name of the target PVC that is created by Velero restore
	PVC string `json:"pvc"`

	// PV is the name of the target PV that PVC binds to.
	// It is created by this snapshot restore.
	PV string `json:"pv"`

	// Namespace is the namespace of the target PVC
	Namespace string `json:"namespace"`

	// StorageClass is the name of the storage class used by the target PVC
	StorageClass string `json:"storageClass"`

	// Resources specify the resource requirements of the target PVC
	Resources corev1.ResourceRequirements `json:"resources"`

	// PVOperationTimeout specifies the time used to wait for PVC/PV operations,
	// before returning error as timeout.
	OperationTimeout metav1.Duration `json:"operationTimeout"`
}

// SnapshotRestorePhase represents the lifecycle phase of a SnapshotRestore.
// +kubebuilder:validation:Enum=New;Accepted;Prepared;InProgress;Completed;Failed
type SnapshotRestorePhase string

const (
	SnapshotRestorePhaseNew        SnapshotRestorePhase = "New"
	SnapshotRestorePhaseAccepted   SnapshotRestorePhase = "Accepted"
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
