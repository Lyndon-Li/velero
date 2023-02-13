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
	"io/ioutil"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/internal/credentials"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/repository"
	"github.com/vmware-tanzu/velero/pkg/uploader"
	"github.com/vmware-tanzu/velero/pkg/uploader/provider"

	repokey "github.com/vmware-tanzu/velero/pkg/repository/keys"
)

// For unit test to mock function
var NewUploaderProviderFunc = provider.NewUploaderProvider

type SnapshotBackupOutput struct {
	SnapshotID string `json:"snapshotID"`
	Error      string `json:"error"`
	Message    string `json:"message"`
	ExitCode   int32  `json:"exitCode"`
}

type SnapshotBackup struct {
	Ctx               context.Context
	Client            client.Client
	CredentialGetter  *credentials.CredentialGetter
	Log               logrus.FieldLogger
	RepositoryEnsurer *repository.RepositoryEnsurer
}

type SnapshotBackupProgressUpdater struct {
	SnapshotBackup *velerov1api.SnapshotBackup
	Log            logrus.FieldLogger
	Ctx            context.Context
	Cli            client.Client
}

func NewSnapshotBackup(ctx context.Context, client client.Client, getter *credentials.CredentialGetter, log logrus.FieldLogger) *SnapshotBackup {
	return &SnapshotBackup{
		Ctx:               ctx,
		Client:            client,
		CredentialGetter:  getter,
		Log:               log,
		RepositoryEnsurer: repository.NewRepositoryEnsurer(client, log),
	}
}

func (s *SnapshotBackup) Run(snapshotBackupName string, namespace string) {
	ctx := s.Ctx
	cancelCtx, cancel := context.WithCancel(s.Ctx)

	defer func() {
		cancel()
	}()

	log := s.Log.WithFields(logrus.Fields{
		"snapshotbackup": snapshotBackupName,
	})

	ssb := &velerov1api.SnapshotBackup{}
	if err := s.Client.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      snapshotBackupName,
	}, ssb); err != nil {
		s.errorOut(ssb, err, "error getting snapshot backup", log)
		return
	}

	backupLocation := &velerov1api.BackupStorageLocation{}
	if err := s.Client.Get(ctx, client.ObjectKey{
		Namespace: ssb.Namespace,
		Name:      ssb.Spec.BackupStorageLocation,
	}, backupLocation); err != nil {
		s.errorOut(ssb, err, "error getting backup storage location", log)
		return
	}

	backupRepo, err := s.RepositoryEnsurer.EnsureRepo(ctx, ssb.Namespace, ssb.Spec.SourceNamespace, ssb.Spec.BackupStorageLocation, GetUploaderType(ssb.Spec.DataMover))
	if err != nil {
		s.errorOut(ssb, err, "error ensure backup repository", log)
		return
	}

	var uploaderProv provider.Provider
	uploaderProv, err = NewUploaderProviderFunc(ctx, s.Client, GetUploaderType(ssb.Spec.DataMover), "",
		backupLocation, backupRepo, s.CredentialGetter, repokey.RepoKeySelector(), log)
	if err != nil {
		s.errorOut(ssb, err, "error creating uploader", log)
		return
	}

	// If this is a PVC, look for the most recent completed pod volume backup for it and get
	// its snapshot ID to do new backup based on it. Without this,
	// if the pod using the PVC (and therefore the directory path under /host_pods/) has
	// changed since the PVC's last backup, for backup, it will not be able to identify a suitable
	// parent snapshot to use, and will have to do a full rescan of the contents of the PVC.
	var parentSnapshotID string
	if pvcUID, ok := ssb.Labels[velerov1api.PVCUIDLabel]; ok {
		parentSnapshotID = s.getParentSnapshot(ctx, s.Log, pvcUID, ssb)
		if parentSnapshotID == "" {
			log.Info("No parent snapshot found for PVC, not based on parent snapshot for this backup")
		} else {
			log.WithField("parentSnapshotID", parentSnapshotID).Info("Based on parent snapshot for this backup")
		}
	}

	defer func() {
		if err := uploaderProv.Close(ctx); err != nil {
			log.Errorf("failed to close uploader provider with error %v", err)
		}
	}()

	onCtrlC(cancel)

	snapshotID, emptySnapshot, err := uploaderProv.RunBackup(cancelCtx, GetPodMountPath(), ssb.Spec.Tags, parentSnapshotID, s.NewSnapshotBackupProgressUpdater(ssb, log, ctx))
	if err != nil {
		s.errorOut(ssb, err, "error running backup", log)
		return
	}

	err = s.complete(ssb, snapshotID, emptySnapshot)
	if err != nil {
		log.WithError(err).Error("failed to complete snapshot backup")
	}
}

// getParentSnapshot finds the most recent completed PodVolumeBackup for the
// specified PVC and returns its snapshot ID. Any errors encountered are
// logged but not returned since they do not prevent a backup from proceeding.
func (s *SnapshotBackup) getParentSnapshot(ctx context.Context, log logrus.FieldLogger, pvcUID string, ssb *velerov1api.SnapshotBackup) string {
	log = log.WithField("pvcUID", pvcUID)
	log.Infof("Looking for most recent completed SnapshotBackup for this PVC")

	listOpts := &client.ListOptions{
		Namespace: ssb.Namespace,
	}
	matchingLabels := client.MatchingLabels(map[string]string{velerov1api.PVCUIDLabel: pvcUID})
	matchingLabels.ApplyToList(listOpts)

	var ssbList velerov1api.SnapshotBackupList
	if err := s.Client.List(ctx, &ssbList, listOpts); err != nil {
		log.WithError(errors.WithStack(err)).Error("getting list of SnapshotBackups for this PVC")
	}

	// Go through all the podvolumebackups for the PVC and look for the most
	// recent completed one to use as the parent.
	var mostRecentSSB velerov1api.SnapshotBackup
	for _, ssbItem := range ssbList.Items {
		if GetUploaderType(ssbItem.Spec.DataMover) != GetUploaderType(ssb.Spec.DataMover) {
			continue
		}
		if ssbItem.Status.Phase != velerov1api.SnapshotBackupPhaseCompleted {
			continue
		}

		if ssbItem.Spec.BackupStorageLocation != ssb.Spec.BackupStorageLocation {
			// Check the backup storage location is the same as spec in order to
			// support backup to multiple backup-locations. Otherwise, there exists
			// a case that backup volume snapshot to the second location would
			// failed, since the founded parent ID is only valid for the first
			// backup location, not the second backup location. Also, the second
			// backup should not use the first backup parent ID since its for the
			// first backup location only.
			continue
		}

		if mostRecentSSB.Status == (velerov1api.SnapshotBackupStatus{}) || ssb.Status.StartTimestamp.After(mostRecentSSB.Status.StartTimestamp.Time) {
			mostRecentSSB = ssbItem
		}
	}

	if mostRecentSSB.Status == (velerov1api.SnapshotBackupStatus{}) {
		log.Info("No completed SnapshotBackup found for PVC")
		return ""
	}

	log.WithFields(map[string]interface{}{
		"parentSnapshotBackup": mostRecentSSB.Name,
		"parentSnapshotID":     mostRecentSSB.Status.SnapshotID,
	}).Info("Found most recent completed SnapshotBackup for PVC")

	return mostRecentSSB.Status.SnapshotID
}

func (s *SnapshotBackup) NewSnapshotBackupProgressUpdater(ssb *velerov1api.SnapshotBackup, log logrus.FieldLogger, ctx context.Context) *SnapshotBackupProgressUpdater {
	return &SnapshotBackupProgressUpdater{ssb, log, ctx, s.Client}
}

//UpdateProgress which implement ProgressUpdater interface to update snapshot backup progress status
func (s *SnapshotBackupProgressUpdater) UpdateProgress(p *uploader.UploaderProgress) {
	original := s.SnapshotBackup.DeepCopy()
	s.SnapshotBackup.Status.Progress = velerov1api.DataMoveOperationProgress{TotalBytes: p.TotalBytes, BytesDone: p.BytesDone}
	if s.Cli == nil {
		s.Log.Errorf("failed to update snapshot backup progress with uninitailize client")
		return
	}
	if err := s.Cli.Patch(s.Ctx, s.SnapshotBackup, client.MergeFrom(original)); err != nil {
		s.Log.WithError(err).Errorf("update backup snapshot progress")
	}
}

func (s *SnapshotBackup) errorOut(ssb *velerov1api.SnapshotBackup, err error, msg string, log logrus.FieldLogger) {
	original := ssb.DeepCopy()
	ssb.Status.Message = errors.WithMessage(err, msg).Error()

	if err := s.Client.Patch(s.Ctx, ssb, client.MergeFrom(original)); err != nil {
		s.Log.WithError(err).Errorf("Failed to update snapshot backup error message")
	}
}

func (s *SnapshotBackup) complete(ssb *velerov1api.SnapshotBackup, snapshotID string, emptySnapshot bool) error {
	original := ssb.DeepCopy()
	ssb.Status.SnapshotID = snapshotID
	if emptySnapshot {
		ssb.Status.Message = "volume was empty so no snapshot was taken"
	}

	if err := s.Client.Patch(s.Ctx, ssb, client.MergeFrom(original)); err != nil {
		return errors.Wrap(err, "failed to update snapshot backup status on complete")
	}

	return nil
}

func writeSnapshotOutput(output *SnapshotBackupOutput) error {
	jsonByte, err := json.Marshal(&output)
	if err != nil {
		return errors.Wrap(err, "error to marshal output")
	}

	outputFile := "/dev/termination-log"

	if err := ioutil.WriteFile(outputFile, jsonByte, 0644); err != nil { //nolint:gosec
		return errors.Wrap(err, "error to write output file")
	}

	return nil
}
