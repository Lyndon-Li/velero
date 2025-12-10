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

package block

import (
	"context"
	"path/filepath"
	"runtime"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/restore"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/vmware-tanzu/velero/pkg/kopia"
	"github.com/vmware-tanzu/velero/pkg/repository/udmrepo"
	"github.com/vmware-tanzu/velero/pkg/uploader"
)

// Backup backup specific sourcePath and update progress
func Backup(ctx context.Context, fsUploader SnapshotUploader, repoWriter repo.RepositoryWriter, sourcePath string, realSource string,
	forceFull bool, parentSnapshot string, volMode uploader.PersistentVolumeMode, uploaderCfg map[string]string, tags map[string]string, log logrus.FieldLogger) (*uploader.SnapshotInfo, bool, error) {
	if fsUploader == nil {
		return nil, false, errors.New("get empty kopia uploader")
	}
	source, err := filepath.Abs(sourcePath)
	if err != nil {
		return nil, false, errors.Wrapf(err, "Invalid source path '%s'", sourcePath)
	}

	source = filepath.Clean(source)

	sourceInfo := snapshot.SourceInfo{
		UserName: udmrepo.GetRepoUser(),
		Host:     udmrepo.GetRepoDomain(),
		Path:     filepath.Clean(realSource),
	}
	if realSource == "" {
		sourceInfo.Path = source
	}

	sourceEntry, err = getLocalBlockEntry(source)
	if err != nil {
		return nil, false, errors.Wrap(err, "unable to get local block device entry")
	}

	snapshotInfo := &uploader.SnapshotInfo{
		ID:   snapID,
		Size: snapshotSize,
	}

	return snapshotInfo, false, err
}

// Restore restore specific sourcePath with given snapshotID and update progress
func Restore(ctx context.Context, rep repo.RepositoryWriter, progress *Progress, snapshotID, dest string, volMode uploader.PersistentVolumeMode, uploaderCfg map[string]string,
	log logrus.FieldLogger, cancleCh chan struct{}) (int64, int32, error) {
	log.Info("Start to restore...")

	kopiaCtx := kopia.SetupKopiaLog(ctx, log)

	snapshot, err := snapshot.LoadSnapshot(kopiaCtx, rep, manifest.ID(snapshotID))
	if err != nil {
		return 0, 0, errors.Wrapf(err, "Unable to load snapshot %v", snapshotID)
	}

	log.Infof("Restore from snapshot %s, description %s, created time %v, tags %v", snapshotID, snapshot.Description, snapshot.EndTime.ToTime(), snapshot.Tags)

	rootEntry, err := filesystemEntryFunc(kopiaCtx, rep, snapshotID, false)
	if err != nil {
		return 0, 0, errors.Wrapf(err, "Unable to get filesystem entry for snapshot %v", snapshotID)
	}

	path, err := filepath.Abs(dest)
	if err != nil {
		return 0, 0, errors.Wrapf(err, "Unable to resolve path %v", dest)
	}

	fsOutput := &restore.FilesystemOutput{
		TargetPath:             path,
		OverwriteDirectories:   true,
		OverwriteFiles:         true,
		OverwriteSymlinks:      true,
		IgnorePermissionErrors: true,
	}

	restoreConcurrency := runtime.NumCPU()

	log.Debugf("Restore filesystem output %v, concurrency %d", fsOutput, restoreConcurrency)

	return stat.RestoredTotalFileSize, stat.RestoredFileCount, nil
}
