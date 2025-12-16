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
	"io"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/vmware-tanzu/velero/pkg/repository/udmrepo"
	"github.com/vmware-tanzu/velero/pkg/uploader"
	"github.com/vmware-tanzu/velero/pkg/uploader/cbt"
)

// Backup backup specific sourcePath and update progress
func Backup(ctx context.Context, blkup Uploader, repoWriter udmrepo.BackupRepo, sourcePath string, realSource string,
	parentSnapshot *uploader.SnapshotInfo, cbt cbt.Iterator, uploaderCfg map[string]string, tags map[string]string, log logrus.FieldLogger) (uploader.SnapshotInfo, bool, error) {
	if blkup == nil {
		return uploader.SnapshotInfo{}, false, errors.New("get empty block uploader")
	}

	source, err := filepath.Abs(sourcePath)
	if err != nil {
		return uploader.SnapshotInfo{}, false, errors.Wrapf(err, "invalid source path %s", sourcePath)
	}

	source = filepath.Clean(source)

	sourceInfo := sourceInfo{
		realSource: filepath.Clean(realSource),
	}

	if realSource == "" {
		sourceInfo.realSource = source
	}

	sourceInfo.dev, err = openBlockDevice(source, true)
	if err != nil {
		return uploader.SnapshotInfo{}, false, errors.Wrapf(err, "error opening block device %s", source)
	}

	sourceInfo.size, err = sourceInfo.dev.Seek(0, io.SeekEnd)
	if err != nil {
		return uploader.SnapshotInfo{}, false, errors.Wrapf(err, "error getting length of block device %s", source)
	}

	sourceInfo.dev.Seek(0, io.SeekStart)
	if err != nil {
		return uploader.SnapshotInfo{}, false, errors.Wrapf(err, "error reset pos of block device %s", source)
	}

	snapID, backupSize, err := SnapshotSource(ctx, repoWriter, blkup, sourceInfo, parentSnapshot, cbt, tags, uploaderCfg, log, "Block Uploader")
	snapshotInfo := uploader.SnapshotInfo{
		ID:              snapID,
		Size:            sourceInfo.size,
		IncrementalSize: backupSize,
	}

	return snapshotInfo, false, err
}

func SnapshotSource(
	ctx context.Context,
	rep udmrepo.BackupRepo,
	u Uploader,
	source sourceInfo,
	parentSnapshot *uploader.SnapshotInfo,
	cbt cbt.Iterator,
	snapshotTags map[string]string,
	uploaderCfg map[string]string,
	log logrus.FieldLogger,
	description string,
) (string, int64, error) {
	log.Info("Start to snapshot...")
	snapshotStartTime := time.Now()

	var previous *udmrepo.Snapshot
	if parentSnapshot != nil {
		snap, err := loadParentSnapshot(ctx, rep, parentSnapshot)
		if err != nil {
			log.WithError(err).Warn("Failed to load previous snapshot, fallback to full backup")
		} else {
			previous = snap
			log.Infof("Using provided parent snapshot %s", parentSnapshot.ID)
		}
	} else {
		log.Info("Running full snapshot")
	}

	if previous != nil {
		log.Infof("Using parent snapshot %s, start time %v, end time %v, description %s", parentSnapshot.ID, previous.StartTime, previous.EndTime, previous.Description)
	}

	snap, backupSize, err := u.Backup(source, previous, cbt, uploaderCfg)
	if err != nil {
		return "", 0, errors.Wrapf(err, "Failed to run uploader backup for si %v", source)
	}

	snap.Tags = snapshotTags
	snap.Description = description

	snapID, err := rep.SaveSnapshot(ctx, snap)
	if err != nil {
		return "", 0, errors.Wrapf(err, "Failed to save snapshot %v", snap)
	}

	if err = rep.Flush(ctx); err != nil {
		return "", 0, errors.Wrapf(err, "Failed to flush repository")
	}

	log.Infof("Created snapshot with root %v and ID %v in %v", snap.RootObject, snapID, time.Since(snapshotStartTime).Truncate(time.Second))

	return string(snapID), backupSize, nil
}

func loadParentSnapshot(ctx context.Context, rep udmrepo.BackupRepo, parent *uploader.SnapshotInfo) (*udmrepo.Snapshot, error) {
	snap, err := rep.GetSnapshot(ctx, udmrepo.ID(parent.ID))
	if err != nil {
		return nil, errors.Wrapf(err, "error loading snapshot %s", parent.ID)
	}

	if snap.Tags == nil {
		return nil, errors.Errorf("no tags from snapshot %s", parent.ID)
	}

	if id, found := snap.Tags[uploader.SnapshotSourceIDTag]; !found {
		return nil, errors.Errorf("no snapshot source from snapshot %s", parent.ID)
	} else if parent.SourceID != id {
		return nil, errors.Errorf("source ID is not expected (%s vs. %s) from snapshot %s", parent.SourceID, id, parent.ID)
	}

	if id, found := snap.Tags[uploader.SnapshotChangeIDTag]; !found {
		return nil, errors.Errorf("no change ID from snapshot %s", parent.ID)
	} else if parent.ChangeID != id {
		return nil, errors.Errorf("change ID is not expected (%s vs. %s) from snapshot %s", parent.ChangeID, id, parent.ID)
	}

	return &snap, nil
}

// Restore restore specific sourcePath with given snapshotID and update progress
func Restore(ctx context.Context, blkup Uploader, rep udmrepo.BackupRepo, snapshotID, dest string, uploaderCfg map[string]string, log logrus.FieldLogger) (int64, error) {
	log.Info("Start to restore...")

	snapshot, err := rep.GetSnapshot(ctx, udmrepo.ID(snapshotID))
	if err != nil {
		return 0, errors.Wrapf(err, "Unable to load snapshot %v", snapshotID)
	}

	log.Infof("Restore from snapshot %s, description %s, created time %v, tags %v", snapshotID, snapshot.Description, snapshot.EndTime, snapshot.Tags)

	destPath, err := filepath.Abs(dest)
	if err != nil {
		return 0, errors.Wrapf(err, "invalid dest path '%s'", dest)
	}

	destPath = filepath.Clean(destPath)

	destDev, err := openBlockDevice(destPath, false)
	if err != nil {
		return 0, errors.Wrapf(err, "error opening block device '%s'", destPath)
	}

	size, err := blkup.Restore(snapshot, destInfo{dev: destDev, path: destPath}, uploaderCfg)
	if err != nil {
		return 0, errors.Wrapf(err, "error restoring to block dev %s", destPath)
	}

	return size, nil
}

func findPreviousSnapshot(ctx context.Context, rep udmrepo.BackupRepo, path string, requestor string, noLaterThan *time.Time, log logrus.FieldLogger) (udmrepo.Snapshot, error) {
	snaps, err := rep.ListSnapshot(ctx, path)
	if err != nil {
		return udmrepo.Snapshot{}, errors.Wrapf(err, "error list snapshots for %s", path)
	}

	var previous *udmrepo.Snapshot

	for _, snap := range snaps {
		log.Debugf("Found one snapshot %s, start time %v, tags %v", snap.ID, snap.StartTime, snap.Tags)

		requester, found := snap.Tags[uploader.SnapshotRequesterTag]
		if !found {
			continue
		}

		if requester != requestor {
			continue
		}

		uploaderName, found := snap.Tags[uploader.SnapshotUploaderTag]
		if !found {
			continue
		}

		if uploaderName != uploader.BlockType {
			continue
		}

		if noLaterThan != nil && snap.StartTime.After(*noLaterThan) {
			continue
		}

		if previous == nil || snap.StartTime.After(previous.StartTime) {
			previous = &snap
		}
	}

	return *previous, nil
}

func GetParentSnapshot(ctx context.Context, rep udmrepo.BackupRepo, sourcePath string, realSource string, parentSnapshot string, requestor string, log logrus.FieldLogger) (*uploader.SnapshotInfo, error) {
	source, err := filepath.Abs(sourcePath)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid source path '%s'", sourcePath)
	}

	source = filepath.Clean(source)
	if realSource != "" {
		source = filepath.Clean(realSource)
	}

	var previous udmrepo.Snapshot
	if parentSnapshot != "" {
		log.Infof("Using provided parent snapshot %s", parentSnapshot)

		snap, err := rep.GetSnapshot(ctx, udmrepo.ID(parentSnapshot))
		if err != nil {
			return nil, errors.Wrapf(err, "error loading snapshot %v from kopia", parentSnapshot)
		}

		previous = snap
	} else {
		log.Infof("Searching for parent snapshot")

		mani, err := findPreviousSnapshot(ctx, rep, source, requestor, nil, log)
		if err != nil {
			return nil, errors.Wrapf(err, "error finding previous snapshot for %s", source)
		}

		previous = mani
	}

	info := &uploader.SnapshotInfo{
		ID: string(previous.ID),
	}

	if previous.Tags != nil {
		info.SourceID = previous.Tags[uploader.SnapshotSourceIDTag]
		info.ChangeID = previous.Tags[uploader.SnapshotChangeIDTag]
	}

	return info, nil
}
