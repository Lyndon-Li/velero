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
	"maps"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/vmware-tanzu/velero/pkg/cbtservice"
	"github.com/vmware-tanzu/velero/pkg/repository/udmrepo"
	"github.com/vmware-tanzu/velero/pkg/uploader"
	"github.com/vmware-tanzu/velero/pkg/uploader/cbt"
)

type parentBackupInfo struct {
	parentObject udmrepo.ID
	changeID     string
}

// Backup backup specific sourcePath and update progress
func Backup(ctx context.Context, blkup Uploader, repoWriter udmrepo.BackupRepo, sourcePath string, realSource string, cbtSource cbtservice.SourceInfo,
	parentSnapshot string, cbtservice cbtservice.Service, uploaderCfg map[string]string, tags map[string]string, log logrus.FieldLogger) (uploader.SnapshotInfo, bool, error) {
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

	snapID, backupSize, err := SnapshotSource(ctx, repoWriter, blkup, sourceInfo, parentSnapshot, cbtSource, cbtservice, tags, uploaderCfg, log, "Block Uploader")
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
	parentSnapshot string,
	cbtSource cbtservice.SourceInfo,
	cbtservice cbtservice.Service,
	snapshotTags map[string]string,
	uploaderCfg map[string]string,
	log logrus.FieldLogger,
	description string,
) (string, int64, error) {
	log.Info("Start to snapshot...")
	snapshotStartTime := time.Now()

	parentBackup := getParentBackupInfo(ctx, rep, parentSnapshot, cbtSource.VolumeID, log)

	bitmap := cbt.NewBitmap(blockSize, source.size, cbtSource.Snapshot, parentBackup.changeID)

	err := cbt.SetBitmapOrFull(ctx, cbtservice, bitmap)
	if err != nil {
		parentBackup.parentObject = ""
		log.WithError(err).Warnf("Failed to create CBT with source %v, fallback to full backup", cbtSource)
	}

	snap, backupSize, err := u.Backup(source, parentBackup.parentObject, bitmap.Iterator(), uploaderCfg)
	if err != nil {
		return "", 0, errors.Wrapf(err, "Failed to run uploader backup for si %v", source)
	}

	snap.Tags = make(map[string]string)
	snap.Tags[uploader.CBTChangeIDTag] = cbtSource.ChangeID
	snap.Tags[uploader.CBTVolumeIDTag] = cbtSource.VolumeID
	if snapshotTags != nil {
		maps.Copy(snap.Tags, snapshotTags)
	}

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

func getParentBackupInfo(ctx context.Context, rep udmrepo.BackupRepo, parentSnapshot string, volumeID string, log logrus.FieldLogger) parentBackupInfo {
	var previous *udmrepo.Snapshot
	if parentSnapshot != "" {
		snap, err := rep.GetSnapshot(ctx, udmrepo.ID(parentSnapshot))
		if err != nil {
			log.WithError(err).Warn("Failed to load previous snapshot, fallback to full backup")
		} else {
			previous = &snap
			log.Infof("Using provided parent snapshot %s", parentSnapshot)
		}
	} else {
		log.Info("No parent snapshot, running full snapshot")
	}

	parentInfo := parentBackupInfo{}
	if previous != nil {
		if previous.Tags == nil {
			log.Warnf("No tag from parent snapshot %s, fallback to full backup", parentSnapshot)
		} else if previous.Tags[uploader.CBTChangeIDTag] == "" {
			log.Warnf("No ChangeID tag from parent snapshot %s, fallback to full backup", parentSnapshot)
		} else if previous.Tags[uploader.CBTVolumeIDTag] == "" {
			log.Warnf("No VolumeID tag from parent snapshot %s, fallback to full backup", parentSnapshot)
		} else if previous.Tags[uploader.CBTVolumeIDTag] != volumeID {
			log.Warnf("VolumeID %s from parent snapshot %s is not expected as %s, fallback to full backup", previous.Tags[uploader.CBTVolumeIDTag], parentSnapshot, volumeID)
		} else {
			parentInfo.parentObject = previous.RootObject
			parentInfo.changeID = previous.Tags[uploader.CBTChangeIDTag]

			log.Infof("Using parent snapshot %s, start time %v, end time %v, description %s", parentSnapshot, previous.StartTime, previous.EndTime, previous.Description)
		}
	}

	return parentInfo
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

func GetParentSnapshot(ctx context.Context, rep udmrepo.BackupRepo, sourcePath string, realSource string, parentSnapshot string, requestor string, log logrus.FieldLogger) (string, error) {
	source, err := filepath.Abs(sourcePath)
	if err != nil {
		return "", errors.Wrapf(err, "invalid source path '%s'", sourcePath)
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
			return "", errors.Wrapf(err, "error loading snapshot %v from kopia", parentSnapshot)
		}

		previous = snap
	} else {
		log.Infof("Searching for parent snapshot")

		mani, err := findPreviousSnapshot(ctx, rep, source, requestor, nil, log)
		if err != nil {
			return "", errors.Wrapf(err, "error finding previous snapshot for %s", source)
		}

		previous = mani
	}

	return string(previous.ID), nil
}
