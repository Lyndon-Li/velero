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

package provider

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/vmware-tanzu/velero/internal/credentials"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	repokeys "github.com/vmware-tanzu/velero/pkg/repository/keys"
	"github.com/vmware-tanzu/velero/pkg/repository/udmrepo"
	"github.com/vmware-tanzu/velero/pkg/uploader"
	"github.com/vmware-tanzu/velero/pkg/uploader/block"
	"github.com/vmware-tanzu/velero/pkg/uploader/cbt"
)

var blockBackupFunc = block.Backup
var blockRestoreFunc = block.Restore
var blockGetParentSnapshotFunc = block.GetParentSnapshot

type blockProvider struct {
	requestorType string
	bkRepo        udmrepo.BackupRepo
	credGetter    *credentials.CredentialGetter
	log           logrus.FieldLogger
	canceling     int32
}

// NewBlockUploaderProvider initialized with open or create a repository
func NewBlockUploaderProvider(
	requestorType string,
	ctx context.Context,
	credGetter *credentials.CredentialGetter,
	backupRepo *velerov1api.BackupRepository,
	log logrus.FieldLogger,
) (Provider, error) {
	bp := &blockProvider{
		requestorType: requestorType,
		log:           log,
		credGetter:    credGetter,
	}

	repoUID := string(backupRepo.GetUID())
	repoOpt, err := udmrepo.NewRepoOptions(
		udmrepo.WithPassword(bp, ""),
		udmrepo.WithConfigFile("", repoUID),
		udmrepo.WithDescription("Initial velero block uploader provider"),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "error to get repo options")
	}

	repoSvc := BackupRepoServiceCreateFunc(backupRepo.Spec.RepositoryType, log)
	log.WithField("repoUID", repoUID).Info("Opening backup repo")

	bp.bkRepo, err = repoSvc.Open(ctx, *repoOpt)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to find backup repository")
	}

	return bp, nil
}

func (bp *blockProvider) Close(ctx context.Context) error {
	return bp.bkRepo.Close(ctx)
}

func (bp *blockProvider) GetPassword(param any) (string, error) {
	if bp.credGetter.FromSecret == nil {
		return "", errors.New("invalid credentials interface")
	}
	rawPass, err := bp.credGetter.FromSecret.Get(repokeys.RepoKeySelector())
	if err != nil {
		return "", errors.Wrap(err, "error to get password")
	}

	return strings.TrimSpace(rawPass), nil
}

func (bp *blockProvider) RunBackup(
	ctx context.Context,
	path string,
	realSource string,
	tags map[string]string,
	parentSnapshot *uploader.SnapshotInfo,
	cbt cbt.Iterator,
	volMode uploader.PersistentVolumeMode,
	uploaderCfg map[string]string,
	updater uploader.ProgressUpdater) (uploader.SnapshotInfo, bool, error) {
	if updater == nil {
		return uploader.SnapshotInfo{}, false, errors.New("Need to initial backup progress updater first")
	}

	if path == "" {
		return uploader.SnapshotInfo{}, false, errors.New("path is empty")
	}

	if cbt != nil {
		if parentSnapshot == nil {
			return uploader.SnapshotInfo{}, false, errors.New("no parent snapshot but CBT is provided")
		}

		if parentSnapshot.SourceID != cbt.SourceID() {
			return uploader.SnapshotInfo{}, false, errors.New("cbt sourceID is not correct")
		}

		if cbt.ChangeID() == "" {
			return uploader.SnapshotInfo{}, false, errors.New("cbt changeID is empty")
		}
	} else if parentSnapshot != nil {
		return uploader.SnapshotInfo{}, false, errors.New("no CBT but parent snapshot is provided")
	}

	log := bp.log.WithFields(logrus.Fields{
		"path":           path,
		"realSource":     realSource,
		"parentSnapshot": parentSnapshot,
	})

	blkUploader := block.NewUploader(ctx, bp.bkRepo, updater, log)

	if tags == nil {
		tags = make(map[string]string)
	}
	tags[uploader.SnapshotRequesterTag] = bp.requestorType
	tags[uploader.SnapshotUploaderTag] = uploader.BlockType

	if realSource != "" {
		realSource = fmt.Sprintf("%s/%s/%s", bp.requestorType, uploader.BlockType, realSource)
	}

	snapshotInfo, _, err := blockBackupFunc(ctx, blkUploader, bp.bkRepo, path, realSource, parentSnapshot, cbt, uploaderCfg, tags, log)

	if err == block.ErrCanceled {
		log.Warn("Block backup is canceled")
		return snapshotInfo, false, ErrorCanceled
	}

	if err != nil {
		return snapshotInfo, false, errors.Wrapf(err, "Failed to run block backup")
	}

	updater.UpdateProgress(
		&uploader.Progress{
			TotalBytes: snapshotInfo.Size,
			BytesDone:  snapshotInfo.Size,
		},
	)

	log.Debugf("Block backup finished, snapshot ID %s, backup size %d", snapshotInfo.ID, snapshotInfo.Size)

	return snapshotInfo, false, nil
}

func (bp *blockProvider) RunRestore(
	ctx context.Context,
	snapshotID string,
	volumePath string,
	volMode uploader.PersistentVolumeMode,
	uploaderCfg map[string]string,
	updater uploader.ProgressUpdater) (int64, error) {
	log := bp.log.WithFields(logrus.Fields{
		"snapshotID": snapshotID,
		"volumePath": volumePath,
	})
	log.Info("Starting restore")

	blkUploader := block.NewUploader(ctx, bp.bkRepo, updater, log)

	size, err := block.Restore(ctx, blkUploader, bp.bkRepo, snapshotID, volumePath, uploaderCfg, log)

	if err == block.ErrCanceled {
		log.Warn("Block restore is canceled")
		return 0, ErrorCanceled
	}

	if err != nil {
		return 0, errors.Wrapf(err, "Failed to run block restore")
	}

	updater.UpdateProgress(&uploader.Progress{
		TotalBytes: size,
		BytesDone:  size,
	})

	log.Info("Block restore finished, restore size %d", size)

	return size, nil
}

func (bp *blockProvider) GetParentSnapshot(ctx context.Context, path string, realSource string, parentSnapshot string) (*uploader.SnapshotInfo, error) {
	if path == "" {
		return nil, errors.New("path is empty")
	}

	if realSource != "" {
		realSource = fmt.Sprintf("%s/%s/%s", bp.requestorType, uploader.KopiaType, realSource)
	}

	snapshotInfo, err := blockGetParentSnapshotFunc(ctx, bp.bkRepo, path, realSource, parentSnapshot, bp.requestorType, bp.log)
	if err != nil {
		return nil, errors.Wrapf(err, "error getting parent snapshot for path %s, realSource %s, snapshot ID %s", path, realSource, parentSnapshot)
	}

	return snapshotInfo, nil
}
