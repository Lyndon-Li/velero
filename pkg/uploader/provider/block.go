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
	"sync/atomic"

	"github.com/kopia/kopia/snapshot/upload"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/vmware-tanzu/velero/internal/credentials"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	repokeys "github.com/vmware-tanzu/velero/pkg/repository/keys"
	"github.com/vmware-tanzu/velero/pkg/repository/udmrepo"
	"github.com/vmware-tanzu/velero/pkg/uploader"
)

// blockProvider recorded info related with blockProvider
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
	//repoUID which is used to generate kopia repository config with unique directory path
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

// RunBackup which will backup specific path and update backup progress
// return snapshotID, isEmptySnapshot, error
func (bp *blockProvider) RunBackup(
	ctx context.Context,
	path string,
	realSource string,
	tags map[string]string,
	forceFull bool,
	parentSnapshot string,
	volMode uploader.PersistentVolumeMode,
	uploaderCfg map[string]string,
	updater uploader.ProgressUpdater) (string, bool, int64, int64, error) {
	if updater == nil {
		return "", false, 0, 0, errors.New("Need to initial backup progress updater first")
	}

	if path == "" {
		return "", false, 0, 0, errors.New("path is empty")
	}

	log := bp.log.WithFields(logrus.Fields{
		"path":           path,
		"realSource":     realSource,
		"parentSnapshot": parentSnapshot,
	})

	quit := make(chan struct{})
	log.Info("Starting backup")
	go bp.checkContext(ctx, quit, nil, kpUploader)

	defer func() {
		close(quit)
	}()

	if tags == nil {
		tags = make(map[string]string)
	}
	tags[uploader.SnapshotRequesterTag] = bp.requestorType
	tags[uploader.SnapshotUploaderTag] = uploader.BlockType

	if realSource != "" {
		realSource = fmt.Sprintf("%s/%s/%s", bp.requestorType, uploader.BlockType, realSource)
	}

	if bp.bkRepo.GetAdvancedFeatures().MultiPartBackup {
		if uploaderCfg == nil {
			uploaderCfg = make(map[string]string)
		}
	}

	// which ensure that the statistic data of TotalBytes equal to BytesDone when finished
	updater.UpdateProgress(
		&uploader.Progress{
			TotalBytes: snapshotInfo.Size,
			BytesDone:  snapshotInfo.Size,
		},
	)

	log.Debugf("Kopia backup finished, snapshot ID %s, backup size %d", snapshotInfo.ID, snapshotInfo.Size)
	return snapshotInfo.ID, false, snapshotInfo.Size, progress.GetIncrementalSize(), nil
}

// RunRestore which will restore specific path and update restore progress
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

	restoreCancel := make(chan struct{})
	quit := make(chan struct{})

	log.Info("Starting restore")
	defer func() {
		close(quit)
	}()

	go bp.checkContext(ctx, quit, restoreCancel, nil)

	// which ensure that the statistic data of TotalBytes equal to BytesDone when finished
	updater.UpdateProgress(&uploader.Progress{
		TotalBytes: size,
		BytesDone:  size,
	})

	output := fmt.Sprintf("Kopia restore finished, restore size %d, file count %d", size, fileCount)

	log.Info(output)

	return size, nil
}

func (bp *blockProvider) checkContext(ctx context.Context, finishChan chan struct{}, restoreChan chan struct{}, uploader *upload.Uploader) {
	select {
	case <-finishChan:
		bp.log.Infof("Action finished")
		return
	case <-ctx.Done():
		atomic.StoreInt32(&bp.canceling, 1)

		if uploader != nil {
			uploader.Cancel()
			bp.log.Infof("Backup is been canceled")
		}
		if restoreChan != nil {
			close(restoreChan)
			bp.log.Infof("Restore is been canceled")
		}
		return
	}
}
