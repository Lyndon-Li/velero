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

type SnapshotRestoreOutput struct {
	Error    string `json:"error"`
	Message  string `json:"message"`
	ExitCode int32  `json:"exitCode"`
}

type SnapshotRestore struct {
	Ctx               context.Context
	Client            client.Client
	CredentialGetter  *credentials.CredentialGetter
	Log               logrus.FieldLogger
	RepositoryEnsurer *repository.RepositoryEnsurer
}

type SnapshotRestoreProgressUpdater struct {
	SnapshotRestore *velerov1api.SnapshotRestore
	Log             logrus.FieldLogger
	Ctx             context.Context
	Cli             client.Client
}

func NewSnapshotRestore(ctx context.Context, client client.Client, getter *credentials.CredentialGetter, log logrus.FieldLogger) *SnapshotRestore {
	return &SnapshotRestore{
		Ctx:               ctx,
		Client:            client,
		CredentialGetter:  getter,
		Log:               log,
		RepositoryEnsurer: repository.NewRepositoryEnsurer(client, log),
	}
}

func (s *SnapshotRestore) Run(snapshotRestoreName string, namespace string) {
	ctx := s.Ctx
	cancelCtx, cancel := context.WithCancel(s.Ctx)

	defer func() {
		cancel()
	}()

	log := s.Log.WithFields(logrus.Fields{
		"snapshotrestore": snapshotRestoreName,
	})

	ssr := &velerov1api.SnapshotRestore{}
	if err := s.Client.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      snapshotRestoreName,
	}, ssr); err != nil {
		s.errorOut(ssr, err, "error getting snapshot restore", log)
		return
	}

	backupLocation := &velerov1api.BackupStorageLocation{}
	if err := s.Client.Get(ctx, client.ObjectKey{
		Namespace: ssr.Namespace,
		Name:      ssr.Spec.BackupStorageLocation,
	}, backupLocation); err != nil {
		s.errorOut(ssr, err, "error getting backup storage location", log)
		return
	}

	backupRepo, err := s.RepositoryEnsurer.EnsureRepo(ctx, ssr.Namespace, ssr.Spec.SourceNamespace, ssr.Spec.BackupStorageLocation, GetUploaderType(ssr.Spec.DataMover))
	if err != nil {
		s.errorOut(ssr, err, "error ensure backup repository", log)
		return
	}

	var uploaderProv provider.Provider
	uploaderProv, err = provider.NewUploaderProvider(ctx, s.Client, GetUploaderType(ssr.Spec.DataMover),
		"", backupLocation, backupRepo, s.CredentialGetter, repokey.RepoKeySelector(), log)
	if err != nil {
		s.errorOut(ssr, err, "error creating uploader", log)
		return
	}

	defer func() {
		if err := uploaderProv.Close(ctx); err != nil {
			log.Errorf("failed to close uploader provider with error %v", err)
		}
	}()

	onCtrlC(cancel)

	if err = uploaderProv.RunRestore(cancelCtx, ssr.Spec.SnapshotID, GetPodMountPath(), s.NewSnapshotRestoreProgressUpdater(ssr, log, ctx)); err != nil {
		s.errorOut(ssr, err, "error running restore", log)
		return
	}

	err = s.complete(ssr)
	if err != nil {
		log.WithError(err).Error("failed to complete snapshot restore")
	}
}

func (s *SnapshotRestore) NewSnapshotRestoreProgressUpdater(ssr *velerov1api.SnapshotRestore, log logrus.FieldLogger, ctx context.Context) *SnapshotRestoreProgressUpdater {
	return &SnapshotRestoreProgressUpdater{ssr, log, ctx, s.Client}
}

//UpdateProgress which implement ProgressUpdater interface to update pvr progress status
func (s *SnapshotRestoreProgressUpdater) UpdateProgress(p *uploader.UploaderProgress) {
	restoreVCName := s.SnapshotRestore.Name
	original := s.SnapshotRestore.DeepCopy()
	s.SnapshotRestore.Status.Progress = velerov1api.DataMoveOperationProgress{TotalBytes: p.TotalBytes, BytesDone: p.BytesDone}
	if s.Cli == nil {
		s.Log.Errorf("failed to update snapshot %s restore progress with uninitailize client", restoreVCName)
		return
	}
	if err := s.Cli.Patch(s.Ctx, s.SnapshotRestore, client.MergeFrom(original)); err != nil {
		s.Log.Errorf("update restore snapshot %s  progress with %v", restoreVCName, err)
	}
}

func (s *SnapshotRestore) errorOut(ssr *velerov1api.SnapshotRestore, err error, msg string, log logrus.FieldLogger) {
	original := ssr.DeepCopy()
	ssr.Status.Message = errors.WithMessage(err, msg).Error()

	if err := s.Client.Patch(s.Ctx, ssr, client.MergeFrom(original)); err != nil {
		s.Log.WithError(err).Errorf("Failed to update snapshot restore error message")
	}
}

func (s *SnapshotRestore) complete(ssb *velerov1api.SnapshotRestore) error {
	return nil
}
