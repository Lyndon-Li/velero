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
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	kbclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/pkg/buildinfo"
	"github.com/vmware-tanzu/velero/pkg/client"
	"github.com/vmware-tanzu/velero/pkg/cmd"
	"github.com/vmware-tanzu/velero/pkg/cmd/util/signals"
	"github.com/vmware-tanzu/velero/pkg/util/logging"
)

type snapshotRestorer struct {
	logger     logrus.FieldLogger
	ctx        context.Context
	cancelFunc context.CancelFunc
	kbclient   kbclient.Client
	namespace  string
	request    string
}

func NewSnapshotRestoreCommand(f client.Factory) *cobra.Command {
	logLevelFlag := logging.LogLevelFlag(logrus.InfoLevel)
	formatFlag := logging.NewFormatFlag()

	command := &cobra.Command{
		Use:    "snapshot-restore",
		Short:  "Run the velero snapshot restore data path",
		Long:   "Run the velero snapshot restore data path",
		Hidden: true,
		Run: func(c *cobra.Command, args []string) {
			logLevel := logLevelFlag.Parse()
			logrus.Infof("Setting log-level to %s", strings.ToUpper(logLevel.String()))

			logger := logging.DefaultLogger(logLevel, formatFlag.Parse())
			logger.Infof("Starting Velero snapshot restore data path %s (%s)", buildinfo.Version, buildinfo.FormattedGitSHA())

			f.SetBasename(fmt.Sprintf("%s-%s", c.Parent().Name(), c.Name()))
			s, err := newSnapshotRestorer(f, args[0], logger)
			cmd.CheckError(err)

			s.run()
		},
	}

	command.Flags().Var(logLevelFlag, "log-level", fmt.Sprintf("The level at which to log. Valid values are %s.", strings.Join(logLevelFlag.AllowedValues(), ", ")))
	command.Flags().Var(formatFlag, "log-format", fmt.Sprintf("The format for log output. Valid values are %s.", strings.Join(formatFlag.AllowedValues(), ", ")))

	return command
}

func newSnapshotRestorer(factory client.Factory, request string, logger logrus.FieldLogger) (*snapshotRestorer, error) {
	ctx, cancelFunc := context.WithCancel(context.Background())

	s := &snapshotRestorer{
		request:    request,
		logger:     logger,
		ctx:        ctx,
		cancelFunc: cancelFunc,
		namespace:  factory.Namespace(),
	}

	var err error
	s.kbclient, err = factory.KubebuilderClient()
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *snapshotRestorer) run() {
	signals.CancelOnShutdown(s.cancelFunc, s.logger)

	s.logger.Infof("Starting run data path for snapshot backup %s", s.request)

	// credSecretStore, err := credentials.NewNamespacedSecretStore(s.kbclient, s.namespace)
	// if err != nil {
	// 	s.logger.Fatalf("Failed to create secret file store: %v", err)
	// }

	// credentialGetter := &credentials.CredentialGetter{FromFile: nil, FromSecret: credSecretStore}

	// datapath := datapath.NewSnapshotRestore(s.ctx, s.kbclient, credentialGetter, s.logger)
	// if err := datapath.Run(s.request, s.namespace); err != nil {
	// 	s.logger.Fatalf("Failed to run data path for snapshot backup %s, error %v", s.request, err)
	// }
}
