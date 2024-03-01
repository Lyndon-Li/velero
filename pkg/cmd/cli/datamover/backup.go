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
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/vmware-tanzu/velero/internal/credentials"
	"github.com/vmware-tanzu/velero/pkg/buildinfo"
	"github.com/vmware-tanzu/velero/pkg/client"
	"github.com/vmware-tanzu/velero/pkg/cmd/util/signals"
	"github.com/vmware-tanzu/velero/pkg/datamover"
	"github.com/vmware-tanzu/velero/pkg/datapath"
	"github.com/vmware-tanzu/velero/pkg/repository"
	"github.com/vmware-tanzu/velero/pkg/util/filesystem"
	"github.com/vmware-tanzu/velero/pkg/util/logging"

	ctrl "sigs.k8s.io/controller-runtime"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velerov2alpha1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"
)

type dataMoverBackupConfig struct {
	thisPodName     string
	resourceTimeout time.Duration
}

func NewBackupCommand(f client.Factory) *cobra.Command {
	config := dataMoverBackupConfig{}

	logLevelFlag := logging.LogLevelFlag(logrus.InfoLevel)
	formatFlag := logging.NewFormatFlag()

	command := &cobra.Command{
		Use:    "backup",
		Short:  "Run the velero data-mover backup",
		Long:   "Run the velero data-mover backup",
		Hidden: true,
		Run: func(c *cobra.Command, args []string) {
			logLevel := logLevelFlag.Parse()
			logrus.Infof("Setting log-level to %s", strings.ToUpper(logLevel.String()))

			logger := logging.DefaultLogger(logLevel, formatFlag.Parse())
			logger.Infof("Starting Velero data-mover backup %s (%s)", buildinfo.Version, buildinfo.FormattedGitSHA())

			f.SetBasename(fmt.Sprintf("%s-%s", c.Parent().Name(), c.Name()))
			s, err := newdataMoverBackup(logger, f, config)
			if err != nil {
				exitWithMessage(logger, false, "Failed to create data mover backup, %v", err)
			}

			s.run()
		},
	}

	command.Flags().Var(logLevelFlag, "log-level", fmt.Sprintf("The level at which to log. Valid values are %s.", strings.Join(logLevelFlag.AllowedValues(), ", ")))
	command.Flags().Var(formatFlag, "log-format", fmt.Sprintf("The format for log output. Valid values are %s.", strings.Join(formatFlag.AllowedValues(), ", ")))
	command.Flags().StringVar(&config.thisPodName, "this-pod", config.thisPodName, "The pod name where the data mover backup is running")
	command.Flags().DurationVar(&config.resourceTimeout, "resource-timeout", config.resourceTimeout, "How long to wait for resource processes which are not covered by other specific timeout parameters. Default is 10 minutes.")

	return command
}

const (
	// defaultCredentialsDirectory is the path on disk where credential
	// files will be written to
	defaultCredentialsDirectory = "/tmp/credentials"
)

type dataMoverBackup struct {
	logger      logrus.FieldLogger
	ctx         context.Context
	cancelFunc  context.CancelFunc
	mgr         manager.Manager
	namespace   string
	nodeName    string
	config      dataMoverBackupConfig
	kubeClient  kubernetes.Interface
	dataPathMgr *datapath.Manager
	thisPod     *v1.Pod
	dataUpload  *velerov2alpha1api.DataUpload
}

func newdataMoverBackup(logger logrus.FieldLogger, factory client.Factory, config dataMoverBackupConfig) (*dataMoverBackup, error) {
	ctx, cancelFunc := context.WithCancel(context.Background())

	clientConfig, err := factory.ClientConfig()
	if err != nil {
		cancelFunc()
		return nil, err
	}

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	scheme := runtime.NewScheme()
	if err := velerov2alpha1api.AddToScheme(scheme); err != nil {
		cancelFunc()
		return nil, err
	}
	if err := v1.AddToScheme(scheme); err != nil {
		cancelFunc()
		return nil, err
	}

	nodeName := os.Getenv("NODE_NAME")

	// use a field selector to filter to only pods scheduled on this node.
	cacheOption := cache.Options{
		SelectorsByObject: cache.SelectorsByObject{
			&v1.Pod{}: {
				Field: fields.Set{"spec.nodeName": nodeName}.AsSelector(),
			},
			&velerov2alpha1api.DataUpload{}: {
				Field: fields.Set{"metadata.namespace": factory.Namespace()}.AsSelector(),
			},
		},
	}
	mgr, err := ctrl.NewManager(clientConfig, ctrl.Options{
		Scheme:   scheme,
		NewCache: cache.BuilderWithOptions(cacheOption),
	})
	if err != nil {
		cancelFunc()
		return nil, err
	}

	s := &dataMoverBackup{
		logger:     logger,
		ctx:        ctx,
		cancelFunc: cancelFunc,
		mgr:        mgr,
		config:     config,
		namespace:  factory.Namespace(),
		nodeName:   nodeName,
	}

	s.kubeClient, err = factory.KubeClient()
	if err != nil {
		return nil, err
	}

	s.dataPathMgr = datapath.NewManager(1)

	return s, nil
}

func (s *dataMoverBackup) run() {
	signals.CancelOnShutdown(s.cancelFunc, s.logger)

	s.logger.Info("Starting micro service")

	pod := &v1.Pod{}
	if err := s.mgr.GetClient().Get(s.ctx, types.NamespacedName{
		Namespace: s.namespace,
		Name:      s.config.thisPodName,
	}, pod); err != nil {
		exitWithMessage(s.logger, false, "Failed to get this pod: %v", err)
	}

	s.thisPod = pod
	s.logger.Infof("Got this pod %s", pod.Name)

	duName, exist := pod.Labels[velerov1api.DataUploadLabel]
	if !exist {
		exitWithMessage(s.logger, false, "This pod doesn't have dataupload info")
	}

	du := &velerov2alpha1api.DataUpload{}
	if err := s.mgr.GetClient().Get(s.ctx, types.NamespacedName{
		Namespace: s.namespace,
		Name:      duName,
	}, du); err != nil {
		exitWithMessage(s.logger, false, "Failed to get DataUpload %s: %v", duName, err)
	}

	s.dataUpload = du
	s.logger.Infof("Got DataUpload %s", du.Name)

	credentialFileStore, err := credentials.NewNamespacedFileStore(
		s.mgr.GetClient(),
		s.namespace,
		defaultCredentialsDirectory,
		filesystem.NewFileSystem(),
	)
	if err != nil {
		exitWithMessage(s.logger, false, "Failed to create credentials file store: %v", err)
	}

	credSecretStore, err := credentials.NewNamespacedSecretStore(s.mgr.GetClient(), s.namespace)
	if err != nil {
		exitWithMessage(s.logger, false, "Failed to create secret file store: %v", err)
	}

	credentialGetter := &credentials.CredentialGetter{FromFile: credentialFileStore, FromSecret: credSecretStore}
	repoEnsurer := repository.NewEnsurer(s.mgr.GetClient(), s.logger, s.config.resourceTimeout)

	duService := datamover.NewBackupMicroService(s.ctx, s.mgr.GetClient(), s.kubeClient, s.dataUpload, s.thisPod, s.dataPathMgr,
		repoEnsurer, credentialGetter, s.logger)

	s.logger.Info("Starting data upload")

	result, err := duService.RunCancelableDataUpload(s.ctx)
	if err != nil {
		exitWithMessage(s.logger, false, "Failed to run data path: %v", err)
	}

	exitWithMessage(s.logger, true, result)
}
