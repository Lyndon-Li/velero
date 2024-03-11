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

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/vmware-tanzu/velero/internal/credentials"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velerov2alpha1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"
	"github.com/vmware-tanzu/velero/pkg/buildinfo"
	"github.com/vmware-tanzu/velero/pkg/client"
	"github.com/vmware-tanzu/velero/pkg/cmd/util/signals"
	"github.com/vmware-tanzu/velero/pkg/datamover"
	"github.com/vmware-tanzu/velero/pkg/datapath"
	"github.com/vmware-tanzu/velero/pkg/repository"
	"github.com/vmware-tanzu/velero/pkg/util/filesystem"
	"github.com/vmware-tanzu/velero/pkg/util/logging"

	ctlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	ctlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type dataMoverRestoreConfig struct {
	thisPodName     string
	thisContainer   string
	thisVolume      string
	resourceTimeout time.Duration
}

func NewRestoreCommand(f client.Factory) *cobra.Command {
	logLevelFlag := logging.LogLevelFlag(logrus.InfoLevel)
	formatFlag := logging.NewFormatFlag()

	config := dataMoverRestoreConfig{}

	command := &cobra.Command{
		Use:    "restore",
		Short:  "Run the velero data-mover restore",
		Long:   "Run the velero data-mover restore",
		Hidden: true,
		Run: func(c *cobra.Command, args []string) {
			logLevel := logLevelFlag.Parse()
			logrus.Infof("Setting log-level to %s", strings.ToUpper(logLevel.String()))

			logger := logging.DefaultLogger(logLevel, formatFlag.Parse())
			logger.Infof("Starting Velero data-mover restore %s (%s)", buildinfo.Version, buildinfo.FormattedGitSHA())

			f.SetBasename(fmt.Sprintf("%s-%s", c.Parent().Name(), c.Name()))
			s, err := newdataMoverRestore(logger, f, config)
			if err != nil {
				exitWithMessage(logger, false, "Failed to create data mover restore, %v", err)
			}

			s.run()
		},
	}

	command.Flags().Var(logLevelFlag, "log-level", fmt.Sprintf("The level at which to log. Valid values are %s.", strings.Join(logLevelFlag.AllowedValues(), ", ")))
	command.Flags().Var(formatFlag, "log-format", fmt.Sprintf("The format for log output. Valid values are %s.", strings.Join(formatFlag.AllowedValues(), ", ")))
	command.Flags().StringVar(&config.thisPodName, "this-pod", config.thisPodName, "The pod name where the data mover restore is running")
	command.Flags().StringVar(&config.thisContainer, "this-container", config.thisContainer, "The container name where the data mover restore is running")
	command.Flags().StringVar(&config.thisVolume, "this-volume", config.thisVolume, "The volume name of target volume")
	command.Flags().DurationVar(&config.resourceTimeout, "resource-timeout", config.resourceTimeout, "How long to wait for resource processes which are not covered by other specific timeout parameters. Default is 10 minutes.")

	return command
}

type dataMoverRestore struct {
	logger      logrus.FieldLogger
	ctx         context.Context
	cancelFunc  context.CancelFunc
	client      ctlclient.Client
	cache       ctlcache.Cache
	namespace   string
	nodeName    string
	config      dataMoverRestoreConfig
	kubeClient  kubernetes.Interface
	dataPathMgr *datapath.Manager
	thisPod     *v1.Pod
}

func newdataMoverRestore(logger logrus.FieldLogger, factory client.Factory, config dataMoverRestoreConfig) (*dataMoverRestore, error) {
	ctx, cancelFunc := context.WithCancel(context.Background())

	clientConfig, err := factory.ClientConfig()
	if err != nil {
		cancelFunc()
		return nil, errors.Wrap(err, "error to create client config")
	}

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	scheme := runtime.NewScheme()
	if err := velerov1api.AddToScheme(scheme); err != nil {
		cancelFunc()
		return nil, errors.Wrap(err, "error to add velero v1 scheme")
	}

	if err := velerov2alpha1api.AddToScheme(scheme); err != nil {
		cancelFunc()
		return nil, errors.Wrap(err, "error to add velero v2alpha1 scheme")
	}

	if err := v1.AddToScheme(scheme); err != nil {
		cancelFunc()
		return nil, errors.Wrap(err, "error to add core v1 scheme")
	}

	nodeName := os.Getenv("NODE_NAME")

	// use a field selector to filter to only pods scheduled on this node.
	cacheOption := ctlcache.Options{
		Scheme: scheme,
		SelectorsByObject: ctlcache.SelectorsByObject{
			&v1.Pod{}: {
				Field: fields.Set{"spec.nodeName": nodeName}.AsSelector(),
			},
			&velerov2alpha1api.DataDownload{}: {
				Field: fields.Set{"metadata.namespace": factory.Namespace()}.AsSelector(),
			},
		},
	}

	cli, err := ctlclient.New(clientConfig, ctlclient.Options{
		Scheme: scheme,
	})
	if err != nil {
		cancelFunc()
		return nil, errors.Wrap(err, "error to create client")
	}

	cache, err := ctlcache.New(clientConfig, cacheOption)
	if err != nil {
		cancelFunc()
		return nil, errors.Wrap(err, "error to create client cache")
	}

	s := &dataMoverRestore{
		logger:     logger,
		ctx:        ctx,
		cancelFunc: cancelFunc,
		client:     cli,
		cache:      cache,
		config:     config,
		namespace:  factory.Namespace(),
		nodeName:   nodeName,
	}

	s.kubeClient, err = factory.KubeClient()
	if err != nil {
		cancelFunc()
		return nil, errors.Wrap(err, "error to create kube client")
	}

	s.dataPathMgr = datapath.NewManager(1)

	return s, nil
}

func (s *dataMoverRestore) run() {
	signals.CancelOnShutdown(s.cancelFunc, s.logger)
	go s.cache.Start(s.ctx)

	s.logger.Infof("Starting micro service in node %s", s.nodeName)

	pod := &v1.Pod{}
	if err := s.client.Get(s.ctx, types.NamespacedName{
		Namespace: s.namespace,
		Name:      s.config.thisPodName,
	}, pod); err != nil {
		s.cancelFunc()
		exitWithMessage(s.logger, false, "Failed to get this pod: %v", err)
	}

	s.thisPod = pod
	s.logger.Infof("Got this pod %s", pod.Name)

	ddName, exist := pod.Labels[velerov1api.DataDownloadLabel]
	if !exist {
		s.cancelFunc()
		exitWithMessage(s.logger, false, "This pod doesn't have datadownload info")
	}

	s.logger.Infof("Got DataDownload %s", ddName)

	credentialFileStore, err := credentials.NewNamespacedFileStore(
		s.client,
		s.namespace,
		defaultCredentialsDirectory,
		filesystem.NewFileSystem(),
	)
	if err != nil {
		s.cancelFunc()
		exitWithMessage(s.logger, false, "Failed to create credentials file store: %v", err)
	}

	credSecretStore, err := credentials.NewNamespacedSecretStore(s.client, s.namespace)
	if err != nil {
		s.cancelFunc()
		exitWithMessage(s.logger, false, "Failed to create secret file store: %v", err)
	}

	credentialGetter := &credentials.CredentialGetter{FromFile: credentialFileStore, FromSecret: credSecretStore}
	repoEnsurer := repository.NewEnsurer(s.client, s.logger, s.config.resourceTimeout)

	dpService := datamover.NewRestoreMicroService(s.ctx, s.client, s.kubeClient, ddName, s.namespace, s.thisPod, s.config.thisContainer, s.config.thisVolume, s.dataPathMgr,
		repoEnsurer, credentialGetter, s.logger)

	ddInformer, err := s.cache.GetInformer(s.ctx, &velerov2alpha1api.DataDownload{})
	if err != nil {
		s.cancelFunc()
		exitWithMessage(s.logger, false, "Failed to get controller-runtime informer from manager for DataDownload: %v", err)
	}

	dpService.SetupWatcher(s.ctx, ddInformer)

	s.logger.Infof("Starting data path service %s", ddName)

	result, err := dpService.RunCancelableDataDownload(s.ctx)
	if err != nil {
		s.cancelFunc()
		exitWithMessage(s.logger, false, "Failed to run data path service: %v", err)
	}

	s.logger.WithField("du", ddName).Info("Data path service completed")

	dpService.Shutdown()

	s.logger.WithField("du", ddName).Info("Data path service is shut down")

	s.cancelFunc()

	exitWithMessage(s.logger, true, result)
}
