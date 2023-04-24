/*
Copyright the Velero contributors.

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

package restore

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1api "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/vmware-tanzu/velero/pkg/label"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	"github.com/vmware-tanzu/velero/pkg/util/boolptr"

	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velerov1alpha1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1alpha1"
)

type DataUploadRetrieveAction struct {
	logger          logrus.FieldLogger
	configMapClient corev1client.ConfigMapInterface
}

func NewDataUploadRetrieveAction(logger logrus.FieldLogger, configMapClient corev1client.ConfigMapInterface) *DataUploadRetrieveAction {
	return &DataUploadRetrieveAction{
		logger:          logger,
		configMapClient: configMapClient,
	}
}

func (d *DataUploadRetrieveAction) AppliesTo() (velero.ResourceSelector, error) {
	return velero.ResourceSelector{
		IncludedResources: []string{"snapshotbackups"},
	}, nil
}

func (d *DataUploadRetrieveAction) Execute(input *velero.RestoreItemActionExecuteInput) (*velero.RestoreItemActionExecuteOutput, error) {
	d.logger.Info("Executing DataUploadRetrieveAction")

	ssb := velerov1alpha1api.SnapshotBackup{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(input.ItemFromBackup.UnstructuredContent(), &ssb); err != nil {
		return nil, errors.Wrap(err, "unable to convert unstructured item to SnapshotBackup")
	}

	rootCM, err := d.configMapClient.Get(context.Background(), input.Restore.Name, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get root CM for restore %s", input.Restore.Name)
	}

	backupResult := velerov1alpha1api.SnapshotBackupResult{
		BackupStorageLocation: ssb.Spec.BackupStorageLocation,
		DataMover:             ssb.Spec.DataMover,
		SnapshotID:            ssb.Status.SnapshotID,
		SourceNamespace:       ssb.Spec.SourceNamespace,
		DataMoverResult:       ssb.Status.DataMoverResult,
	}

	jsonBytes, err := json.Marshal(backupResult)
	if err != nil {
		return nil, errors.Wrap(err, "error converting backup result to JSON")
	}

	cm := corev1api.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: corev1api.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      ssb.Name,
			Namespace: ssb.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: velerov1api.SchemeGroupVersion.String(),
					Kind:       "ConfigMap",
					Name:       rootCM.Name,
					UID:        rootCM.UID,
					Controller: boolptr.True(),
				},
			},
			Labels: map[string]string{
				velerov1api.BackupNameLabel: label.GetValidName(input.Restore.Spec.BackupName),
			},
		},
		Data: map[string]string{
			string(input.Restore.UID): string(jsonBytes),
		},
	}

	_, err = d.configMapClient.Create(context.Background(), &cm, metav1.CreateOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "error to create backup result cm")
	}

	return &velero.RestoreItemActionExecuteOutput{
		SkipRestore: true,
	}, nil
}
