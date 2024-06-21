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
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/datapath"
	"github.com/vmware-tanzu/velero/pkg/uploader"
	"k8s.io/apimachinery/pkg/runtime"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"

	velerotest "github.com/vmware-tanzu/velero/pkg/test"

	datapathMock "github.com/vmware-tanzu/velero/pkg/datapath/mocks"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientFake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type msTestHelper struct {
	eventReason  string
	eventMsg     string
	marshalErr   error
	marshalBytes []byte
	withEvent    bool
	eventLock    sync.Mutex
}

func (bt *msTestHelper) Event(_ runtime.Object, _ bool, reason string, message string, a ...any) {
	bt.eventLock.Lock()
	defer bt.eventLock.Unlock()

	bt.withEvent = true
	bt.eventReason = reason
	bt.eventMsg = fmt.Sprintf(message, a...)
}
func (bt *msTestHelper) Shutdown() {}

func (bt *msTestHelper) Marshal(v any) ([]byte, error) {
	if bt.marshalErr != nil {
		return nil, bt.marshalErr
	}

	return bt.marshalBytes, nil
}

func (bt *msTestHelper) EventReason() string {
	bt.eventLock.Lock()
	defer bt.eventLock.Unlock()

	return bt.eventReason
}

func (bt *msTestHelper) EventMessage() string {
	bt.eventLock.Lock()
	defer bt.eventLock.Unlock()

	return bt.eventMsg
}

func TestOnDataUploadFailed(t *testing.T) {
	dataUploadName := "fake-data-upload"
	bt := &msTestHelper{}

	bs := &BackupMicroService{
		dataUploadName: dataUploadName,
		dataPathMgr:    datapath.NewManager(1),
		eventRecorder:  bt,
		resultSignal:   make(chan dataPathResult),
		logger:         velerotest.NewLogger(),
	}

	expectedErr := "Data path for data upload fake-data-upload failed: fake-error"
	expectedEventReason := datapath.EventReasonFailed
	expectedEventMsg := "Data path for data upload fake-data-upload failed, error fake-error"

	go bs.OnDataUploadFailed(context.TODO(), velerov1api.DefaultNamespace, dataUploadName, errors.New("fake-error"))

	result := <-bs.resultSignal
	assert.EqualError(t, result.err, expectedErr)
	assert.Equal(t, expectedEventReason, bt.EventReason())
	assert.Equal(t, expectedEventMsg, bt.EventMessage())
}

func TestOnDataUploadCancelled(t *testing.T) {
	dataUploadName := "fake-data-upload"
	bt := &msTestHelper{}

	bs := &BackupMicroService{
		dataUploadName: dataUploadName,
		dataPathMgr:    datapath.NewManager(1),
		eventRecorder:  bt,
		resultSignal:   make(chan dataPathResult),
		logger:         velerotest.NewLogger(),
	}

	expectedErr := datapath.ErrCancelled
	expectedEventReason := datapath.EventReasonCancelled
	expectedEventMsg := "Data path for data upload fake-data-upload cancelled"

	go bs.OnDataUploadCancelled(context.TODO(), velerov1api.DefaultNamespace, dataUploadName)

	result := <-bs.resultSignal
	assert.EqualError(t, result.err, expectedErr)
	assert.Equal(t, expectedEventReason, bt.EventReason())
	assert.Equal(t, expectedEventMsg, bt.EventMessage())
}

func TestOnDataUploadCompleted(t *testing.T) {
	tests := []struct {
		name                string
		expectedErr         string
		expectedEventReason string
		expectedEventMsg    string
		marshalErr          error
		marshallStr         string
	}{
		{
			name:        "marshal fail",
			marshalErr:  errors.New("fake-marshal-error"),
			expectedErr: "Failed to marshal backup result { false { }}: fake-marshal-error",
		},
		{
			name:                "succeed",
			marshallStr:         "fake-complete-string",
			expectedEventReason: datapath.EventReasonCompleted,
			expectedEventMsg:    "fake-complete-string",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dataUploadName := "fake-data-upload"

			bt := &msTestHelper{
				marshalErr:   test.marshalErr,
				marshalBytes: []byte(test.marshallStr),
			}

			bs := &BackupMicroService{
				dataPathMgr:   datapath.NewManager(1),
				eventRecorder: bt,
				resultSignal:  make(chan dataPathResult),
				logger:        velerotest.NewLogger(),
			}

			funcMarshal = bt.Marshal

			go bs.OnDataUploadCompleted(context.TODO(), velerov1api.DefaultNamespace, dataUploadName, datapath.Result{})

			result := <-bs.resultSignal
			if test.marshalErr != nil {
				assert.EqualError(t, result.err, test.expectedErr)
			} else {
				assert.NoError(t, result.err)
				assert.Equal(t, test.expectedEventReason, bt.EventReason())
				assert.Equal(t, test.expectedEventMsg, bt.EventMessage())
			}
		})
	}
}

func TestOnDataUploadProgress(t *testing.T) {
	tests := []struct {
		name                string
		expectedEventReason string
		expectedEventMsg    string
		marshalErr          error
		marshallStr         string
	}{
		{
			name:       "marshal fail",
			marshalErr: errors.New("fake-marshal-error"),
		},
		{
			name:                "succeed",
			marshallStr:         "fake-progress-string",
			expectedEventReason: datapath.EventReasonProgress,
			expectedEventMsg:    "fake-progress-string",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dataUploadName := "fake-data-upload"

			bt := &msTestHelper{
				marshalErr:   test.marshalErr,
				marshalBytes: []byte(test.marshallStr),
			}

			bs := &BackupMicroService{
				dataPathMgr:   datapath.NewManager(1),
				eventRecorder: bt,
				logger:        velerotest.NewLogger(),
			}

			funcMarshal = bt.Marshal

			bs.OnDataUploadProgress(context.TODO(), velerov1api.DefaultNamespace, dataUploadName, &uploader.Progress{})

			if test.marshalErr != nil {
				assert.False(t, bt.withEvent)
			} else {
				assert.True(t, bt.withEvent)
				assert.Equal(t, test.expectedEventReason, bt.EventReason())
				assert.Equal(t, test.expectedEventMsg, bt.EventMessage())
			}
		})
	}
}

func TestCancelDataUpload(t *testing.T) {
	tests := []struct {
		name                string
		expectedEventReason string
		expectedEventMsg    string
		expectedErr         string
	}{
		{
			name:                "no fs backup",
			expectedEventReason: datapath.EventReasonCancelled,
			expectedEventMsg:    "Data path for data upload fake-data-upload cancelled",
			expectedErr:         datapath.ErrCancelled,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dataUploadName := "fake-data-upload"
			du := builder.ForDataUpload(velerov1api.DefaultNamespace, dataUploadName).Result()

			bt := &msTestHelper{}

			bs := &BackupMicroService{
				dataPathMgr:   datapath.NewManager(1),
				eventRecorder: bt,
				resultSignal:  make(chan dataPathResult),
				logger:        velerotest.NewLogger(),
			}

			go bs.cancelDataUpload(du)

			result := <-bs.resultSignal

			assert.EqualError(t, result.err, test.expectedErr)
			assert.True(t, bt.withEvent)
			assert.Equal(t, test.expectedEventReason, bt.EventReason())
			assert.Equal(t, test.expectedEventMsg, bt.EventMessage())
		})
	}
}

func TestRunCancelableDataUpload(t *testing.T) {
	ctxTimeout, cancelFun := context.WithTimeout(context.Background(), time.Second)
	tests := []struct {
		name           string
		ctx            context.Context
		kubeClientObj  []runtime.Object
		fsBRCreator    func(string, string, client.Client, string, datapath.Callbacks, logrus.FieldLogger) datapath.AsyncBR
		dataMgr        *datapath.Manager
		datpathErr     error
		dataPathResult string
		expectedResult string
		expectedErr    string
		expectEvent    string
	}{
		{
			name:        "no du",
			ctx:         context.Background(),
			expectedErr: "error waiting for du: context deadline exceeded",
		},
		{
			name: "no in progress du",
			ctx:  context.Background(),
			kubeClientObj: []runtime.Object{
				builder.ForDataUpload(velerov1api.DefaultNamespace, "fake-data-upload").Result(),
			},
			expectedErr: "error waiting for du: context deadline exceeded",
		},
		{
			name: "create fs br fail",
			ctx:  context.Background(),
			kubeClientObj: []runtime.Object{
				builder.ForDataUpload(velerov1api.DefaultNamespace, "fake-data-upload").Phase(v2alpha1.DataUploadPhaseInProgress).
					Labels(map[string]string{velerov1api.AsyncOperationIDLabel: "faike-id"}).Result(),
			},
			dataMgr:     datapath.NewManager(0),
			expectedErr: "error to create data path: Concurrent number exceeds",
		},
		{
			name: "fs br init fail",
			ctx:  context.Background(),
			kubeClientObj: []runtime.Object{
				builder.ForDataUpload(velerov1api.DefaultNamespace, "fake-data-upload").Phase(v2alpha1.DataUploadPhaseInProgress).
					Labels(map[string]string{velerov1api.AsyncOperationIDLabel: "faike-id"}).Result(),
			},
			dataMgr: datapath.NewManager(1),
			fsBRCreator: func(string, string, client.Client, string, datapath.Callbacks, logrus.FieldLogger) datapath.AsyncBR {
				asyncBR := datapathMock.NewAsyncBR(t)
				asyncBR.On("Init", mock.Anything, mock.Anything).Return(errors.New("fake-init-error"))
				return asyncBR
			},
			expectedErr: "error to initialize data path: fake-init-error",
		},
		{
			name: "fs br start fail",
			ctx:  context.Background(),
			kubeClientObj: []runtime.Object{
				builder.ForDataUpload(velerov1api.DefaultNamespace, "fake-data-upload").Phase(v2alpha1.DataUploadPhaseInProgress).
					Labels(map[string]string{velerov1api.AsyncOperationIDLabel: "faike-id"}).Result(),
			},
			dataMgr: datapath.NewManager(1),
			fsBRCreator: func(string, string, client.Client, string, datapath.Callbacks, logrus.FieldLogger) datapath.AsyncBR {
				asyncBR := datapathMock.NewAsyncBR(t)
				asyncBR.On("Init", mock.Anything, mock.Anything).Return(nil)
				asyncBR.On("StartBackup", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("fake-start-error"))
				return asyncBR
			},
			expectedErr: "error starting data path backup: fake-start-error",
		},
		{
			name: "context timeout",
			ctx:  ctxTimeout,
			kubeClientObj: []runtime.Object{
				builder.ForDataUpload(velerov1api.DefaultNamespace, "fake-data-upload").Phase(v2alpha1.DataUploadPhaseInProgress).
					Labels(map[string]string{velerov1api.AsyncOperationIDLabel: "faike-id"}).Result(),
			},
			dataMgr: datapath.NewManager(1),
			fsBRCreator: func(string, string, client.Client, string, datapath.Callbacks, logrus.FieldLogger) datapath.AsyncBR {
				asyncBR := datapathMock.NewAsyncBR(t)
				asyncBR.On("Init", mock.Anything, mock.Anything).Return(nil)
				asyncBR.On("StartBackup", mock.Anything, mock.Anything, mock.Anything).Return(nil)
				return asyncBR
			},
			expectEvent: datapath.EventReasonStarted,
			expectedErr: "timed out waiting for fs backup to complete",
		},
		{
			name: "data path error",
			ctx:  context.Background(),
			kubeClientObj: []runtime.Object{
				builder.ForDataUpload(velerov1api.DefaultNamespace, "fake-data-upload").Phase(v2alpha1.DataUploadPhaseInProgress).
					Labels(map[string]string{velerov1api.AsyncOperationIDLabel: "faike-id"}).Result(),
			},
			dataMgr: datapath.NewManager(1),
			fsBRCreator: func(string, string, client.Client, string, datapath.Callbacks, logrus.FieldLogger) datapath.AsyncBR {
				asyncBR := datapathMock.NewAsyncBR(t)
				asyncBR.On("Init", mock.Anything, mock.Anything).Return(nil)
				asyncBR.On("StartBackup", mock.Anything, mock.Anything, mock.Anything).Return(nil)
				return asyncBR
			},
			expectEvent: datapath.EventReasonStarted,
			datpathErr:  errors.New("fake-data-path-error"),
			expectedErr: "fake-data-path-error",
		},
		{
			name: "succeed",
			ctx:  context.Background(),
			kubeClientObj: []runtime.Object{
				builder.ForDataUpload(velerov1api.DefaultNamespace, "fake-data-upload").Phase(v2alpha1.DataUploadPhaseInProgress).
					Labels(map[string]string{velerov1api.AsyncOperationIDLabel: "faike-id"}).Result(),
			},
			dataMgr: datapath.NewManager(1),
			fsBRCreator: func(string, string, client.Client, string, datapath.Callbacks, logrus.FieldLogger) datapath.AsyncBR {
				asyncBR := datapathMock.NewAsyncBR(t)
				asyncBR.On("Init", mock.Anything, mock.Anything).Return(nil)
				asyncBR.On("StartBackup", mock.Anything, mock.Anything, mock.Anything).Return(nil)
				return asyncBR
			},
			expectEvent:    datapath.EventReasonStarted,
			dataPathResult: "fake-data-path-result",
			expectedResult: "fake-data-path-result",
		},
	}

	scheme := runtime.NewScheme()
	v2alpha1.AddToScheme(scheme)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			waitStartTimeout = time.Second

			fakeClientBuilder := clientFake.NewClientBuilder()
			fakeClientBuilder = fakeClientBuilder.WithScheme(scheme)
			fakeClient := fakeClientBuilder.WithRuntimeObjects(test.kubeClientObj...).Build()

			bt := &msTestHelper{}

			bs := &BackupMicroService{
				namespace:      velerov1api.DefaultNamespace,
				dataUploadName: "fake-data-upload",
				client:         fakeClient,
				dataPathMgr:    test.dataMgr,
				eventRecorder:  bt,
				resultSignal:   make(chan dataPathResult),
				logger:         velerotest.NewLogger(),
			}

			datapath.FSBRCreator = test.fsBRCreator

			if test.dataPathResult != "" || test.datpathErr != nil {
				go func() {
					bs.resultSignal <- dataPathResult{
						err:    test.datpathErr,
						result: test.dataPathResult,
					}
				}()
			}

			result, err := bs.RunCancelableDataPath(test.ctx)

			if test.expectedErr != "" {
				assert.EqualError(t, err, test.expectedErr)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.expectedResult, result)
			}

			if test.expectEvent != "" {
				assert.Equal(t, test.expectEvent, bt.EventReason())
			}
		})
	}

	cancelFun()
}
