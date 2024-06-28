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
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/datapath"
	datapathMock "github.com/vmware-tanzu/velero/pkg/datapath/mocks"
	"github.com/vmware-tanzu/velero/pkg/uploader"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientFake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"

	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

func TestOnDataDownloadFailed(t *testing.T) {
	dataDownloadName := "fake-data-download"
	bt := &msTestHelper{}

	bs := &RestoreMicroService{
		dataDownloadName: dataDownloadName,
		dataPathMgr:      datapath.NewManager(1),
		eventRecorder:    bt,
		resultSignal:     make(chan dataPathResult),
		logger:           velerotest.NewLogger(),
	}

	expectedErr := "Data path for data download fake-data-download failed: fake-error"
	expectedEventReason := datapath.EventReasonFailed
	expectedEventMsg := "Data path for data download fake-data-download failed, error fake-error"

	go bs.OnDataDownloadFailed(context.TODO(), velerov1api.DefaultNamespace, dataDownloadName, errors.New("fake-error"))

	result := <-bs.resultSignal
	assert.EqualError(t, result.err, expectedErr)
	assert.Equal(t, expectedEventReason, bt.EventReason())
	assert.Equal(t, expectedEventMsg, bt.EventMessage())
}

func TestOnDataDownloadCancelled(t *testing.T) {
	dataDownloadName := "fake-data-download"
	bt := &msTestHelper{}

	bs := &RestoreMicroService{
		dataDownloadName: dataDownloadName,
		dataPathMgr:      datapath.NewManager(1),
		eventRecorder:    bt,
		resultSignal:     make(chan dataPathResult),
		logger:           velerotest.NewLogger(),
	}

	expectedErr := datapath.ErrCancelled
	expectedEventReason := datapath.EventReasonCancelled
	expectedEventMsg := "Data path for data download fake-data-download cancelled"

	go bs.OnDataDownloadCancelled(context.TODO(), velerov1api.DefaultNamespace, dataDownloadName)

	result := <-bs.resultSignal
	assert.EqualError(t, result.err, expectedErr)
	assert.Equal(t, expectedEventReason, bt.EventReason())
	assert.Equal(t, expectedEventMsg, bt.EventMessage())
}

func TestOnDataDownloadCompleted(t *testing.T) {
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
			expectedErr: "Failed to marshal restore result {{ }}: fake-marshal-error",
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
			dataDownloadName := "fake-data-download"

			bt := &msTestHelper{
				marshalErr:   test.marshalErr,
				marshalBytes: []byte(test.marshallStr),
			}

			bs := &RestoreMicroService{
				dataPathMgr:   datapath.NewManager(1),
				eventRecorder: bt,
				resultSignal:  make(chan dataPathResult),
				logger:        velerotest.NewLogger(),
			}

			funcMarshal = bt.Marshal

			go bs.OnDataDownloadCompleted(context.TODO(), velerov1api.DefaultNamespace, dataDownloadName, datapath.Result{})

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

func TestOnDataDownloadProgress(t *testing.T) {
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
			dataDownloadName := "fake-data-download"

			bt := &msTestHelper{
				marshalErr:   test.marshalErr,
				marshalBytes: []byte(test.marshallStr),
			}

			bs := &RestoreMicroService{
				dataPathMgr:   datapath.NewManager(1),
				eventRecorder: bt,
				logger:        velerotest.NewLogger(),
			}

			funcMarshal = bt.Marshal

			bs.OnDataDownloadProgress(context.TODO(), velerov1api.DefaultNamespace, dataDownloadName, &uploader.Progress{})

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

func TestCancelDataDownload(t *testing.T) {
	tests := []struct {
		name                string
		expectedEventReason string
		expectedEventMsg    string
		expectedErr         string
	}{
		{
			name:                "no fs restore",
			expectedEventReason: datapath.EventReasonCancelled,
			expectedEventMsg:    "Data path for data download fake-data-download cancelled",
			expectedErr:         datapath.ErrCancelled,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dataDownloadName := "fake-data-download"
			dd := builder.ForDataDownload(velerov1api.DefaultNamespace, dataDownloadName).Result()

			bt := &msTestHelper{}

			bs := &RestoreMicroService{
				dataPathMgr:   datapath.NewManager(1),
				eventRecorder: bt,
				resultSignal:  make(chan dataPathResult),
				logger:        velerotest.NewLogger(),
			}

			go bs.cancelDataDownload(dd)

			result := <-bs.resultSignal

			assert.EqualError(t, result.err, test.expectedErr)
			assert.True(t, bt.withEvent)
			assert.Equal(t, test.expectedEventReason, bt.EventReason())
			assert.Equal(t, test.expectedEventMsg, bt.EventMessage())
		})
	}
}

func TestRunCancelableDataDownload(t *testing.T) {
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
			ctx:         ctxTimeout,
			expectedErr: "error waiting for dd: context deadline exceeded",
		},
		{
			name: "no in progress du",
			ctx:  ctxTimeout,
			kubeClientObj: []runtime.Object{
				builder.ForDataDownload(velerov1api.DefaultNamespace, "fake-data-download").Result(),
			},
			expectedErr: "error waiting for dd: context deadline exceeded",
		},
		{
			name: "create fs br fail",
			ctx:  context.Background(),
			kubeClientObj: []runtime.Object{
				builder.ForDataDownload(velerov1api.DefaultNamespace, "fake-data-download").Phase(v2alpha1.DataDownloadPhaseInProgress).
					Labels(map[string]string{velerov1api.AsyncOperationIDLabel: "faike-id"}).Result(),
			},
			dataMgr:     datapath.NewManager(0),
			expectedErr: "error to create data path: Concurrent number exceeds",
		},
		{
			name: "fs br init fail",
			ctx:  context.Background(),
			kubeClientObj: []runtime.Object{
				builder.ForDataDownload(velerov1api.DefaultNamespace, "fake-data-download").Phase(v2alpha1.DataDownloadPhaseInProgress).
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
				builder.ForDataDownload(velerov1api.DefaultNamespace, "fake-data-download").Phase(v2alpha1.DataDownloadPhaseInProgress).
					Labels(map[string]string{velerov1api.AsyncOperationIDLabel: "faike-id"}).Result(),
			},
			dataMgr: datapath.NewManager(1),
			fsBRCreator: func(string, string, client.Client, string, datapath.Callbacks, logrus.FieldLogger) datapath.AsyncBR {
				asyncBR := datapathMock.NewAsyncBR(t)
				asyncBR.On("Init", mock.Anything, mock.Anything).Return(nil)
				asyncBR.On("StartRestore", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("fake-start-error"))
				return asyncBR
			},
			expectedErr: "error starting data path restore: fake-start-error",
		},
		{
			name: "context timeout",
			ctx:  ctxTimeout,
			kubeClientObj: []runtime.Object{
				builder.ForDataDownload(velerov1api.DefaultNamespace, "fake-data-download").Phase(v2alpha1.DataDownloadPhaseInProgress).
					Labels(map[string]string{velerov1api.AsyncOperationIDLabel: "faike-id"}).Result(),
			},
			dataMgr: datapath.NewManager(1),
			fsBRCreator: func(string, string, client.Client, string, datapath.Callbacks, logrus.FieldLogger) datapath.AsyncBR {
				asyncBR := datapathMock.NewAsyncBR(t)
				asyncBR.On("Init", mock.Anything, mock.Anything).Return(nil)
				asyncBR.On("StartRestore", mock.Anything, mock.Anything, mock.Anything).Return(nil)
				return asyncBR
			},
			expectEvent: datapath.EventReasonStarted,
			expectedErr: "timed out waiting for fs restore to complete",
		},
		{
			name: "data path error",
			ctx:  context.Background(),
			kubeClientObj: []runtime.Object{
				builder.ForDataDownload(velerov1api.DefaultNamespace, "fake-data-download").Phase(v2alpha1.DataDownloadPhaseInProgress).
					Labels(map[string]string{velerov1api.AsyncOperationIDLabel: "faike-id"}).Result(),
			},
			dataMgr: datapath.NewManager(1),
			fsBRCreator: func(string, string, client.Client, string, datapath.Callbacks, logrus.FieldLogger) datapath.AsyncBR {
				asyncBR := datapathMock.NewAsyncBR(t)
				asyncBR.On("Init", mock.Anything, mock.Anything).Return(nil)
				asyncBR.On("StartRestore", mock.Anything, mock.Anything, mock.Anything).Return(nil)
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
				builder.ForDataDownload(velerov1api.DefaultNamespace, "fake-data-download").Phase(v2alpha1.DataDownloadPhaseInProgress).
					Labels(map[string]string{velerov1api.AsyncOperationIDLabel: "faike-id"}).Result(),
			},
			dataMgr: datapath.NewManager(1),
			fsBRCreator: func(string, string, client.Client, string, datapath.Callbacks, logrus.FieldLogger) datapath.AsyncBR {
				asyncBR := datapathMock.NewAsyncBR(t)
				asyncBR.On("Init", mock.Anything, mock.Anything).Return(nil)
				asyncBR.On("StartRestore", mock.Anything, mock.Anything, mock.Anything).Return(nil)
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
			fakeClientBuilder := clientFake.NewClientBuilder()
			fakeClientBuilder = fakeClientBuilder.WithScheme(scheme)
			fakeClient := fakeClientBuilder.WithRuntimeObjects(test.kubeClientObj...).Build()

			bt := &msTestHelper{}

			rs := &RestoreMicroService{
				namespace:        velerov1api.DefaultNamespace,
				dataDownloadName: "fake-data-download",
				client:           fakeClient,
				dataPathMgr:      test.dataMgr,
				eventRecorder:    bt,
				resultSignal:     make(chan dataPathResult),
				logger:           velerotest.NewLogger(),
			}

			datapath.FSBRCreator = test.fsBRCreator

			if test.dataPathResult != "" || test.datpathErr != nil {
				go func() {
					rs.resultSignal <- dataPathResult{
						err:    test.datpathErr,
						result: test.dataPathResult,
					}
				}()
			}

			result, err := rs.RunCancelableDataPath(test.ctx)

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
