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

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/datapath"
	"github.com/vmware-tanzu/velero/pkg/uploader"
	"k8s.io/apimachinery/pkg/runtime"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"

	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

type backupMsTestHelper struct {
	eventReason  string
	eventMsg     string
	marshalErr   error
	marshalBytes []byte
	withEvent    bool
	eventLock    sync.Mutex
}

func (bt *backupMsTestHelper) Event(_ runtime.Object, _ bool, reason string, message string, a ...any) {
	bt.eventLock.Lock()
	defer bt.eventLock.Unlock()

	bt.withEvent = true
	bt.eventReason = reason
	bt.eventMsg = fmt.Sprintf(message, a...)
}
func (bt *backupMsTestHelper) Shutdown() {}

func (bt *backupMsTestHelper) Marshal(v any) ([]byte, error) {
	if bt.marshalErr != nil {
		return nil, bt.marshalErr
	}

	return bt.marshalBytes, nil
}

func (bt *backupMsTestHelper) EventReason() string {
	bt.eventLock.Lock()
	defer bt.eventLock.Unlock()

	return bt.eventReason
}

func (bt *backupMsTestHelper) EventMessage() string {
	bt.eventLock.Lock()
	defer bt.eventLock.Unlock()

	return bt.eventMsg
}

func TestOnDataUploadFailed(t *testing.T) {
	dataUploadName := "fake-data-upload"
	bt := &backupMsTestHelper{}

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
	bt := &backupMsTestHelper{}

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

			bt := &backupMsTestHelper{
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
		expectedErr         string
		expectedEventReason string
		expectedEventMsg    string
		marshalErr          error
		marshallStr         string
	}{
		{
			name:        "marshal fail",
			marshalErr:  errors.New("fake-marshal-error"),
			expectedErr: "Failed to marshal backup result",
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

			bt := &backupMsTestHelper{
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

			bt := &backupMsTestHelper{}

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
