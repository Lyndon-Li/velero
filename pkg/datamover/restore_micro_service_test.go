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

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/datapath"
	"github.com/vmware-tanzu/velero/pkg/uploader"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"

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
