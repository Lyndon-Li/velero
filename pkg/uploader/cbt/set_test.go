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

package cbt

import (
	"context"
	"errors"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/vmware-tanzu/velero/pkg/cbtservice"
	cbtservicemocks "github.com/vmware-tanzu/velero/pkg/cbtservice/mocks"
)

func TestGetBackupBitmap(t *testing.T) {
	tests := []struct {
		name       string
		nilService bool
		snapshot   string
		changeID   string
		setupMocks func(*cbtservicemocks.Service)
		isFull     bool
		expected   []uint64 // offsets expected in the bitmap
	}{
		{
			name:       "nil service",
			nilService: true,
			snapshot:   "snap-1",
			changeID:   "change-1",
			isFull:     true,
			expected:   []uint64{0, 4096},
		},
		{
			name:     "empty snapshot",
			snapshot: "",
			changeID: "change-1",
			isFull:   true,
			expected: []uint64{0, 4096},
		},
		{
			name:     "empty change ID, allocated blocks success",
			snapshot: "snap-1",
			changeID: "",
			setupMocks: func(svc *cbtservicemocks.Service) {
				svc.On("GetAllocatedBlocks", mock.Anything, "snap-1", mock.Anything).Run(func(args mock.Arguments) {
					record := args.Get(2).(func([]cbtservice.Range) error)
					record([]cbtservice.Range{
						{Offset: 4096, Length: 4096},
					})
				}).Return(nil)
			},
			isFull:   true,
			expected: []uint64{4096},
		},
		{
			name:     "changed blocks success, allocated blocks success",
			snapshot: "snap-1",
			changeID: "change-1",
			setupMocks: func(svc *cbtservicemocks.Service) {
				svc.On("GetChangedBlocks", mock.Anything, "snap-1", "change-1", mock.Anything).Run(func(args mock.Arguments) {
					record := args.Get(3).(func([]cbtservice.Range) error)
					record([]cbtservice.Range{
						{Offset: 0, Length: 4096},
					})
				}).Return(nil)
				svc.On("GetAllocatedBlocks", mock.Anything, "snap-1", mock.Anything).Run(func(args mock.Arguments) {
					record := args.Get(2).(func([]cbtservice.Range) error)
					record([]cbtservice.Range{
						{Offset: 0, Length: 8192},
					})
				}).Return(nil)
			},
			isFull:   false,
			expected: []uint64{0}, // Intersection of {0} and {0, 4096} is {0}
		},
		{
			name:     "changed blocks error, allocated blocks success",
			snapshot: "snap-1",
			changeID: "change-1",
			setupMocks: func(svc *cbtservicemocks.Service) {
				svc.On("GetChangedBlocks", mock.Anything, "snap-1", "change-1", mock.Anything).Return(errors.New("changed error"))
				svc.On("GetAllocatedBlocks", mock.Anything, "snap-1", mock.Anything).Run(func(args mock.Arguments) {
					record := args.Get(2).(func([]cbtservice.Range) error)
					record([]cbtservice.Range{
						{Offset: 4096, Length: 4096},
					})
				}).Return(nil)
			},
			isFull:   true,
			expected: []uint64{4096},
		},
		{
			name:     "changed blocks success, allocated blocks error",
			snapshot: "snap-1",
			changeID: "change-1",
			setupMocks: func(svc *cbtservicemocks.Service) {
				svc.On("GetChangedBlocks", mock.Anything, "snap-1", "change-1", mock.Anything).Run(func(args mock.Arguments) {
					record := args.Get(3).(func([]cbtservice.Range) error)
					record([]cbtservice.Range{
						{Offset: 0, Length: 4096},
					})
				}).Return(nil)
				svc.On("GetAllocatedBlocks", mock.Anything, "snap-1", mock.Anything).Return(errors.New("alloc error"))
			},
			isFull:   false,
			expected: []uint64{0},
		},
		{
			name:     "both errors",
			snapshot: "snap-1",
			changeID: "change-1",
			setupMocks: func(svc *cbtservicemocks.Service) {
				svc.On("GetChangedBlocks", mock.Anything, "snap-1", "change-1", mock.Anything).Return(errors.New("changed error"))
				svc.On("GetAllocatedBlocks", mock.Anything, "snap-1", mock.Anything).Return(errors.New("alloc error"))
			},
			isFull:   true,
			expected: []uint64{0, 4096},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svcMock := new(cbtservicemocks.Service)

			if tt.setupMocks != nil {
				tt.setupMocks(svcMock)
			}

			var svc cbtservice.Service
			if !tt.nilService {
				svc = svcMock
			}

			blockSize := 4096
			sourceSize := int64(8192)
			volumeID := "vol-1"
			logger := logrus.New()

			bitmap, isFull := GetBackupBitmap(context.Background(), svc, blockSize, sourceSize, tt.snapshot, tt.changeID, volumeID, logger)

			require.Equal(t, tt.isFull, isFull)

			// Verify bitmap contents
			iter := bitmap.Iterator()
			var actual []uint64
			for {
				offset, hasNext := iter.Next()
				if !hasNext {
					break
				}
				actual = append(actual, offset)
			}

			if tt.expected == nil {
				require.Empty(t, actual)
			} else {
				require.Equal(t, tt.expected, actual)
			}

			if !tt.nilService {
				svcMock.AssertExpectations(t)
			}
		})
	}
}
