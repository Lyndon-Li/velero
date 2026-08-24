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

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"github.com/vmware-tanzu/velero/pkg/cbtservice"
	"github.com/vmware-tanzu/velero/pkg/uploader/cbt/types"
)

// GetBackupBitmap translates the allocated/changed blocks from CBT service to the given bitmap or set the bitmap to full when error happens
func GetBackupBitmap(ctx context.Context, service cbtservice.Service, blockSize int, sourceSize int64, snapshot string, changeID string, volumeID string, log logrus.FieldLogger) (types.Bitmap, bool) {
	full := NewBitmap(uint(blockSize), uint64(sourceSize), snapshot, changeID, volumeID)
	full.SetFull()

	if service == nil {
		log.Warnf("CBT service is not available, fallback to real full for snapshot %v", snapshot)
		return full, true
	}

	if snapshot == "" {
		log.Warnf("Snapshot is not available, fallback to real full for snapshot %v", snapshot)
		return full, true
	}

	changed := NewBitmap(uint(blockSize), uint64(sourceSize), snapshot, changeID, volumeID)
	changedErr := setChangedBitmap(ctx, service, changed)

	allocated := NewBitmap(uint(blockSize), uint64(sourceSize), snapshot, changeID, volumeID)
	allocatedErr := setAllocatedBitmap(ctx, service, allocated)

	if changedErr != nil && allocatedErr != nil {
		log.WithFields(logrus.Fields{
			"changeErr":    changedErr,
			"allocatedErr": allocatedErr,
		}).Warnf("Failed to get bitmap for snapshot %v, fallback to real full", snapshot)

		return full, true
	}

	if changedErr != nil {
		log.WithField("changedErr", changedErr).Warnf("Failed to get changed bitmap for snapshot %v, fallback to full", snapshot)
		return allocated, true
	}

	if allocatedErr != nil {
		log.WithField("allocatedErr", allocatedErr).Warnf("Failed to get allocated bitmap for snapshot %v, more data may be taken by the backup", snapshot)
		return changed, false
	}

	changed.And(allocated)
	return changed, false
}

func setChangedBitmap(ctx context.Context, service cbtservice.Service, bitmap types.Bitmap) error {
	if bitmap.ChangeID() == "" {
		return errors.New("change ID is not available")
	}

	if err := service.GetChangedBlocks(ctx, bitmap.Snapshot(), bitmap.ChangeID(), func(blocks []cbtservice.Range) error {
		for _, b := range blocks {
			bitmap.Set(b.Offset, b.Length)
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "error getting changed blocks")
	}

	return nil
}

func setAllocatedBitmap(ctx context.Context, service cbtservice.Service, bitmap types.Bitmap) error {
	if err := service.GetAllocatedBlocks(ctx, bitmap.Snapshot(), func(blocks []cbtservice.Range) error {
		for _, b := range blocks {
			bitmap.Set(b.Offset, b.Length)
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "error getting allocated blocks")
	}

	return nil
}
