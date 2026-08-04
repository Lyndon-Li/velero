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

// SetBitmapOrFull translates the allocated/changed blocks from CBT service to the given bitmap or set the bitmap to full when error happens
func SetBitmapOrFull(ctx context.Context, service cbtservice.Service, bitmap types.Bitmap, log logrus.FieldLogger) (err error) {
	defer func() {
		if err != nil {
			bitmap.SetFull()
		}
	}()

	if service == nil {
		return errors.New("CBT service is absent")
	}

	if bitmap.Snapshot() == "" {
		return errors.New("invalid snapshot")
	}

	if bitmap.ChangeID() == "" {
		log.Info("Begin CBT iteration for allocated blocks")
		err := errors.Wrapf(service.GetAllocatedBlocks(ctx, bitmap.Snapshot(), func(blocks []cbtservice.Range) error {
			for _, b := range blocks {
				bitmap.Set(b.Offset, b.Length)
				log.Infof("CBT range: offset %v, length %v", b.Offset, b.Length)
			}

			return nil
		}), "error getting allocated blocks from CBT service")
		log.Info("End CBT iteration for allocated blocks")

		return err
	}

	log.Info("Begin CBT iteration for changed blocks")
	err = errors.Wrapf(service.GetChangedBlocks(ctx, bitmap.Snapshot(), bitmap.ChangeID(), func(blocks []cbtservice.Range) error {
		for _, b := range blocks {
			bitmap.Set(b.Offset, b.Length)
			log.Infof("CBT range: offset %v, length %v", b.Offset, b.Length)
		}

		return nil
	}), "error getting changed blocks from CBT service")
	log.Info("End CBT iteration for changed blocks")

	return err
}
