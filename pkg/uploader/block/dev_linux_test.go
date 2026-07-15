//go:build linux
// +build linux

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

package block

import (
	"os"
	"syscall"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlkZeroOut(t *testing.T) {
	t.Run("closed file returns error", func(t *testing.T) {
		f, err := os.CreateTemp(t.TempDir(), "blkzeroout-test-*")
		require.NoError(t, err)
		err = f.Close()
		require.NoError(t, err)

		err = blkZeroOut(f, 0, 1024)
		assert.Error(t, err)
	})

	t.Run("regular file returns ioctl error", func(t *testing.T) {
		f, err := os.CreateTemp(t.TempDir(), "blkzeroout-test-*")
		require.NoError(t, err)
		defer f.Close()

		err = blkZeroOut(f, 0, 1024)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "error calling ioctl on block dev")

		// On regular files, ioctl with BLKZEROOUT should fail with ENOTTY (inappropriate ioctl for device) or EINVAL
		isENOTTY := errors.Is(err, syscall.ENOTTY)
		isEINVAL := errors.Is(err, syscall.EINVAL)
		assert.True(t, isENOTTY || isEINVAL, "expected error to be ENOTTY or EINVAL, got: %v", err)
	})
}
