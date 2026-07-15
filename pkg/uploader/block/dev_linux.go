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
	"unsafe"

	"github.com/cockroachdb/errors"
)

// implement in following PRs
func openBlockDevice(path string, read bool) (*os.File, error) {
	return nil, errors.New("Not implemented")
}

func blkZeroOut(dest *os.File, start int64, length int64) error {
	const BLKZEROOUT = 0x127b

	zeroRange := [2]uint64{uint64(start), uint64(length)}

	rawConn, err := dest.SyscallConn()
	if err != nil {
		return errors.Wrap(err, "error getting raw connection")
	}

	var ioctlErr syscall.Errno = 0
	if err := rawConn.Control(func(fd uintptr) {
		if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, BLKZEROOUT, uintptr(unsafe.Pointer(&zeroRange[0]))); errno != 0 {
			ioctlErr = errno
		}
	}); err != nil {
		return errors.Wrap(err, "error control block dev")
	}

	if ioctlErr != 0 {
		return errors.Errorf("error calling ioctl on block dev, err %v", ioctlErr)
	}

	return nil
}
