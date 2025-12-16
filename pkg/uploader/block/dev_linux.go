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
	"path/filepath"
	"syscall"

	"github.com/pkg/errors"
)

const ErrNotPermitted = "operation not permitted"

func openBlockDevice(path string, read bool) (*os.File, error) {
	devPath, err := resolveSymlink(path)
	if err != nil {
		return nil, errors.Wrap(err, "resolveSymlink")
	}

	fileInfo, err := os.Lstat(devPath)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get thedevice information %s", devPath)
	}

	if (fileInfo.Sys().(*syscall.Stat_t).Mode & syscall.S_IFMT) != syscall.S_IFBLK {
		return nil, errors.Errorf("path %s is not a block device", devPath)
	}

	flag := os.O_RDWR
	mode := os.FileMode(0666)
	if read {
		flag = os.O_RDONLY
		mode = 0
	}

	device, err := os.OpenFile(devPath, flag|syscall.O_DIRECT, mode)
	if err != nil {
		if os.IsPermission(err) || err.Error() == ErrNotPermitted {
			return nil, errors.Wrapf(err, "no permission to open device %s, make sure that node agent is running in privileged mode", devPath)
		}
		return nil, errors.Wrapf(err, "unable to open device %s", devPath)
	}

	return device, nil
}

func resolveSymlink(path string) (string, error) {
	st, err := os.Lstat(path)
	if err != nil {
		return "", errors.Wrap(err, "stat")
	}

	if (st.Mode() & os.ModeSymlink) == 0 {
		return path, nil
	}

	return filepath.EvalSymlinks(path)
}
