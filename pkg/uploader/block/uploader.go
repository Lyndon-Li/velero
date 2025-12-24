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
	"context"
	"io"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/vmware-tanzu/velero/pkg/repository/udmrepo"
	"github.com/vmware-tanzu/velero/pkg/uploader"
	"github.com/vmware-tanzu/velero/pkg/uploader/cbt"
	"github.com/vmware-tanzu/velero/pkg/uploader/util/freelist"
)

var ErrCanceled = errors.New("uploader is canceled")

type sourceInfo struct {
	dev        *os.File
	realSource string
	size       int64
}

type destInfo struct {
	dev  *os.File
	path string
}

type Uploader interface {
	Backup(sourceInfo, *udmrepo.Snapshot, cbt.Iterator, map[string]string) (udmrepo.Snapshot, int64, error)
	Restore(udmrepo.Snapshot, destInfo, map[string]string) (int64, error)
}

type blockUploader struct {
	ctx        context.Context
	repoWriter udmrepo.BackupRepo
	progress   uploader.ProgressUpdater
	log        logrus.FieldLogger
}

func NewUploader(ctx context.Context, repoWriter udmrepo.BackupRepo, progress uploader.ProgressUpdater, log logrus.FieldLogger) Uploader {
	return &blockUploader{
		ctx:        ctx,
		repoWriter: repoWriter,
		progress:   progress,
		log:        log,
	}
}

func (bu *blockUploader) Backup(source sourceInfo, parent *udmrepo.Snapshot, cbt cbt.Iterator, configs map[string]string) (udmrepo.Snapshot, int64, error) {
	snapStart := bu.repoWriter.Time()

	destObj := bu.repoWriter.NewObjectWriter(bu.ctx, udmrepo.ObjectWriteOptions{
		Description:  "FILE:" + source.realSource,
		Prefix:       "",
		DataType:     udmrepo.ObjectDataTypeData,
		FullPath:     source.realSource,
		AccessMode:   udmrepo.ObjectDataAccessModeFile,
		ParentObject: parent.RootObject,
	})

	defer destObj.Close()

	id, backupSize, err := bu.BackupData(source.dev, destObj, cbt, source.size)
	if err != nil {
		return udmrepo.Snapshot{}, 0, errors.Wrap(err, "error to backup file with incremental")
	}

	backupMode := udmrepo.ObjectDataBackupModeInc
	if parent == nil {
		backupMode = udmrepo.ObjectDataBackupModeFull
	}

	entryId, err := bu.repoWriter.WriteMetadata(bu.ctx, &udmrepo.Metadata{
		SubObjects: []udmrepo.ObjectMetadata{
			{
				ID:       id,
				Type:     udmrepo.ObjectDataTypeData,
				FileSize: source.size,
			},
		},
	},
		udmrepo.ObjectWriteOptions{
			FullPath:    "/",
			DataType:    udmrepo.ObjectDataTypeMetadata,
			Description: "block backup root",
			Prefix:      "z",
			AccessMode:  udmrepo.ObjectDataAccessModeFile,
			BackupMode:  backupMode,
		})
	if err != nil {
		return udmrepo.Snapshot{}, 0, errors.Wrap(err, "error to write metadata")
	}

	snapEnd := bu.repoWriter.Time()

	return udmrepo.Snapshot{
		StartTime:   snapStart,
		EndTime:     snapEnd,
		Description: "",
		RootObject:  entryId,
		Size:        source.size,
	}, backupSize, nil
}

func (bu *blockUploader) Restore(snapshot udmrepo.Snapshot, dest destInfo, configs map[string]string) (int64, error) {
	snap, err := bu.repoWriter.GetSnapshot(bu.ctx, snapshot.ID)
	if err != nil {
		return 0, errors.Wrapf(err, "error loading snapshot manifest, id %v", snapshot.ID)
	}

	meta, err := bu.repoWriter.ReadMetadata(bu.ctx, snap.RootObject)
	if err != nil {
		return 0, errors.Wrapf(err, "error readding snapshot metadata for root %v", snap.RootObject)
	}

	reader, err := bu.repoWriter.OpenObject(bu.ctx, meta.SubObjects[0].ID)
	if err != nil {
		return 0, errors.Wrapf(err, "error opening object %v", meta.SubObjects[0])
	}
	defer reader.Close()

	size, err := bu.copyDataFull(reader, dest.dev, snapshot.Size)
	if err != nil {
		return 0, errors.Wrapf(err, "error restoring data to volume %s", dest.path)
	}

	return size, nil
}

func (bu *blockUploader) BackupData(dev *os.File, dest udmrepo.ObjectWriter, cbt cbt.Iterator, totalLength int64) (udmrepo.ID, int64, error) {
	var size int64
	var err error

	if cbt == nil {
		size, err = bu.copyDataFull(dev, dest, totalLength)
		if err != nil {
			return "", size, errors.Wrap(err, "error copying file data full")
		}
	} else {
		size, err = bu.copyDataIncremental(dev, dest, cbt, totalLength)
		if err != nil {
			return "", size, errors.Wrap(err, "error copying file data incremental")
		}
	}

	id, err := dest.Result()
	return id, size, err
}

type readResult struct {
	buffer []byte
	offset int64
	length int
	err    error
}

func (r *readResult) resetBuffer(list *freelist.FreeList) {
	if r.buffer != nil {
		list.Return(r.buffer)
		r.buffer = nil
	}
}

const (
	bufferSize        = 100 << 20
	fullReadBlockSize = 2 << 20
)

func (bu *blockUploader) copyDataFull(reader io.Reader, writer io.Writer, totalLength int64) (int64, error) {
	list := freelist.New(bufferSize, fullReadBlockSize)
	resultChan := make(chan readResult, list.Capacity())

	quit := make(chan struct{})
	defer close(quit)

	go func() {
		defer close(resultChan)

		for {
			var buffer []byte

			select {
			case <-bu.ctx.Done():
				return
			case <-quit:
				return
			case buffer = <-list.Chunks():
			}

			length, err := reader.Read(buffer)
			if err == nil && length <= 0 {
				err = io.ErrUnexpectedEOF
			}

			r := readResult{
				buffer: buffer,
				length: length,
				err:    err,
			}

			if r.err != nil {
				r.resetBuffer(list)
			}

			resultChan <- r

			if r.err != nil {
				return
			}
		}
	}()

	var written int64
	var result readResult
	var writeErr error
	var readerRunning bool

	for written < totalLength {
		select {
		case <-bu.ctx.Done():
			writeErr = ErrCanceled
		case result, readerRunning = <-resultChan:
			if !readerRunning {
				if bu.ctx.Err() != nil {
					writeErr = ErrCanceled
				} else {
					writeErr = io.ErrUnexpectedEOF
				}
			}
		}

		if writeErr != nil {
			break
		}

		if result.err != nil {
			writeErr = result.err
			break
		}

		n, err := writer.Write(result.buffer[0:result.length])
		if err != nil {
			writeErr = err
			break
		}

		if result.length != n {
			writeErr = io.ErrShortWrite
			break
		}

		written += int64(n)
		result.resetBuffer(list)

		bu.progress.UpdateProgress(&uploader.Progress{BytesDone: written, TotalBytes: totalLength})
	}

	result.resetBuffer(list)

	if writeErr != nil {
		return written, writeErr
	}

	return written, nil
}

func (bu *blockUploader) copyDataIncremental(reader io.ReaderAt, writer udmrepo.ObjectWriter, cbt cbt.Iterator, totalLength int64) (int64, error) {
	list := freelist.New(bufferSize, cbt.BlockSize())
	resultChan := make(chan readResult, list.Capacity())
	totalCount := cbt.Count()

	quit := make(chan struct{})
	defer close(quit)

	go func() {
		defer close(resultChan)

		offset, valid := cbt.Next()
		var buffer []byte
		for valid {
			select {
			case <-bu.ctx.Done():
				return
			case <-quit:
				return
			case buffer = <-list.Chunks():
			}

			length, err := reader.ReadAt(buffer, offset)
			if err == nil && length <= 0 {
				err = io.ErrUnexpectedEOF
			}

			r := readResult{
				buffer: buffer,
				offset: offset,
				length: length,
				err:    err,
			}

			if r.err != nil {
				r.resetBuffer(list)
			}

			resultChan <- r

			if r.err != nil {
				return
			}

			offset, valid = cbt.Next()
		}
	}()

	var lastPos int64
	var result readResult
	var written int64
	var curCount int64
	var writeErr error
	var readerRunning bool

	for curCount < int64(totalCount) {
		select {
		case <-bu.ctx.Done():
			writeErr = ErrCanceled
		case result, readerRunning = <-resultChan:
			if !readerRunning {
				if bu.ctx.Err() != nil {
					writeErr = ErrCanceled
				} else {
					writeErr = io.ErrUnexpectedEOF
				}
			}
		}

		if writeErr != nil {
			break
		}

		if result.err != nil {
			writeErr = result.err
			break
		}

		n, err := writer.WriteAt(result.buffer[0:result.length], result.offset)
		if err != nil {
			writeErr = err
			break
		}

		if result.length != n {
			writeErr = io.ErrShortWrite
			break
		}

		written += int64(result.length)
		lastPos = result.offset + int64(result.length)
		result.resetBuffer(list)
		curCount++

		bu.progress.UpdateProgress(&uploader.Progress{BytesDone: lastPos, TotalBytes: totalLength})
	}

	result.resetBuffer(list)

	if writeErr != nil {
		return written, writeErr
	}

	if lastPos != totalLength {
		if _, err := writer.WriteAt(nil, totalLength); err != nil {
			return written, errors.Wrap(err, "unable to clone from parent object")
		}

		bu.progress.UpdateProgress(&uploader.Progress{BytesDone: totalLength, TotalBytes: totalLength})
	}

	return written, nil
}
