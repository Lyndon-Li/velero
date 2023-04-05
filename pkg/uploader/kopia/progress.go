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

package kopia

import (
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/vmware-tanzu/velero/pkg/uploader"
)

func NewKopiProgress(interval time.Duration, updater uploader.ProgressUpdater, log logrus.FieldLogger) *KopiaProgress {
	p := &KopiaProgress{
		updater: updater,
		log:     log,
		stop:    make(chan struct{}),
	}

	go func() {
		ticker := time.NewTicker(interval)
		lastUpdate := progressData{}
		for {
			select {
			case <-p.stop:
				ticker.Stop()
				return
			case <-ticker.C:
				lastUpdate = p.UpdateProgress(lastUpdate)
			}
		}
	}()

	return p
}

type progressData struct {
	// all int64 must precede all int32 due to alignment requirements on ARM
	// +checkatomic
	uploadedBytes int64 //the total bytes has uploaded
	cachedBytes   int64 //the total bytes has cached
	hashededBytes int64 //the total bytes has hashed
	// +checkatomic
	uploadedFiles int32 //the total files has ignored
	// +checkatomic
	ignoredErrorCount int32 //the total errors has ignored
	// +checkatomic
	fatalErrorCount     int32 //the total errors has occurred
	estimatedFileCount  int32 // +checklocksignore the total count of files to be processed
	estimatedTotalBytes int64 // +checklocksignore	the total size of files to be processed
	// +checkatomic
	processedBytes int64 // which statistic all bytes has been processed currently
}

// KopiaProgress represents a backup or restore counters.
type KopiaProgress struct {
	data progressData

	updater uploader.ProgressUpdater //which kopia progress will call the UpdateProgress interface, the third party will implement the interface to do the progress update
	log     logrus.FieldLogger       // output info into log when backup
	stop    chan struct{}
}

func (p *KopiaProgress) Stop() {
	close(p.stop)
}

//UploadedBytes the total bytes has uploaded currently
func (p *KopiaProgress) UploadedBytes(numBytes int64) {
	atomic.AddInt64(&p.data.uploadedBytes, numBytes)
	atomic.AddInt32(&p.data.uploadedFiles, 1)
}

//Error statistic the total Error has occurred
func (p *KopiaProgress) Error(path string, err error, isIgnored bool) {
	if isIgnored {
		atomic.AddInt32(&p.data.ignoredErrorCount, 1)
		p.log.Warnf("Ignored error when processing %v: %v", path, err)
	} else {
		atomic.AddInt32(&p.data.fatalErrorCount, 1)
		p.log.Errorf("Error when processing %v: %v", path, err)
	}
}

//EstimatedDataSize statistic the total size of files to be processed and total files to be processed
func (p *KopiaProgress) EstimatedDataSize(fileCount int, totalBytes int64) {
	atomic.StoreInt64(&p.data.estimatedTotalBytes, totalBytes)
	atomic.StoreInt32(&p.data.estimatedFileCount, int32(fileCount))
}

//UpdateProgress which calls Updater UpdateProgress interface, update progress by third-party implementation
func (p *KopiaProgress) UpdateProgress(lastUpdate progressData) progressData {
	newData := progressData{
		estimatedTotalBytes: atomic.LoadInt64(&p.data.estimatedTotalBytes),
		processedBytes:      atomic.LoadInt64(&p.data.processedBytes),
	}

	if newData.estimatedTotalBytes > lastUpdate.estimatedTotalBytes || newData.processedBytes > lastUpdate.processedBytes {
		p.updater.UpdateProgress(&uploader.UploaderProgress{TotalBytes: newData.estimatedTotalBytes, BytesDone: newData.processedBytes})
		return newData
	} else {
		return lastUpdate
	}
}

//UploadStarted statistic the total Error has occurred
func (p *KopiaProgress) UploadStarted() {}

//CachedFile statistic the total bytes been cached currently
func (p *KopiaProgress) CachedFile(fname string, numBytes int64) {
	atomic.AddInt64(&p.data.cachedBytes, numBytes)
}

//HashedBytes statistic the total bytes been hashed currently
func (p *KopiaProgress) HashedBytes(numBytes int64) {
	atomic.AddInt64(&p.data.processedBytes, numBytes)
	atomic.AddInt64(&p.data.hashededBytes, numBytes)
}

//HashingFile statistic the file been hashed currently
func (p *KopiaProgress) HashingFile(fname string) {}

//ExcludedFile statistic the file been excluded currently
func (p *KopiaProgress) ExcludedFile(fname string, numBytes int64) {}

//ExcludedDir statistic the dir been excluded currently
func (p *KopiaProgress) ExcludedDir(dirname string) {}

//FinishedHashingFile which will called when specific file finished hash
func (p *KopiaProgress) FinishedHashingFile(fname string, numBytes int64) {
}

//StartedDirectory called when begin to upload one directory
func (p *KopiaProgress) StartedDirectory(dirname string) {}

//FinishedDirectory called when finish to upload one directory
func (p *KopiaProgress) FinishedDirectory(dirname string) {
}

//UploadFinished which report the files flushed after the Upload has completed.
func (p *KopiaProgress) UploadFinished() {
}

//ProgressBytes which statistic all bytes has been processed currently
func (p *KopiaProgress) ProgressBytes(processedBytes int64, totalBytes int64) {
	atomic.StoreInt64(&p.data.processedBytes, processedBytes)
	atomic.StoreInt64(&p.data.estimatedTotalBytes, totalBytes)
}
