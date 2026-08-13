/*
Copyright the Velero contributors.

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

package archive

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/vmware-tanzu/velero/pkg/util/filesystem"
)

// Extractor unzips/extracts a backup tarball to a local
// temp directory.
type Extractor struct {
	log                logrus.FieldLogger
	fs                 filesystem.Interface
	maxExtractionSize  int64
	totalExtractedSize int64
}

func NewExtractor(log logrus.FieldLogger, fs filesystem.Interface, maxExtractionSize int64) *Extractor {
	return &Extractor{
		log:                log,
		fs:                 fs,
		maxExtractionSize:  maxExtractionSize,
		totalExtractedSize: 0,
	}
}

// UnzipAndExtractBackup extracts a reader on a gzipped tarball to a local temp directory
func (e *Extractor) UnzipAndExtractBackup(src io.Reader) (string, error) {
	gzr, err := gzip.NewReader(src)
	if err != nil {
		e.log.Infof("error creating gzip reader: %v", err)
		return "", err
	}
	defer gzr.Close()

	return e.readBackup(tar.NewReader(gzr))
}

// limitReader is an io.Reader that reads from r but returns an error if the amount of data read exceeds limit.
type limitReader struct {
	r     io.Reader
	limit int64
	read  int64
}

func (l *limitReader) Read(p []byte) (n int, err error) {
	if l.read >= l.limit {
		// We reached the limit. Check if there is more data.
		var b [1]byte
		if n, err := l.r.Read(b[:]); n > 0 || err == nil {
			return 0, fmt.Errorf("decompressed backup exceeds maximum allowed size of %d bytes", l.limit)
		} else if err != io.EOF {
			return 0, err
		}
		return 0, io.EOF
	}
	if int64(len(p)) > l.limit-l.read {
		p = p[:l.limit-l.read]
	}
	n, err = l.r.Read(p)
	l.read += int64(n)
	return n, err
}

func (e *Extractor) writeFile(target string, r io.Reader) error {
	file, err := e.fs.Create(target)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := io.Copy(file, r); err != nil {
		return err
	}
	return nil
}

// sanitizeArchivePath sanitizes archive file path from "G305: Zip Slip vulnerability"
func sanitizeArchivePath(destDir, sourcePath string) (targetPath string, err error) {
	targetPath = filepath.Join(destDir, sourcePath)
	if strings.HasPrefix(targetPath, filepath.Clean(destDir)) {
		return targetPath, nil
	}

	return "", fmt.Errorf("invalid archive path %q: escapes target directory", sourcePath)
}

func (e *Extractor) readBackup(tarRdr *tar.Reader) (string, error) {
	dir, err := e.fs.TempDir("", "")
	if err != nil {
		e.log.Infof("error creating temp dir: %v", err)
		return "", err
	}

	for {
		header, err := tarRdr.Next()

		if err == io.EOF {
			break
		}
		if err != nil {
			e.log.Infof("error reading tar: %v", err)
			return "", err
		}

		// Enforce maximum file size from the header to prevent memory/storage exhaustion
		// before we even start reading the file contents.
		maxSize := e.maxExtractionSize
		if header.Size > maxSize {
			err := fmt.Errorf("decompressed backup exceeds maximum allowed size of %d bytes", maxSize)
			e.log.Infof("error checking file size: %v", err)
			return "", err
		}

		// Also check if the cumulative size of all files exceeds the maximum size
		// This prevents zip bombs that consist of millions of tiny files
		if e.totalExtractedSize+header.Size > maxSize {
			err := fmt.Errorf("total decompressed backup size exceeds maximum allowed size of %d bytes", maxSize)
			e.log.Infof("error checking total extracted size: %v", err)
			return "", err
		}
		e.totalExtractedSize += header.Size

		target, err := sanitizeArchivePath(dir, header.Name)
		if err != nil {
			e.log.Infof("error sanitizing archive path: %s", err.Error())
			return "", err
		}

		switch header.Typeflag {
		case tar.TypeDir:
			err := e.fs.MkdirAll(target, header.FileInfo().Mode())
			if err != nil {
				e.log.Infof("mkdirall error: %v", err)
				return "", err
			}

		case tar.TypeReg:
			// make sure we have the directory created
			err := e.fs.MkdirAll(filepath.Dir(target), header.FileInfo().Mode())
			if err != nil {
				e.log.Infof("mkdirall error: %v", err)
				return "", err
			}

			// Limit the maximum size of a single file to prevent zip bombs
			// This protects against forged headers where header.Size is small but the actual data is large
			lr := &limitReader{
				r:     tarRdr,
				limit: maxSize - (e.totalExtractedSize - header.Size), // Limit to whatever is remaining of our quota
			}

			// create the file
			if err := e.writeFile(target, lr); err != nil {
				e.log.Infof("error copying: %v", err)
				return "", err
			}
		}
	}

	return dir, nil
}
