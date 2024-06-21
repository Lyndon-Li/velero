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

package logging

import (
	"bytes"
	"io"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	ListeningLevel   = logrus.ErrorLevel
	ListeningMessage = "merge-log-57847fd0-0c7c-48e3-b5f7-984b293d8376"
	LogSourceKey     = "log-source"
)

type MergeHook struct {
}

type HookWriter struct {
	orgWriter io.Writer
	source    string
	logger    *logrus.Logger
}

func newHookWriter(orgWriter io.Writer, source string, logger *logrus.Logger) io.Writer {
	return &HookWriter{
		orgWriter: orgWriter,
		source:    source,
		logger:    logger,
	}
}

func (h *MergeHook) Levels() []logrus.Level {
	return []logrus.Level{ListeningLevel}
}

func (h *MergeHook) Fire(entry *logrus.Entry) error {
	if entry.Message != ListeningMessage {
		return nil
	}

	source, exist := entry.Data[LogSourceKey]
	if !exist {
		return nil
	}

	entry.Logger.SetOutput(newHookWriter(entry.Logger.Out, source.(string), entry.Logger))

	return nil
}

func (w *HookWriter) Write(p []byte) (n int, err error) {
	if !bytes.Contains(p, []byte(ListeningMessage)) {
		return w.orgWriter.Write(p)
	}

	defer func() {
		w.logger.Out = w.orgWriter
	}()

	sourceFile, err := os.OpenFile(w.source, os.O_RDONLY, 0600)
	if err != nil {
		return 0, err
	}
	defer sourceFile.Close()

	total := 0

	buffer := make([]byte, 2048)
	for {
		read, err := sourceFile.Read(buffer)
		if err == io.EOF {
			return total, nil
		}

		if err != nil {
			return total, errors.Wrapf(err, "error to read source file %s at pos %v", w.source, total)
		}

		written, err := w.orgWriter.Write(buffer[0:read])
		if err != nil {
			return total, errors.Wrapf(err, "error to write log at pos %v", total)
		}

		if written != read {
			return total, errors.Errorf("error to write log at pos %v, read %v but written %v", total, read, written)
		}

		total += read
	}
}
