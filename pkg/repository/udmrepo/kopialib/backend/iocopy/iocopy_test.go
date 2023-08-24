package iocopy

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testBuf    = "Hello, World!"
	lenTestBuf = len(testBuf)
)

type errorWriter struct{}

func (errorWriter) Write(p []byte) (n int, err error) {
	return 0, errors.New("write error") //nolint:goerr113
}

func TestGetBuffer(t *testing.T) {
	buf := GetBuffer()
	require.Len(t, buf, BufSize)
}

func TestReleaseBuffer(t *testing.T) {
	buf := GetBuffer()
	ReleaseBuffer(buf)
	buf2 := GetBuffer()
	require.Equal(t, &buf[0], &buf2[0], "Buffer was not recycled after ReleaseBuffer")
}

func TestCopy(t *testing.T) {
	src := strings.NewReader(testBuf)
	dst := &bytes.Buffer{}

	n, err := Copy(dst, src)
	require.NoError(t, err)
	require.Equal(t, n, int64(lenTestBuf))
	require.Equal(t, dst.String(), testBuf)
}

func TestJustCopy(t *testing.T) {
	src := strings.NewReader(testBuf)
	dst := &bytes.Buffer{}

	err := JustCopy(dst, src)
	require.NoError(t, err)
	require.NoError(t, err)
	require.Equal(t, dst.String(), testBuf)
}

func TestCopyError(t *testing.T) {
	src := strings.NewReader(testBuf)
	dst := errorWriter{}

	_, err := Copy(dst, src)
	require.Error(t, err)
}

func TestJustCopyError(t *testing.T) {
	src := strings.NewReader(testBuf)
	dst := errorWriter{}

	err := JustCopy(dst, src)
	require.Error(t, err)
}

type customReader struct {
	io.Reader
}

func TestCustomReader(t *testing.T) {
	src := customReader{strings.NewReader(testBuf)}
	dst := &bytes.Buffer{}

	n, err := Copy(dst, src)
	require.NoError(t, err)
	require.Equal(t, n, int64(lenTestBuf))
	require.Equal(t, dst.String(), testBuf)
}

type customWriter struct {
	io.Writer
}

func TestCopyWithCustomReaderAndWriter(t *testing.T) {
	src := customReader{strings.NewReader(testBuf)}
	dst := &bytes.Buffer{}
	customDst := customWriter{dst}

	n, err := Copy(customDst, src)
	require.NoError(t, err)
	require.Equal(t, n, int64(lenTestBuf))
	require.Equal(t, dst.String(), testBuf)
}
