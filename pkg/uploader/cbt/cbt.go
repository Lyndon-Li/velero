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
	"math"

	"github.com/RoaringBitmap/roaring"
)

type CBT interface {
	Set(int64, int64)
	Iterator() Iterator
}

type Iterator interface {
	SourceID() string
	ChangeID() string
	BlockSize() int
	Count() uint64
	Next() (int64, bool)
}

const (
	InvalidOffset64 = ^int64(0)
)

type cbtImpl struct {
	bitmap       *roaring.Bitmap
	blockSize    int
	blockSizeLog int
	sourceID     string
	changeID     string
}

type cbtIterator struct {
	cbtImpl
	iterator roaring.IntPeekable
}

func NewCBT(blockSize int, length int64, sourceID string, changeID string) CBT {
	return &cbtImpl{
		bitmap:       roaring.New(),
		blockSize:    blockSize,
		blockSizeLog: int(math.Log2(float64(blockSize))),
		sourceID:     sourceID,
		changeID:     changeID,
	}
}

func (c *cbtImpl) Set(offset, length int64) {
	start := uint64(offset >> c.blockSizeLog)
	end := uint64((offset + length + int64(c.blockSize) - 1) >> c.blockSizeLog)

	c.bitmap.AddRange(start, end)
}

func (c *cbtImpl) Iterator() Iterator {
	if c.bitmap == nil {
		return nil
	}

	return &cbtIterator{
		cbtImpl:  *c,
		iterator: c.bitmap.Iterator(),
	}
}

func (c *cbtIterator) Next() (int64, bool) {
	if !c.iterator.HasNext() {
		return InvalidOffset64, false
	}

	return int64(c.iterator.Next()) << c.blockSizeLog, true
}

func (c *cbtIterator) Count() uint64 {
	return c.bitmap.GetCardinality()
}

func (c *cbtIterator) BlockSize() int {
	return c.blockSize
}

func (c *cbtIterator) SourceID() string {
	return c.sourceID
}

func (c *cbtIterator) ChangeID() string {
	return c.changeID
}
