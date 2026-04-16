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
	"math/bits"

	"github.com/RoaringBitmap/roaring"
)

type Bitmap interface {
	Set(int64, int64)
	SetFull()
	SourceID() string
	ChangeID() string
	Iterator() BitmapIterator
}

type BitmapIterator interface {
	SourceID() string
	ChangeID() string
	BlockSize() int
	Count() uint64
	Next() (int64, bool)
}

const (
	InvalidOffset64 = ^int64(0)
)

type bitmapImpl struct {
	bitmap       *roaring.Bitmap
	blockSize    int
	blockSizeLog int
	length       int64
	sourceID     string
	changeID     string
}

type bitmapIterator struct {
	bitmapImpl
	iterator roaring.IntPeekable
}

func NewBitmap(blockSize int, length int64, sourceID string, changeID string) Bitmap {
	return &bitmapImpl{
		bitmap:       roaring.New(),
		blockSize:    blockSize,
		blockSizeLog: bits.Len(uint(blockSize)) - 1,
		length:       length,
		sourceID:     sourceID,
		changeID:     changeID,
	}
}

func (c *bitmapImpl) Set(offset, length int64) {
	start := uint64(offset >> c.blockSizeLog)
	end := uint64((offset + length + int64(c.blockSize) - 1) >> c.blockSizeLog)

	c.bitmap.AddRange(start, end)
}

func (c *bitmapImpl) SetFull() {
	start := uint64(0)
	end := uint64((c.length + int64(c.blockSize) - 1) >> c.blockSizeLog)

	c.bitmap.AddRange(start, end)
}

func (c *bitmapImpl) SourceID() string {
	return c.sourceID
}

func (c *bitmapImpl) ChangeID() string {
	return c.changeID
}

func (c *bitmapImpl) Iterator() BitmapIterator {
	if c.bitmap == nil {
		return nil
	}

	return &bitmapIterator{
		bitmapImpl: *c,
		iterator:   c.bitmap.Iterator(),
	}
}

func (c *bitmapIterator) Next() (int64, bool) {
	if !c.iterator.HasNext() {
		return InvalidOffset64, false
	}

	return int64(c.iterator.Next()) << c.blockSizeLog, true
}

func (c *bitmapIterator) Count() uint64 {
	return c.bitmap.GetCardinality()
}

func (c *bitmapIterator) BlockSize() int {
	return c.blockSize
}

func (c *bitmapIterator) SourceID() string {
	return c.sourceID
}

func (c *bitmapIterator) ChangeID() string {
	return c.changeID
}
