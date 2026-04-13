package cbtservice

import "context"

type Record struct {
	Offset int64
	Length int64
}

type ChangeID string

type Service interface {
	GetAllocatedBlocks(ctx context.Context, changeId ChangeID, record func(Record) error) error
	GetChangedBlocks(ctx context.Context, base ChangeID, target ChangeID, record func(Record) error) error
}
