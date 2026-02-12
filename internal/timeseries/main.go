package timeseries

import (
	"context"
	"iter"
)

type DB interface {
	ListStreams(ctx context.Context, labels Labels) ([]Stream, error)
	ListEvents(ctx context.Context, streams []uint64, start, end uint64) iter.Seq[Event]

	StoreEvents(ctx context.Context, events []EventIngest) error

	Close() error
}

type Stream interface {
	ID() uint64
	Labels() Labels
}

type Labels map[string]string

type Event interface {
	Error() error
	Timestamp() uint64
	Data() map[string]any
	Labels() Labels
}

type EventIngest struct {
	StreamLabels Labels
	Timestamp    uint64
	Data         map[string]any
}
