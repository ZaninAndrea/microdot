package blob

import (
	"context"
	"io"
)

type Bucket interface {
	PutObject(ctx context.Context, key string, content io.Reader) error
	GetObject(ctx context.Context, key string) (io.ReadCloser, error)
	GetObjectRange(ctx context.Context, key string, start, end int) (io.ReadCloser, error)
	DeleteObject(ctx context.Context, key string) error
}
