package blob

import (
	"context"
	"io"
	"iter"

	"github.com/ZaninAndrea/microdot/pkg/containers"
)

type Bucket interface {
	PutObject(ctx context.Context, key string, content io.Reader) error
	GetObject(ctx context.Context, key string) (io.ReadCloser, error)
	GetObjectRange(ctx context.Context, key string, start, end int) (io.ReadCloser, error)
	DeleteObject(ctx context.Context, key string) error
	ListObjects(ctx context.Context, prefix string) iter.Seq[containers.Result[string]]
}
