package blob

import (
	"context"
	"fmt"
	"io"
	"iter"

	"github.com/ZaninAndrea/microdot/pkg/containers"
)

var NO_SUCH_KEY_ERROR = fmt.Errorf("NO_SUCH_KEY")
var ETAG_CHANGED_ERROR = fmt.Errorf("ETAG_CHANGED")
var OBJECT_ALREADY_EXISTS_ERROR = fmt.Errorf("OBJECT_ALREADY_EXISTS")

type Bucket interface {
	PutObject(ctx context.Context, key string, content io.Reader, replaceExisting bool) error
	PutObjectIfMatch(ctx context.Context, key string, content io.Reader, etag string) error
	GetObject(ctx context.Context, key string) (reader io.ReadCloser, etag string, err error)
	GetObjectRange(ctx context.Context, key string, start, end int) (io.ReadCloser, error)
	DeleteObject(ctx context.Context, key string, ifMatch *string) error
	ListObjects(ctx context.Context, prefix string) iter.Seq[containers.Result[string]]
}
