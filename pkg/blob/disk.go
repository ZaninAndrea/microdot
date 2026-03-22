package blob

import (
	"context"
	"io"
	"iter"
	"os"
	"path/filepath"

	"github.com/ZaninAndrea/microdot/pkg/containers"
)

type DiskBucket struct {
	basePath string
}

func NewDiskBucket(basePath string) (*DiskBucket, error) {
	absPath, err := filepath.Abs(basePath)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(absPath, 0755); err != nil {
		return nil, err
	}
	return &DiskBucket{
		basePath: absPath,
	}, nil
}

func (b *DiskBucket) PutObject(ctx context.Context, key string, content io.Reader) (retErr error) {
	fullPath := filepath.Join(b.basePath, key)
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	f, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	defer func() {
		closeErr := f.Close()
		if retErr == nil {
			retErr = closeErr
		}
	}()

	_, retErr = io.Copy(f, content)
	return
}

func (b *DiskBucket) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	fullPath := filepath.Join(b.basePath, key)
	return os.Open(fullPath)
}

func (b *DiskBucket) GetObjectRange(ctx context.Context, key string, start, end int) (io.ReadCloser, error) {
	fullPath := filepath.Join(b.basePath, key)
	f, err := os.Open(fullPath)
	if err != nil {
		return nil, err
	}

	if _, err := f.Seek(int64(start), io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}

	limit := int64(end - start + 1)
	return &limitReadCloser{
		r: io.LimitReader(f, limit),
		c: f,
	}, nil
}

type limitReadCloser struct {
	r io.Reader
	c io.Closer
}

func (l *limitReadCloser) Read(p []byte) (int, error) {
	return l.r.Read(p)
}

func (l *limitReadCloser) Close() error {
	return l.c.Close()
}

func (b *DiskBucket) DeleteObject(ctx context.Context, key string) error {
	fullPath := filepath.Join(b.basePath, key)
	return os.Remove(fullPath)
}

func (b *DiskBucket) ListObjects(ctx context.Context, prefix string) iter.Seq[containers.Result[string]] {
	return func(yield func(containers.Result[string]) bool) {
		err := filepath.Walk(filepath.Join(b.basePath, prefix), func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				relPath, err := filepath.Rel(b.basePath, path)
				if err != nil {
					return err
				}
				if !yield(containers.Ok(relPath)) {
					return io.EOF
				}
			}
			return nil
		})
		if err != nil && err != io.EOF {
			yield(containers.Err[string](err))
		}
	}
}
