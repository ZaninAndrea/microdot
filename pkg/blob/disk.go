package blob

import (
	"context"
	"io"
	"os"
	"path/filepath"
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

func (b *DiskBucket) DeleteObject(ctx context.Context, key string) error {
	fullPath := filepath.Join(b.basePath, key)
	return os.Remove(fullPath)
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
