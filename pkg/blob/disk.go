package blob

import (
	"context"
	"io"
	"iter"
	"os"
	"path/filepath"
	"strconv"

	"github.com/ZaninAndrea/microdot/pkg/containers"
)

type DiskBucket struct {
	basePath string
}

var _ Bucket = (*DiskBucket)(nil)

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

func (b *DiskBucket) PutObject(ctx context.Context, key string, content io.Reader, replaceExisting bool) (retErr error) {
	fullPath := filepath.Join(b.basePath, key)
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	flags := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	if !replaceExisting {
		flags |= os.O_EXCL
	}

	f, err := os.OpenFile(fullPath, flags, 0644)
	if err != nil {
		if os.IsExist(err) {
			return OBJECT_ALREADY_EXISTS_ERROR
		}

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

func (b *DiskBucket) PutObjectIfMatch(ctx context.Context, key string, content io.Reader, etag string) error {
	fullPath := filepath.Join(b.basePath, key)

	currentEtag, err := computeEtag(fullPath)
	if err != nil {
		return err
	}
	if currentEtag != etag {
		return ETAG_CHANGED_ERROR
	}

	return b.PutObject(ctx, key, content, true)
}

func (b *DiskBucket) GetObject(ctx context.Context, key string) (io.ReadCloser, string, error) {
	fullPath := filepath.Join(b.basePath, key)

	// Compute the ETag as the file's modification time in Unix nanoseconds
	etag, err := computeEtag(fullPath)
	if err != nil {
		return nil, "", err
	}

	// Open the file for reading
	f, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, "", NO_SUCH_KEY_ERROR
		}
		return nil, "", err
	}

	return f, etag, nil
}

func (b *DiskBucket) GetObjectRange(ctx context.Context, key string, start, end int) (io.ReadCloser, error) {
	fullPath := filepath.Join(b.basePath, key)
	f, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, NO_SUCH_KEY_ERROR
		}
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

func (b *DiskBucket) DeleteObject(ctx context.Context, key string, ifMatch *string) error {
	fullPath := filepath.Join(b.basePath, key)

	if ifMatch != nil {
		currentEtag, err := computeEtag(fullPath)
		if err != nil {
			return err
		}
		if currentEtag != *ifMatch {
			return ETAG_CHANGED_ERROR
		}
	}

	err := os.Remove(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return NO_SUCH_KEY_ERROR
		}
		return err
	}
	return nil
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

func computeEtag(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}

	return strconv.FormatInt(info.ModTime().UnixNano(), 10), nil
}
