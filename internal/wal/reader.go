package wal

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"iter"

	"github.com/ZaninAndrea/microdot/pkg/blob"
	"github.com/ZaninAndrea/microdot/pkg/containers"
)

type Reader struct {
	bucket blob.Bucket
}

func NewReader(bucket blob.Bucket) *Reader {
	return &Reader{
		bucket: bucket,
	}
}

func (r *Reader) Iter(ctx context.Context) iter.Seq[containers.Result[record]] {
	return func(yield func(containers.Result[record]) bool) {
		for obj := range r.bucket.ListObjects(ctx, WAL_FILE_PREFIX) {
			if obj.Err != nil {
				if !yield(containers.Err[record](obj.Err)) {
					return
				}
				continue
			}

			for rec := range r.iterObject(ctx, obj.Value) {
				if !yield(rec) {
					return
				}
			}
		}
	}
}

func (r *Reader) iterObject(ctx context.Context, key string) iter.Seq[containers.Result[record]] {
	return func(yield func(containers.Result[record]) bool) {
		reader, err := r.bucket.GetObject(ctx, key)
		if err != nil {
			yield(containers.Err[record](err))
			return
		}

		// Read the object line by line
		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			// Decode the JSON line into a map[string]any.
			// UseNumber() is needed to preserve the full precision of uint64 values, which would otherwise
			// be degraded if decoded as float64.
			var doc record
			dec := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
			dec.UseNumber()
			err := dec.Decode(&doc)
			if err != nil {
				if !yield(containers.Err[record](err)) {
					return
				}
				continue
			}

			for key, value := range doc.Data {
				if n, ok := value.(json.Number); ok {
					if i, err := n.Int64(); err == nil {
						doc.Data[key] = i
					} else if f, err := n.Float64(); err == nil {
						doc.Data[key] = f
					}
				}
			}

			if !yield(containers.Ok(doc)) {
				return
			}
		}

		if err := scanner.Err(); err != nil {
			yield(containers.Err[record](err))
		}
	}
}
