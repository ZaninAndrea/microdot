package stream

import (
	"context"
	"iter"
	"slices"

	"github.com/ZaninAndrea/microdot/internal/archive"
	"github.com/ZaninAndrea/microdot/internal/db/types"
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

type FindResult struct {
	ID       uint64
	Document types.Document
}

func (r *Reader) IterDocuments(ctx context.Context, streamID uint64, ids []uint64) iter.Seq[containers.Result[FindResult]] {
	metadataFile, err := r.bucket.GetObject(ctx, metadataFileName(streamID, 0))
	if err != nil {
		return func(yield func(containers.Result[FindResult]) bool) {
			yield(containers.Err[FindResult](err))
		}
	}

	reader, err := archive.NewReader(dataFileName(streamID, 0), metadataFile)
	if err != nil {
		return nil, err
	}

	idColumnIdx := slices.IndexFunc(reader.Columns(), func(col archive.ColumnDef) bool {
		return col.Key == "_id"
	})

	return func(yield func(containers.Result[FindResult]) bool) {
		if d.idColumnIdx < 0 {
			return
		}

		columns := d.reader.Columns()
		for row := range d.reader.Rows() {
			if row.IsErr() {
				if !yield(containers.Err[FindResult](row.Error())) {
					return
				}
				continue
			}

			idAny := row.Value[d.idColumnIdx]
			idInt, ok := idAny.(int64)
			if !ok {
				continue
			}

			id := uint64(idInt)
			if !slices.Contains(ids, id) {
				continue
			}

			document := make(map[string]any, len(columns))
			for i, col := range columns {
				document[col.Key] = row.Value[i]
			}

			if !yield(containers.Ok(FindResult{ID: id, Document: document})) {
				return
			}
		}
	}
}

func (d *diskStream) Close() error {
	return d.reader.Close()
}
