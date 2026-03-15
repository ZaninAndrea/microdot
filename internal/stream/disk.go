package stream

import (
	"io"
	"iter"
	"os"
	"path"
	"slices"

	"github.com/ZaninAndrea/microdot/internal/archive"
	"github.com/ZaninAndrea/microdot/pkg/containers"
)

type diskStream struct {
	reader      *archive.Reader
	idColumnIdx int
}

func openDiskStreamFS(folder, name string) (*diskStream, error) {
	dataFile, err := os.Open(path.Join(folder, name+".data.bin"))
	if err != nil {
		return nil, err
	}

	metadataFile, err := os.Open(path.Join(folder, name+".metadata.bin"))
	if err != nil {
		_ = dataFile.Close()
		return nil, err
	}

	return openDiskStream(dataFile, metadataFile)
}

func openDiskStream(dataFile, metadataFile io.ReadSeekCloser) (*diskStream, error) {
	reader, err := archive.NewReader(dataFile, metadataFile)
	if err != nil {
		return nil, err
	}

	idColumnIdx := slices.IndexFunc(reader.Columns(), func(col archive.ColumnDef) bool {
		return col.Key == "_id"
	})

	return &diskStream{reader: reader, idColumnIdx: idColumnIdx}, nil
}

func (d *diskStream) getDocuments(ids []uint64) iter.Seq[containers.Result[findResult]] {
	return func(yield func(containers.Result[findResult]) bool) {
		if d.idColumnIdx < 0 {
			return
		}

		columns := d.reader.Columns()
		for row := range d.reader.Rows() {
			if row.IsErr() {
				if !yield(containers.Err[findResult](row.Error())) {
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

			if !yield(containers.Ok(findResult{ID: id, Document: document})) {
				return
			}
		}
	}
}

func (d *diskStream) Close() error {
	return d.reader.Close()
}
