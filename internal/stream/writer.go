package stream

import (
	"context"
	"fmt"
	"io"
	"iter"
	"maps"
	"slices"

	"github.com/ZaninAndrea/microdot/internal/archive"
	"github.com/ZaninAndrea/microdot/internal/db/types"
	"github.com/ZaninAndrea/microdot/pkg/blob"
	"github.com/ZaninAndrea/microdot/pkg/containers"
	"golang.org/x/sync/errgroup"
)

type Writer struct {
	bucket blob.Bucket
}

func NewWriter(bucket blob.Bucket) *Writer {
	return &Writer{
		bucket: bucket,
	}
}

// AppendDocuments appends the given documents to the stream.
// The provided documents iterator should be safe to consume multiple times.
func (w *Writer) AppendDocuments(
	ctx context.Context,
	streamID uint64,
	labels types.Labels,
	documents iter.Seq[containers.Result[types.Document]],
) error {
	// Extract the column definitions
	columns, rows, err := consolidateData(documents)
	if err != nil {
		return err
	}

	// Pipe the data to blob storage
	var dataReader, metadataReader io.ReadCloser
	var dataWriter, metadataWriter io.WriteCloser
	dataReader, dataWriter = io.Pipe()
	metadataReader, metadataWriter = io.Pipe()

	eg, uploadContext := errgroup.WithContext(ctx)
	eg.Go(func() error {
		dataFileName := fmt.Sprintf("%s%d/%d.data", STREAM_FILE_PREFIX, streamID, 0) // TODO: use a proper file naming scheme to avoid collisions
		return w.bucket.PutObject(uploadContext, dataFileName, dataReader, false)
	})
	eg.Go(func() error {
		metadataFileName := fmt.Sprintf("%s%d/%d.metadata", STREAM_FILE_PREFIX, streamID, 0) // TODO: use a proper file naming scheme to avoid collisions
		return w.bucket.PutObject(uploadContext, metadataFileName, metadataReader, false)
	})

	// Stream the data in archive format to the pipe
	writer, err := archive.NewWriter(
		columns,
		labels,
		dataWriter,
		metadataWriter,
	)
	if err != nil {
		return err
	}
	for row := range rows {
		if row.IsErr() {
			return row.Error()
		}

		if err := writer.Write([]archive.Row{row.Value}); err != nil {
			return err
		}
	}

	err = writer.Close()
	if err != nil {
		return err
	}

	// Wait for the uploads to complete
	return eg.Wait()
}

// ConsolidateData reads all entries from the documents stream, infers the column definitions, and
// returns an iterator that yield the archive.Row entries.
// ConsolidateData will consume the documents iterator twice.
func consolidateData(documents iter.Seq[containers.Result[types.Document]]) ([]archive.ColumnDef, iter.Seq[containers.Result[archive.Row]], error) {
	columns, err := inferColumns(documents)
	if err != nil {
		return nil, nil, err
	}

	rowIter := func(yield func(containers.Result[archive.Row]) bool) {
		for doc := range documents {
			if doc.IsErr() {
				if !yield(containers.Err[archive.Row](doc.Error())) {
					return
				}
				continue
			}

			row := archive.Row{}
			for _, col := range columns {
				row = append(row, doc.Value[col.Key])
			}

			if !yield(containers.Ok(row)) {
				return
			}
		}
	}

	return columns, rowIter, nil
}

func inferColumns(documents iter.Seq[containers.Result[types.Document]]) ([]archive.ColumnDef, error) {
	columns := make(map[string]archive.ColumnDef)
	for doc := range documents {
		if doc.IsErr() {
			return nil, doc.Error()
		}

		for key, value := range doc.Value {
			// Infer the column based on the value type
			var inferredType archive.ColumnType
			switch value.(type) {
			case float64:
				inferredType = archive.ColumnTypeFloat64
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
				inferredType = archive.ColumnTypeInt64
			case string:
				inferredType = archive.ColumnTypeString
			case bool:
				inferredType = archive.ColumnTypeBool
			default:
				return nil, fmt.Errorf("unsupported value type for key %s: %T", key, value)
			}

			// Store the inferred column type in the columns map, if it already exists
			// merge the column type potentially widening it.
			if _, exists := columns[key]; !exists {
				columns[key] = archive.ColumnDef{Key: key, Type: inferredType}
			} else {
				existingType := columns[key].Type
				columns[key] = archive.ColumnDef{Key: key, Type: getCommonSupertype(existingType, inferredType)}
			}
		}
	}

	return slices.Collect(maps.Values(columns)), nil
}

var superTypes = map[archive.ColumnType][]archive.ColumnType{
	archive.ColumnTypeInt64:   {archive.ColumnTypeInt64, archive.ColumnTypeFloat64, archive.ColumnTypeString},
	archive.ColumnTypeFloat64: {archive.ColumnTypeFloat64, archive.ColumnTypeString},
	archive.ColumnTypeBool:    {archive.ColumnTypeBool, archive.ColumnTypeString},
	archive.ColumnTypeString:  {archive.ColumnTypeString},
}

// getCommonSupertype returns the strictest common supertype of two column types, which is
// the strictest type that both input types can be safely cast to without loss of information.
//
// For example, the common supertype of int64 and float64 is float64, since all int64 values can be represented as float64.
func getCommonSupertype(a, b archive.ColumnType) archive.ColumnType {
	if a == b {
		return a
	}

	for _, supertype := range superTypes[a] {
		if slices.Contains(superTypes[b], supertype) {
			return supertype
		}
	}

	// This should never happen since all types are compatible with string, but we return string as a fallback.
	return archive.ColumnTypeString
}
