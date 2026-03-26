package db

import (
	"context"
	"fmt"
	"iter"
	"strings"

	"github.com/ZaninAndrea/microdot/internal/db/types"
	"github.com/ZaninAndrea/microdot/internal/wal"
	"github.com/ZaninAndrea/microdot/pkg/blob"
	"github.com/ZaninAndrea/microdot/pkg/containers"
)

type DB struct {
	walWriter *wal.Writer
	walReader *wal.Reader
}

func NewDB(bucket blob.Bucket) (*DB, error) {
	walWriter := wal.NewWriter(bucket)
	walReader := wal.NewReader(bucket)
	return &DB{
		walWriter: walWriter,
		walReader: walReader,
	}, nil
}

func (d *DB) AddDocument(streamLabels types.Labels, data types.Document) error {
	// Check mandatory fields
	if _, ok := data["msg"]; !ok {
		return fmt.Errorf("missing 'msg' field in document")
	}
	if _, ok := data["msg"].(string); !ok {
		return fmt.Errorf("'msg' field must be a string")
	}
	if _, ok := data["ts"]; !ok {
		return fmt.Errorf("missing 'ts' field in document")
	}
	if _, ok := data["ts"].(int64); !ok {
		return fmt.Errorf("'ts' field must be an int64")
	}
	if _, ok := data["_id"]; ok {
		return fmt.Errorf("document cannot contain '_id' field")
	}

	return d.walWriter.AddDocument(context.Background(), streamLabels, data)
}

type QueryResult struct {
	StreamID   uint64
	DocumentID uint64
	Document   types.Document
}

func (d *DB) Query(streamLabels types.Labels, query string) iter.Seq[containers.Result[QueryResult]] {
	return func(yield func(containers.Result[QueryResult]) bool) {
		for record := range d.walReader.Iter(context.Background()) {
			if record.IsErr() {
				err := record.Error()
				if !yield(containers.Err[QueryResult](err)) {
					return
				}
				continue
			}

			if matchesLabels(record.Value.StreamLabels, streamLabels) && matchesQuery(record.Value.Data, query) {
				queryResult := QueryResult{
					Document: record.Value.Data,
				}
				if !yield(containers.Ok(queryResult)) {
					return
				}
			}
		}
	}

	// 	matches, err := d.trigramIndex.Search(query)
	// 	if err != nil {
	// 		return func(yield func(containers.Result[QueryResult]) bool) {
	// 			yield(containers.Err[QueryResult](err))
	// 		}
	// 	}

	// 	// Group matches by stream ID
	// 	var streamToDocIDs = make(map[uint64][]uint64)
	// 	for _, match := range matches {
	// 		if _, ok := streamToDocIDs[uint64(match.StreamID)]; !ok {
	// 			streamToDocIDs[uint64(match.StreamID)] = make([]uint64, 0)
	// 		}

	// 		streamToDocIDs[uint64(match.StreamID)] = append(streamToDocIDs[uint64(match.StreamID)], uint64(match.DocumentID))
	// 	}

	// 	return func(yield func(containers.Result[QueryResult]) bool) {
	// 		for streamID, docIDs := range streamToDocIDs {
	// 			for result := range d.bufferManager.GetDocuments(streamID, docIDs) {
	// 				if result.IsErr() {
	// 					if !yield(containers.Err[QueryResult](result.Error())) {
	// 						return
	// 					}
	// 					continue
	// 				}

	// 				queryResult := QueryResult{
	// 					StreamID:   streamID,
	// 					DocumentID: result.Value.ID,
	// 					Document:   result.Value.Document,
	// 				}

	//				if !yield(containers.Ok(queryResult)) {
	//					return
	//				}
	//			}
	//		}
	//	}
}

func (d *DB) Close() error {
	return nil
}

func matchesLabels(recordLabels, queryLabels types.Labels) bool {
	for key, value := range queryLabels {
		if recordValue, ok := recordLabels[key]; !ok || recordValue != value {
			return false
		}
	}

	return true
}

func matchesQuery(document types.Document, query string) bool {
	msgValue, ok := document["msg"]
	if !ok {
		return false
	}
	msgStr, ok := msgValue.(string)
	if !ok {
		return false
	}

	return strings.Contains(msgStr, query)
}
