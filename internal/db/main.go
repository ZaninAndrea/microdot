package db

import (
	"fmt"
	"iter"

	"github.com/ZaninAndrea/microdot/pkg/blob"
	"github.com/ZaninAndrea/microdot/pkg/containers"
)

type DB struct {
	walWriter *walWriter
}

func NewDB(bucket blob.Bucket) (*DB, error) {
	walWriter := newWalWriter(bucket)
	return &DB{
		walWriter: walWriter,
	}, nil
}

type Labels map[string]string

func (d *DB) AddDocument(streamLabels Labels, data map[string]any) error {
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

	return d.walWriter.AddDocument(streamLabels, data)
}

type QueryResult struct {
	StreamID   uint64
	DocumentID uint64
	Document   map[string]any
}

func (d *DB) Query(streamLabels Labels, query string) iter.Seq[containers.Result[QueryResult]] {
	return nil
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
