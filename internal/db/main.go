package db

import (
	"fmt"
	"iter"
	"os"
	"path"

	"github.com/ZaninAndrea/microdot/internal/stream"
	"github.com/ZaninAndrea/microdot/internal/trigram"
	"github.com/ZaninAndrea/microdot/pkg/containers"
)

type DB struct {
	trigramIndex *trigram.Index

	bufferManager *bufferManager
}

func NewDB(basePath string) (*DB, error) {
	trigramFolder := path.Join(basePath, "trigram")
	err := os.MkdirAll(trigramFolder, os.ModePerm)
	if err != nil {
		return nil, err
	}
	trigramIndex, err := trigram.NewIndex(trigramFolder)
	if err != nil {
		return nil, err
	}

	streamsFolder := path.Join(basePath, "streams")
	err = os.MkdirAll(streamsFolder, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return &DB{
		trigramIndex:  trigramIndex,
		bufferManager: newBufferManager(streamsFolder),
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

	// Store the primary copy of the document
	docID, err := d.bufferManager.AddDocument(streamLabels, data)
	if err != nil {
		return err
	}

	// Add the document to the trigram index
	err = d.trigramIndex.Add(
		int64(stream.HashLabels(streamLabels)),
		int64(docID),
		data["msg"].(string),
	)
	if err != nil {
		return err
	}

	return nil
}

type QueryResult struct {
	StreamID   uint64
	DocumentID uint64
	Document   map[string]any
}

func (d *DB) Query(streamLabels Labels, query string) iter.Seq[containers.Result[QueryResult]] {
	matches, err := d.trigramIndex.Search(query)
	if err != nil {
		return func(yield func(containers.Result[QueryResult]) bool) {
			yield(containers.Err[QueryResult](err))
		}
	}

	// Group matches by stream ID
	var streamToDocIDs = make(map[uint64][]uint64)
	for _, match := range matches {
		if _, ok := streamToDocIDs[uint64(match.StreamID)]; !ok {
			streamToDocIDs[uint64(match.StreamID)] = make([]uint64, 0)
		}

		streamToDocIDs[uint64(match.StreamID)] = append(streamToDocIDs[uint64(match.StreamID)], uint64(match.DocumentID))
	}

	return func(yield func(containers.Result[QueryResult]) bool) {
		for streamID, docIDs := range streamToDocIDs {
			for result := range d.bufferManager.GetDocuments(streamID, docIDs) {
				if result.IsErr() {
					if !yield(containers.Err[QueryResult](result.Error())) {
						return
					}
					continue
				}

				queryResult := QueryResult{
					StreamID:   streamID,
					DocumentID: result.Value.ID,
					Document:   result.Value.Document,
				}

				if !yield(containers.Ok(queryResult)) {
					return
				}
			}
		}
	}
}

func (d *DB) Close() error {
	if err := d.trigramIndex.Close(); err != nil {
		return err
	}
	return nil
}
