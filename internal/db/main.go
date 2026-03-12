package db

import (
	"fmt"
	"os"
	"path"

	"github.com/ZaninAndrea/microdot/internal/trigram"
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
		int64(hashLabels(streamLabels)),
		int64(docID),
		data["msg"].(string),
	)
	if err != nil {
		return err
	}

	return nil
}

func (d *DB) Query(streamLabels Labels, query string) ([]map[string]any, error) {
	return nil, nil
}

func (d *DB) Close() error {
	if err := d.trigramIndex.Close(); err != nil {
		return err
	}
	return nil
}
