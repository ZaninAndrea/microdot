package db

import (
	"fmt"
	"os"
	"path"
	"time"

	"github.com/ZaninAndrea/microdot/internal/trigram"
)

type DB struct {
	wal          *WAL
	trigramIndex *trigram.Index

	bufferManager *bufferManager
}

func NewDB(basePath string) (*DB, error) {
	walFolder := path.Join(basePath, "wal")
	err := os.MkdirAll(walFolder, os.ModePerm)
	if err != nil {
		return nil, err
	}
	wal, err := NewWAL(walFolder)
	if err != nil {
		return nil, err
	}

	trigramFolder := path.Join(basePath, "trigram")
	err = os.MkdirAll(trigramFolder, os.ModePerm)
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
		wal:           wal,
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
	if _, ok := data["ts"]; !ok {
		data["ts"] = time.Now().UnixMilli()
	}

	// Store the primary copy of the document
	err := d.bufferManager.AddDocument(streamLabels, data)
	if err != nil {
		return err
	}

	// Add the document to the trigram index
	err = d.wal.AddDocument(streamLabels, data)
	if err != nil {
		return err
	}

	msg := data["msg"].(string)
	d.trigramIndex.Add(int64(hashLabels(streamLabels)), msg)

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
