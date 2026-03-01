package db

import (
	"fmt"
	"time"

	"github.com/ZaninAndrea/microdot/internal/trigram"
)

type DB struct {
	wal          *WAL
	trigramIndex *trigram.MemoryInvertedIndex

	bufferManager *bufferManager
}

func NewDB(walPath string, streamsPath string) (*DB, error) {
	wal, err := NewWAL(walPath)
	if err != nil {
		return nil, err
	}

	trigramIndex := trigram.NewMemoryInvertedIndex()

	return &DB{
		wal:           wal,
		trigramIndex:  trigramIndex,
		bufferManager: newBufferManager(streamsPath),
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
