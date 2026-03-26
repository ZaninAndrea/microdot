package wal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/ZaninAndrea/microdot/internal/db/types"
	"github.com/ZaninAndrea/microdot/pkg/blob"
)

type Writer struct {
	bucket blob.Bucket

	mu             sync.Mutex
	activeWriter   *io.WriteCloser
	writeCloseErr  chan error
	flushListeners []chan error
}

func NewWriter(bucket blob.Bucket) *Writer {
	return &Writer{
		bucket: bucket,
	}
}

func (w *Writer) AddDocument(ctx context.Context, labels types.Labels, doc types.Document) error {
	err := <-w.write(record{
		StreamLabels: labels,
		Data:         doc,
	})
	if err != nil {
		return err
	}

	return nil
}

func (w *Writer) Close(ctx context.Context) error {
	w.flush()

	return nil
}

func (w *Writer) write(r record) chan error {
	jsonBytes, err := json.Marshal(r)
	if err != nil {
		errChan := make(chan error, 1)
		errChan <- err
		return errChan
	}
	jsonBytes = append(jsonBytes, '\n')

	// Write to blob storage
	listener := make(chan error, 1)
	func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.flushListeners = append(w.flushListeners, listener)

		w.createWriterIfMissing()
		_, err = (*w.activeWriter).Write(jsonBytes)
	}()

	if err != nil {
		if flushErr := <-listener; flushErr != nil {
			err = flushErr
		}

		errChan := make(chan error, 1)
		errChan <- err
		return errChan
	}

	return listener
}

var FLUSH_INTERVAL = 1 * time.Second

// createWriterIfMissing creates a new writer if there isn't an active one.
// It should be called with the writerLock held.
func (w *Writer) createWriterIfMissing() {
	if w.activeWriter != nil {
		return
	}

	var writer io.WriteCloser
	reader, writer := io.Pipe()
	w.activeWriter = &writer

	errChannel := make(chan error, 1)
	w.writeCloseErr = errChannel

	// Write data to blob storage
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*FLUSH_INTERVAL)
		defer cancel()

		err := w.bucket.PutObject(ctx, w.walFileName(), reader)
		reader.Close()
		errChannel <- err
	}()

	// Close the writer and flush data to blob storage
	go func() {
		<-time.After(FLUSH_INTERVAL)
		w.flush()
	}()
}

func (w *Writer) flush() {
	w.mu.Lock()
	defer w.mu.Unlock()

	err := (*w.activeWriter).Close()
	w.activeWriter = nil
	if err != nil {
		w.broadcastFlush(err)
		return
	}

	err = <-w.writeCloseErr
	w.broadcastFlush(err)
}

func (w *Writer) broadcastFlush(err error) {
	for _, listener := range w.flushListeners {
		listener <- err
	}
	w.flushListeners = nil
}

func (w *Writer) walFileName() string {
	return WAL_FILE_PREFIX + fmt.Sprintf("%d_%d.log", time.Now().UnixNano(), rand.Intn(1_000_000))
}
