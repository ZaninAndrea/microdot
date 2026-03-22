package db

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/ZaninAndrea/microdot/pkg/blob"
)

type walWriter struct {
	bucket blob.Bucket

	mu             sync.Mutex
	activeWriter   *io.WriteCloser
	writeCloseErr  chan error
	flushListeners []chan error
}

func newWalWriter(bucket blob.Bucket) *walWriter {
	return &walWriter{
		bucket: bucket,
	}
}

func (w *walWriter) AddDocument(streamLabels Labels, data map[string]any) error {
	err := <-w.write(streamLabels, data)
	if err != nil {
		return err
	}

	return nil
}

func (w *walWriter) Close() error {
	w.flush()

	return nil
}

func (w *walWriter) write(streamLabels Labels, data map[string]any) chan error {
	// Marshal data to JSON
	var jsonData struct {
		StreamLabels Labels
		Data         map[string]any
	}
	jsonData.StreamLabels = streamLabels
	jsonData.Data = data

	jsonBytes, err := json.Marshal(jsonData)
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
func (w *walWriter) createWriterIfMissing() {
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

		err := w.bucket.PutObject(ctx, walFileName(), reader)
		reader.Close()
		errChannel <- err
	}()

	// Close the writer and flush data to blob storage
	go func() {
		<-time.After(FLUSH_INTERVAL)
		w.flush()
	}()
}

func (w *walWriter) flush() {
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

func (w *walWriter) broadcastFlush(err error) {
	for _, listener := range w.flushListeners {
		listener <- err
	}
	w.flushListeners = nil
}

func walFileName() string {
	return fmt.Sprintf("wal/%d_%d.log", time.Now().UnixNano(), rand.Intn(1_000_000))
}
