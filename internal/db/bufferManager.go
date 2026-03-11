package db

import (
	"hash/fnv"

	"github.com/ZaninAndrea/microdot/internal/stream"
)

type bufferManager struct {
	streams  map[uint64]*stream.Stream
	rootPath string
}

func newBufferManager(rootPath string) *bufferManager {
	return &bufferManager{
		streams:  make(map[uint64]*stream.Stream),
		rootPath: rootPath,
	}
}

// AddDocument adds a document to the appropriate stream based on the provided labels.
// It returns the generated document ID and any error encountered during the process.
func (bm *bufferManager) AddDocument(streamLabels Labels, data map[string]any) (uint64, error) {
	streamID := hashLabels(streamLabels)

	if _, exists := bm.streams[streamID]; !exists {
		// Create a new stream for the given labels
		newStream, err := stream.NewStream(streamLabels, bm.rootPath)
		if err != nil {
			return 0, err
		}

		bm.streams[streamID] = newStream
	}

	// TODO: Handle closing of streams when they are too many

	return bm.streams[streamID].AddDocument(data)
}

func hashLabels(labels Labels) uint64 {
	hash := fnv.New64a()
	for key, value := range labels {
		hash.Write([]byte(key))
		hash.Write([]byte(value))
	}

	return hash.Sum64()
}
