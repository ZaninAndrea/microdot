package db

import (
	"iter"

	"github.com/ZaninAndrea/microdot/internal/stream"
	"github.com/ZaninAndrea/microdot/pkg/containers"
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
	streamID := stream.HashLabels(streamLabels)

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

func (bm *bufferManager) GetDocuments(labelsHash uint64, ids []uint64) iter.Seq[containers.Result[stream.FindResult]] {
	if _, exists := bm.streams[labelsHash]; !exists {
		newStream, err := stream.OpenStream(labelsHash, bm.rootPath)
		if err != nil {
			return func(yield func(containers.Result[stream.FindResult]) bool) {
				yield(containers.Err[stream.FindResult](err))
			}
		}

		bm.streams[labelsHash] = newStream
	}

	return bm.streams[labelsHash].GetDocuments(ids)
}
