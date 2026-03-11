package stream

import (
	"fmt"
	"hash/fnv"
	"iter"

	"github.com/ZaninAndrea/microdot/internal/archive"
	"github.com/ZaninAndrea/microdot/pkg/containers"
)

type Stream struct {
	labels     map[string]string
	labelsHash uint64
	rootPath   string

	wal *wal

	idCounter uint64
}

func NewStream(labels map[string]string, rootPath string) (*Stream, error) {
	labelsHash := hashLabels(labels)
	wal, err := newWAL(labelsHash, rootPath)
	if err != nil {
		return nil, err
	}

	s := &Stream{
		labels:     labels,
		labelsHash: labelsHash,
		rootPath:   rootPath,
		wal:        wal,
		idCounter:  0,
	}

	return s, nil
}

func hashLabels(labels map[string]string) uint64 {
	hash := fnv.New64a()
	for key, value := range labels {
		hash.Write([]byte(key))
		hash.Write([]byte(value))
	}

	return hash.Sum64()
}

func (s *Stream) AddDocument(data map[string]any) (uint64, error) {
	if _, ok := data["_id"]; ok {
		return 0, fmt.Errorf("document cannot contain '_id' field")
	}

	docId := s.generateID(data["ts"].(int64))
	data["_id"] = docId

	err := s.wal.Append(data)
	if err != nil {
		return 0, err
	}

	if s.wal.logCount >= MAX_WAL_LOGS {
		if err := s.compressWAL(); err != nil {
			return 0, err
		}

		if err := s.wal.Delete(); err != nil {
			return 0, err
		}

		if err := s.wal.Open(); err != nil {
			return 0, err
		}
	}

	return docId, nil
}

type findResult struct {
	ID       uint64
	Document map[string]any
}

func (s *Stream) GetDocuments(ids []uint64) iter.Seq[containers.Result[findResult]] {
	return s.wal.GetDocuments(ids)
}

func (s *Stream) generateID(unixTimestamp int64) uint64 {
	const COUNTER_BITS = 20

	s.idCounter++
	if s.idCounter >= 1<<COUNTER_BITS {
		s.idCounter = 0
	}

	ts := uint64(unixTimestamp) << COUNTER_BITS
	return ts | s.idCounter
}

func (s *Stream) compressWAL() error {
	columns, rows, err := s.wal.ConsolidateData()
	if err != nil {
		return err
	}

	writer, err := archive.NewWriterFS(
		columns,
		s.rootPath,
		fmt.Sprintf("%x", s.labelsHash),
	)
	if err != nil {
		return err
	}

	for row := range rows {
		if row.IsErr() {
			return row.Error()
		}

		if err := writer.Write([]archive.Row{row.Value}); err != nil {
			return err
		}
	}

	err = writer.Close()
	if err != nil {
		return err
	}

	return nil
}

func (s *Stream) Close() error {
	return s.wal.Close()
}
