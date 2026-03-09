package stream

import (
	"fmt"
	"hash/fnv"

	"github.com/ZaninAndrea/microdot/internal/archive"
)

type Stream struct {
	labels     map[string]string
	labelsHash uint64
	rootPath   string

	wal *WAL
}

func NewStream(labels map[string]string, rootPath string) (*Stream, error) {
	labelsHash := hashLabels(labels)
	wal, err := NewWAL(labelsHash, rootPath)
	if err != nil {
		return nil, err
	}

	s := &Stream{
		labels:     labels,
		labelsHash: labelsHash,
		rootPath:   rootPath,
		wal:        wal,
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

func (s *Stream) AddDocument(data map[string]any) error {
	err := s.wal.Append(data)
	if err != nil {
		return err
	}

	if s.wal.logCount >= MAX_WAL_LOGS {
		if err := s.compressWAL(); err != nil {
			return err
		}

		if err := s.wal.Delete(); err != nil {
			return err
		}

		if err := s.wal.Open(); err != nil {
			return err
		}
	}

	return nil
}

func (s *Stream) compressWAL() error {
	columns, rows, err := s.wal.ConsolidateData()
	if err != nil {
		return err
	}

	// Pass through the WAL file again to write the compressed version
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
