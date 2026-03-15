package stream

import (
	"fmt"
	"hash/fnv"
	"iter"
	"os"
	"strings"

	"github.com/ZaninAndrea/microdot/internal/archive"
	"github.com/ZaninAndrea/microdot/pkg/cache"
	"github.com/ZaninAndrea/microdot/pkg/containers"
)

type Stream struct {
	labels     map[string]string
	labelsHash uint64
	rootPath   string

	wal         *wal
	disks       cache.LRU[string, *diskStream]
	diskEntries []string

	idCounter uint64
}

const STREAM_CACHE_SIZE = 100

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

	diskEntries, err := loadDiskEntries(rootPath, labelsHash)
	if err != nil {
		return nil, err
	}

	s.diskEntries = diskEntries
	s.disks = *cache.NewLRU(
		STREAM_CACHE_SIZE,
		func(name string) (*diskStream, error) {
			return openDiskStreamFS(rootPath, name)
		},
		func(d *diskStream) { _ = d.Close() },
	)

	return s, nil
}

func loadDiskEntries(rootPath string, labelsHash uint64) ([]string, error) {
	files, err := os.ReadDir(rootPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	prefix := fmt.Sprintf("%x", labelsHash)
	diskEntries := make([]string, 0)
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".data.bin") {
			continue
		}

		name := strings.TrimSuffix(file.Name(), ".data.bin")
		if strings.HasPrefix(name, prefix) {
			diskEntries = append(diskEntries, name)
		}
	}

	return diskEntries, nil
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
	return func(yield func(containers.Result[findResult]) bool) {
		for _, name := range s.diskEntries {
			disk, err := s.disks.Get(name)
			if err != nil {
				continue
			}

			for result := range disk.getDocuments(ids) {
				if !yield(result) {
					return
				}
			}
		}

		for result := range s.wal.getDocuments(ids) {
			if !yield(result) {
				return
			}
		}
	}
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

	archive_idx := len(s.diskEntries)
	archiveName := fmt.Sprintf("%x_%d", s.labelsHash, archive_idx)

	writer, err := archive.NewWriterFS(
		columns,
		s.labels,
		s.rootPath,
		archiveName,
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

	s.diskEntries = append(s.diskEntries, archiveName)

	return nil
}

func (s *Stream) Close() error {
	s.disks.Purge()
	return s.wal.Close()
}
