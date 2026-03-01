package stream

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path"
)

type Stream struct {
	labels     map[string]string
	labelsHash uint64
	rootPath   string

	wal         *os.File
	walLogCount int
}

const walName = "wal.jsonl"

func NewStream(labels map[string]string, rootPath string) (*Stream, error) {
	s := &Stream{
		labels:     labels,
		labelsHash: hashLabels(labels),
		rootPath:   rootPath,
	}

	if err := s.openWAL(); err != nil {
		return nil, err
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

func (s *Stream) openWAL() error {
	walFilePath := path.Join(s.rootPath, fmt.Sprintf("%x_%s", s.labelsHash, walName))

	// If the WAL file doesn't exist, create it. Otherwise, open it for appending.
	if _, err := os.Stat(walFilePath); os.IsNotExist(err) {
		if err := os.MkdirAll(s.rootPath, 0755); err != nil {
			return err
		}

		wal, err := os.OpenFile(walFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}

		s.wal = wal
		s.walLogCount = 0
		return nil
	} else {
		logCount, err := countFileLines(walFilePath)
		if err != nil {
			return err
		}
		s.walLogCount = logCount

		wal, err := os.OpenFile(walFilePath, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		s.wal = wal

		return nil
	}
}

func countFileLines(filePath string) (int, error) {
	wal, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer wal.Close()

	// Iterate 1KB chunks of the file and count the number of newline characters
	buf := make([]byte, 1024)
	count := 0
	for {
		n, err := wal.Read(buf)
		if err != nil && err != io.EOF {
			return 0, err
		}
		if n == 0 {
			break
		}
		for _, b := range buf[:n] {
			if b == '\n' {
				count++
			}
		}
	}

	return count, nil
}

func (s *Stream) AddDocument(data map[string]any) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	jsonData = append(jsonData, '\n')

	_, err = s.wal.Write(jsonData)
	if err != nil {
		return err
	}

	s.walLogCount++

	return nil
}

func (s *Stream) Close() error {
	return s.wal.Close()
}
