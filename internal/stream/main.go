package stream

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"maps"
	"math"
	"os"
	"path"
	"slices"

	"github.com/ZaninAndrea/microdot/internal/archive"
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

const MAX_WAL_LOGS = 3

func (s *Stream) AddDocument(data map[string]any) error {
	err := s.appendWAL(data)
	if err != nil {
		return err
	}

	if s.walLogCount >= MAX_WAL_LOGS {
		if err := s.compressWAL(); err != nil {
			return err
		}

		if err := s.openWAL(); err != nil {
			return err
		}
	}

	return nil
}

func (s *Stream) appendWAL(data map[string]any) error {
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

func (s *Stream) compressWAL() error {
	// Close the WAL file in write mode
	err := s.wal.Close()
	if err != nil {
		return err
	}

	// Reopen the WAL file in read mode
	walFilePath := path.Join(s.rootPath, fmt.Sprintf("%x_%s", s.labelsHash, walName))
	wal, err := os.OpenFile(walFilePath, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer wal.Close()

	columns, err := inferColumns(wal)
	if err != nil {
		return err
	}

	// Pass through the WAL file again to write the compressed version
	writer, err := archive.NewWriterFS(
		slices.Collect(maps.Values(columns)),
		s.rootPath,
		fmt.Sprintf("%x", s.labelsHash),
	)
	if err != nil {
		return err
	}

	_, err = wal.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(wal)
	for scanner.Scan() {
		var doc map[string]any
		err := json.Unmarshal(scanner.Bytes(), &doc)
		if err != nil {
			return err
		}

		row := archive.Row{}
		for _, col := range columns {
			row = append(row, doc[col.Key])
		}

		if err := writer.Write([]archive.Row{row}); err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	err = writer.Close()
	if err != nil {
		return err
	}

	// Remove the original WAL file after compression
	return os.Remove(walFilePath)
}

func inferColumns(wal *os.File) (map[string]archive.ColumnDef, error) {
	columns := make(map[string]archive.ColumnDef)
	scanner := bufio.NewScanner(wal)
	for scanner.Scan() {
		var doc map[string]any
		err := json.Unmarshal(scanner.Bytes(), &doc)
		if err != nil {
			return nil, err
		}

		for key, value := range doc {
			// Infer the column based on the value type
			var inferredType archive.ColumnType
			switch v := value.(type) {
			case float64:
				if v == math.Trunc(v) {
					inferredType = archive.ColumnTypeInt64
				} else {
					inferredType = archive.ColumnTypeFloat64
				}
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
				inferredType = archive.ColumnTypeInt64
			case string:
				inferredType = archive.ColumnTypeString
			case bool:
				inferredType = archive.ColumnTypeBool
			default:
				return nil, fmt.Errorf("unsupported value type for key %s: %T", key, value)
			}

			// Store the inferred column type in the columns map, if it already exists
			// merge the column type potentially widening it.
			if _, exists := columns[key]; !exists {
				columns[key] = archive.ColumnDef{Key: key, Type: inferredType}
			} else {
				existingType := columns[key].Type
				columns[key] = archive.ColumnDef{Key: key, Type: getCommonSupertype(existingType, inferredType)}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return columns, nil
}

func (s *Stream) Close() error {
	return s.wal.Close()
}

var superTypes = map[archive.ColumnType][]archive.ColumnType{
	archive.ColumnTypeInt64:   {archive.ColumnTypeInt64, archive.ColumnTypeFloat64, archive.ColumnTypeString},
	archive.ColumnTypeFloat64: {archive.ColumnTypeFloat64, archive.ColumnTypeString},
	archive.ColumnTypeBool:    {archive.ColumnTypeBool, archive.ColumnTypeString},
	archive.ColumnTypeString:  {archive.ColumnTypeString},
}

// getCommonSupertype returns the strictest common supertype of two column types, which is
// the strictest type that both input types can be safely cast to without loss of information.
//
// For example, the common supertype of int64 and float64 is float64, since all int64 values can be represented as float64.
func getCommonSupertype(a, b archive.ColumnType) archive.ColumnType {
	if a == b {
		return a
	}

	for _, supertype := range superTypes[a] {
		if slices.Contains(superTypes[b], supertype) {
			return supertype
		}
	}

	// This should never happen since all types are compatible with string, but we return string as a fallback.
	return archive.ColumnTypeString
}
