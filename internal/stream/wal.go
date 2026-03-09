package stream

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"maps"
	"math"
	"os"
	"path"
	"slices"

	"github.com/ZaninAndrea/microdot/internal/archive"
	"github.com/ZaninAndrea/microdot/pkg/containers"
)

const (
	walName      = "wal.jsonl"
	MAX_WAL_LOGS = 3
)

type WAL struct {
	rootPath   string
	labelsHash uint64

	file     *os.File
	logCount int
}

func NewWAL(labelsHash uint64, rootPath string) (*WAL, error) {
	w := &WAL{
		labelsHash: labelsHash,
		rootPath:   rootPath,
	}

	if err := w.Open(); err != nil {
		return nil, err
	}

	return w, nil
}

func (w *WAL) Open() error {
	walFilePath := w.filePath()

	// If the WAL file doesn't exist, create it. Otherwise, open it for appending.
	if _, err := os.Stat(walFilePath); os.IsNotExist(err) {
		if err := os.MkdirAll(w.rootPath, 0755); err != nil {
			return err
		}

		f, err := os.OpenFile(walFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}

		w.file = f
		w.logCount = 0
		return nil
	} else {
		logCount, err := countFileLines(walFilePath)
		if err != nil {
			return err
		}
		w.logCount = logCount

		f, err := os.OpenFile(walFilePath, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		w.file = f

		return nil
	}
}

func (w *WAL) Append(data map[string]any) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	jsonData = append(jsonData, '\n')

	_, err = w.file.Write(jsonData)
	if err != nil {
		return err
	}

	w.logCount++

	return nil
}

func (w *WAL) ConsolidateData() ([]archive.ColumnDef, iter.Seq[containers.Result[archive.Row]], error) {
	// Close the WAL file in write mode
	err := w.Close()
	if err != nil {
		return nil, nil, err
	}

	// Reopen the WAL file in read mode
	wal, err := os.OpenFile(w.filePath(), os.O_RDONLY, 0644)
	if err != nil {
		return nil, nil, err
	}
	defer wal.Close()

	columns, err := inferColumns(wal)
	if err != nil {
		return nil, nil, err
	}

	rowIter := func(yield func(containers.Result[archive.Row]) bool) {
		if _, err := wal.Seek(0, io.SeekStart); err != nil {
			yield(containers.Err[archive.Row](err))
			return
		}

		scanner := bufio.NewScanner(wal)
		for scanner.Scan() {
			var doc map[string]any
			err := json.Unmarshal(scanner.Bytes(), &doc)
			if err != nil {
				if !yield(containers.Err[archive.Row](err)) {
					return
				}
				continue
			}

			row := archive.Row{}
			for _, col := range columns {
				row = append(row, doc[col.Key])
			}

			if !yield(containers.Ok(row)) {
				return
			}
		}

		if err := scanner.Err(); err != nil {
			yield(containers.Err[archive.Row](err))
		}
	}

	return columns, rowIter, nil
}

func (w *WAL) Close() error {
	if w.file == nil {
		return nil
	}
	return w.file.Close()
}

func (w *WAL) Delete() error {
	return os.Remove(w.filePath())
}

func (w *WAL) filePath() string {
	return path.Join(w.rootPath, fmt.Sprintf("%x_%s", w.labelsHash, walName))
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

func inferColumns(wal *os.File) ([]archive.ColumnDef, error) {
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

	return slices.Collect(maps.Values(columns)), nil
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
