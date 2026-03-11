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

type wal struct {
	rootPath   string
	labelsHash uint64

	file     *os.File
	logCount int
}

func newWAL(labelsHash uint64, rootPath string) (*wal, error) {
	w := &wal{
		labelsHash: labelsHash,
		rootPath:   rootPath,
	}

	if err := w.Open(); err != nil {
		return nil, err
	}

	return w, nil
}

func (w *wal) Open() error {
	walFilePath := w.filePath()

	// If the WAL file doesn't exist, create it. Otherwise, open it for appending.
	if _, err := os.Stat(walFilePath); os.IsNotExist(err) {
		if err := os.MkdirAll(w.rootPath, 0755); err != nil {
			return err
		}

		f, err := os.OpenFile(walFilePath, os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			return err
		}

		w.file = f
		w.logCount = 0
		return nil
	} else {
		f, err := os.OpenFile(walFilePath, os.O_APPEND, 0644)
		if err != nil {
			return err
		}
		w.file = f

		logCount, err := w.countFileLines()
		if err != nil {
			return err
		}
		w.logCount = logCount

		return nil
	}
}

func (w *wal) Append(data map[string]any) error {
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

// ConsolidateData reads all entries from the WAL, infers the column definitions, and returns an iterator over the rows.
func (w *wal) ConsolidateData() ([]archive.ColumnDef, iter.Seq[containers.Result[archive.Row]], error) {
	columns, err := w.inferColumns()
	if err != nil {
		return nil, nil, err
	}

	rowIter := func(yield func(containers.Result[archive.Row]) bool) {
		if _, err := w.file.Seek(0, io.SeekStart); err != nil {
			yield(containers.Err[archive.Row](err))
			return
		}

		scanner := bufio.NewScanner(w.file)
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

func (w *wal) Close() error {
	if w.file == nil {
		return nil
	}
	return w.file.Close()
}

func (w *wal) Delete() error {
	return os.Remove(w.filePath())
}

func (w *wal) Iter() iter.Seq[containers.Result[map[string]any]] {
	return func(yield func(containers.Result[map[string]any]) bool) {
		if _, err := w.file.Seek(0, io.SeekStart); err != nil {
			yield(containers.Err[map[string]any](err))
			return
		}

		scanner := bufio.NewScanner(w.file)
		for scanner.Scan() {
			var doc map[string]any
			err := json.Unmarshal(scanner.Bytes(), &doc)
			if err != nil {
				if !yield(containers.Err[map[string]any](err)) {
					return
				}
				continue
			}

			if !yield(containers.Ok(doc)) {
				return
			}
		}

		if err := scanner.Err(); err != nil {
			yield(containers.Err[map[string]any](err))
		}
	}
}

func (w *wal) GetDocuments(ids []uint64) iter.Seq[containers.Result[findResult]] {
	return func(yield func(containers.Result[findResult]) bool) {
		for doc := range w.Iter() {
			if doc.IsErr() {
				if !yield(containers.Err[findResult](doc.Error())) {
					return
				}
				continue
			}

			idValue, ok := doc.Value["_id"]
			if !ok {
				continue
			}

			idUint, ok := idValue.(uint64)
			if !ok {
				continue
			}

			if slices.Contains(ids, idUint) {
				result := findResult{
					ID:       idUint,
					Document: doc.Value,
				}
				if !yield(containers.Ok(result)) {
					return
				}
			}
		}
	}
}

func (w *wal) filePath() string {
	return path.Join(w.rootPath, fmt.Sprintf("%x_%s", w.labelsHash, walName))
}

func (w *wal) inferColumns() ([]archive.ColumnDef, error) {
	columns := make(map[string]archive.ColumnDef)
	for doc := range w.Iter() {
		if doc.IsErr() {
			return nil, doc.Error()
		}

		for key, value := range doc.Value {
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

	return slices.Collect(maps.Values(columns)), nil
}

func (w *wal) countFileLines() (int, error) {
	// Iterate 1KB chunks of the file and count the number of newline characters
	buf := make([]byte, 1024)
	count := 0
	for {
		n, err := w.file.Read(buf)
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
