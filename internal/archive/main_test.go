package archive

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error { return nil }

func NopWriteCloser(w io.Writer) io.WriteCloser {
	return nopWriteCloser{w}
}

func TestReadWriteCycle(t *testing.T) {
	t.Run("Base case", func(t *testing.T) {
		// Generate some test data
		columns := []ColumnDef{
			{Key: "ts", Type: ColumnTypeInt64},
			{Key: "value", Type: ColumnTypeFloat64},
			{Key: "meta", Type: ColumnTypeString},
		}

		rows := make([]Row, 0, 1500)
		for i := 0; i < 1500; i++ {
			rows = append(rows, Row{
				int64(2000 + i),
				float64(i) * 0.1,
				fmt.Sprintf("generated_%d", i),
			})
		}

		checkReadWriteCycle(t, columns, rows)
	})

	t.Run("Empty dataset", func(t *testing.T) {
		columns := []ColumnDef{
			{Key: "ts", Type: ColumnTypeInt64},
			{Key: "value", Type: ColumnTypeFloat64},
			{Key: "meta", Type: ColumnTypeString},
		}

		rows := []Row{}

		checkReadWriteCycle(t, columns, rows)
	})

	t.Run("Single row", func(t *testing.T) {
		columns := []ColumnDef{
			{Key: "ts", Type: ColumnTypeInt64},
			{Key: "value", Type: ColumnTypeFloat64},
			{Key: "meta", Type: ColumnTypeString},
		}

		rows := []Row{
			{int64(2000), float64(0.1), "generated_0"},
		}

		checkReadWriteCycle(t, columns, rows)
	})
}

func checkReadWriteCycle(t *testing.T, columns []ColumnDef, rows []Row) {
	// Write the data to buffers
	var dataBuf bytes.Buffer
	var metaBuf bytes.Buffer

	writer, err := NewWriter(columns, NopWriteCloser(&dataBuf), NopWriteCloser(&metaBuf))
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	err = writer.Write(rows)
	if err != nil {
		t.Fatalf("Failed to write rows: %v", err)
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Read the data back
	reader, err := NewReader(io.NopCloser(bytes.NewReader(dataBuf.Bytes())), io.NopCloser(bytes.NewReader(metaBuf.Bytes())))
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Check that the columns metadata is correct
	readCols := reader.Columns()
	if len(readCols) != len(columns) {
		t.Fatalf("Columns output mismatch. Expected %d, got %d", len(columns), len(readCols))
	}
	for i, col := range columns {
		if readCols[i].Key != col.Key || readCols[i].Type != col.Type {
			t.Errorf("Column %d mismatch. Expected %v, got %v", i, col, readCols[i])
		}
	}

	// Check that all rows are read correctly
	i := 0
	for res := range reader.Rows() {
		if res.IsErr() {
			t.Fatalf("Error reading row %d: %v", i, res.Err)
		}
		row := res.Unwrap()

		expectedRow := rows[i]
		if len(row) != len(expectedRow) {
			t.Fatalf("Row %d length mismatch", i)
		}

		if row[0] != expectedRow[0] {
			t.Errorf("Row %d col 0 mismatch: got %v, want %v", i, row[0], expectedRow[0])
		}
		if row[1] != expectedRow[1] {
			t.Errorf("Row %d col 1 mismatch: got %v, want %v", i, row[1], expectedRow[1])
		}
		if row[2] != expectedRow[2] {
			t.Errorf("Row %d col 2 mismatch: got %v, want %v", i, row[2], expectedRow[2])
		}

		i++
	}

	if i != len(rows) {
		t.Errorf("Read %d rows, expected %d", i, len(rows))
	}
}
