package archive

import (
	"bytes"
	"encoding/binary"
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

type nopReadSeekCloser struct {
	io.ReadSeeker
}

func (nopReadSeekCloser) Close() error { return nil }

func NopReadSeekCloser(r io.ReadSeeker) io.ReadSeekCloser {
	return nopReadSeekCloser{r}
}

func TestReadWriteCycle(t *testing.T) {
	t.Run("Base case", func(t *testing.T) {
		// Generate some test data
		columns := []ColumnDef{
			{Key: "ts", Type: ColumnTypeInt64},
			{Key: "value", Type: ColumnTypeFloat64},
			{Key: "meta", Type: ColumnTypeString},
			{Key: "flag", Type: ColumnTypeBool},
		}

		rows := make([]Row, 0, 1500)
		for i := 0; i < 1500; i++ {
			rows = append(rows, Row{
				int64(2000 + i),
				float64(i) * 0.1,
				fmt.Sprintf("generated_%d", i),
				i%2 == 0, // flag is true for even rows, false for odd rows
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
	reader, err := NewReader(NopReadSeekCloser(bytes.NewReader(dataBuf.Bytes())), NopReadSeekCloser(bytes.NewReader(metaBuf.Bytes())))
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

func BenchmarkCompression(b *testing.B) {
	columns := []ColumnDef{
		{Key: "ts", Type: ColumnTypeInt64},
		{Key: "value", Type: ColumnTypeFloat64},
		{Key: "meta", Type: ColumnTypeString},
	}

	rows := make([]Row, 0, 100000)
	for i := 0; i < 100000; i++ {
		rows = append(rows, Row{
			int64(2000 + i),
			float64(i) * 0.1,
			fmt.Sprintf("generated_%d", i),
		})
	}

	inputBytes := benchmarkEstimateInputBytes(columns, rows)
	var totalOutputBytes uint64

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		var dataBuf bytes.Buffer
		var metaBuf bytes.Buffer

		writer, err := NewWriter(columns, NopWriteCloser(&dataBuf), NopWriteCloser(&metaBuf))
		if err != nil {
			b.Fatalf("Failed to create writer: %v", err)
		}

		err = writer.Write(rows)
		if err != nil {
			b.Fatalf("Failed to write rows: %v", err)
		}

		err = writer.Close()
		if err != nil {
			b.Fatalf("Failed to close writer: %v", err)
		}

		totalOutputBytes += uint64(dataBuf.Len() + metaBuf.Len())
	}

	avgOutputBytes := float64(totalOutputBytes) / float64(b.N)
	if avgOutputBytes > 0 {
		b.ReportMetric(100.0*(1.0-(avgOutputBytes/float64(inputBytes))), "%_compression_ratio")
	}
}

func benchmarkEstimateInputBytes(columns []ColumnDef, rows []Row) uint64 {
	rowCount := uint64(len(rows))
	var total uint64

	for columnIndex := range columns {
		switch columns[columnIndex].Type {
		case ColumnTypeInt64:
			total += 8 * rowCount
		case ColumnTypeFloat64:
			total += 8 * rowCount
		case ColumnTypeBool:
			total += 1 * rowCount
		case ColumnTypeString:
			for _, row := range rows {
				s := row[columnIndex].(string)
				total += uint64(benchmarkUvarintLen(uint64(len(s))))
				total += uint64(len(s))
			}
		}
	}

	return total
}

func benchmarkUvarintLen(value uint64) int {
	var buf [binary.MaxVarintLen64]byte
	return binary.PutUvarint(buf[:], value)
}
