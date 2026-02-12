package archive

import (
	"io"
	"os"
	"path"

	"github.com/ZaninAndrea/microdot/internal/compression"
)

type Writer struct {
	dataFile     structuredWriter
	metadataFile structuredWriter
	columns      []ColumnDef

	bufferedRows []Row
	blocks       []BlockMetadata
}

func NewWriter(columns []ColumnDef, dataFile, metadataFile io.WriteCloser) (*Writer, error) {
	if len(columns) == 0 {
		return nil, ErrNoColumns
	}

	writer := &Writer{
		dataFile:     structuredWriter{w: dataFile},
		metadataFile: structuredWriter{w: metadataFile},
		columns:      columns,
		bufferedRows: []Row{},
		blocks:       []BlockMetadata{},
	}

	err := writer.writeMetadataHeader()
	if err != nil {
		return nil, err
	}

	return writer, nil
}

func NewWriterFS(columns []ColumnDef, folder, name string) (*Writer, error) {
	// Open the data and metadata files for writing
	dataFile, err := os.Create(path.Join(folder, name+".data.bin"))
	if err != nil {
		return nil, err
	}

	metadataFile, err := os.Create(path.Join(folder, name+".metadata.bin"))
	if err != nil {
		return nil, err
	}

	return NewWriter(columns, dataFile, metadataFile)
}

func (w *Writer) writeMetadataHeader() error {
	if err := w.metadataFile.WriteUInt32(FORMAT_VERSION); err != nil {
		return err
	}

	if err := w.metadataFile.WriteUvarint(uint64(len(w.columns))); err != nil {
		return err
	}

	for _, col := range w.columns {
		if err := w.metadataFile.WriteString(col.Key); err != nil {
			return err
		}

		if err := w.metadataFile.WriteUInt16(uint16(col.Type)); err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) Write(rows []Row) error {
	w.bufferedRows = append(w.bufferedRows, rows...)

	for len(w.bufferedRows) >= CHUNK_SIZE {
		if err := w.writeChunk(); err != nil {
			return err
		}
		w.bufferedRows = w.bufferedRows[CHUNK_SIZE:]
	}

	return nil
}

func (w *Writer) writeChunk() error {
	chunkEnd := CHUNK_SIZE
	if len(w.bufferedRows) < CHUNK_SIZE {
		chunkEnd = len(w.bufferedRows)
	}

	chunkMetadata := []ChunkMetadata{}
	rows := w.bufferedRows[:chunkEnd]
	for i := range w.columns {
		startOffset := w.dataFile.Offset()
		switch w.columns[i].Type {
		case ColumnTypeInt64:
			if err := w.writeInt64Column(rows, i); err != nil {
				return err
			}
		case ColumnTypeFloat64:
			if err := w.writeFloat64Column(rows, i); err != nil {
				return err
			}
		case ColumnTypeString:
			if err := w.writeStringColumn(rows, i); err != nil {
				return err
			}
		case ColumnTypeBool:
			if err := w.writeBoolColumn(rows, i); err != nil {
				return err
			}
		default:
			return ErrUnsupportedColumnType
		}

		chunkLength := w.dataFile.Offset() - startOffset

		chunkMetadata = append(chunkMetadata, ChunkMetadata{
			Offset: startOffset,
			Length: chunkLength,
		})
	}

	w.blocks = append(w.blocks, BlockMetadata{
		Chunks: chunkMetadata,
	})

	return nil
}

func (w *Writer) writeInt64Column(rows []Row, columnIndex int) error {
	values := make([]int64, len(rows))
	for i, row := range rows {
		values[i] = row[columnIndex].(int64)
	}
	encoded := compression.EncodeDeltaOfDelta(values)

	_, err := w.dataFile.Write(encoded)
	if err != nil {
		return err
	}

	return nil
}

func (w *Writer) writeFloat64Column(rows []Row, columnIndex int) error {
	for _, row := range rows {
		if err := w.dataFile.WriteFloat64(row[columnIndex].(float64)); err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) writeStringColumn(rows []Row, columnIndex int) error {
	for _, row := range rows {
		if err := w.dataFile.WriteString(row[columnIndex].(string)); err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) writeBoolColumn(rows []Row, columnIndex int) error {
	values := make([]bool, len(rows))
	for i, row := range rows {
		values[i] = row[columnIndex].(bool)
	}
	encoded := compression.EncodeBitPacking(values)

	_, err := w.dataFile.Write(encoded)
	if err != nil {
		return err
	}

	return nil
}

func (w *Writer) writeMetadataChunks() error {
	if err := w.metadataFile.WriteUvarint(uint64(len(w.blocks))); err != nil {
		return err
	}

	for _, block := range w.blocks {
		for _, chunk := range block.Chunks {
			if err := w.metadataFile.WriteUInt64(uint64(chunk.Offset)); err != nil {
				return err
			}

			if err := w.metadataFile.WriteUInt64(uint64(chunk.Length)); err != nil {
				return err
			}
		}
	}

	return nil
}

func (w *Writer) Close() error {
	if len(w.bufferedRows) > 0 {
		if err := w.writeChunk(); err != nil {
			return err
		}
	}

	if err := w.writeMetadataChunks(); err != nil {
		return err
	}

	// Flush and close the data and metadata files
	err := w.dataFile.Close()
	if err != nil {
		return err
	}

	err = w.metadataFile.Close()
	if err != nil {
		return err
	}

	return nil
}
