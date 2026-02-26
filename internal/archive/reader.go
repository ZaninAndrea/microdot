package archive

import (
	"bytes"
	"fmt"
	"io"
	"iter"
	"os"
	"path"

	"github.com/ZaninAndrea/microdot/pkg/compression"
	"github.com/ZaninAndrea/microdot/pkg/containers"
)

type Reader struct {
	dataFile     StructuredReader
	metadataFile StructuredReader
	columnDefs   []ColumnDef
	blockCount   uint64
	blocks       []blockMetadata
}

func NewReader(dataFile, metadataFile io.ReadSeekCloser) (*Reader, error) {
	reader := &Reader{
		dataFile:     StructuredReader{r: dataFile},
		metadataFile: StructuredReader{r: metadataFile},
	}

	err := reader.readMetadataHeader()
	if err != nil {
		return nil, err
	}

	return reader, nil
}

func NewReaderFS(folder, name string) (*Reader, error) {
	// Open the data and metadata files for reading
	dataFile, err := os.Open(path.Join(folder, name+".data.bin"))
	if err != nil {
		return nil, err
	}

	metadataFile, err := os.Open(path.Join(folder, name+".metadata.bin"))
	if err != nil {
		return nil, err
	}

	return NewReader(dataFile, metadataFile)
}

func (r *Reader) readMetadataHeader() error {
	formatVersion, err := r.metadataFile.ReadUInt32()
	if err != nil {
		return err
	}

	if formatVersion != FORMAT_VERSION {
		return ErrUnsupportedFormatVersion
	}

	numColumns, err := r.metadataFile.ReadUvarint()
	if err != nil {
		return err
	}

	r.columnDefs = make([]ColumnDef, numColumns)
	for i := uint64(0); i < numColumns; i++ {
		name, err := r.metadataFile.ReadString()
		if err != nil {
			return err
		}

		colTypeInt, err := r.metadataFile.ReadUInt16()
		if err != nil {
			return err
		}

		r.columnDefs[i] = ColumnDef{
			Key:  name,
			Type: ColumnType(colTypeInt),
		}
	}

	// Read the number of blocks
	blockCount, err := r.metadataFile.ReadUvarint()
	if err != nil {
		return err
	}
	r.blockCount = blockCount

	return nil
}

func (r *Reader) Columns() []ColumnDef {
	return r.columnDefs
}

func (r *Reader) Close() error {
	// Close the data and metadata files
	err := r.dataFile.Close()
	if err != nil {
		return err
	}

	err = r.metadataFile.Close()
	if err != nil {
		return err
	}

	return nil
}

// blockMetadata returns the metadata for the i-th block.
// If available the metadata is returned from the cache, otherwise it is read from the metadata file and cached for future use.
func (r *Reader) blockMetadata(i int) (blockMetadata, error) {
	if i < 0 || i >= int(r.blockCount) {
		return blockMetadata{}, fmt.Errorf("block index out of range")
	}

	// Read the metadata file sequentially until we have the metadata for the requested block
	for j := len(r.blocks); j <= i; j++ {
		blockMeta := blockMetadata{
			Chunks: make([]chunkMetadata, len(r.columnDefs)),
		}

		for j := range r.columnDefs {
			offset, err := r.metadataFile.ReadUInt64()
			if err != nil {
				return blockMetadata{}, err
			}

			length, err := r.metadataFile.ReadUInt64()
			if err != nil {
				return blockMetadata{}, err
			}

			blockMeta.Chunks[j] = chunkMetadata{
				Offset: offset,
				Length: length,
			}
		}

		r.blocks = append(r.blocks, blockMeta)
	}

	return r.blocks[i], nil
}

// Rows returns an iterator over the rows in the archive.
//
// It reads the data one block at a time and buffers the rows in memory.
func (r *Reader) Rows() iter.Seq[containers.Result[Row]] {
	return func(yield func(containers.Result[Row]) bool) {
		for blockIndex := 0; blockIndex < int(r.blockCount); blockIndex++ {
			blockMeta, err := r.blockMetadata(blockIndex)
			if err != nil {
				yield(containers.Err[Row](err))
				return
			}

			// Read a whole block of columns
			columns, err := r.readBlockColumns(blockMeta)
			if err != nil {
				yield(containers.Err[Row](err))
				return
			}

			// Return the rows one by one, building them as we go
			numRows := len(columns[0])
			for i := 0; i < numRows; i++ {
				row := make(Row, len(r.columnDefs))
				for j := range r.columnDefs {
					row[j] = columns[j][i]
				}

				if !yield(containers.Ok(row)) {
					return
				}
			}
		}
	}
}

// readBlockColumns reads the columns of a block and returns them as a 2D slice of any ([][]any).
// The outer slice represents the columns, while the inner slices represent the values of each column.
func (r *Reader) readBlockColumns(blockMeta blockMetadata) ([][]any, error) {
	columns := make([][]any, len(r.columnDefs))
	for i, columnDef := range r.columnDefs {
		switch columnDef.Type {
		case ColumnTypeInt64:
			data, err := r.readInt64Column(blockMeta.Chunks[i])
			if err != nil {
				return nil, err
			}
			columns[i] = data
		case ColumnTypeFloat64:
			data, err := r.readFloat64Column(blockMeta.Chunks[i])
			if err != nil {
				return nil, err
			}
			columns[i] = data
		case ColumnTypeString:
			data, err := r.readStringColumn(blockMeta.Chunks[i])
			if err != nil {
				return nil, err
			}
			columns[i] = data
		case ColumnTypeBool:
			data, err := r.readBoolColumn(blockMeta.Chunks[i])
			if err != nil {
				return nil, err
			}
			columns[i] = data
		default:
			return nil, ErrUnsupportedColumnType
		}
	}

	return columns, nil
}

func (r *Reader) readInt64Column(chunkMetadata chunkMetadata) ([]any, error) {
	chunkReader, err := r.getChunkReader(chunkMetadata)
	if err != nil {
		return nil, err
	}
	defer chunkReader.Close()

	data, err := chunkReader.ReadLZ4()
	if err != nil {
		return nil, err
	}

	return compression.DecodeDeltaOfDelta(data)
}

func (r *Reader) readBoolColumn(chunkMetadata chunkMetadata) ([]any, error) {
	chunkReader, err := r.getChunkReader(chunkMetadata)
	if err != nil {
		return nil, err
	}
	defer chunkReader.Close()

	data, err := chunkReader.ReadLZ4()
	if err != nil {
		return nil, err
	}

	return compression.DecodeBitPacking(data)
}

func (r *Reader) readFloat64Column(chunkMetadata chunkMetadata) ([]any, error) {
	chunkReader, err := r.getChunkReader(chunkMetadata)
	if err != nil {
		return nil, err
	}
	defer chunkReader.Close()

	// Read the float64 values from the byte slice
	values := make([]any, chunkMetadata.Length/8)
	for i := 0; i < len(values); i++ {
		values[i], err = chunkReader.ReadFloat64()
		if err != nil {
			return nil, err
		}
	}

	return values, nil
}

func (r *Reader) readStringColumn(chunkMetadata chunkMetadata) ([]any, error) {
	chunkReader, err := r.getChunkReader(chunkMetadata)
	if err != nil {
		return nil, err
	}
	defer chunkReader.Close()

	// Read the string values from the byte slice
	values := make([]any, 0)
	for {
		str, err := chunkReader.ReadString()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		values = append(values, str)
	}

	return values, nil
}

func (r *Reader) getChunkReader(chunkMetadata chunkMetadata) (*StructuredReader, error) {
	data := make([]byte, chunkMetadata.Length)
	_, err := r.dataFile.Read(data)
	if err != nil {
		return nil, err
	}

	return &StructuredReader{r: byteReadCloser{Reader: bytes.NewReader(data)}}, nil
}

type byteReadCloser struct {
	*bytes.Reader
}

func (b byteReadCloser) Close() error {
	return nil
}
