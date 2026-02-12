package archive

import "fmt"

// The timeseries is stored in a custom binary format which consists of two files:
// - The data file
// 	- For each block:
// 		- For each column:
// 			- The compressed chunk data (bytes)
//  	- A checksum for the block
// - The metadata file:
//  - The format version (uint32)
// 	- The number of columns (uvarint)
// 	- List of columns. Starts with the number of columns, then for each column:
// 		- Name (string with length explicitly stated at the beginning)
// 		- Type (an integer indicating an enum)
// 	- The blocks metadata:
//  	- The number of blocks (varint)
// 		- For each block:
// 			- For each column:
//	 			- The chunk offset in the file (uint64)
//	 			- The length of the compressed chunk (uint64)
//	 			- In the future we may also add metadata, such as min/max values for the chunk
//
// Each column data is split into 1000 row blocks, each chunk (block-column pair) is compressed separately.

var ErrUnsupportedColumnType = fmt.Errorf("unsupported column type")
var ErrUnsupportedFormatVersion = fmt.Errorf("unsupported format version")
var ErrNoColumns = fmt.Errorf("at least one column is required")

const FORMAT_VERSION uint32 = 1
const CHUNK_SIZE int = 1000

type ColumnType uint16

var (
	ColumnTypeInt64   ColumnType = 0
	ColumnTypeFloat64 ColumnType = 1
	ColumnTypeString  ColumnType = 2
	ColumnTypeBool    ColumnType = 3
)

type ColumnDef struct {
	Key  string
	Type ColumnType
}

type Row []any

type BlockMetadata struct {
	Chunks []ChunkMetadata
}

type ChunkMetadata struct {
	Offset uint64
	Length uint64
}
