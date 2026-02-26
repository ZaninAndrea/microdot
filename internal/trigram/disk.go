package trigram

import (
	"errors"
	"io"
	"os"
	"path"

	"github.com/ZaninAndrea/microdot/internal/archive"
	"github.com/ZaninAndrea/microdot/pkg/compression"
)

/* The DiskInvertedIndex stores the inverted index on disk using the following binary format which constists of two files.
DATA FILE:
- For each trigram:
	- For each block:
		- For each posting:
			- DocumentID (encoded as delta-of-delta)
			- Position (encoded as delta-of-delta)
METADATA FILE:
- The format version (uint32)
- Trigram count (uvarint)
- For each trigram:
	- Trigram (3 bytes)
	- Block count (uvarint)
	- For each block:
		- Posting count (uvarint)
		- Block offset (uvarint)

The postings are encoded in POSTING_BLOCK_SIZE blocks using delta-of-delta.
*/

const FORMAT_VERSION = 1
const POSTING_BLOCK_SIZE = 1024

func (f *MemoryInvertedIndex) WriteToDiskFS(folder, name string) error {
	// Open the data and metadata files for writing
	dataFile, err := os.Create(path.Join(folder, name+".data.bin"))
	if err != nil {
		return err
	}

	metadataFile, err := os.Create(path.Join(folder, name+".metadata.bin"))
	if err != nil {
		return err
	}
	return f.WriteToDisk(dataFile, metadataFile)
}

func (f *MemoryInvertedIndex) WriteToDisk(dataFile, metadataFile io.WriteCloser) error {
	dataWriter := archive.NewStructuredWriter(dataFile)
	metadataWriter := archive.NewStructuredWriter(metadataFile)

	defer dataFile.Close()
	defer metadataFile.Close()

	// Write the format version and trigram count to the metadata file
	if err := metadataWriter.WriteUInt32(FORMAT_VERSION); err != nil {
		return err
	}
	if err := metadataWriter.WriteUvarint(uint64(len(f.postingList))); err != nil {
		return err
	}

	// Write the trigram data one by one
	for trigram, postings := range f.postingList {
		// Write the trigram (3 bytes)
		if _, err := metadataWriter.Write(trigram[:]); err != nil {
			return err
		}

		// Write the block count for this trigram
		blockCount := (len(postings) + POSTING_BLOCK_SIZE - 1) / POSTING_BLOCK_SIZE
		if err := metadataWriter.WriteUvarint(uint64(blockCount)); err != nil {
			return err
		}

		// Write the block data and metadata
		for i := 0; i < blockCount; i++ {
			block := postings[i*POSTING_BLOCK_SIZE : min((i+1)*POSTING_BLOCK_SIZE, len(postings))]

			// Write block metadata (posting count and block offset)
			if err := metadataWriter.WriteUvarint(uint64(len(block))); err != nil {
				return err
			}
			blockOffset := dataWriter.Offset()
			if err := metadataWriter.WriteUvarint(blockOffset); err != nil {
				return err
			}

			// Write the block data to the data file
			encoder := &compression.DeltaOfDeltaPairEncoder{Writer: dataWriter}
			for _, posting := range block {
				if err := encoder.Encode(posting.DocumentID, posting.Position); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func LoadFromDiskFS(folder, name string) (*MemoryInvertedIndex, error) {
	// Open the data and metadata files for reading
	dataFile, err := os.Open(path.Join(folder, name+".data.bin"))
	if err != nil {
		return nil, err
	}

	metadataFile, err := os.Open(path.Join(folder, name+".metadata.bin"))
	if err != nil {
		return nil, err
	}
	defer dataFile.Close()
	defer metadataFile.Close()

	return LoadFromDisk(dataFile, metadataFile)
}

func LoadFromDisk(dataFile, metadataFile io.ReadSeekCloser) (*MemoryInvertedIndex, error) {
	dataReader := archive.NewStructuredReader(dataFile)
	metadataReader := archive.NewStructuredReader(metadataFile)

	// Read the format version and trigram count from the metadata file
	formatVersion, err := metadataReader.ReadUInt32()
	if err != nil {
		return nil, err
	}
	if formatVersion != FORMAT_VERSION {
		return nil, errors.New("unsupported format version")
	}

	trigramCount, err := metadataReader.ReadUvarint()
	if err != nil {
		return nil, err
	}

	// Read the trigram data one by one
	postingList := make(map[Trigram][]Posting)
	for i := uint64(0); i < trigramCount; i++ {
		var trigram [3]byte
		if _, err := metadataReader.Read(trigram[:]); err != nil {
			return nil, err
		}

		blockCount, err := metadataReader.ReadUvarint()
		if err != nil {
			return nil, err
		}

		var postings []Posting
		for j := uint64(0); j < blockCount; j++ {
			postingCount, err := metadataReader.ReadUvarint()
			if err != nil {
				return nil, err
			}

			blockOffset, err := metadataReader.ReadUvarint()
			if err != nil {
				return nil, err
			}

			// Seek to the block offset in the data file and read the block data
			if _, err := dataReader.Seek(int64(blockOffset), io.SeekStart); err != nil {
				return nil, err
			}

			decoder := &compression.DeltaOfDeltaPairDecoder{Reader: dataReader}
			for k := uint64(0); k < postingCount; k++ {
				pair, err := decoder.Decode()
				if err != nil {
					return nil, err
				}
				postings = append(postings, Posting{DocumentID: pair[0], Position: pair[1]})
			}
		}

		postingList[trigram] = postings
	}

	return &MemoryInvertedIndex{postingList: postingList}, nil
}
