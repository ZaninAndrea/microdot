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

type diskIndex struct {
	dataReader     *archive.StructuredReader
	metadataReader *archive.StructuredReader

	metadata map[trigram][]blockMetadata
}

type blockMetadata struct {
	postingsCount uint64
	blockOffset   uint64
}

func openDiskIndexFS(folder string, name string) (*diskIndex, error) {
	// Open the data and metadata files for reading
	dataFile, err := os.Open(path.Join(folder, name+".data.bin"))
	if err != nil {
		return nil, err
	}

	metadataFile, err := os.Open(path.Join(folder, name+".metadata.bin"))
	if err != nil {
		return nil, err
	}

	return openDiskIndex(dataFile, metadataFile)
}

func openDiskIndex(dataFile, metadataFile io.ReadSeekCloser) (*diskIndex, error) {
	index := &diskIndex{dataReader: archive.NewStructuredReader(dataFile), metadataReader: archive.NewStructuredReader(metadataFile)}
	if err := index.loadMetadata(); err != nil {
		return nil, err
	}
	return index, nil
}

func (d *diskIndex) loadMetadata() error {
	// Read the format version and trigram count from the metadata file
	formatVersion, err := d.metadataReader.ReadUInt32()
	if err != nil {
		return err
	}
	if formatVersion != FORMAT_VERSION {
		return errors.New("unsupported format version")
	}

	// Read the trigram count from the metadata file
	trigramCount, err := d.metadataReader.ReadUvarint()
	if err != nil {
		return err
	}

	// For each trigram, read the trigram and the block metadata (number of postings and offset)
	d.metadata = make(map[trigram][]blockMetadata)
	for i := uint64(0); i < trigramCount; i++ {
		var tr [3]byte
		if _, err := d.metadataReader.Read(tr[:]); err != nil {
			return err
		}

		blockCount, err := d.metadataReader.ReadUvarint()
		if err != nil {
			return err
		}

		var blocks []blockMetadata
		for j := uint64(0); j < blockCount; j++ {
			postingCount, err := d.metadataReader.ReadUvarint()
			if err != nil {
				return err
			}

			blockOffset, err := d.metadataReader.ReadUvarint()
			if err != nil {
				return err
			}

			blocks = append(blocks, blockMetadata{postingsCount: postingCount, blockOffset: blockOffset})
		}

		d.metadata[trigram(tr)] = blocks
	}

	return nil
}

func (d *diskIndex) GetPostings(trigram trigram) ([]Posting, error) {
	blocks, ok := d.metadata[trigram]
	if !ok {
		return nil, nil
	}

	var postings []Posting
	for _, block := range blocks {
		blockPostings, err := d.readPostingsBlock(block)
		if err != nil {
			return nil, err
		}
		postings = append(postings, blockPostings...)
	}

	return postings, nil
}

func (d *diskIndex) LoadAll() (*memoryIndex, error) {
	postingList := make(map[trigram][]Posting)
	for trigram, blocks := range d.metadata {
		var postings []Posting
		for _, block := range blocks {
			blockPostings, err := d.readPostingsBlock(block)
			if err != nil {
				return nil, err
			}
			postings = append(postings, blockPostings...)
		}
		postingList[trigram] = postings
	}

	return &memoryIndex{postingList: postingList}, nil
}

func (d *diskIndex) readPostingsBlock(block blockMetadata) ([]Posting, error) {
	// Seek to the block offset in the data file
	if _, err := d.dataReader.Seek(int64(block.blockOffset), io.SeekStart); err != nil {
		return nil, err
	}

	// Read the postings in the block using the delta-of-delta decoder
	var postings []Posting
	decoder := &compression.DeltaOfDeltaPairDecoder{Reader: d.dataReader}
	for k := uint64(0); k < block.postingsCount; k++ {
		pair, err := decoder.Decode()
		if err != nil {
			return nil, err
		}
		postings = append(postings, Posting{DocumentID: pair[0], Position: pair[1]})
	}

	return postings, nil
}

func (d *diskIndex) Close() error {
	if err := d.dataReader.Close(); err != nil {
		return err
	}
	if err := d.metadataReader.Close(); err != nil {
		return err
	}
	return nil
}

type indexStore interface {
	ListTrigrams() []trigram
	GetPostings(trigram trigram) ([]Posting, error)
}

func writeToDiskFS(indexToWrite indexStore, folder, name string) error {
	// Open the data and metadata files for writing
	dataFile, err := os.Create(path.Join(folder, name+".data.bin"))
	if err != nil {
		return err
	}

	metadataFile, err := os.Create(path.Join(folder, name+".metadata.bin"))
	if err != nil {
		return err
	}
	return writeToDisk(indexToWrite, dataFile, metadataFile)
}

func writeToDisk(indexToWrite indexStore, dataFile, metadataFile io.WriteCloser) error {
	dataWriter := archive.NewStructuredWriter(dataFile)
	metadataWriter := archive.NewStructuredWriter(metadataFile)

	defer dataFile.Close()
	defer metadataFile.Close()

	trigrams := indexToWrite.ListTrigrams()

	// Write the format version and trigram count to the metadata file
	if err := metadataWriter.WriteUInt32(FORMAT_VERSION); err != nil {
		return err
	}
	if err := metadataWriter.WriteUvarint(uint64(len(trigrams))); err != nil {
		return err
	}

	// Write the trigram data one by one
	for _, trigram := range trigrams {
		postings, err := indexToWrite.GetPostings(trigram)
		if err != nil {
			return err
		}

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
