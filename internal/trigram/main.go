package trigram

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ZaninAndrea/microdot/pkg/cache"
)

type trigram [3]byte

const invalidUTF8 byte = 0xFF

type Posting struct {
	DocumentID int64
	Position   int64
}

// Index is a trigram index implemented as an LSM tree.
type Index struct {
	mem        *memoryIndex
	memEntries int

	disks       cache.LRU[string, *diskIndex]
	baseFolder  string
	diskEntries []string
}

const INDEX_CACHE_SIZE = 1000

func NewIndex(baseFolder string) (*Index, error) {
	// Read the list of files ending in .data.bin in the base folder to initialize diskEntries
	var diskEntries []string
	files, err := os.ReadDir(baseFolder)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".data.bin") {
			name := strings.TrimSuffix(file.Name(), ".data.bin")
			diskEntries = append(diskEntries, name)
		}
	}

	disks := *cache.NewLRU(
		INDEX_CACHE_SIZE,
		func(name string) (*diskIndex, error) {
			return openDiskIndexFS(baseFolder, name)
		},
		func(d *diskIndex) { d.Close() },
	)

	return &Index{
		mem:         newMemoryIndex(),
		baseFolder:  baseFolder,
		diskEntries: diskEntries,
		disks:       disks,
	}, nil
}

func (i *Index) Add(documentID int64, content string) error {
	i.mem.Add(documentID, content)
	i.memEntries++

	if i.memEntries >= INDEX_CACHE_SIZE {
		err := i.flushMemIndex()
		if err != nil {
			return err
		}
	}

	return nil
}

func (i *Index) flushMemIndex() error {
	if i.memEntries == 0 {
		return nil
	}

	name := fmt.Sprintf("%d", time.Now().UnixNano())

	err := writeToDiskFS(i.mem, i.baseFolder, name)
	if err != nil {
		return err
	}

	i.mem = newMemoryIndex()
	i.memEntries = 0
	i.diskEntries = append(i.diskEntries, name)
	return nil
}

func (i *Index) Search(query string) ([]Posting, error) {
	var postings []Posting
	if i.memEntries > 0 {
		memPostings, err := search(query, i.mem)
		if err != nil {
			return nil, err
		}

		postings = append(postings, memPostings...)
	}

	for _, name := range i.diskEntries {
		diskIndex, err := i.disks.Get(name)
		if err != nil {
			continue
		}

		diskPostings, err := search(query, diskIndex)
		if err != nil {
			continue
		}
		postings = append(postings, diskPostings...)
	}

	return postings, nil
}

func (i *Index) Close() error {
	return i.flushMemIndex()
}
