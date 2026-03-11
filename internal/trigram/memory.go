package trigram

import (
	"cmp"
	"maps"
	"slices"
)

type memoryIndex struct {
	postingList map[trigram][]Posting
}

func newMemoryIndex() *memoryIndex {
	return &memoryIndex{
		postingList: make(map[trigram][]Posting),
	}
}

func (f *memoryIndex) Add(documentID int64, content string) {
	for i, trigram := range getTrigrams(content) {
		if _, ok := f.postingList[trigram]; !ok {
			f.postingList[trigram] = make([]Posting, 0)
		}

		postingToInsert := Posting{DocumentID: documentID, Position: int64(i - 2)}

		insertionIndex, exists := slices.BinarySearchFunc(
			f.postingList[trigram],
			postingToInsert,
			comparePosting,
		)
		if exists {
			continue
		}

		f.postingList[trigram] = slices.Insert(f.postingList[trigram], insertionIndex, postingToInsert)
	}
}

func (f *memoryIndex) ListTrigrams() []trigram {
	return slices.Collect(maps.Keys(f.postingList))
}

func (f *memoryIndex) GetPostings(trigram trigram) ([]Posting, error) {
	postings, ok := f.postingList[trigram]
	if !ok {
		return nil, nil
	}
	return postings, nil
}

func comparePosting(a, b Posting) int {
	return cmp.Or(
		cmp.Compare(a.DocumentID, b.DocumentID),
		cmp.Compare(a.Position, b.Position),
	)
}
