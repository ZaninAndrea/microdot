package trigram

import (
	"cmp"
	"slices"
)

type MemoryInvertedIndex struct {
	postingList map[Trigram][]Posting
}

func NewMemoryInvertedIndex() *MemoryInvertedIndex {
	return &MemoryInvertedIndex{
		postingList: make(map[Trigram][]Posting),
	}
}

func (f *MemoryInvertedIndex) Add(documentID int64, content string) {
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

func (f *MemoryInvertedIndex) GetPostings(trigram Trigram) ([]Posting, error) {
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
