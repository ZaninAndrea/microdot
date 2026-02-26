package trigram

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
)

type Trigram [3]byte

const invalidUTF8 byte = 0xFF

type Posting struct {
	DocumentID int64
	Position   int64
}

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

// String returns a human readable representation of the inverted index.
func (f *MemoryInvertedIndex) String() string {
	orderedTrigrams := slices.SortedFunc(maps.Keys(f.postingList), compareTrigram)

	var result string
	for _, trigram := range orderedTrigrams {
		result += string(trigram[:]) + ": "
		for _, posting := range f.postingList[trigram] {
			result += fmt.Sprintf("%d@%d ", posting.DocumentID, posting.Position)
		}
		result += "\n"
	}

	return result
}

// Search returns the postings matching the given query. For each match the posting of the first trigram is returned.
func (f *MemoryInvertedIndex) Search(query string) []Posting {
	trigrams := getTrigrams(query)
	if len(trigrams) == 0 {
		return nil
	}

	// Match also inside the string
	trigrams = trigrams[2 : len(trigrams)-2]

	// Read the posting list for the first trigram
	pl, ok := f.postingList[trigrams[0]]
	if !ok {
		return nil
	}
	if len(trigrams) == 1 {
		return pl
	}

	// Merge with the posting list of the other trigrams one at a time
	resultSet := pl
	for i := 1; i < len(trigrams); i++ {
		newResultSet := []Posting{}

		setA := resultSet
		setB, ok := f.postingList[trigrams[i]]
		if !ok {
			return nil
		}

		indexA := 0
		indexB := 0
		for indexA < len(setA) && indexB < len(setB) {
			postingA := setA[indexA]
			postingB := setB[indexB]

			if postingA.DocumentID < postingB.DocumentID {
				indexA++
			} else if postingA.DocumentID > postingB.DocumentID {
				indexB++
			} else if postingB.Position-postingA.Position < int64(i) {
				indexB++
			} else if postingB.Position-postingA.Position > int64(i) {
				indexA++
			} else {
				newResultSet = append(newResultSet, postingA)
				indexA++
			}
		}

		resultSet = newResultSet
	}

	return resultSet
}

func getTrigrams(content string) []Trigram {
	bytes := append([]byte(content), invalidUTF8, invalidUTF8)

	trigrams := make([]Trigram, 0, len(content))
	var currentTrigram Trigram = [3]byte{invalidUTF8, invalidUTF8, invalidUTF8}
	for i := 0; i < len(bytes); i++ {
		currentTrigram[0] = currentTrigram[1]
		currentTrigram[1] = currentTrigram[2]
		currentTrigram[2] = bytes[i]

		trigrams = append(trigrams, currentTrigram)
	}

	return trigrams
}

func comparePosting(a, b Posting) int {
	return cmp.Or(
		cmp.Compare(a.DocumentID, b.DocumentID),
		cmp.Compare(a.Position, b.Position),
	)
}

func compareTrigram(a, b Trigram) int {
	return cmp.Or(
		cmp.Compare(a[0], b[0]),
		cmp.Compare(a[1], b[1]),
		cmp.Compare(a[2], b[2]),
	)
}
