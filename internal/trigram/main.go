package trigram

import (
	"cmp"
	"fmt"
	"slices"
)

type Trigram [3]byte

const invalidUTF8 byte = 0xFF

type Posting struct {
	DocumentID int
	Position   int
}

type InvertedIndex struct {
	postingList map[Trigram][]Posting
}

func NewInvertedIndex() *InvertedIndex {
	return &InvertedIndex{
		postingList: make(map[Trigram][]Posting),
	}
}

func (f *InvertedIndex) Add(documentID int, content string) {
	for i, trigram := range getTrigrams(content) {
		if _, ok := f.postingList[trigram]; !ok {
			f.postingList[trigram] = make([]Posting, 0)
		}

		f.postingList[trigram] = append(f.postingList[trigram], Posting{
			DocumentID: documentID,
			Position:   i,
		})

		slices.SortFunc(f.postingList[trigram], comparePosting)
	}
}

// String returns a human readable representation of the inverted index.
func (f *InvertedIndex) String() string {
	var result string
	for trigram, postings := range f.postingList {
		result += string(trigram[:]) + ": "
		for _, posting := range postings {
			result += fmt.Sprintf("%d@%d ", posting.DocumentID, posting.Position)
		}
		result += "\n"
	}

	return result
}

func (f *InvertedIndex) Search(query string) []int {
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
		return extractDocumentIDs(pl)
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
			} else if postingB.Position-postingA.Position < 1 {
				indexB++
			} else if postingB.Position-postingA.Position > 1 {
				indexA++
			} else {
				newResultSet = append(newResultSet, postingB)
				indexA++
			}
		}

		resultSet = newResultSet
	}

	return extractDocumentIDs(resultSet)
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

func extractDocumentIDs(s []Posting) []int {
	result := make([]int, len(s))
	for i, p := range s {
		result[i] = p.DocumentID
	}
	return result
}

func comparePosting(a, b Posting) int {
	return cmp.Or(
		cmp.Compare(a.DocumentID, b.DocumentID),
		cmp.Compare(a.Position, b.Position),
	)
}
