package trigram

// search returns the postings matching the given query. For each match the posting of the first trigram is returned.
func search(
	query string,
	index interface {
		GetPostings(trigram) ([]Posting, error)
	},
) ([]Posting, error) {
	trigrams := getTrigrams(query)
	if len(trigrams) == 0 {
		return nil, nil
	}

	// Match also inside the string
	trigrams = trigrams[2 : len(trigrams)-2]

	// Read the posting list for the first trigram
	pl, err := index.GetPostings(trigrams[0])
	if err != nil {
		return nil, err
	}
	if len(trigrams) == 1 {
		return pl, nil
	}

	// Merge with the posting list of the other trigrams one at a time
	resultSet := pl
	for i := 1; i < len(trigrams); i++ {
		newResultSet := []Posting{}

		setA := resultSet
		setB, err := index.GetPostings(trigrams[i])
		if err != nil {
			return nil, err
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

	return resultSet, nil
}

func getTrigrams(content string) []trigram {
	bytes := append([]byte(content), invalidUTF8, invalidUTF8)

	trigrams := make([]trigram, 0, len(content))
	var currentTrigram trigram = [3]byte{invalidUTF8, invalidUTF8, invalidUTF8}
	for i := 0; i < len(bytes); i++ {
		currentTrigram[0] = currentTrigram[1]
		currentTrigram[1] = currentTrigram[2]
		currentTrigram[2] = bytes[i]

		trigrams = append(trigrams, currentTrigram)
	}

	return trigrams
}
