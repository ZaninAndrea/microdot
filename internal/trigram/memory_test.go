package trigram

import (
	"testing"
)

func TestMemoryIndex(t *testing.T) {
	mi := newMemoryIndex()

	docID := int64(1)
	content := "hello world"
	mi.Add(docID, content)

	postings, err := search("hello", mi)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	found := false
	for _, p := range postings {
		if p.DocumentID == docID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected to find document %d for query 'hello'", docID)
	}

	// Query "universe", should not be found
	postings, err = search("universe", mi)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(postings) > 0 {
		t.Errorf("Expected no postings for query 'universe', got %d", len(postings))
	}
}
