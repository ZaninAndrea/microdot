package trigram

import (
	"os"
	"testing"
)

func TestIndex_AddAndSearch(t *testing.T) {
	baseFolder, err := os.MkdirTemp("", "trigram_index_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(baseFolder)

	idx, err := NewIndex(baseFolder)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
	defer idx.Close()

	docs := []struct {
		id      int64
		content string
	}{
		{1, "hello world"},
		{2, "hello universe"},
		{3, "world peace"},
	}

	for _, d := range docs {
		if err := idx.Add(d.id, d.content); err != nil {
			t.Errorf("Failed to add document %d: %v", d.id, err)
		}
	}

	tests := []struct {
		name     string
		query    string
		expected []int64
	}{
		{"Single term match multiple docs", "hello", []int64{1, 2}},
		{"Single term match multiple docs 2", "world", []int64{1, 3}},
		{"Single term match unique doc", "universe", []int64{2}},
		{"Single term match unique doc 2", "peace", []int64{3}},
		{"No match", "nonexistent", []int64{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			postings, err := idx.Search(tt.query)
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}

			foundIDs := make(map[int64]bool)
			for _, p := range postings {
				foundIDs[p.DocumentID] = true
			}

			if len(foundIDs) != len(tt.expected) {
				t.Errorf("Expected %d documents, got %d", len(tt.expected), len(foundIDs))
			}

			for _, id := range tt.expected {
				if !foundIDs[id] {
					t.Errorf("Expected document ID %d not found", id)
				}
			}
		})
	}
}

func TestIndex_Persistence(t *testing.T) {
	baseFolder, err := os.MkdirTemp("", "trigram_index_persistence_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(baseFolder)

	// Phase 1: Create index, add doc, close (flush)
	{
		idx, err := NewIndex(baseFolder)
		if err != nil {
			t.Fatalf("Failed to create index: %v", err)
		}
		if err := idx.Add(1, "persistent data"); err != nil {
			t.Fatalf("Failed to add document: %v", err)
		}
		if err := idx.Close(); err != nil {
			t.Fatalf("Failed to close index: %v", err)
		}
	}

	// Phase 2: Reopen index and search
	{
		idx, err := NewIndex(baseFolder)
		if err != nil {
			t.Fatalf("Failed to reopen index: %v", err)
		}
		defer idx.Close()

		postings, err := idx.Search("persistent")
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}

		found := false
		for _, p := range postings {
			if p.DocumentID == 1 {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected to find document 1 after reopening")
		}
	}
}
