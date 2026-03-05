package trigram

import (
	"os"
	"slices"
	"testing"
)

func TestDiskInvertedIndex(t *testing.T) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "trigram_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create and populate the index
	index := newMemoryIndex()
	index.Add(1, "hello world")
	index.Add(2, "hello universe")
	index.Add(3, "world peace")

	// Write the index to disk
	indexName := "test_index"
	if err := writeToDiskFS(index, tempDir, indexName); err != nil {
		t.Fatalf("Failed to write index to disk: %v", err)
	}

	// Load the index from disk
	diskIndex, err := openDiskIndexFS(tempDir, indexName)
	if err != nil {
		t.Fatalf("Failed to open disk inverted index: %v", err)
	}

	loadedIndex, err := diskIndex.LoadAll()
	if err != nil {
		t.Fatalf("Failed to load index from disk: %v", err)
	}

	// Verify the loaded index content by searching
	tests := []struct {
		query    string
		expected []int64 // Expected DocumentIDs
	}{
		{"hello", []int64{1, 2}},
		{"world", []int64{1, 3}},
		{"universe", []int64{2}},
		{"peace", []int64{3}},
		{"xyz", []int64{}}, // Not found
	}

	for _, test := range tests {
		postings, err := search(test.query, loadedIndex)
		if err != nil {
			t.Fatalf("Failed to search for query %q: %v", test.query, err)
		}
		var docIDs []int64
		for _, p := range postings {
			docIDs = append(docIDs, p.DocumentID)
		}
		// Sort docIDs for comparison
		slices.Sort(docIDs)
		slices.Sort(test.expected)

		if !slices.Equal(docIDs, test.expected) {
			t.Errorf("Search(%q) = %v; want %v", test.query, docIDs, test.expected)
		}
	}
}

func TestDiskInvertedIndex_LargeData(t *testing.T) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "trigram_test_large")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	index := newMemoryIndex()

	// Add enough data to trigger multiple blocks
	count := POSTING_BLOCK_SIZE * 3
	for i := 1; i <= count; i++ {
		index.Add(int64(i), "commonword")
	}

	indexName := "large_index"
	if err := writeToDiskFS(index, tempDir, indexName); err != nil {
		t.Fatalf("Failed to write large index to disk: %v", err)
	}

	diskIndex, err := openDiskIndexFS(tempDir, indexName)
	if err != nil {
		t.Fatalf("Failed to open disk inverted index: %v", err)
	}
	loadedIndex, err := diskIndex.LoadAll()
	if err != nil {
		t.Fatalf("Failed to load large index from disk: %v", err)
	}

	// Search for the common word
	postings, err := search("commonword", loadedIndex)
	if err != nil {
		t.Fatalf("Failed to search for 'commonword': %v", err)
	}
	if len(postings) != count {
		t.Errorf("Expected %d results for 'commonword', got %d", count, len(postings))
	}

	// Check first and last
	if len(postings) > 0 {
		if postings[0].DocumentID != 1 {
			t.Errorf("Expected first document ID to be 1, got %d", postings[0].DocumentID)
		}
		if postings[len(postings)-1].DocumentID != int64(count) {
			t.Errorf("Expected last document ID to be %d, got %d", count, postings[len(postings)-1].DocumentID)
		}
	}
}
