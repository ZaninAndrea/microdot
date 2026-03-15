package db

import (
	"fmt"
	"testing"
)

func TestAddDocumentAndQuery(t *testing.T) {
	baseDir := t.TempDir()

	database, err := NewDB(baseDir)
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}
	defer database.Close()

	labels := Labels{"service": "api", "env": "test"}
	doc := map[string]any{
		"msg": "error while handling request",
		"ts":  int64(1700000000),
	}

	if err := database.AddDocument(labels, doc); err != nil {
		t.Fatalf("failed to add document: %v", err)
	}

	results := make([]QueryResult, 0)
	for result := range database.Query(labels, "handling") {
		if result.IsErr() {
			t.Fatalf("query returned error: %v", result.Error())
		}

		results = append(results, result.Value)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 query result, got %d", len(results))
	}

	if results[0].Document["msg"] != "error while handling request" {
		t.Fatalf("unexpected document msg: %v", results[0].Document["msg"])
	}
}

func TestAddManyDocumentsAndQueryAcrossArchives(t *testing.T) {
	baseDir := t.TempDir()

	database, err := NewDB(baseDir)
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}
	defer database.Close()

	labels := Labels{"service": "api", "env": "test"}
	const totalDocs = 1000

	expectedMessages := make(map[string]struct{}, totalDocs)
	for i := range totalDocs {
		msg := fmt.Sprintf("bulk message %d", i)
		expectedMessages[msg] = struct{}{}

		doc := map[string]any{
			"msg": msg,
			"ts":  int64(1700001000 + i),
		}

		if err := database.AddDocument(labels, doc); err != nil {
			t.Fatalf("failed to add document %d: %v", i, err)
		}
	}

	foundMessages := make(map[string]struct{}, totalDocs)
	for result := range database.Query(labels, "bulk") {
		if result.IsErr() {
			t.Fatalf("query returned error: %v", result.Error())
		}

		msg, ok := result.Value.Document["msg"].(string)
		if !ok {
			t.Fatalf("result document has non-string msg: %T", result.Value.Document["msg"])
		}

		foundMessages[msg] = struct{}{}
	}

	if len(foundMessages) != totalDocs {
		t.Fatalf("expected %d query results, got %d", totalDocs, len(foundMessages))
	}

	for msg := range expectedMessages {
		if _, ok := foundMessages[msg]; !ok {
			t.Fatalf("missing expected message in query results: %s", msg)
		}
	}
}

func TestPersistence(t *testing.T) {
	baseDir := t.TempDir()

	// Create a new database and add a document
	database, err := NewDB(baseDir)
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}

	labels := Labels{"service": "api", "env": "test"}
	doc := map[string]any{
		"msg": "persistent message",
		"ts":  int64(1700000000),
	}

	if err := database.AddDocument(labels, doc); err != nil {
		t.Fatalf("failed to add document: %v", err)
	}

	if err := database.Close(); err != nil {
		t.Fatalf("failed to close db: %v", err)
	}

	// Reopen the database and query for the document
	database, err = NewDB(baseDir)
	if err != nil {
		t.Fatalf("failed to reopen db: %v", err)
	}
	defer database.Close()

	results := make([]QueryResult, 0)
	for result := range database.Query(labels, "persistent") {
		if result.IsErr() {
			t.Fatalf("query returned error: %v", result.Error())
		}

		results = append(results, result.Value)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 query result, got %d", len(results))
	}

	if results[0].Document["msg"] != "persistent message" {
		t.Fatalf("unexpected document msg: %v", results[0].Document["msg"])
	}
}
