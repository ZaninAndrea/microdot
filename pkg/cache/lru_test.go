package cache

import (
	"errors"
	"testing"
)

func TestLRU_Get_Factory(t *testing.T) {
	created := 0
	factory := func(key int) (string, error) {
		created++
		return "value", nil
	}

	lru := NewLRU(2, factory, nil)

	// First access, should create
	val, err := lru.Get(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "value" {
		t.Errorf("expected 'value', got %v", val)
	}
	if created != 1 {
		t.Errorf("expected 1 creation, got %d", created)
	}

	// Second access, should retrieve from cache
	val, err = lru.Get(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "value" {
		t.Errorf("expected 'value', got %v", val)
	}
	if created != 1 {
		t.Errorf("expected 1 creation (no new creation), got %d", created)
	}
}

func TestLRU_Eviction(t *testing.T) {
	evicted := []int{}
	onEvict := func(v int) {
		evicted = append(evicted, v)
	}

	factory := func(key int) (int, error) {
		return key * 10, nil
	}

	lru := NewLRU(2, factory, onEvict)

	// Fill cache: [1, 2]
	lru.Get(1)
	lru.Get(2)

	// Access 1, making it most recent: [2, 1] (LRU is 2)
	lru.Get(1)

	// Add 3, should evict 2 (LRU): [1, 3]
	lru.Get(3)

	if len(evicted) != 1 || evicted[0] != 20 { // 2*10 = 20
		t.Errorf("expected 20 to be evicted, got %v", evicted)
	}

	// Add 4, should evict 1: [3, 4]
	lru.Get(4)

	if len(evicted) != 2 || evicted[1] != 10 { // 1*10 = 10
		t.Errorf("expected 10 to be evicted next, got %v", evicted)
	}
}

func TestLRU_Remove(t *testing.T) {
	evicted := []int{}
	onEvict := func(v int) {
		evicted = append(evicted, v)
	}

	factory := func(key int) (int, error) {
		return key, nil
	}

	lru := NewLRU(2, factory, onEvict)

	lru.Get(1)
	lru.Get(2)

	lru.Remove(1)

	if len(evicted) != 1 || evicted[0] != 1 {
		t.Errorf("expected 1 to be evicted via Remove, got %v", evicted)
	}

	// Only 2 remains. Add 3.
	lru.Get(3)
	if len(evicted) != 1 {
		t.Errorf("expected no additional eviction yet")
	}

	// Add 4. Now 2 should be evicted (LRU)
	lru.Get(4)
	if len(evicted) != 2 || evicted[1] != 2 {
		t.Errorf("expected 2 to be evicted, got %v", evicted)
	}
}

func TestLRU_Purge(t *testing.T) {
	evictedCount := 0
	onEvict := func(v int) {
		evictedCount++
	}
	factory := func(k int) (int, error) { return k, nil }

	lru := NewLRU(10, factory, onEvict)
	lru.Get(1)
	lru.Get(2)
	lru.Get(3)

	lru.Purge()

	if evictedCount != 3 {
		t.Errorf("expected 3 items evicted, got %d", evictedCount)
	}
}

func TestLRU_FactoryError(t *testing.T) {
	factory := func(k int) (int, error) {
		return 0, errors.New("fail")
	}
	lru := NewLRU(2, factory, nil)

	_, err := lru.Get(1)
	if err == nil {
		t.Error("expected error")
	}
}
