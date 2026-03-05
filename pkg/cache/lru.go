package cache

import (
	"container/list"
)

// LRU is a thread-safe LRU cache that manages the lifecycle of resources.
// It automatically opens new resources via a factory function and closes evicted resources.
type LRU[K comparable, V any] struct {
	maxSize   int
	items     map[K]*list.Element
	evictList *list.List
	factory   func(key K) (V, error)
	onEvict   func(v V)
}

type entry[K comparable, V any] struct {
	key   K
	value V
}

// NewLRU creates a new LRU cache.
// maxSize is the maximum number of items to keep open.
// factory is called to create a new item when it's not in the cache.
// onEvict is called when an item is evicted to close/cleanup the resource.
func NewLRU[K comparable, V any](maxSize int, factory func(key K) (V, error), onEvict func(v V)) *LRU[K, V] {
	return &LRU[K, V]{
		maxSize:   maxSize,
		items:     make(map[K]*list.Element),
		evictList: list.New(),
		factory:   factory,
		onEvict:   onEvict,
	}
}

// Get returns the value associated with the key.
// If the key is in the cache, it is moved to the front.
// If not, it is created using the factory function.
func (c *LRU[K, V]) Get(key K) (V, error) {
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		return ent.Value.(*entry[K, V]).value, nil
	}

	// Create new item
	value, err := c.factory(key)
	if err != nil {
		var zero V
		return zero, err
	}

	// Evict if necessary
	if c.evictList.Len() >= c.maxSize {
		c.removeOldest()
	}

	// Add new item
	ent := c.evictList.PushFront(&entry[K, V]{key: key, value: value})
	c.items[key] = ent

	return value, nil
}

// Remove removes the provided key from the cache.
func (c *LRU[K, V]) Remove(key K) {
	if ent, ok := c.items[key]; ok {
		c.removeElement(ent)
	}
}

// removeOldest removes the oldest item from the cache.
func (c *LRU[K, V]) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
	}
}

// removeElement removes an element from the cache.
func (c *LRU[K, V]) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	kv := e.Value.(*entry[K, V])
	delete(c.items, kv.key)
	if c.onEvict != nil {
		c.onEvict(kv.value)
	}
}

// Purge clears the cache, evicting all items.
func (c *LRU[K, V]) Purge() {
	for _, ent := range c.items {
		kv := ent.Value.(*entry[K, V])
		if c.onEvict != nil {
			c.onEvict(kv.value)
		}
	}
	c.items = make(map[K]*list.Element)
	c.evictList.Init()
}
