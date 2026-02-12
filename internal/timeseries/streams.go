package timeseries

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"maps"
	"slices"

	badger "github.com/dgraph-io/badger/v4"
)

type stream struct {
	id     uint64
	labels Labels
}

func (s *stream) ID() uint64 {
	return s.id
}

func (s *stream) Labels() Labels {
	return s.labels
}

func (d *db) ListStreams(ctx context.Context, labels Labels) ([]Stream, error) {
	// Iterate over all streams by prefix, filtering by labels
	var streams []Stream
	err := d.badger.View(func(txn *badger.Txn) error {
		prefix := []byte("stream:")
		iter := txn.NewIterator(badger.IteratorOptions{
			Prefix: prefix,
		})
		defer iter.Close()

		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			item := iter.Item()
			key := item.Key()

			// Parse labels from key
			storedLabels := make(Labels)
			labelPart := key[len(prefix):]
			pairs := bytes.Split(labelPart, []byte(";"))
			for _, pair := range pairs {
				if len(pair) == 0 {
					continue
				}
				kv := bytes.SplitN(pair, []byte("="), 2)
				if len(kv) != 2 {
					continue
				}
				storedLabels[string(kv[0])] = string(kv[1])
			}

			// Check if all requested labels match
			matches := true
			for k, v := range labels {
				if storedLabels[k] != v {
					matches = false
					break
				}
			}
			if !matches {
				continue
			}

			// Get the stream ID
			var streamId uint64
			err := item.Value(func(val []byte) error {
				_id, n := binary.Uvarint(val)
				if n <= 0 {
					return fmt.Errorf("failed to decode varint for stream id")
				}
				streamId = _id
				return nil
			})
			if err != nil {
				return err
			}

			streams = append(streams, &stream{
				id:     streamId,
				labels: storedLabels,
			})
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return streams, nil
}

func marshalLabels(labels Labels) []byte {
	sortedKeys := slices.Collect(maps.Keys(labels))
	slices.Sort(sortedKeys)

	result := []byte("stream:")
	for _, k := range sortedKeys {
		result = append(result, []byte(k+"="+labels[k]+";")...)
	}
	return result
}

func getEventId(labels Labels, txn *badger.Txn) (*uint64, error) {
	item, err := txn.Get(marshalLabels(labels))
	if err == badger.ErrKeyNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	var id uint64
	err = item.Value(func(val []byte) error {
		// Decode the varint
		_id, n := binary.Uvarint(val)
		if n <= 0 {
			return fmt.Errorf("failed to decode varint for event id")
		}
		id = _id
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &id, nil
}

func createEventId(labels Labels, txn *badger.Txn) (uint64, error) {
	eventId, err := getNextEventId(txn)
	if err != nil {
		return 0, err
	}

	// Store the mapping from labels to id
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, eventId)
	err = txn.Set(marshalLabels(labels), buf[:n])
	if err != nil {
		return 0, err
	}

	return eventId, nil
}

func getNextEventId(txn *badger.Txn) (uint64, error) {
	item, err := txn.Get([]byte("eventSeq"))
	if err == badger.ErrKeyNotFound {
		err := txn.Set([]byte("eventSeq"), []byte{1})
		if err != nil {
			return 0, err
		}

		return 1, nil
	}

	// Decode the varint
	var seq uint64
	err = item.Value(func(val []byte) error {
		_seq, n := binary.Uvarint(val)
		if n <= 0 {
			return fmt.Errorf("failed to decode varint for event sequence")
		}
		seq = _seq
		return nil
	})
	if err != nil {
		return 0, err
	}

	// Increment and store back
	newSeq := seq + 1
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, newSeq)
	err = txn.Set([]byte("eventSeq"), buf[:n])
	if err != nil {
		return 0, err
	}

	return newSeq, nil
}
