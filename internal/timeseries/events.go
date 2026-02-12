package timeseries

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"iter"

	badger "github.com/dgraph-io/badger/v4"
)

type event struct {
	timestamp uint64
	data      map[string]any
	labels    Labels
	err       error
}

func (e *event) Error() error {
	return e.err
}

func (e *event) Timestamp() uint64 {
	return e.timestamp
}

func (e *event) Data() map[string]any {
	return e.data
}

func (e *event) Labels() Labels {
	return e.labels
}

func (d *db) ListEvents(ctx context.Context, streams []uint64, start, end uint64) iter.Seq[Event] {
	return func(yield func(Event) bool) {
		err := d.badger.View(func(txn *badger.Txn) error {
			// Return the events for each stream
			for _, streamId := range streams {
				// Compose the prefix for the stream ID
				idVarBuf := make([]byte, binary.MaxVarintLen64)
				n := binary.PutUvarint(idVarBuf, streamId)
				prefix := idVarBuf[:n]

				// Compute the start key
				startBuf := make([]byte, binary.MaxVarintLen64+8)
				copy(startBuf, prefix)
				binary.BigEndian.PutUint64(startBuf[n:], start)
				startBuf = startBuf[:n+8]

				// Iterate over the events in the time range
				iter := txn.NewIterator(badger.IteratorOptions{
					Prefix: prefix,
				})
				defer iter.Close()
				for iter.Seek(startBuf); iter.ValidForPrefix(prefix); iter.Next() {
					item := iter.Item()
					key := item.Key()

					timestamp := binary.BigEndian.Uint64(key[n:])
					if timestamp > end {
						break
					}

					// Unmarshal the data
					var data map[string]any
					err := item.Value(func(val []byte) error {
						data = make(map[string]any)
						pairs := bytes.Split(val, []byte(";"))
						for _, pair := range pairs {
							if len(pair) == 0 {
								continue
							}
							kv := bytes.SplitN(pair, []byte("="), 2)
							if len(kv) != 2 {
								continue
							}
							data[string(kv[0])] = string(kv[1])
						}
						return nil
					})
					if err != nil {
						return err
					}

					e := &event{
						timestamp: timestamp,
						data:      data,
					}

					// Yield the event
					if !yield(e) {
						return nil
					}
				}
			}

			return nil
		})
		if err != nil {
			yield(&event{err: err})
		}
	}
}

func (d *db) StoreEvents(ctx context.Context, events []EventIngest) error {
	return d.badger.Update(func(txn *badger.Txn) error {
		for _, event := range events {
			id, err := getEventId(event.StreamLabels, txn)
			if err != nil {
				return err
			}
			if id == nil {
				newId, err := createEventId(event.StreamLabels, txn)
				if err != nil {
					return err
				}
				id = &newId
			}

			// Compose the key as <eventId><timestamp>
			key := make([]byte, binary.MaxVarintLen64+8)
			n := binary.PutUvarint(key, *id)
			binary.BigEndian.PutUint64(key[n:], event.Timestamp)
			keyLen := n + 8

			// Serialize the data (for simplicity, we just use a naive approach here)
			// In a real implementation, consider using a more efficient serialization method
			dataBytes := []byte{}
			for k, v := range event.Data {
				dataBytes = append(dataBytes, []byte(fmt.Sprintf("%s=%v;", k, v))...)
			}

			err = txn.Set(key[:keyLen], dataBytes)
			if err != nil {
				return err
			}
		}

		return nil
	})
}
