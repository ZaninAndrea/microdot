package timeseries

import (
	"fmt"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

func Open(path string) (DB, error) {
	opts := badger.DefaultOptions(path).WithLogger(nil)
	badgerDB, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	newDb := &db{badger: badgerDB}

	go func() {
		for {
			fmt.Println("Running ValueLogGC...")
			err := newDb.badger.RunValueLogGC(0.7)
			if err == badger.ErrNoRewrite {
				time.Sleep(1 * time.Minute)
			} else if err != nil {
				fmt.Println("ValueLogGC error:", err)
				break
			}

			fmt.Println("ValueLogGC completed")
		}
	}()

	return newDb, nil
}

type db struct {
	badger *badger.DB
}

func (d *db) Close() error {
	return d.badger.Close()
}
