package main

import (
	"context"

	"github.com/ZaninAndrea/microdot/internal/timeseries"
)

func main() {
	db, err := timeseries.Open("./tmp/badger")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	ctx := context.Background()

	err = db.StoreEvents(ctx, []timeseries.EventIngest{
		{
			StreamLabels: timeseries.Labels{
				"host": "server1",
				"env":  "prod",
			},
			Timestamp: 1625079600,
			Data: map[string]any{
				"cpu": "0.75",
				"mem": "512",
			},
		},
		{
			StreamLabels: timeseries.Labels{
				"host": "server1",
				"env":  "prod",
			},
			Timestamp: 1625079610,
			Data: map[string]any{
				"cpu": "0.80",
				"mem": "520",
			},
		},
	})
	if err != nil {
		panic(err)
	}

	// List streams with specific labels
	streams, err := db.ListStreams(ctx, timeseries.Labels{
		"host": "server1",
	})
	if err != nil {
		panic(err)
	}

	var streamIds []uint64
	for _, stream := range streams {
		streamIds = append(streamIds, stream.ID())
	}

	// List events in a time range
	eventsSeq := db.ListEvents(ctx, streamIds, 1625079500, 1625079700)
	for ev := range eventsSeq {
		if ev.Error() != nil {
			panic(ev.Error())
		}
		println("Timestamp:", ev.Timestamp())
		for k, v := range ev.Data() {
			println(" ", k, ":", v.(string))
		}
		println()
	}
}
