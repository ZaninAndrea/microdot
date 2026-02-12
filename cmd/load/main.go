package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ZaninAndrea/microdot/internal/timeseries"
)

func main() {
	db, err := timeseries.Open("./tmp/load")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	batchSize := 100
	baseTs := uint64(time.Now().Unix())

	var totalEvents uint64
	ticker := time.NewTicker(time.Second)
	quit := make(chan struct{})
	go func() {
		var last uint64
		for {
			select {
			case <-ticker.C:
				current := atomic.LoadUint64(&totalEvents)
				delta := current - last
				last = current
				log.Printf("ingest rate: %d events/sec, total: %d\n", delta, current)
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()

Loop:
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			break Loop
		default:
		}

		batch := make([]timeseries.EventIngest, 0, batchSize)
		for j := 0; j < batchSize; j++ {
			ev := timeseries.EventIngest{
				StreamLabels: timeseries.Labels{
					"host": "server" + strconv.Itoa((j+i*batchSize)%1000),
					"env":  "bench",
				},
				Timestamp: baseTs + uint64(i),
				Data: map[string]any{
					"cpu": strconv.FormatFloat(0.5, 'f', 2, 64),
					"mem": strconv.Itoa(256 + (i % 1024)),
				},
			}
			batch = append(batch, ev)
		}

		if err := db.StoreEvents(ctx, batch); err != nil {
			panic(err)
		}
		atomic.AddUint64(&totalEvents, uint64(len(batch)))
	}

	close(quit)
	final := atomic.LoadUint64(&totalEvents)
	log.Printf("ingest finished: total events=%d\n", final)
}
