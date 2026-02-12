package timeseries_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/ZaninAndrea/microdot/internal/timeseries"
)

func BenchmarkInsert(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "timeseries-bench-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "badger")
	db, err := timeseries.Open(dbPath)
	if err != nil {
		b.Fatalf("failed to open timeseries db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	baseTs := uint64(time.Now().Unix())

	b.ReportAllocs()
	b.ResetTimer()

	batchSizes := []int{1, 10, 100, 1000}
	for _, batchSize := range batchSizes {
		b.Run("BatchSize_"+strconv.Itoa(batchSize), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
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
					b.Fatalf("StoreEvents failed: %v", err)
				}
			}

			totalEvents := b.N * batchSize
			b.ReportMetric(float64(totalEvents), "events")
			b.ReportMetric(float64(totalEvents)/b.Elapsed().Seconds(), "events/sec")
		})
	}
}
