package compression_test

import (
	"fmt"
	"math/rand"
	"testing"
	"testing/quick"

	"github.com/ZaninAndrea/microdot/pkg/compression"
)

func TestBitPackIdentity(t *testing.T) {
	f := func(raw []bool) bool {
		encoded := compression.EncodeBitPacking(raw)
		decoded, err := compression.DecodeBitPacking(encoded)
		if err != nil {
			t.Logf("Error decoding: %v. Encoded: %b", err, encoded)
			return false
		}

		if len(decoded) != len(raw) {
			t.Logf("Length mismatch: expected %d, got %d. Encoded: %b", len(raw), len(decoded), encoded)
			return false
		}

		for i := range raw {
			if decoded[i] != raw[i] {
				t.Logf("Mismatch at index %d: expected %v, got %v. Encoded: %b", i, raw[i], decoded[i], encoded)
				return false
			}
		}

		return true
	}

	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func BenchmarkBitPack(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}
	for _, n := range sizes {
		rng := rand.New(rand.NewSource(12345))
		data := make([]bool, n)
		for i := range data {
			data[i] = rng.Intn(2) == 1
		}

		b.Run(fmt.Sprintf("Encode_%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = compression.EncodeBitPacking(data)
			}
		})

		encoded := compression.EncodeBitPacking(data)

		b.Run(fmt.Sprintf("Decode_%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = compression.DecodeBitPacking(encoded)
			}
		})
	}
}
