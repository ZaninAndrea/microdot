package compression_test

import (
	"fmt"
	"math/rand"
	"testing"
	"testing/quick"

	"github.com/ZaninAndrea/microdot/internal/compression"
)

func TestBitPackIdentity(t *testing.T) {
	f := func(raw []bool) bool {
		enc := compression.BitPackingEncoder{}
		encoded := enc.Encode(raw)
		encoded = append(encoded, enc.Flush()...)

		dec := compression.BitPackingDecoder{}
		decoded := dec.Decode(encoded)

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
			var enc compression.BitPackingEncoder
			for i := 0; i < b.N; i++ {
				res := enc.Encode(data)
				_ = append(res, enc.Flush()...)
			}
		})

		enc := compression.BitPackingEncoder{}
		encoded := enc.Encode(data)
		encoded = append(encoded, enc.Flush()...)

		b.Run(fmt.Sprintf("Decode_%d", n), func(b *testing.B) {
			b.ReportAllocs()
			var dec compression.BitPackingDecoder
			for i := 0; i < b.N; i++ {
				_ = dec.Decode(encoded)
			}
		})
	}
}
