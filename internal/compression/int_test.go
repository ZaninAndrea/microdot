package compression_test

import (
	"reflect"
	"testing"
	"testing/quick"

	"github.com/ZaninAndrea/microdot/internal/compression"
)

func TestDeltaOfDelta(t *testing.T) {
	t.Run("Identity", func(t *testing.T) {
		f := func(raw []int64) bool {
			encoded := compression.EncodeDeltaOfDelta(raw)
			decoded, err := compression.DecodeDeltaOfDelta(encoded)
			if err != nil {
				t.Logf("Decode failed: %v", err)
				return false
			}

			if len(raw) == 0 {
				return len(decoded) == 0
			}

			return reflect.DeepEqual(raw, decoded)
		}

		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})
}

func TestDecodeDeltaOfDelta_Errors(t *testing.T) {
	tests := []struct {
		name    string
		encoded []byte
	}{
		{
			name:    "TruncatedVarint",
			encoded: []byte{0x80}, // Continuation bit set, but unexpected EOF
		},
		{
			name:    "TruncatedVarintMiddle",
			encoded: []byte{0x02, 0x80}, // First valid, second truncated
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := compression.DecodeDeltaOfDelta(tc.encoded)
			if err == nil {
				t.Errorf("Expected error for %s, got nil", tc.name)
			}
		})
	}
}

func FuzzDeltaOfDelta(f *testing.F) {
	f.Add([]byte{})
	f.Add(compression.EncodeDeltaOfDelta([]int64{0}))
	f.Add(compression.EncodeDeltaOfDelta([]int64{100, 200, 300}))
	f.Add(compression.EncodeDeltaOfDelta([]int64{-5, -10, -15}))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoded, err := compression.DecodeDeltaOfDelta(data)
		if err != nil {
			return // Invalid input is fine
		}

		// Re-encode and check if it decodes to the same thing
		encoded := compression.EncodeDeltaOfDelta(decoded)
		decoded2, err := compression.DecodeDeltaOfDelta(encoded)
		if err != nil {
			t.Fatalf("Failed to decode re-encoded data: %v", err)
		}

		if len(decoded) == 0 && len(decoded2) == 0 {
			return
		}

		if !reflect.DeepEqual(decoded, decoded2) {
			t.Fatalf("Round trip mismatch. Original decoded: %v, Re-decoded: %v", decoded, decoded2)
		}
	})
}
