package compression_test

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/ZaninAndrea/microdot/pkg/compression"
)

func TestDeltaOfDeltaSliceCodec_RoundTrip(t *testing.T) {
	input := [][]int64{
		{1, 2, 3},
		{2, 4, 6},
		{10, 20, 30},
	}

	var buf bytes.Buffer
	encoder := compression.DeltaOfDeltaSliceEncoder{Writer: &buf}
	for _, values := range input {
		if err := encoder.Encode(values); err != nil {
			t.Fatalf("encode failed: %v", err)
		}
	}

	decoder := compression.DeltaOfDeltaSliceDecoder{Reader: bytes.NewReader(buf.Bytes())}
	for i, expected := range input {
		decoded, err := decoder.Decode(len(expected))
		if err != nil {
			t.Fatalf("decode %d failed: %v", i, err)
		}

		if !reflect.DeepEqual(expected, decoded) {
			t.Fatalf("decode %d mismatch: expected %v, got %v", i, expected, decoded)
		}
	}
}

func TestDeltaOfDeltaSliceDecoder_TruncatedRecord(t *testing.T) {
	data := []byte{0x01}
	decoder := compression.DeltaOfDeltaSliceDecoder{Reader: bytes.NewReader(data)}

	_, err := decoder.Decode(2)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected unexpected EOF, got %v", err)
	}
}
