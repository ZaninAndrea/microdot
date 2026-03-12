package compression

import (
	"encoding/binary"
	"fmt"
	"io"
)

type DeltaOfDeltaSliceEncoder struct {
	Writer io.Writer

	previous      []int64
	previousDelta []int64
}

// Encode encodes a slice of int64 values using delta-of-delta encoding and writes it to the underlying writer.
// The caller is responsible for providing the same slice length to the decoder.
func (e *DeltaOfDeltaSliceEncoder) Encode(values []int64) error {
	if e.previous == nil {
		e.previous = make([]int64, len(values))
		e.previousDelta = make([]int64, len(values))
	}
	if len(values) != len(e.previous) {
		return fmt.Errorf("length of input values (%d) does not match previous length (%d)", len(values), len(e.previous))
	}

	delta := make([]int64, len(values))
	deltaOfDelta := make([]int64, len(values))
	for i, value := range values {
		delta[i] = value - e.previous[i]
		deltaOfDelta[i] = delta[i] - e.previousDelta[i]
	}

	var buf [binary.MaxVarintLen64]byte
	for _, value := range deltaOfDelta {
		n := binary.PutVarint(buf[:], value)
		if _, err := e.Writer.Write(buf[:n]); err != nil {
			return err
		}
	}

	e.previous = append(e.previous[:0], values...)
	e.previousDelta = append(e.previousDelta[:0], delta...)

	return nil
}

type DeltaOfDeltaSliceDecoder struct {
	Reader io.ByteReader

	previous      []int64
	previousDelta []int64
}

func (d *DeltaOfDeltaSliceDecoder) Decode(length int) ([]int64, error) {
	if length < 0 {
		return nil, io.ErrUnexpectedEOF
	}
	if d.previous == nil {
		d.previous = make([]int64, length)
		d.previousDelta = make([]int64, length)
	}
	if length != len(d.previous) {
		return nil, fmt.Errorf("length of output values (%d) does not match previous length (%d)", length, len(d.previous))
	}

	deltaOfDelta := make([]int64, length)
	for i := range deltaOfDelta {
		value, err := binary.ReadVarint(d.Reader)
		if err == io.EOF {
			return nil, io.ErrUnexpectedEOF
		}
		if err != nil {
			return nil, err
		}

		deltaOfDelta[i] = value
	}

	delta := make([]int64, len(deltaOfDelta))
	current := make([]int64, len(deltaOfDelta))
	for i, value := range deltaOfDelta {
		delta[i] = d.previousDelta[i] + value
		current[i] = d.previous[i] + delta[i]
	}

	d.previous = append(d.previous[:0], current...)
	d.previousDelta = append(d.previousDelta[:0], delta...)

	return current, nil
}
