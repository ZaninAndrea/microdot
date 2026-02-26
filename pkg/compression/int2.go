package compression

import (
	"encoding/binary"
	"io"
)

type DeltaOfDeltaPairEncoder struct {
	Writer io.Writer

	previous      [2]int64
	previousDelta [2]int64
}

// Encode encodes a pair of int64 values using delta-of-delta encoding and writes it to the underlying writer.
// This is designed to be called repeatedly for a sequence of pairs, where each pair is encoded relative to the previous one.
func (e *DeltaOfDeltaPairEncoder) Encode(a, b int64) error {
	delta := [2]int64{a - e.previous[0], b - e.previous[1]}
	deltaOfDelta := [2]int64{delta[0] - e.previousDelta[0], delta[1] - e.previousDelta[1]}

	var buf [binary.MaxVarintLen64 * 2]byte
	n := binary.PutVarint(buf[:], deltaOfDelta[0])
	n += binary.PutVarint(buf[n:], deltaOfDelta[1])

	if _, err := e.Writer.Write(buf[:n]); err != nil {
		return err
	}

	e.previous = [2]int64{a, b}
	e.previousDelta = delta

	return nil
}

type DeltaOfDeltaPairDecoder struct {
	Reader io.ByteReader

	previous      [2]int64
	previousDelta [2]int64
}

func (d *DeltaOfDeltaPairDecoder) Decode() ([2]int64, error) {
	var deltaOfDelta [2]int64
	for i := 0; i < 2; i++ {
		value, err := binary.ReadVarint(d.Reader)
		if err == io.EOF && i != 0 {
			// If we reach EOF after reading the first value, it's an error because we expect pairs.
			return [2]int64{}, io.ErrUnexpectedEOF
		} else if err != nil {
			return [2]int64{}, err
		}

		deltaOfDelta[i] = value
	}

	delta := [2]int64{d.previousDelta[0] + deltaOfDelta[0], d.previousDelta[1] + deltaOfDelta[1]}
	current := [2]int64{d.previous[0] + delta[0], d.previous[1] + delta[1]}

	d.previous = current
	d.previousDelta = delta

	return current, nil
}
