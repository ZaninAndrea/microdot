package compression

import (
	"bytes"
	"encoding/binary"
	"io"
)

func EncodeDeltaOfDelta(values []int64) []byte {
	if len(values) == 0 {
		return []byte{}
	}

	encoded := []byte{}

	var previous int64 = 0
	var previousDelta int64 = 0
	for i := range values {
		delta := values[i] - previous
		deltaOfDelta := delta - previousDelta
		encoded = binary.AppendVarint(encoded, deltaOfDelta)

		previous = values[i]
		previousDelta = delta
	}

	return encoded
}

func DecodeDeltaOfDelta(encoded []byte) ([]int64, error) {
	if len(encoded) == 0 {
		return []int64{}, nil
	}

	reader := bytes.NewReader(encoded)
	data := []int64{}

	var previous int64 = 0
	var previousDelta int64 = 0
	for {
		deltaOfDelta, err := binary.ReadVarint(reader)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		delta := previousDelta + deltaOfDelta
		current := previous + delta

		data = append(data, current)

		previous = current
		previousDelta = delta
	}

	return data, nil
}
