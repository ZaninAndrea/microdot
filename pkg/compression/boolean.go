package compression

import (
	"encoding/binary"
	"fmt"
	"unsafe"
)

// EncodeBitPacking encodes a slice of booleans into a bit-packed byte slice.
func EncodeBitPacking(values []bool) []byte {
	if len(values) == 0 {
		return []byte{}
	}

	encodedLen := (len(values) + 7) / 8
	encoded := make([]byte, encodedLen)

	// Process full bytes (groups of 8 booleans)
	fullBytes := len(values) / 8
	for i := 0; i < fullBytes; i++ {
		// optimization to convert a bool to 0 or 1 as uint8,
		// then pack 8 bools into a single byte
		offset := i * 8
		encoded[i] = uint8(*(*uint8)(unsafe.Pointer(&values[offset]))) |
			(uint8(*(*uint8)(unsafe.Pointer(&values[offset+1]))) << 1) |
			(uint8(*(*uint8)(unsafe.Pointer(&values[offset+2]))) << 2) |
			(uint8(*(*uint8)(unsafe.Pointer(&values[offset+3]))) << 3) |
			(uint8(*(*uint8)(unsafe.Pointer(&values[offset+4]))) << 4) |
			(uint8(*(*uint8)(unsafe.Pointer(&values[offset+5]))) << 5) |
			(uint8(*(*uint8)(unsafe.Pointer(&values[offset+6]))) << 6) |
			(uint8(*(*uint8)(unsafe.Pointer(&values[offset+7]))) << 7)
	}

	// Process remaining booleans
	if remaining := len(values) % 8; remaining > 0 {
		offset := fullBytes * 8
		var b uint8
		for j := 0; j < remaining; j++ {
			b |= uint8(*(*uint8)(unsafe.Pointer(&values[offset+j]))) << j
		}
		encoded[encodedLen-1] = b
	}

	// Prefix the encoded data with the value count as a varint
	result := make([]byte, 0, len(encoded)+binary.MaxVarintLen64)
	result = binary.AppendUvarint(result, uint64(len(values)))
	result = append(result, encoded...)

	return result
}

// DecodeBitPacking decodes a bit-packed byte slice into a slice of booleans.
// The returned slice will have a length equal to len(encoded) * 8.
func DecodeBitPacking(encoded []byte) ([]any, error) {
	if len(encoded) == 0 {
		return []any{}, nil
	}

	valueCount, n := binary.Uvarint(encoded)
	if n <= 0 {
		return nil, fmt.Errorf("invalid varint encoding")
	}

	encoded = encoded[n:]

	decodedLen := len(encoded) * 8
	decoded := make([]any, decodedLen)
	for i := 0; i < decodedLen; i++ {
		decoded[i] = (encoded[i/8] & (1 << (i % 8))) != 0
	}

	return decoded[:valueCount], nil
}
