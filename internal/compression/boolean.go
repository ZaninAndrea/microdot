package compression

import "unsafe"

type BitPackingEncoder struct {
	leftoverValues []bool
}

func (bp *BitPackingEncoder) Encode(values []bool) []byte {
	if len(values) == 0 {
		return []byte{}
	}

	encodedLen := len(values) / 8
	if len(values)%8 != 0 {
		bp.leftoverValues = values[encodedLen*8:]
		values = values[:encodedLen*8]
	} else {
		bp.leftoverValues = nil
	}

	encoded := make([]byte, encodedLen)
	for i := 0; i < encodedLen; i++ {
		// unsafe optimization to convert a bool to 0 or 1 as uint8,
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

	return encoded
}

func (bp *BitPackingEncoder) Flush() []byte {
	if len(bp.leftoverValues) == 0 {
		return []byte{}
	}

	encoded := make([]byte, 1)
	for i := 0; i < len(bp.leftoverValues); i++ {
		encoded[0] |= uint8(*(*uint8)(unsafe.Pointer(&bp.leftoverValues[i]))) << (i % 8)
	}

	bp.leftoverValues = nil
	return encoded
}

type BitPackingDecoder struct{}

func (bd *BitPackingDecoder) Decode(encoded []byte) []bool {
	if len(encoded) == 0 {
		return []bool{}
	}

	decodedLen := len(encoded) * 8
	decoded := make([]bool, decodedLen)
	for i := 0; i < decodedLen; i++ {
		decoded[i] = (encoded[i/8] & (1 << (i % 8))) != 0
	}

	return decoded
}

func (bd *BitPackingDecoder) Flush() []bool {
	return []bool{}
}
