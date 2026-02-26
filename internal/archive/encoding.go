package archive

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"

	"github.com/pierrec/lz4/v4"
)

type StructuredWriter struct {
	w      io.WriteCloser
	offset uint64
}

func NewStructuredWriter(w io.WriteCloser) *StructuredWriter {
	return &StructuredWriter{w: w, offset: 0}
}

// Write writes data to the underlying writer with no special formatting.
func (sw *StructuredWriter) Write(p []byte) (int, error) {
	n, err := sw.w.Write(p)
	sw.offset += uint64(n)
	return n, err
}

// Close closes the underlying writer.
func (sw *StructuredWriter) Close() error {
	return sw.w.Close()
}

func (sw *StructuredWriter) Offset() uint64 {
	return sw.offset
}

// WriteVarint writes a variable-length integer to the underlying writer.
func (sw *StructuredWriter) WriteVarint(value int64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutVarint(buf[:], value)
	_, err := sw.Write(buf[:n])
	return err
}

// WriteUvarint writes an unsigned variable-length integer to the underlying writer.
func (sw *StructuredWriter) WriteUvarint(value uint64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], value)
	_, err := sw.Write(buf[:n])
	return err
}

// WriteBytes writes a byte slice to the underlying writer, prefixed with its length as a varint.
func (sw *StructuredWriter) WriteBytes(data []byte) error {
	// First write the length of the data as a varint
	err := sw.WriteUvarint(uint64(len(data)))
	if err != nil {
		return err
	}

	// Then write the actual data
	_, err = sw.Write(data)
	return err
}

// WriteString writes a string to the underlying writer, prefixed with its length as a varint.
func (sw *StructuredWriter) WriteString(s string) error {
	return sw.WriteBytes([]byte(s))
}

// WriteUInt64 writes a 64-bit unsigned integer to the underlying writer.
func (sw *StructuredWriter) WriteUInt64(value uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	_, err := sw.Write(buf[:])
	return err
}

// WriteInt64 writes a 64-bit signed integer to the underlying writer.
func (sw *StructuredWriter) WriteInt64(value int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(value))
	_, err := sw.Write(buf[:])
	return err
}

// WriteFloat64 writes a 64-bit floating-point number to the underlying writer.
func (sw *StructuredWriter) WriteFloat64(value float64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], math.Float64bits(value))
	_, err := sw.Write(buf[:])
	return err
}

// WriteUInt32 writes a 32-bit unsigned integer to the underlying writer.
func (sw *StructuredWriter) WriteUInt32(value uint32) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], value)
	_, err := sw.Write(buf[:])
	return err
}

// WriteInt32 writes a 32-bit signed integer to the underlying writer.
func (sw *StructuredWriter) WriteInt32(value int32) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(value))
	_, err := sw.Write(buf[:])
	return err
}

// WriteFloat32 writes a 32-bit floating-point number to the underlying writer.
func (sw *StructuredWriter) WriteFloat32(value float32) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], math.Float32bits(value))
	_, err := sw.Write(buf[:])
	return err
}

// WriteUInt16 writes a 16-bit unsigned integer to the underlying writer.
func (sw *StructuredWriter) WriteUInt16(value uint16) error {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], value)
	_, err := sw.Write(buf[:])
	return err
}

// WriteInt16 writes a 16-bit signed integer to the underlying writer.
func (sw *StructuredWriter) WriteInt16(value int16) error {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], uint16(value))
	_, err := sw.Write(buf[:])
	return err
}

// WriteUint8 writes an 8-bit unsigned integer to the underlying writer.
func (sw *StructuredWriter) WriteUint8(value uint8) error {
	var buf [1]byte
	buf[0] = value
	_, err := sw.Write(buf[:])
	return err
}

// WriteInt8 writes an 8-bit signed integer to the underlying writer.
func (sw *StructuredWriter) WriteInt8(value int8) error {
	var buf [1]byte
	buf[0] = uint8(value)
	_, err := sw.Write(buf[:])
	return err
}

// WriteLZ4 compresses the input data using LZ4 and writes it to the underlying writer.
func (sw *StructuredWriter) WriteLZ4(p []byte) error {
	compressedWriter := lz4.NewWriter(sw)

	_, err := compressedWriter.Write(p)
	if err != nil {
		return err
	}

	err = compressedWriter.Close()
	if err != nil {
		return err
	}

	return nil
}

type StructuredReader struct {
	r io.ReadSeekCloser
}

func NewStructuredReader(r io.ReadSeekCloser) *StructuredReader {
	return &StructuredReader{r: r}
}

// Read reads data from the underlying reader.
func (sr *StructuredReader) Read(p []byte) (n int, err error) {
	return sr.r.Read(p)
}

func (sr *StructuredReader) Seek(offset int64, whence int) (int64, error) {
	return sr.r.Seek(offset, whence)
}

// Close closes the underlying reader.
func (sr *StructuredReader) Close() error {
	return sr.r.Close()
}

// ReadByte reads a single byte from the underlying reader.
// This is required for binary.ReadVarint.
func (sr *StructuredReader) ReadByte() (byte, error) {
	var buf [1]byte
	_, err := io.ReadFull(sr.r, buf[:])
	return buf[0], err
}

// ReadVarint reads a variable-length integer from the underlying reader.
func (sr *StructuredReader) ReadVarint() (int64, error) {
	return binary.ReadVarint(sr)
}

// ReadUvarint reads an unsigned variable-length integer from the underlying reader.
func (sr *StructuredReader) ReadUvarint() (uint64, error) {
	return binary.ReadUvarint(sr)
}

// ReadBytes reads a byte slice from the underlying reader, prefixed with its length as a varint.
func (sr *StructuredReader) ReadBytes() ([]byte, error) {
	length, err := sr.ReadUvarint()
	if err != nil {
		return nil, err
	}

	data := make([]byte, length)
	_, err = io.ReadFull(sr.r, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// ReadString reads a string from the underlying reader, prefixed with its length as a varint.
func (sr *StructuredReader) ReadString() (string, error) {
	data, err := sr.ReadBytes()
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// ReadUInt64 reads a 64-bit unsigned integer from the underlying reader.
func (sr *StructuredReader) ReadUInt64() (uint64, error) {
	var buf [8]byte
	_, err := io.ReadFull(sr.r, buf[:])
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(buf[:]), nil
}

// ReadInt64 reads a 64-bit signed integer from the underlying reader.
func (sr *StructuredReader) ReadInt64() (int64, error) {
	var buf [8]byte
	_, err := io.ReadFull(sr.r, buf[:])
	if err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(buf[:])), nil
}

// ReadFloat64 reads a 64-bit floating-point number from the underlying reader.
func (sr *StructuredReader) ReadFloat64() (float64, error) {
	var buf [8]byte
	_, err := io.ReadFull(sr.r, buf[:])
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(binary.BigEndian.Uint64(buf[:])), nil
}

// ReadUInt32 reads a 32-bit unsigned integer from the underlying reader.
func (sr *StructuredReader) ReadUInt32() (uint32, error) {
	var buf [4]byte
	_, err := io.ReadFull(sr.r, buf[:])
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(buf[:]), nil
}

// ReadInt32 reads a 32-bit signed integer from the underlying reader.
func (sr *StructuredReader) ReadInt32() (int32, error) {
	var buf [4]byte
	_, err := io.ReadFull(sr.r, buf[:])
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(buf[:])), nil
}

// ReadFloat32 reads a 32-bit floating-point number from the underlying reader.
func (sr *StructuredReader) ReadFloat32() (float32, error) {
	var buf [4]byte
	_, err := io.ReadFull(sr.r, buf[:])
	if err != nil {
		return 0, err
	}
	return math.Float32frombits(binary.BigEndian.Uint32(buf[:])), nil
}

// ReadUInt16 reads a 16-bit unsigned integer from the underlying reader.
func (sr *StructuredReader) ReadUInt16() (uint16, error) {
	var buf [2]byte
	_, err := io.ReadFull(sr.r, buf[:])
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(buf[:]), nil
}

// ReadInt16 reads a 16-bit signed integer from the underlying reader.
func (sr *StructuredReader) ReadInt16() (int16, error) {
	var buf [2]byte
	_, err := io.ReadFull(sr.r, buf[:])
	if err != nil {
		return 0, err
	}
	return int16(binary.BigEndian.Uint16(buf[:])), nil
}

// ReadUint8 reads an 8-bit unsigned integer from the underlying reader.
func (sr *StructuredReader) ReadUint8() (uint8, error) {
	var buf [1]byte
	_, err := io.ReadFull(sr.r, buf[:])
	if err != nil {
		return 0, err
	}
	return buf[0], nil
}

// ReadInt8 reads an 8-bit signed integer from the underlying reader.
func (sr *StructuredReader) ReadInt8() (int8, error) {
	var buf [1]byte
	_, err := io.ReadFull(sr.r, buf[:])
	if err != nil {
		return 0, err
	}
	return int8(buf[0]), nil
}

// ReadLZ4 reads LZ4-compressed data from the underlying reader and decompresses it.
func (sr *StructuredReader) ReadLZ4() ([]byte, error) {
	compressedReader := lz4.NewReader(sr)

	buffer := bytes.NewBuffer(nil)

	_, err := compressedReader.WriteTo(buffer)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}
