package compression

type Encoder[T any] interface {
	Encode(values []T) []byte
	Flush() []byte
}

type Decoder[T any] interface {
	Decode(encoded []byte) ([]T, error)
	Flush() ([]T, error)
}
