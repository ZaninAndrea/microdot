package stream

import "github.com/ZaninAndrea/microdot/pkg/blob"

type Reader struct {
	bucket blob.Bucket
}

func NewReader(bucket blob.Bucket) *Reader {
	return &Reader{
		bucket: bucket,
	}
}
