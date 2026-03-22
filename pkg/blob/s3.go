package blob

import (
	"context"
	"io"
	"iter"
	"strconv"

	"github.com/ZaninAndrea/microdot/pkg/containers"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Bucket struct {
	client  *s3.Client
	manager *transfermanager.Client
	name    string
}

func NewS3Bucket(client *s3.Client, name string) *S3Bucket {
	return &S3Bucket{
		client:  client,
		manager: transfermanager.New(client),
		name:    name,
	}
}

func (b *S3Bucket) PutObject(ctx context.Context, key string, content io.Reader) error {
	_, err := b.manager.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket: new(b.name),
		Key:    new(key),
		Body:   content,
	})
	return err
}

func (b *S3Bucket) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	result, err := b.manager.GetObject(ctx, &transfermanager.GetObjectInput{
		Bucket: new(b.name),
		Key:    new(key),
	})
	if err != nil {
		return nil, err
	}

	return io.NopCloser(result.Body), nil
}

func (b *S3Bucket) GetObjectRange(ctx context.Context, key string, start, end int) (io.ReadCloser, error) {
	result, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: new(b.name),
		Key:    new(key),
		Range:  new("bytes=" + strconv.Itoa(start) + "-" + strconv.Itoa(end)),
	})
	if err != nil {
		return nil, err
	}

	return result.Body, nil
}

func (b *S3Bucket) DeleteObject(ctx context.Context, key string) error {
	_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: new(b.name),
		Key:    new(key),
	})
	return err
}

func (b *S3Bucket) ListObjects(ctx context.Context, prefix string) iter.Seq[containers.Result[string]] {
	return func(yield func(containers.Result[string]) bool) {
		paginator := s3.NewListObjectsV2Paginator(b.client, &s3.ListObjectsV2Input{
			Bucket: new(b.name),
			Prefix: new(prefix),
		})

		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				yield(containers.Err[string](err))
				return
			}

			for _, obj := range page.Contents {
				if !yield(containers.Ok(*obj.Key)) {
					return
				}
			}
		}
	}
}
