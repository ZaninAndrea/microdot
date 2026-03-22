package blob

import (
	"context"
	"io"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Bucket struct {
	client *s3.Client
	name   string
}

func NewS3Bucket(client *s3.Client, name string) *S3Bucket {
	return &S3Bucket{
		client: client,
		name:   name,
	}
}

func (b *S3Bucket) PutObject(ctx context.Context, key string, content io.Reader) error {
	uploader := manager.NewUploader(b.client)
	_, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(b.name),
		Key:    aws.String(key),
		Body:   content,
	})
	return err
}

func (b *S3Bucket) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	result, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: new(b.name),
		Key:    new(key),
	})
	if err != nil {
		return nil, err
	}

	return result.Body, nil
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
