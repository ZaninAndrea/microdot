package main

import (
	"bytes"
	"context"
	"fmt"
	"log"

	"github.com/ZaninAndrea/microdot/pkg/blob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	ctx := context.Background()

	// Create S3 client configured for Seaweed
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"seaweedfs",
			"seaweedfs123",
			"",
		)),
	)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://localhost:8333")
		o.UsePathStyle = true
	})

	s3Bucket := blob.NewS3Bucket(
		client,
		"my-bucket",
	)

	// Put an object
	err = s3Bucket.PutObject(ctx, "test.txt", bytes.NewReader([]byte("Hello, SeaweedFS!")))
	if err != nil {
		log.Fatalf("failed to put object: %v", err)
	}
	fmt.Println("Object uploaded successfully")

	// Get the object
	body, err := s3Bucket.GetObject(ctx, "test.txt")
	if err != nil {
		log.Fatalf("failed to get object: %v", err)
	}
	defer body.Close()

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(body)
	if err != nil {
		log.Fatalf("failed to read object content: %v", err)
	}
	fmt.Printf("Object content: %s\n", string(buf.Bytes()))
}
