package main

import (
	"context"
	"log"
	"time"

	"github.com/ZaninAndrea/microdot/internal/db"
	"github.com/ZaninAndrea/microdot/pkg/blob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	myDB, err := db.NewDB(initBucket())
	if err != nil {
		panic(err)
	}
	defer myDB.Close()

	for i := 0; i < 10; i++ {
		go func() {
			err = myDB.AddDocument(db.Labels{"stream": "example"}, map[string]any{"msg": "Hello, World!", "ts": time.Now().UnixMilli()})
			if err != nil {
				panic(err)
			}
		}()
	}

	err = myDB.AddDocument(db.Labels{"stream": "example2"}, map[string]any{"msg": "Ciao, Mondo!", "ts": time.Now().UnixMilli()})
	if err != nil {
		panic(err)
	}
	err = myDB.AddDocument(db.Labels{"stream": "example2"}, map[string]any{"msg": "Ciao, Mondo!", "ts": time.Now().UnixMilli()})
	if err != nil {
		panic(err)
	}

	// results := myDB.Query(db.Labels{"stream": "example2"}, "Ciao")
	// fmt.Println("Query results:")
	// for res := range results {
	// 	if res.IsErr() {
	// 		println("Error:", res.Error().Error())
	// 		continue
	// 	}

	// 	// Print the stream labels
	// 	fmt.Printf("[%d] %d: %s\n", res.Value.StreamID, res.Value.DocumentID, res.Value.Document["msg"])
	// }
}

func initBucket() blob.Bucket {
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
		"microdot",
	)
	return s3Bucket
}
