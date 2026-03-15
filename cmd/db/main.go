package main

import (
	"fmt"
	"time"

	"github.com/ZaninAndrea/microdot/internal/db"
)

func main() {
	myDB, err := db.NewDB("./tmp")
	if err != nil {
		panic(err)
	}
	defer myDB.Close()

	err = myDB.AddDocument(db.Labels{"stream": "example"}, map[string]any{"msg": "Hello, World!", "ts": time.Now().UnixMilli()})
	if err != nil {
		panic(err)
	}
	err = myDB.AddDocument(db.Labels{"stream": "example2"}, map[string]any{"msg": "Ciao, Mondo!", "ts": time.Now().UnixMilli()})
	if err != nil {
		panic(err)
	}
	err = myDB.AddDocument(db.Labels{"stream": "example2"}, map[string]any{"msg": "Ciao, Mondo!", "ts": time.Now().UnixMilli()})
	if err != nil {
		panic(err)
	}
	err = myDB.AddDocument(db.Labels{"stream": "example2"}, map[string]any{"msg": "Ciao, Mondo!", "ts": time.Now().UnixMilli()})
	if err != nil {
		panic(err)
	}
	err = myDB.AddDocument(db.Labels{"stream": "example2"}, map[string]any{"msg": "Ciao, Mondo!", "ts": time.Now().UnixMilli()})
	if err != nil {
		panic(err)
	}
	err = myDB.AddDocument(db.Labels{"stream": "example2"}, map[string]any{"msg": "Ciao, Mondo!", "ts": time.Now().UnixMilli()})
	if err != nil {
		panic(err)
	}

	results := myDB.Query(db.Labels{"stream": "example2"}, "Ciao")
	fmt.Println("Query results:")
	for res := range results {
		if res.IsErr() {
			println("Error:", res.Error().Error())
			continue
		}

		// Print the stream labels
		fmt.Printf("[%d] %d: %s\n", res.Value.StreamID, res.Value.DocumentID, res.Value.Document["msg"])
	}
}
