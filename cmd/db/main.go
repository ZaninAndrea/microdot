package main

import (
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
}
