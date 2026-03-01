package main

import "github.com/ZaninAndrea/microdot/internal/db"

func main() {
	myDB, err := db.NewDB("./tmp/wal", "./tmp/streams")
	if err != nil {
		panic(err)
	}

	err = myDB.AddDocument(db.Labels{"stream": "example"}, map[string]any{"msg": "Hello, World!"})
	if err != nil {
		panic(err)
	}
	err = myDB.AddDocument(db.Labels{"stream": "example2"}, map[string]any{"msg": "Ciao, Mondo!"})
	if err != nil {
		panic(err)
	}
}
