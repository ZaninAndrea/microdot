package main

import (
	"fmt"

	"github.com/ZaninAndrea/microdot/internal/trigram"
)

func main() {
	index, err := trigram.NewIndex("./tmp")
	if err != nil {
		panic(err)
	}
	defer index.Close()

	index.Add(1, 1, "hello world")
	for i := 2; i < 10; i++ {
		index.Add(1, int64(i), fmt.Sprintf("document %d content", i))
	}

	fmt.Println(index.Search("llo worl"))

}
