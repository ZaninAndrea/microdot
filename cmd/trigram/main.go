package main

import (
	"fmt"

	"github.com/ZaninAndrea/microdot/internal/trigram"
)

func main() {
	ii := trigram.NewMemoryInvertedIndex()
	ii.Add(1, "hello world")
	ii.Add(2, "ciao mondo")

	err := ii.WriteToDiskFS("./tmp", "test")
	if err != nil {
		panic(err)
	}
	ii, err = trigram.LoadFromDiskFS("./tmp", "test")
	if err != nil {
		panic(err)
	}

	fmt.Println(ii.String())
	fmt.Println("--------")
	fmt.Println(ii.Search("llo wo"))
}
