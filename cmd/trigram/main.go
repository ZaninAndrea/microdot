package main

import (
	"fmt"

	"github.com/ZaninAndrea/microdot/internal/trigram"
)

func main() {
	ii := trigram.NewMemoryInvertedIndex()
	ii.Add(1, "hello world")
	ii.Add(2, "ciao mondo")

	err := trigram.WriteToDiskFS(ii, "./tmp", "test")
	if err != nil {
		panic(err)
	}

	dii, err := trigram.OpenDiskInvertedIndexFS("./tmp", "test")
	if err != nil {
		panic(err)
	}

	ii, err = dii.LoadAll()
	if err != nil {
		panic(err)
	}

	fmt.Println(ii.String())
	fmt.Println("--------")
	fmt.Println(ii.Search("llo wo"))
}
