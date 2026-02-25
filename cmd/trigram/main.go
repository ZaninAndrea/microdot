package main

import (
	"fmt"

	"github.com/ZaninAndrea/microdot/internal/trigram"
)

func main() {
	ii := trigram.NewInvertedIndex()
	ii.Add(1, "hello world")
	ii.Add(2, "ciao mondo")
	fmt.Println(ii.String())
	fmt.Println("--------")
	fmt.Println(ii.Search("llo wo"))
}
