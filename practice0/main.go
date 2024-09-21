package main

import (
	"fmt"
	"math/rand"
	"time"
)

func main() {
	b := [100]byte{}

	go func() {
		for {
			filler(b[:len(b)/2], '0', '1')
			time.Sleep(1 * time.Second)
		}
	}()

	go func() {
		for {
			filler(b[len(b)/2:], 'X', 'Y')
			time.Sleep(1 * time.Second)
		}
	}()

	go func() {
		for {
			fmt.Println(string(b[:]))
			time.Sleep(1 * time.Second)
		}
	}()

	select {}
}

func filler(b []byte, ifzero byte, ifnot byte) {
	for i := 0; i < len(b); i++ {
		if rand.Intn(2) == 0 {
			b[i] = ifzero
		} else {
			b[i] = ifnot
		}
	}
}
