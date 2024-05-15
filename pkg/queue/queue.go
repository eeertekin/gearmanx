package queue

import "fmt"

func Async(fn string, payload []byte) {
	fmt.Printf("[queue] %s(async) with payload : %s\n", fn, payload)
}

func Sync(fn string, payload []byte) []byte {
	fmt.Printf("[queue] %s(sync) with payload : %s\n", fn, payload)

	return []byte("hello world")
}
