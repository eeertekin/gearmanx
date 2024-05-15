package main

import (
	"encoding/binary"
	"fmt"
	"net"
)

func toByteArray(i int32) (arr []byte) {
	arr = make([]byte, 4)

	binary.BigEndian.PutUint32(arr, uint32(i))
	return
}

func main() {
	req := []byte("\x00REQ")
	req = append(req, toByteArray(26)...)
	req = append(req, toByteArray(10)...)
	req = append(req, "exceptions"...)
	fmt.Printf("%v\n", req)

	conn, err := net.Dial("tcp", "localhost:4733")
	if err != nil {
		fmt.Printf("err> %s\n", err)
		return
	}

	conn.Write(req)

	buf := make([]byte, 1024)
	res := []byte("\x00RES")
	res = append(res, toByteArray(27)...)
	res = append(res, toByteArray(10)...)
	res = append(res, "exceptions"...)

	fmt.Printf("res: %v\n\n", res)

	for {
		i, err := conn.Read(buf)
		if err != nil {
			fmt.Printf("err> %s\n", err)
			break
		}
		fmt.Printf("%d %v %s\n", i, buf, buf)
	}
}
