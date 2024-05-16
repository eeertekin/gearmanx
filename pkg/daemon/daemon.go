package daemon

import (
	"fmt"
	"net"
	"os"
)

func ListenAndServe(addr string, handler func(net.Conn)) {
	fmt.Printf("# gearmanx\n")
	fmt.Printf("Listening port %s\n\n", addr)

	sock, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}

	defer sock.Close()

	for {
		var conn net.Conn

		conn, err = sock.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			continue
		}

		go handler(conn)
	}
}
