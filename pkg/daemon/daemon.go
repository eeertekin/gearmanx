package daemon

import (
	"fmt"
	"net"
)

type GearmanX struct {
	Addr    string
	Handler func(net.Conn)

	socket net.Listener
}

func New(addr string, handler func(net.Conn)) *GearmanX {
	return &GearmanX{
		Addr:    addr,
		Handler: handler,
	}
}

func (g *GearmanX) Header() {
	fmt.Printf("# gearmanx\n")
	fmt.Printf("Listening on %s\n\n", g.Addr)
}

func (g *GearmanX) Close() {
	g.socket.Close()
}

func (g *GearmanX) ListenAndServe() (err error) {
	g.Header()

	g.socket, err = net.Listen("tcp", g.Addr)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		return err
	}

	defer g.Close()

	for {
		var conn net.Conn

		conn, err = g.socket.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			continue
		}

		go g.Handler(conn)
	}
}
