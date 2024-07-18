package daemon

import (
	"fmt"
	"gearmanx/pkg/storage"
	"gearmanx/pkg/workers"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type GearmanX struct {
	Addr    string
	Handler func(net.Conn)

	Storage string

	socket        net.Listener
	shutting_down bool
}

func New(addr, storage_addr string, handler func(net.Conn)) *GearmanX {
	return &GearmanX{
		Addr:    addr,
		Storage: storage_addr,
		Handler: handler,
	}
}

func (g *GearmanX) Header() {
	fmt.Printf("# gearmanx\n")
	fmt.Printf("  Addr       : %s://%s\n", "tcp", g.Addr)
	fmt.Printf("  Storage    : %s\n\n", g.Storage)
}

func (g *GearmanX) Close() {
	g.socket.Close()
}

func (g *GearmanX) HandleSignals() {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		g.shutting_down = true

		wlist := workers.GetNext()
		for ID, w := range wlist {
			fmt.Printf("[shutdown] Closing next worker connection #%s\n", ID)
			w.Close()
		}

		fmt.Printf("[shutdown] worker sockets closed\n")
		storage.ClearWorkers()

		time.Sleep(3 * time.Second)

		g.Close()
		fmt.Printf("[shutdown] socket closed\n")

		fmt.Printf("[shutdown] exiting with status 1\n")
		os.Exit(1)
	}()

}

func (g *GearmanX) ListenAndServe() (err error) {
	g.Header()

	g.socket, err = net.Listen("tcp", g.Addr)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		return err
	}

	for {
		if g.shutting_down {
			break
		}

		var conn net.Conn

		conn, err = g.socket.Accept()
		if err != nil {
			// fmt.Println("Error accepting: ", err.Error())
			continue
		}

		// TODO: On shutdown process, 1 new connection is still able to reach because of Accept() block the loop, find a better way to bypass it
		if g.shutting_down {
			conn.Close()
			break
		}

		go g.Handler(conn)
	}

	return nil
}
