package daemon

import (
	"fmt"
	"gearmanx/pkg/storage"
	"gearmanx/pkg/workers"
	"net"
	"os"
	"os/signal"
	"syscall"
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

		fmt.Printf("Shutting down .... \n\n")

		for _, fn := range storage.GetFuncs() {
			for _, wID := range workers.GetWorkerIDs(fn) {
				fmt.Printf("[shutdown] Closing worker connection #%s\n", wID)
				workers.Close(wID, fn)
				storage.DeleteWorker(wID, fn)
			}
		}

		g.Close()
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
			fmt.Println("Error accepting: ", err.Error())
			continue
		}

		go g.Handler(conn)
	}

	return nil
}
