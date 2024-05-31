package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"gearmanx/pkg/admin"
	"gearmanx/pkg/consts"
	"gearmanx/pkg/daemon"
	"gearmanx/pkg/handler"
	"gearmanx/pkg/http"
	"gearmanx/pkg/models"
	"gearmanx/pkg/parser"
	"gearmanx/pkg/storage"
	"gearmanx/pkg/workers"
)

func main() {
	// debug.SetGCPercent(-1)
	// debug.SetMemoryLimit(512 * 1024 * 1024)
	go http.Serve()

	backend_uri := flag.String("storage", "redis://127.0.0.1:6379", "storage URI")
	listen_port := flag.Int("p", 4730, "port")
	addr := flag.String("listen", "127.0.0.1", "bind addr")

	_ = flag.String("tcp", "tcp", "protocol")
	_ = flag.Int("j", 3, "retry count")
	_ = flag.Int("t", 16, "thread count")

	flag.Parse()

	if err := storage.NewStorage(*backend_uri); err != nil {
		os.Exit(1)
	}

	gearmanxd := daemon.New(
		fmt.Sprintf("%s:%d", *addr, *listen_port),
		*backend_uri,
		Serve,
	)
	gearmanxd.HandleSignals()

	go workers.Ticker()

	log.Fatal(gearmanxd.ListenAndServe())
}

func Serve(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 1024)
	var err error
	var bsize int

	iam := models.IAM{
		Role: consts.ROLE_CLIENT,
	}

	fragmented_buf := bytes.Buffer{}

	for {
		bsize, err = conn.Read(buf)
		if err == io.EOF {
			break
		}

		if err != nil {
			// fmt.Printf("[serve] err> %s\n", err)
			break
		}

		if admin.Handle(conn, buf) {
			continue
		}

		commands := parser.Parse(buf, bsize, &fragmented_buf)

		// To disable parse packages, open it
		// commands := ParseCommands(buf[0:bsize])
		for i := range commands {
			handler.Run(conn, &iam, commands[i])
		}
	}

	// fmt.Printf("Connection closed %s\n", conn.RemoteAddr())

	if iam.Role == consts.ROLE_WORKER {
		for i := range iam.Functions {
			workers.Unregister(iam.Functions[i], []byte(iam.ID))
		}
	}
}
