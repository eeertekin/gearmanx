package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	"gearmanx/pkg/admin"
	"gearmanx/pkg/clients"
	"gearmanx/pkg/config"
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
	config.Parse()

	// debug.SetGCPercent(-1)
	// debug.SetMemoryLimit(512 * 1024 * 1024)
	go http.Serve()

	if err := storage.NewStorage(config.BackendURI); err != nil {
		os.Exit(1)
	}

	gearmanxd := daemon.New(
		fmt.Sprintf("%s:%d", config.Addr, config.Port),
		config.BackendURI,
		Serve,
	)
	gearmanxd.HandleSignals()

	go workers.Ticker()
	go storage.WakeUpCalls(func(fn string) {
		workers.WakeUpAll(fn)
	})

	go func() {
		status_ticker := time.NewTicker(2 * time.Second)
		for range status_ticker.C {
			storage.StatusUpdate()
		}
	}()

	if err := gearmanxd.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func Serve(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 1024)
	var err error
	var bsize int

	iam := models.IAM{
		Role: consts.ROLE_CLIENT,
		ID:   conn.RemoteAddr().String(),
	}

	clients.Register(&iam)

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

		if admin.Handle(conn, buf[0:bsize]) {
			continue
		}

		commands := parser.Parse(buf, bsize, &fragmented_buf)
		if commands == nil {
			fmt.Printf("[main] parser returned nil\n")
			break
		}
		// To disable parse packages, open it
		// commands := ParseCommands(buf[0:bsize])
		for i := range commands {
			if commands[i] == nil {
				continue
			}
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
