package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"time"

	"gearmanx/pkg/admin"
	"gearmanx/pkg/clients"
	"gearmanx/pkg/command"
	"gearmanx/pkg/config"
	"gearmanx/pkg/consts"
	"gearmanx/pkg/daemon"
	"gearmanx/pkg/handler"
	"gearmanx/pkg/http"
	"gearmanx/pkg/models"
	"gearmanx/pkg/storage"
	"gearmanx/pkg/workers"

	"github.com/valyala/bytebufferpool"
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
		status_ticker := time.NewTicker(1 * time.Second)
		for range status_ticker.C {
			storage.StatusUpdate()
			// PrintMemUsage()
		}
	}()

	if err := gearmanxd.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func Serve(conn net.Conn) {
	defer conn.Close()

	header_buf := make([]byte, 12)
	var err error
	var bsize int

	iam := models.IAM{
		Role: consts.ROLE_CLIENT,
		ID:   conn.RemoteAddr().String(),
	}

	clients.Register(&iam)

	for {
		bsize, err = conn.Read(header_buf)
		if err == io.EOF {
			break
		}

		if err != nil {
			// fmt.Printf("[serve] err> %s\n", err)
			break
		}

		if admin.Handle(conn, header_buf[:bsize]) {
			continue
		}

		c := command.ParseHeader(header_buf[:bsize])
		if c.Size != 0 {
			b := make([]byte, c.Size)
			bx := bytebufferpool.Get()

			for {
				subbsize, err := conn.Read(b)
				if err != nil {
					fmt.Printf("err> %s\n", err)
					break
				}
				bx.Write(b[:subbsize])

				if bx.Len() == c.Size {
					break
				}
			}
			// c.Data = bx.B[0:c.Size]
			c.Data = make([]byte, c.Size)
			copy(c.Data, bx.B[:c.Size])

			bytebufferpool.Put(bx)
		}

		handler.Run(conn, &iam, c)
	}

	// fmt.Printf("Connection closed %s\n", conn.RemoteAddr())

	if iam.Role == consts.ROLE_WORKER {
		for i := range iam.Functions {
			workers.Unregister(iam.Functions[i], []byte(iam.ID))
		}
	}
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Alloc = %v MiB", m.Alloc/1024/1024)
	fmt.Printf("\tTotalAlloc = %v MiB", m.TotalAlloc/1024/1024)
	fmt.Printf("\tSys = %v MiB", m.Sys/1024/1024)
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}
