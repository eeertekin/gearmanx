package main

import (
	"gearmanx/pkg/config"
	"gearmanx/pkg/http"
	"gearmanx/pkg/storage"
	"os"

	"time"
)

func main() {
	config.Parse()

	if err := storage.NewStorage(config.BackendURI); err != nil {
		os.Exit(1)
	}

	go func() {
		status_ticker := time.NewTicker(1 * time.Second)
		for range status_ticker.C {
			storage.StatusUpdate()
		}
	}()

	http.Serve()
}
