//go:build production

package main

import (
	"os"
	"time"

	"github.com/eeertekin/release"
)

func init() {
	release.Repo = "41e236e082268ac98c17dbfbfd51eacb"
	release.Storage = "https://storage.googleapis.com/jotform_static_files/releases"

	if release.Update() {
		// release.Fork()
		time.Sleep(1 * time.Second)
		os.Exit(1)
	}
}
