//go:build production

package main

import (
	"os"

	"github.com/eeertekin/release"
)

func init() {
	release.Repo = "424b1e56633c1c12d4099000db23f80b"
	release.Storage = "https://storage.googleapis.com/jotform_static_files/releases"

	if release.Update() {
		// release.Fork()
		os.Exit(1)
	}
}
