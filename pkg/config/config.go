package config

import (
	"flag"
)

var BackendURI string
var Port int
var Addr string

func Parse() {
	backend_uri := flag.String("storage", "redis://127.0.0.1:6379", "storage URI")
	listen_port := flag.Int("p", 4730, "port")
	addr := flag.String("listen", "127.0.0.1", "bind addr")

	_ = flag.String("tcp", "tcp", "protocol")
	_ = flag.Int("j", 3, "retry count")
	_ = flag.Int("t", 16, "thread count")

	flag.Parse()

	BackendURI = *backend_uri
	Port = *listen_port
	Addr = *addr
}
