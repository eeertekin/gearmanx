package admin

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"

	"gearmanx/pkg/storage"
	"gearmanx/pkg/workers"
)

func WorkersNext(conn net.Conn) {
	for wrk_id, wrk := range workers.NextStatus() {
		conn.Write([]byte(fmt.Sprintf("%s : Sleeping ? %v\n", wrk_id, wrk.IsSleeping())))
	}

	conn.Write([]byte(".\n"))
}

func Status(conn net.Conn) {
	fns := storage.Status()

	ordered_fns := []string{}
	for i := range fns {
		ordered_fns = append(ordered_fns, i)
	}
	sort.Strings(ordered_fns)

	for _, i := range ordered_fns {
		conn.Write([]byte(fmt.Sprintf("%s\t%d\t%d\t%d\n", fns[i].Name, fns[i].Jobs, fns[i].InProgress, fns[i].Workers)))
	}
	conn.Write([]byte(".\n"))
}

func Version(conn net.Conn) {
	conn.Write([]byte("OK gearmanx v0.1\r\n"))
}

func Shutdown(conn net.Conn) {
	fmt.Printf("- shutdown requested by %s\n", conn.RemoteAddr())
	conn.Write([]byte("OK\r\n"))
	os.Exit(1)
}

func Workers(conn net.Conn) {
	res := workers.ListPipe()

	var tmp []string
	var remote_addr []string
	for wrk_id := range res {
		tmp = strings.SplitN(wrk_id, "::wrk::", 2)
		remote_addr = strings.SplitN(tmp[1], ":", 2)

		conn.Write([]byte(fmt.Sprintf("%s %s %x : %s\n", remote_addr[1], remote_addr[0], md5.Sum([]byte(tmp[1])), res[wrk_id])))
	}

	conn.Write([]byte(".\n"))
}

var router map[string]func(net.Conn)
var max_size int

func init() {
	router = map[string]func(net.Conn){
		"status_next": WorkersNext,
		"status":      Status,
		"version":     Version,
		"shutdown":    Shutdown,
		"workers":     Workers,
	}
	for i := range router {
		if len(i) > max_size {
			max_size = len(i)
		}
	}
}

func Handle(conn net.Conn, buf []byte) bool {
	buf = bytes.Trim(buf, "\r\n")
	if len(buf) > max_size {
		return false
	}

	if _, ok := router[string(buf)]; ok {
		router[string(buf)](conn)
		return true
	}

	return false
}
