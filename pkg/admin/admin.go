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

func Handle(conn net.Conn, buf []byte) bool {
	if bytes.HasPrefix(buf, []byte("status_next")) {
		WorkersNext(conn)
		return true
	}

	if bytes.HasPrefix(buf, []byte("status")) {
		Status(conn)
		return true
	}

	if bytes.HasPrefix(buf, []byte("version")) {
		Version(conn)
		return true
	}

	if bytes.HasPrefix(buf, []byte("shutdown")) {
		Shutdown(conn)
		return true
	}

	if bytes.HasPrefix(buf, []byte("workers")) {
		Workers(conn)
		return true
	}

	return false
}
