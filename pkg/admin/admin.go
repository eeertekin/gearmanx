package admin

import (
	"fmt"
	"net"
	"os"
	"sort"
	"strings"

	"gearmanx/pkg/storage"
	"gearmanx/pkg/workers"
)

func WakeUpAll() {
	for _, fn := range storage.GetFuncs() {
		workers.WakeUpAll(fn)
	}
}

func Status(conn net.Conn) {
	fns := storage.Status()

	ordered_fns := []string{}
	for i := range fns {
		ordered_fns = append(ordered_fns, i)
	}
	sort.Strings(ordered_fns)

	for _, i := range ordered_fns {
		conn.Write([]byte(fmt.Sprintf("%s\t\t%d\t%d\t%d\n", fns[i].Name, fns[i].Jobs, fns[i].InProgress, fns[i].Workers)))
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
	res := map[string]string{}

	for fn, fn_group := range workers.ListWorkers() {
		for ID := range fn_group {
			res[fn_group[ID].RemoteAddr+":"+ID] += fn + " "
		}
	}

	for i := range res {
		tmp := strings.Split(i, ":")
		conn.Write([]byte(fmt.Sprintf("%s %s %s : %s\n", tmp[1], tmp[0], tmp[2], res[i])))
	}

	conn.Write([]byte(".\n"))
}
