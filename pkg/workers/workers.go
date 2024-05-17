package workers

import (
	"fmt"
	"gearmanx/pkg/command"
	"gearmanx/pkg/consts"
	"gearmanx/pkg/storage"
	"net"
	"sync"
)

var workers map[string][]Worker

func init() {
	mutex = sync.Mutex{}

	workers = make(map[string][]Worker)
}

type Worker struct {
	Func       string   `json:"func"`
	ID         string   `json:"id"`
	RemoteAddr string   `json:"remote_addr"`
	Conn       net.Conn `json:"-"`
}

var mutex sync.Mutex

func Register(fn string, ID []byte, conn net.Conn) {
	// fmt.Printf("[worker-register] Register %s from %s\n", ID, fn)

	mutex.Lock()
	defer mutex.Unlock()

	storage.AddWorker(string(ID), fn)

	workers[fn] = append(workers[fn], Worker{
		Func:       fn,
		ID:         string(ID),
		RemoteAddr: conn.RemoteAddr().String(),
		Conn:       conn,
	})
}

func Unregister(fn string, ID []byte) {
	fmt.Printf("[worker-unregister] Purge %s from %s\n", ID, fn)
	storage.DeleteWorker(string(ID), fn)
}

func ListWorkers() map[string][]Worker {
	return workers
}

func GetWorker(fn string) *Worker {
	return &workers[fn][0]
}

func WakeUpAll(fn string) {
	// fmt.Printf("[wake-up-all] %s\n", fn)
	for i := range workers[fn] {
		// fmt.Printf("[wake-up] %s\n", workers[fn][i].ID)
		workers[fn][i].Conn.Write(command.NewByteWithData(
			consts.RESPONSE,
			consts.NOOP,
		))
	}
}
