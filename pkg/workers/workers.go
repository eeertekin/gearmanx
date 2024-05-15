package workers

import (
	"fmt"
	"gearmanx/pkg/command"
	"gearmanx/pkg/consts"
	"gearmanx/pkg/redis"
	"net"
	"sync"
)

var workers map[string][]Worker

func init() {
	mutex = sync.Mutex{}

	workers = make(map[string][]Worker)
}

type Worker struct {
	Func string
	ID   string
	Conn net.Conn
}

var mutex sync.Mutex

func Register(fn string, ID []byte, conn net.Conn) {
	// fmt.Printf("[worker-register] Register %s from %s\n", ID, fn)

	mutex.Lock()
	defer mutex.Unlock()

	redis.AddWorker(string(ID), fn)
	workers[fn] = append(workers[fn], Worker{
		Func: fn,
		ID:   string(ID),
		Conn: conn,
	})
}

func Unregister(fn string, ID []byte) {
	fmt.Printf("[worker-unregister] Purge %s from %s\n", ID, fn)
	redis.PurgeWorker(string(ID), fn)
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

// func init() {
// 	workers.Store("reverse", func(payload []byte) (r []byte) {
// 		res := ""
// 		for _, v := range payload {
// 			res = string(v) + res
// 		}
// 		return []byte(res)
// 	})
// }

// func Do(fn string, payload []byte) (res []byte) {
// 	if f, ok := workers.Load(fn); ok {
// 		f2 := f.(func([]byte) []byte)
// 		return f2(payload)
// 	}

// 	return []byte("I don't know")
// }
