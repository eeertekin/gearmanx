package workers

import (
	"fmt"
	"gearmanx/pkg/command"
	"gearmanx/pkg/consts"
	"gearmanx/pkg/storage"
	"net"
	"sync"
	"time"
)

var workers map[string]map[string]net.Conn

func init() {
	mutex = sync.RWMutex{}

	workers = make(map[string]map[string]net.Conn)
}

var mutex sync.RWMutex

func Close(ID, fn string) {
	workers[fn][ID].Close()
}

func Register(fn string, ID []byte, conn net.Conn) {
	// fmt.Printf("[worker-register] Register %s from %s\n", ID, fn)
	mutex.Lock()
	defer mutex.Unlock()

	storage.AddWorker(string(ID), fn, conn.RemoteAddr().String())

	if workers[fn] == nil {
		workers[fn] = make(map[string]net.Conn)
	}

	workers[fn][string(ID)] = conn
}

func Unregister(fn string, ID []byte) {
	mutex.Lock()
	defer mutex.Unlock()

	fmt.Printf("[worker-unregister] Purge %s from %s\n", ID, fn)
	delete(workers[fn], string(ID))
	storage.DeleteWorker(string(ID), fn)
}

func List() map[string]string {
	return storage.GetWorkers()
}

func WakeUpAll(fn string) {
	mutex.RLock()
	defer mutex.RUnlock()

	// fmt.Printf("[wake-up-all] %s\n", fn)
	for i := range workers[fn] {
		// fmt.Printf("[wake-up] %s\n", workers[fn][i].ID)
		workers[fn][i].Write(command.Response(
			consts.NOOP,
		))
	}
}

func GetWorkerIDs(fn string) (ids []string) {
	mutex.RLock()
	defer mutex.RUnlock()

	for i := range workers[fn] {
		ids = append(ids, i)
	}
	return ids
}

func Ticker() {
	ticker := time.NewTicker(5 * time.Second)
	done := make(chan bool)

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			for _, fn := range storage.GetFuncs() {
				storage.UpdateWorkers(fn, GetWorkerIDs(fn))
			}
		}
	}
}
