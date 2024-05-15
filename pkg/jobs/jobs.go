package jobs

import (
	"bytes"
	"fmt"
	"gearmanx/pkg/fnqueue"
	"gearmanx/pkg/models"
	"gearmanx/pkg/redis"
	"sync"
)

var Queue map[string]*fnqueue.FnQueue
var mutex sync.Mutex

func init() {
	Queue = map[string]*fnqueue.FnQueue{}
}

func GetAllJobs() map[string][]string {
	all := map[string][]string{}
	for i := range Queue {
		all[i] = Queue[i].GetAll()
	}
	return all
}

func Add(j *models.Job) {
	j.Payload = bytes.Clone(j.Payload)
	j.ID = bytes.Clone(j.ID)

	mutex.Lock()
	defer mutex.Unlock()

	if _, ok := Queue[j.Func]; !ok {
		fmt.Printf("Creating %s queue\n", j.Func)
		Queue[j.Func] = fnqueue.NewFnQueue(j.Func)
	}

	Queue[j.Func].Add(j)
}

func Get(fn string) *models.Job {
	mutex.Lock()
	defer mutex.Unlock()

	if _, ok := Queue[fn]; !ok {
		Queue[fn] = &fnqueue.FnQueue{
			Fn: fn,
		}
		return nil
	}

	return Queue[fn].Get()
}

func Remove(ID []byte) {

	redis.RemoveFromLists(ID)

	redis.Delete(ID)
}
