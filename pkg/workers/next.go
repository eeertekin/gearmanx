package workers

// workers_next[ID] => *Worker{}
var workers_next map[string]*Worker

func init() {
	workers_next = make(map[string]*Worker)
}

func NextStatus() map[string]*Worker {
	mutex.RLock()
	defer mutex.RUnlock()
	return workers_next
}

func GetNext() map[string]*Worker {
	mutex.RLock()
	defer mutex.RUnlock()

	snapshot := map[string]*Worker{}
	for i := range workers_next {
		snapshot[i] = workers_next[i]
	}
	return snapshot
}
