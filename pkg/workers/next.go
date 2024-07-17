package workers

// workers_next[ID] => *Worker{}
var workers_next map[string]*Worker

func init() {
	workers_next = make(map[string]*Worker)
}

func NextStatus() map[string]*Worker {
	return workers_next
}
