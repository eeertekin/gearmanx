package utils

import (
	"fmt"
	"sync/atomic"
)

var jobID atomic.Int64

func NextHandlerID() []byte {
	jobID.Add(1)
	return []byte(fmt.Sprintf("H:gearmanx:%d", jobID.Load()))
}

var workerID atomic.Int64

func NextWorkerID() []byte {
	workerID.Add(1)
	return []byte(fmt.Sprintf("H:worker:%d", workerID.Load()))
}
