package utils

import (
	"fmt"
	"sync/atomic"

	"github.com/google/uuid"
)

var jobID atomic.Int64

func NextHandlerID() []byte {
	jobID.Add(1)
	return []byte(fmt.Sprintf("H:gearmanx:%d", jobID.Load()))
}

func NextWorkerID() []byte {
	return []byte(uuid.New().String())
}
