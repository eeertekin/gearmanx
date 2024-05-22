package utils

import (
	"fmt"
	"sync/atomic"

	"github.com/rs/xid"
)

var jobID atomic.Int64

func NextHandlerID() []byte {
	jobID.Add(1)
	return []byte(fmt.Sprintf("H:x:%d", jobID.Load()))
}

func NextWorkerID() string {
	return xid.New().String()
}
