package workers

import (
	"gearmanx/pkg/command"
	"gearmanx/pkg/consts"
	"net"
)

type Worker struct {
	conn net.Conn

	sleeping bool
}

func (w *Worker) Write(b []byte) {
	w.conn.Write(b)
}

func (w *Worker) Close() error {
	return w.conn.Close()
}

func (w *Worker) WakeUp() {
	w.conn.Write(command.Response(
		consts.NOOP,
	))
	w.sleeping = false
}

func (w *Worker) Sleep() {
	w.sleeping = true
}

func (w *Worker) IsSleeping() bool {
	return w.sleeping
}
