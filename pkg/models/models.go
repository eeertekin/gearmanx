package models

import "fmt"

type Job struct {
	Func    string
	Payload []byte
	ID      []byte
}

func (j *Job) String() string {
	return fmt.Sprintf("%s :: %s", j.ID, j.Payload)
}

type FuncStatus struct {
	Name       string
	Workers    int64
	Jobs       int64
	InProgress int64
}

type IAM struct {
	Role      int // worker, client
	ID        string
	Functions []string

	WaitingJobs map[string]chan []byte
}
