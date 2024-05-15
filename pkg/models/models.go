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
