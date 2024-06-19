package storage

import (
	"fmt"
	"gearmanx/pkg/models"
	"net/url"
	"os"
)

type Storage interface {
	Close()

	AddJob(job *models.Job) error
	DeleteJob(ID []byte) error
	GetJob(fn string) *models.Job
	WaitJob(ID []byte) []byte
	JobResult(ID, payload []byte)

	AddWorker(ID, fn string)
	DeleteWorker(ID, fn string)

	Status() map[string]*models.FuncStatus

	GetFuncs() []string

	AssignJobToWorker(worker_id string, job_id string, fn string)
	UnassignJobFromWorker(worker_id string, job_id string, fn string)

	UpdateWorkers(fn string, ids []string)
}

func NewStorage(uri string) error {
	u, err := url.Parse(uri)
	if err != nil {
		fmt.Printf("Storage is not available, check URI %s\n", uri)
		os.Exit(1)
	}

	if u.Scheme == "redis" {
		if backend, err = NewRedisBackend(u.Host); err != nil {
			return err
		}
		return nil
	}
	// else if u.Scheme == "mem" {
	// 	if backend, err = NewMemBackend(u.Host); err != nil {
	// 		return err
	// 	}
	// 	return nil
	// }

	return fmt.Errorf("Storage is not available, check URI %s", uri)
}

var backend Storage

func AddJob(job *models.Job) error {
	return backend.AddJob(job)
}

func WaitJob(ID []byte) []byte {
	return backend.WaitJob(ID)
}

func JobResult(ID, payload []byte) {
	backend.JobResult(ID, payload)
}

func DeleteJob(ID []byte) error {
	return backend.DeleteJob(ID)
}

func GetJob(fn string) *models.Job {
	return backend.GetJob(fn)
}

func AddWorker(ID, fn string) {
	backend.AddWorker(ID, fn)
}

func DeleteWorker(ID, fn string) {
	backend.DeleteWorker(ID, fn)
}

func Status() map[string]*models.FuncStatus {
	return backend.Status()
}

func GetFuncs() []string {
	return backend.GetFuncs()
}

func AssignJobToWorker(worker_id string, job_id string, fn string) {
	backend.AssignJobToWorker(worker_id, job_id, fn)
}

func UnassignJobFromWorker(worker_id string, job_id string, fn string) {
	backend.UnassignJobFromWorker(worker_id, job_id, fn)
}

func UpdateWorkers(fn string, ids []string) {
	backend.UpdateWorkers(fn, ids)
}
