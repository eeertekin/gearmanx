package storage

import "gearmanx/pkg/models"

var Backend *Redis

func AddJob(job *models.Job) {
	Backend.AddJob(job)
}

func DeleteJob(ID []byte) {
	Backend.DeleteJob(ID)
}

func GetJob(fn string) *models.Job {
	return Backend.GetJob(fn)
}

func AddWorker(ID, fn string) {
	Backend.AddWorker(ID, fn)
}

func DeleteWorker(ID, fn string) {
	Backend.DeleteWorker(ID, fn)
}

func Status() map[string]*FuncStatus {
	return Backend.Status()
}
