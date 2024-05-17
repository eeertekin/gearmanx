package storage

import "gearmanx/pkg/models"

var Backend *Redis

func AddJob(job *models.Job) {
	Backend.AddJob(job)
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
	} else if u.Scheme == "mem" {
		if backend, err = NewMemBackend(u.Host); err != nil {
			return err
		}
		return nil
	}

	return fmt.Errorf("Storage is not available, check URI %s\n", uri)
}
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
