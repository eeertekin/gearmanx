package clients

import (
	"gearmanx/pkg/models"
	"sync"
)

var data map[string]*models.IAM
var mutex sync.RWMutex

func init() {
	data = make(map[string]*models.IAM)
}

func Register(iam *models.IAM) {
	mutex.Lock()
	defer mutex.Unlock()

	data[iam.ID] = iam
}

func Unregister(iam *models.IAM) {
	mutex.Lock()
	defer mutex.Unlock()

	delete(data, iam.ID)
}

func Notify(ID []byte, payload []byte) {
	for i := range data {
		if data[i].WaitingJobs[string(ID)] != nil {
			data[i].WaitingJobs[string(ID)] <- payload
			break
		}
	}
}
