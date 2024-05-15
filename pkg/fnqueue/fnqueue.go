package fnqueue

import (
	"gearmanx/pkg/models"
	"gearmanx/pkg/redis"
)

type FnQueue struct {
	Fn string
}

func NewFnQueue(fn string) *FnQueue {
	return &FnQueue{
		Fn: fn,
	}
}

func (f *FnQueue) Add(j *models.Job) {
	redis.Add(f.Fn, j)
}

func (f *FnQueue) Size() int {
	return int(redis.Size(f.Fn))
}

func (f *FnQueue) Get() *models.Job {
	if f.Size() == 0 {
		return nil
	}

	return redis.Get(f.Fn)
}

func (f *FnQueue) GetAll() []string {
	return []string{
		"asd",
	}
	// res[fn_name] = append(res[fn_name], fmt.Sprintf("%s", fq.Jobs[i].String()))
}

// type FnQueue struct {
// 	Fn   string
// 	Jobs []*models.Job
// }

// func NewFnQueue(fn string) *FnQueue {
// 	return &FnQueue{
// 		Fn:   fn,
// 		Jobs: []*models.Job{},
// 	}
// }

// func (f *FnQueue) Add(j *models.Job) {
// 	f.Jobs = append(f.Jobs, j)
// }

// func (f *FnQueue) Size() int {
// 	return len(f.Jobs)
// }

// func (f *FnQueue) Get() *models.Job {
// 	if f.Size() == 0 {
// 		return nil
// 	}

// 	job := f.Jobs[0]
// 	if f.Size() > 1 {
// 		f.Jobs = f.Jobs[1:]
// 	} else {
// 		f.Jobs = f.Jobs[:0]
// 	}

// 	return job
// }

// func (f *FnQueue) GetAll() (all []string) {
// 	for i := range f.Jobs {
// 		all = append(all, f.Jobs[i].String())
// 	}
// 	return all
// }
