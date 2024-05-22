package storage

import (
	"context"
	"fmt"
	"gearmanx/pkg/models"
	"time"

	"github.com/redis/go-redis/v9"
)

type Redis struct {
	meta *redis.Client
	ctx       context.Context
	func_list *LocalStorage
}

func NewRedisBackend(addr string) (*Redis, error) {
	r := &Redis{
		ctx:       context.Background(),
		func_list: NewLocalStorage(),
		meta: redis.NewClient(&redis.Options{
			Addr: addr,
			DB:   0,
		}),
	}

	if err := r.meta.Ping(r.ctx).Err(); err != nil {
		fmt.Printf("[storage] redis failed - %s\n", err)
		return nil, err
	}

	return r, nil
}

func (r *Redis) Close() {
	r.meta.Close()
}

func (r *Redis) AddJob(job *models.Job) error {
	if !r.func_list.IsSet(job.Func) {
		r.func_list.Set(job.Func, true)
	}

	pipe := r.meta.Pipeline()

	pipe.Set(r.ctx, "job::"+string(job.ID), job.Payload, -1)
	pipe.RPush(r.ctx, "fn::"+job.Func, job.ID)

	_, err := pipe.Exec(r.ctx)

	return err
}

func (r *Redis) GetJob(fn string) (job *models.Job) {
	ID, err := r.meta.RPopLPush(r.ctx, "fn::"+fn, "inprogress::"+fn).Result()
	if err != nil {
		return nil
	}

	payload, err := r.meta.Get(r.ctx, "job::"+ID).Result()
	if err != nil {
		return nil
	}

	return &models.Job{
		Func:    fn,
		Payload: []byte(payload),
		ID:      []byte(ID),
	}
}

func (r *Redis) Status() map[string]*models.FuncStatus {
	res := map[string]*models.FuncStatus{}

	for _, fn := range r.func_list.GetKeys() {
		f := models.FuncStatus{
			Name: fn,
		}
		f.InProgress, _ = r.meta.LLen(r.ctx, "inprogress::"+fn).Result()
		f.Jobs, _ = r.meta.LLen(r.ctx, "fn::"+fn).Result()
		f.Jobs += f.InProgress

		f.Workers, _ = r.meta.LLen(r.ctx, "worker::"+fn).Result()

		res[fn] = &f
	}

	return res
}

func (r *Redis) DeleteJob(ID []byte) error {
	for _, fn := range r.func_list.GetKeys() {
		r.meta.LRem(r.ctx, "inprogress::"+fn, 1, ID)
	}

	return r.meta.Expire(r.ctx, "job::"+string(ID), time.Second).Err()

	// return r.meta.LPush(r.ctx, "trash", 1, ID).Err()
	// return r.meta.Del(r.ctx, "job::"+string(ID)).Err()
}

func (r *Redis) AssignJobToWorker(worker_id string, job_id string, fn string) {
	r.meta.LPush(r.ctx, fmt.Sprintf("wjobs::%s::%s", worker_id, fn), job_id)
}

func (r *Redis) UnassignJobFromWorker(worker_id string, job_id string, fn string) {
	for _, fn := range r.func_list.GetKeys() {
		r.meta.LRem(r.ctx, fmt.Sprintf("wjobs::%s::%s", worker_id, fn), 1, job_id)
	}
}

func (r *Redis) AddWorker(ID, fn string) {
	if !r.func_list.IsSet(fn) {
		r.func_list.Set(fn, true)
	}
	r.meta.LPush(r.ctx, "worker::"+fn, ID)
}

func (r *Redis) DeleteWorker(ID, fn string) {
	r.meta.LRem(r.ctx, "worker::"+fn, 1, ID)
	key := fmt.Sprintf("wjobs::%s::%s", ID, fn)

	if count, _ := r.meta.LLen(r.ctx, key).Result(); count > 0 {
		assigned_jobs, _ := r.meta.LRange(r.ctx, key, 0, -1).Result()
		for i := range assigned_jobs {
			r.meta.LRem(r.ctx, "inprogress::"+fn, 1, assigned_jobs[i])
			r.meta.LPush(r.ctx, "fn::"+fn, assigned_jobs[i])
		}
		r.meta.Del(r.ctx, key)
	}
}

func (r *Redis) GetFuncs() []string {
	return r.func_list.GetKeys()
}
