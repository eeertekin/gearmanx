package storage

import (
	"context"
	"crypto/md5"
	"fmt"
	"gearmanx/pkg/config"
	"gearmanx/pkg/models"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

type Redis struct {
	meta *redis.Client
	data *redis.Client

	ctx       context.Context
	func_list *LocalStorage
}

var wrk_prefix string
var hostname string

func NewRedisBackend(addr string) (*Redis, error) {
	hostname, _ = os.Hostname()
	wrk_prefix = fmt.Sprintf("%x::wrk::", md5.Sum([]byte(hostname+fmt.Sprintf("%d", config.Port))))

	r := &Redis{
		ctx:       context.Background(),
		func_list: NewLocalStorage(),
		meta: redis.NewClient(&redis.Options{
			Addr: addr,
			DB:   0,
		}),
		data: redis.NewClient(&redis.Options{
			Addr: addr,
			DB:   1,
		}),
	}

	if err := r.meta.Ping(r.ctx).Err(); err != nil {
		fmt.Printf("[storage] redis failed - %s\n", err)
		return nil, err
	}

	metakeys, err := r.meta.Keys(r.ctx, hostname+"::*").Result()
	if err == nil {
		r.meta.Del(r.ctx, metakeys...)
	}

	return r, nil
}

func (r *Redis) Close() {
	r.meta.Close()
	r.data.Close()
}

func (r *Redis) AddJob(job *models.Job) error {
	if !r.func_list.IsSet(job.Func) {
		r.func_list.Set(job.Func, true)
	}

	r.data.Set(r.ctx, string(job.ID), job.Payload, -1)

	return r.meta.LPush(r.ctx, "fn::"+job.Func, job.ID).Err()
}

func (r *Redis) GetJob(fn string) (job *models.Job) {
	ID, err := r.meta.RPopLPush(r.ctx, "fn::"+fn, "inprogress::"+fn).Result()
	if err != nil {
		return nil
	}

	payload, err := r.data.Get(r.ctx, ID).Result()
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

		count := int64(0)
		allwrks, _ := r.meta.Keys(r.ctx, "*::wrk::"+fn).Result()
		for i := range allwrks {
			count, _ = r.meta.LLen(r.ctx, allwrks[i]).Result()
			f.Workers += count
		}

		res[fn] = &f
	}

	return res
}

func (r *Redis) DeleteJob(ID []byte) error {
	for _, fn := range r.func_list.GetKeys() {
		r.meta.LRem(r.ctx, "inprogress::"+fn, 1, ID)
	}

	return r.data.Expire(r.ctx, string(ID), time.Second).Err()
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
	r.meta.LPush(r.ctx, wrk_prefix+fn, ID)
}

func (r *Redis) DeleteWorker(ID, fn string) {
	r.meta.LRem(r.ctx, wrk_prefix+fn, 1, ID)
	key := fmt.Sprintf("wjobs::%s::%s", ID, fn)

	// Move assigned jobs from worker to queue
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

func (r *Redis) UpdateWorkers(fn string, ids []string) {
	r.meta.Del(r.ctx, wrk_prefix+fn)
	r.meta.LPush(r.ctx, wrk_prefix+fn, ids)
	r.meta.Expire(r.ctx, wrk_prefix+fn, 5*time.Second)
}

func (r *Redis) WaitJob(ID []byte) []byte {
	sub := r.meta.Subscribe(r.ctx, "job::channel::"+string(ID))
	msg := <-sub.Channel()

	return []byte(msg.Payload)
}

func (r *Redis) JobResult(ID, payload []byte) {
	err := r.meta.PubSubChannels(r.ctx, "job::channel::"+string(ID)).Err()
	if err != nil {
		fmt.Printf("redis> job result err : %s\n", err)
		return
	}

	r.meta.Publish(r.ctx, "job::channel::"+string(ID), payload)
}
