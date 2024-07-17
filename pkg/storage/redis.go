package storage

import (
	"context"
	"fmt"
	"gearmanx/pkg/config"
	"gearmanx/pkg/models"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Redis struct {
	meta    *redis.Client
	data    *redis.Client
	workers *redis.Client

	job_meta *redis.Client

	ctx       context.Context
	func_list *LocalStorage
}

var wrk_prefix string
var wrk_job_prefix string
var hostname string

func NewRedisBackend(addr string) (*Redis, error) {
	hostname, _ = os.Hostname()
	if strings.Contains(hostname, ".") {
		tmp := strings.SplitN(hostname, ".", 2)
		hostname = tmp[0]
	}
	wrk_prefix = fmt.Sprintf("%s/%d::wrk::", hostname, config.Port)
	wrk_job_prefix = fmt.Sprintf("%s/%d::wrk::jobs::", hostname, config.Port)

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
		workers: redis.NewClient(&redis.Options{
			Addr: addr,
			DB:   2,
		}),
		job_meta: redis.NewClient(&redis.Options{
			Addr: addr,
			DB:   3,
		}),
	}

	if err := r.meta.Ping(r.ctx).Err(); err != nil {
		fmt.Printf("[storage] redis failed - %s\n", err)
		return nil, err
	}

	metakeys, err := r.meta.Keys(r.ctx, hostname+"*").Result()
	if err == nil {
		r.meta.Del(r.ctx, metakeys...)
	}

	r.ClearWorkers()

	return r, nil
}

func (r *Redis) ClearWorkers() {
	worker_keys, _ := r.workers.Keys(r.ctx, fmt.Sprintf("%s/%d::wrk::*", hostname, config.Port)).Result()
	for i := range worker_keys {
		r.workers.Del(r.ctx, worker_keys[i])
	}
}

func (r *Redis) Close() {
	r.meta.Close()
	r.data.Close()
	r.workers.Close()
}

func (r *Redis) AddJob(job *models.Job) error {
	if !r.func_list.IsSet(job.Func) {
		r.func_list.Set(job.Func, true)
		r.meta.SAdd(r.ctx, "global::funcs", job.Func)
	}

	r.data.Set(r.ctx, string(job.ID), job.Payload, 6*time.Hour)
	r.job_meta.Set(r.ctx, string(job.ID), job.Func, 6*time.Hour)

	return r.meta.LPush(r.ctx, "fn::"+job.Func, job.ID).Err()
}

func (r *Redis) GetJob(fn string) (job *models.Job) {
	ID, err := r.meta.RPopLPush(r.ctx, "fn::"+fn, "inprogress::"+fn).Result()
	if err != nil {
		return nil
	}

	payload, err := r.data.Get(r.ctx, ID).Result()
	if err != nil {
		r.meta.LRem(r.ctx, "inprogress::"+fn, 0, ID).Err()
		r.job_meta.Expire(r.ctx, string(ID), time.Second).Err()
		return nil
	}

	return &models.Job{
		Func:    fn,
		Payload: []byte(payload),
		ID:      []byte(ID),
	}
}

func (r *Redis) GetJobSync(fn string) (job *models.Job) {
	ID, err := r.meta.BLMove(r.ctx, "fn::"+fn, "inprogress::"+fn, "RIGHT", "LEFT", 0).Result()
	// ID, err := r.meta.BRPopLPush(r.ctx, "fn::"+fn, "inprogress::"+fn, 60*time.Second).Result()
	if err != nil {
		return nil
	}

	payload, err := r.data.Get(r.ctx, ID).Result()
	if err != nil {
		r.meta.LRem(r.ctx, "inprogress::"+fn, 0, ID).Err()
		r.job_meta.Expire(r.ctx, string(ID), time.Second).Err()
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

	resx, _ := r.meta.HGetAll(r.ctx, "status").Result()

	var tmp []string
	for fn, val := range resx {
		tmp = strings.Split(fn, "::")
		if _, ok := res[tmp[0]]; !ok {
			res[tmp[0]] = &models.FuncStatus{
				Name: tmp[0],
			}
		}
		switch tmp[1] {
		case "jobs":
			res[tmp[0]].Jobs, _ = strconv.ParseInt(val, 10, 32)
		case "workers":
			res[tmp[0]].Workers, _ = strconv.ParseInt(val, 10, 32)
		case "inprogress":
			res[tmp[0]].InProgress, _ = strconv.ParseInt(val, 10, 32)
		}
	}

	return res
}

func (r *Redis) StatusUpdate() {
	wg := sync.WaitGroup{}
	for _, fn := range r.GetFuncs() {
		wg.Add(1)
		go func(fn string) {
			defer wg.Done()
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

			if f.Jobs >= f.Workers {
				f.InProgress = f.Workers
			} else {
				f.InProgress = f.Jobs
			}

			r.meta.HSet(r.ctx, "status",
				f.Name+"::jobs", f.Jobs,
				f.Name+"::inprogress", f.InProgress,
				f.Name+"::workers", f.Workers,
			)
		}(fn)
	}

	wg.Wait()
}

func (r *Redis) DeleteJob(ID []byte) error {
	fn := r.job_meta.Get(r.ctx, string(ID)).Val()
	if fn == "" {
		fmt.Printf("[storage.DeleteJob] %s\n", fn)
		var count int64
		for _, fnx := range r.GetFuncs() {
			if count = r.meta.LRem(r.ctx, "inprogress::"+fnx, 0, string(ID)).Val(); count > 0 {
				break
			}
		}
	} else {
		go r.meta.LRem(r.ctx, "inprogress::"+fn, 0, ID).Err()
	}

	go r.job_meta.Expire(r.ctx, string(ID), time.Second).Err()

	return r.data.Expire(r.ctx, string(ID), time.Second).Err()
}

func (r *Redis) AssignJobToWorker(worker_id string, job_id string, fn string) {
	r.meta.LPush(r.ctx, fmt.Sprintf("%s%s::%s", wrk_job_prefix, worker_id, fn), job_id)
}

func (r *Redis) UnassignJobFromWorker(worker_id string, job_id string, fn string) {
	pipe := r.meta.Pipeline()
	for _, fn := range r.GetFuncs() {
		pipe.LRem(r.ctx, fmt.Sprintf("%s%s::%s", wrk_job_prefix, worker_id, fn), 0, job_id)
	}
	go pipe.Exec(r.ctx)
}

func (r *Redis) AddWorker(ID, fn, remote_addr string) {
	if !r.func_list.IsSet(fn) {
		r.func_list.Set(fn, true)
		r.meta.SAdd(r.ctx, "global::funcs", fn)
	}
	r.workers.SAdd(r.ctx, wrk_prefix+ID, fn)
	r.meta.LPush(r.ctx, wrk_prefix+fn, ID)
}

func (r *Redis) DeleteWorker(ID, fn string) {
	r.workers.Del(r.ctx, wrk_prefix+ID)
	r.meta.LRem(r.ctx, wrk_prefix+fn, 0, ID)
	key := fmt.Sprintf("%s%s::%s", wrk_job_prefix, ID, fn)

	// Move assigned jobs from worker to queue
	if count, _ := r.meta.LLen(r.ctx, key).Result(); count > 0 {
		assigned_jobs, _ := r.meta.LRange(r.ctx, key, 0, -1).Result()
		for i := range assigned_jobs {
			r.meta.LRem(r.ctx, "inprogress::"+fn, 0, assigned_jobs[i])
			r.job_meta.Expire(r.ctx, assigned_jobs[i], time.Second)
		}
	}
	r.meta.Del(r.ctx, key)
}

func (r *Redis) GetFuncs() []string {
	// return r.func_list.GetKeys()
	res, err := r.meta.SMembers(r.ctx, "global::funcs").Result()
	if err != nil {
		return nil
	}
	return res
}

func (r *Redis) UpdateWorkers(fn string, ids []string) {
	r.meta.Del(r.ctx, wrk_prefix+fn)
	r.meta.LPush(r.ctx, wrk_prefix+fn, ids)
	r.meta.Expire(r.ctx, wrk_prefix+fn, 5*time.Second)
}

func (r *Redis) WaitJob(ID []byte) []byte {
	sub := r.meta.Subscribe(r.ctx, "job::channel::"+string(ID))
	defer sub.Close()
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

func (r *Redis) GetWorkers() map[string]string {
	res := map[string]string{}

	workers, err := r.workers.Keys(r.ctx, "*").Result()
	if err != nil {
		fmt.Printf("err> %s\n", err)
		return res
	}

	for i := range workers {
		fns, _ := r.workers.SMembers(r.ctx, workers[i]).Result()
		for k := range fns {
			if _, ok := res[workers[i]]; !ok {
				res[workers[i]] = ""
			}
			res[workers[i]] += fmt.Sprintf("%s ", fns[k])
		}
		res[workers[i]] = strings.Trim(res[workers[i]], " ")
	}

	return res
}

func (r *Redis) WakeUpAll(fn string) {
	err := r.workers.Publish(r.ctx, "wakeup", fn).Err()
	if err != nil {
		fmt.Printf("[wake-up-all] err> %s\n", err)
	}
}

func (r *Redis) WakeUpCalls(cb func(fn string)) {
	sub := r.workers.Subscribe(r.ctx, "wakeup")
	defer sub.Close()
	ch := sub.Channel()

	var fn *redis.Message
	for fn = range ch {
		cb(fn.Payload)
	}
}

func (r *Redis) GetWorkersPipe() map[string]string {
	res := map[string]string{}

	pipe := r.workers.Pipeline()

	iter := r.workers.Scan(r.ctx, 0, "", 100000).Iterator()
	var ID string
	for iter.Next(r.ctx) {
		ID = iter.Val()
		pipe.SMembers(r.ctx, ID)
	}

	cmds, err := pipe.Exec(r.ctx)
	if err != nil {
		fmt.Printf("err> %s\n", err)
	}

	for _, cmd := range cmds {
		ID := cmd.Args()[1].(string)
		fns := cmd.(*redis.StringSliceCmd).Val()
		for k := range fns {
			if fns[k] == "" {
				continue
			}
			if _, ok := res[ID]; !ok {
				res[ID] = ""
			}
			res[ID] += fmt.Sprintf("%s ", fns[k])
		}
		res[ID] = strings.TrimRight(res[ID], " ")
	}

	return res
}
