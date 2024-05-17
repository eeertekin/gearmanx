package storage

import (
	"context"
	"fmt"
	"gearmanx/pkg/models"
	"strings"

	"github.com/redis/go-redis/v9"
)

type Redis struct {
	conn *redis.Client
	ctx  context.Context
}

func NewRedisBackend(addr string) *Redis {
	r := &Redis{
		ctx: context.Background(),
		conn: redis.NewClient(&redis.Options{
			Addr: addr,
			DB:   0,
		}),
	}

	if err := r.conn.Ping(r.ctx).Err(); err != nil {
		fmt.Printf("[storage] redis failed - %s\n", err)
		return nil
	}

	r.conn.FlushAll(r.ctx)

	return r
}

func (r *Redis) AddJob(job *models.Job) error {
	err := r.conn.Set(r.ctx, "job::"+string(job.ID), job.Payload, -1).Err()
	if err != nil {
		return err
	}

	return r.conn.LPush(r.ctx, "fn::"+job.Func, job.ID).Err()
}

func (r *Redis) GetJob(fn string) (job *models.Job) {
	ID, err := r.conn.RPopLPush(r.ctx, "fn::"+fn, "inprogress::"+fn).Result()
	if err != nil {
		return nil
	}

	payload, err := r.conn.Get(r.ctx, "job::"+ID).Result()
	if err != nil {
		return nil
	}

	return &models.Job{
		Func:    fn,
		Payload: []byte(payload),
		ID:      []byte(ID),
	}
}

func (r *Redis) CountJobQueue(fn string) int64 {
	job, err := r.conn.LLen(r.ctx, fn).Result()
	if err != nil {
		panic(err)
	}

	return job
}

func (r *Redis) Status() map[string]*FuncStatus {
	fns, err := r.conn.Keys(r.ctx, "fn::*").Result()
	if err != nil {
		return nil
	}

	for i := range fns {
		fns[i] = strings.TrimPrefix(fns[i], "fn::")
	}

	workers, err := r.conn.Keys(r.ctx, "worker::*").Result()
	if err != nil {
		return nil
	}
	for i := range workers {
		workers[i] = strings.TrimPrefix(workers[i], "worker::")
	}

	fns = append(fns, workers...)

	res := map[string]*FuncStatus{}

	for _, fn := range fns {
		f := FuncStatus{
			Name: fn,
		}
		f.InProgress, _ = r.conn.LLen(r.ctx, "inprogress::"+fn).Result()
		f.Jobs, _ = r.conn.LLen(r.ctx, "fn::"+fn).Result()
		f.Jobs += f.InProgress

		f.Workers, _ = r.conn.LLen(r.ctx, "worker::"+fn).Result()

		res[fn] = &f
	}

	return res
}

func (r *Redis) DeleteJob(ID []byte) any {
	fns, err := r.conn.Keys(r.ctx, "inprogress::*").Result()
	if err != nil {
		panic(err)
	}

	for _, i := range fns {
		r.conn.LRem(r.ctx, i, 1, ID)
	}

	return r.conn.Del(r.ctx, "job::"+string(ID)).Err()
}

func (r *Redis) AddWorker(ID, fn string) {
	r.conn.LPush(r.ctx, "worker::"+fn, ID)
}

func (r *Redis) DeleteWorker(ID, fn string) {
	r.conn.LRem(r.ctx, "worker::"+fn, 1, ID)
}
