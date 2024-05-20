package storage

import (
	"context"
	"fmt"
	"gearmanx/pkg/models"

	"github.com/redis/go-redis/v9"
)

type Redis struct {
	conn      *redis.Client
	ctx       context.Context
	func_list *LocalStorage
}

func NewRedisBackend(addr string) (*Redis, error) {
	r := &Redis{
		ctx:       context.Background(),
		func_list: NewLocalStorage(),
		conn: redis.NewClient(&redis.Options{
			Addr: addr,
			DB:   0,
		}),
	}

	if err := r.conn.Ping(r.ctx).Err(); err != nil {
		fmt.Printf("[storage] redis failed - %s\n", err)
		return nil, err
	}

	// r.conn.FlushAll(r.ctx)
	keys, _ := r.conn.Keys(r.ctx, "inprogress::*").Result()
	for _, key := range keys {
		r.conn.Del(r.ctx, key)
	}

	return r, nil
}

func (r *Redis) Close() {
	r.conn.Close()
}

func (r *Redis) AddJob(job *models.Job) error {
	if !r.func_list.IsSet(job.Func) {
		r.func_list.Set(job.Func, true)
	}

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

func (r *Redis) Status() map[string]*models.FuncStatus {
	res := map[string]*models.FuncStatus{}

	for _, fn := range r.func_list.GetKeys() {
		f := models.FuncStatus{
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

func (r *Redis) DeleteJob(ID []byte) error {
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
	if !r.func_list.IsSet(fn) {
		r.func_list.Set(fn, true)
	}
	r.conn.LPush(r.ctx, "worker::"+fn, ID)
}

func (r *Redis) DeleteWorker(ID, fn string) {
	r.conn.LRem(r.ctx, "worker::"+fn, 1, ID)
}

func (r *Redis) GetFuncs() []string {
	return r.func_list.GetKeys()
}
