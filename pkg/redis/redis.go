package redis

import (
	"context"
	"fmt"
	"gearmanx/pkg/models"
	"strings"

	"github.com/redis/go-redis/v9"
)

var conn *redis.Client
var data_conn *redis.Client
var workers_conn *redis.Client

var ctx context.Context

func init() {
	ctx = context.Background()
	conn = redis.NewClient(&redis.Options{
		Addr: "192.168.64.64:6379",
		DB:   0,
	})

	conn.FlushAll(ctx)

	data_conn = redis.NewClient(&redis.Options{
		Addr: "192.168.64.64:6379",
		DB:   1,
	})

	workers_conn = redis.NewClient(&redis.Options{
		Addr: "192.168.64.64:6379",
		DB:   2,
	})
}

func AddWorker(ID string, fn string) {
	workers_conn.LPush(ctx, fn, ID)
}

func PurgeWorker(ID string, fn string) {
	workers_conn.LRem(ctx, fn, 1, ID)
}

func Add(fn string, job *models.Job) {
	err := data_conn.Set(ctx, string(job.ID), job.Payload, -1).Err()
	if err != nil {
		panic(err)
	}

	err = conn.LPush(ctx, fn, job.ID).Err()
	if err != nil {
		panic(err)
	}
}

func Get(fn string) *models.Job {
	// conn.LPop(ctx, fn).Result()
	ID, err := conn.RPopLPush(ctx, fn, fn+"_inprogress").Result()
	if err != nil {
		return nil
	}

	payload, err := data_conn.Get(ctx, ID).Result()
	if err != nil {
		return nil
	}

	return &models.Job{
		Func:    fn,
		Payload: []byte(payload),
		ID:      []byte(ID),
	}
}

func Size(fn string) int64 {
	job, err := conn.LLen(ctx, fn).Result()
	if err != nil {
		panic(err)
	}

	return job
}

func Delete(ID []byte) {
	err := data_conn.Del(ctx, string(ID)).Err()
	if err != nil {
		panic(err)
	}
}

type FnStat struct {
	Name       string
	Workers    int64
	Jobs       int64
	InProgress int64
}

func (f *FnStat) Fill() {
	var err error
	f.Workers, err = workers_conn.LLen(ctx, f.Name).Result()
	if err != nil {
		fmt.Printf("[status] count error")
	}

	f.InProgress, err = conn.LLen(ctx, f.Name+"_inprogress").Result()
	if err != nil {
		fmt.Printf("[status] count error")
	}

	f.Jobs, err = conn.LLen(ctx, f.Name).Result()
	if err != nil {
		fmt.Printf("[status] count error")
	}
	f.Jobs += f.InProgress
}

func Status() map[string]FnStat {
	fns := map[string]FnStat{}

	jobs, err := conn.Keys(ctx, "*").Result()
	if err != nil {
		panic(err)
	}

	for _, i := range jobs {
		if strings.HasSuffix(i, "_inprogress") {
			continue
		}

		if _, ok := fns[i]; !ok {
			fn := FnStat{
				Name: i,
			}
			fn.Fill()

			fns[i] = fn
		}
	}

	waiting_workers, err := workers_conn.Keys(ctx, "*").Result()
	if err != nil {
		panic(err)
	}

	for _, i := range waiting_workers {
		if _, ok := fns[i]; !ok {
			fn := FnStat{
				Name: i,
			}
			fn.Fill()

			fns[i] = fn
		}
	}

	return fns
}

func RemoveFromLists(ID []byte) {
	fns, err := conn.Keys(ctx, "*").Result()
	if err != nil {
		panic(err)
	}

	for _, i := range fns {
		if !strings.HasSuffix(i, "_inprogress") {
			continue
		}

		conn.LRem(ctx, i, 1, ID)
	}
}
