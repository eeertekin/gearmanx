package http

import (
	"encoding/json"
	"gearmanx/pkg/jobs"
	"gearmanx/pkg/redis"
	"gearmanx/pkg/workers"
	"net/http"
)

func init() {
	http.HandleFunc("GET /workers", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(workers.ListWorkers())
	})

	http.HandleFunc("GET /jobs", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(jobs.GetAllJobs())

		// fmt.Printf("%#v\n", jobs.GetAllJobs())
		// json.NewEncoder(w).Encode(jobs.StrQ)
	})

	http.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(redis.Status()))

		// fmt.Printf("%#v\n", jobs.GetAllJobs())
		// json.NewEncoder(w).Encode(jobs.StrQ)
	})
}

func Serve() {
	http.ListenAndServe(":8081", nil)
}
