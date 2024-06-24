package http

import (
	"encoding/json"
	"fmt"
	"sort"

	"gearmanx/pkg/storage"
	"gearmanx/pkg/workers"
	"net/http"
)

func init() {
	http.HandleFunc("GET /workers", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(workers.List())
	})

	http.HandleFunc("GET /jobs", func(w http.ResponseWriter, r *http.Request) {
		// json.NewEncoder(w).Encode(jobs.GetAllJobs())
	})

	http.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		fns := storage.Status()

		ordered_fns := []string{}
		for i := range fns {
			ordered_fns = append(ordered_fns, i)
		}
		sort.Strings(ordered_fns)

		for _, i := range ordered_fns {
			w.Write([]byte(fmt.Sprintf("%s\t\t%d\t%d\t%d\n", fns[i].Name, fns[i].Jobs, fns[i].InProgress, fns[i].Workers)))
		}
		w.Write([]byte(".\n"))
	})
}

func Serve() {
	http.ListenAndServe(":8081", nil)
}
