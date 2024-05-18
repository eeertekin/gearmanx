package storage

import (
	"database/sql"
	"gearmanx/pkg/models"
	"log"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

type Mem struct {
	DB    *sql.DB
	stmts sync.Map
}

func NewMemBackend(addr string) (*Mem, error) {
	// db, err := sql.Open("sqlite3", "file:foobar.db?mode=memory&cache=shared")
	db, err := sql.Open("sqlite3", "file:foobar.db?cache=shared")

	if err != nil {
		log.Fatal(err)
	}

	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)
	// db.Exec("pragma page_size = 32768;")
	// db.Exec("pragma synchronous = normal;")
	// db.Exec("pragma temp_store = memory;")
	// db.Exec("pragma mmap_size = 30000000000")

	sqlStmt := `
	DROP TABLE IF EXISTS queue;
	CREATE TABLE queue (ID text not null primary key, func text, status int, payload text);
	CREATE INDEX "qfn" ON "queue" ("func");

	DROP TABLE IF EXISTS workers; 
	CREATE TABLE workers (ID text not null, func text);
	CREATE INDEX "wfn" ON "workers" ("func");

	DROP TABLE IF EXISTS fns; 
	CREATE TABLE funcs (name text not null primary key);
	CREATE INDEX "funcfn" ON "funcs" ("name");
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return nil, err
	}

	m := Mem{
		DB: db,
	}

	return &m, nil
}

func (m *Mem) Close() {
	m.DB.Close()
}

func (mem *Mem) GetStmt(SQL string) *sql.Stmt {
	if _, ok := mem.stmts.Load(SQL); !ok {
		stmt, err := mem.DB.Prepare(SQL)
		if err != nil {
			log.Fatal(err)
			return nil
		}
		mem.stmts.Store(SQL, stmt)
		return stmt
	}
	stmt, _ := mem.stmts.Load(SQL)
	return stmt.(*sql.Stmt)
}

func (mem *Mem) AddJob(job *models.Job) error {
	mem.GetStmt("INSERT INTO funcs VALUES(?)").Exec(job.Func)

	_, err := mem.GetStmt("INSERT INTO queue VALUES(?,?,?)").Exec(job.ID, job.Func, job.Payload)
	if err != nil {
		log.Fatal(err)
	}

	return err
}

func (mem *Mem) GetJob(fn string) (job *models.Job) {
	row := mem.GetStmt("SELECT `ID`,`payload` FROM queue WHERE `func` = ? LIMIT 1").QueryRow(fn)

	j := models.Job{
		Func: fn,
	}

	err := row.Scan(&j.ID, &j.Payload)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return nil
	}

	return &j

	// ID, err := r.conn.RPopLPush(r.ctx, "fn::"+fn, "inprogress::"+fn).Result()
	// if err != nil {
	// 	return nil
	// }

	// payload, err := r.conn.Get(r.ctx, "job::"+ID).Result()
	// if err != nil {
	// 	return nil
	// }
}

func (mem *Mem) Status() map[string]*models.FuncStatus {
	var err error

	res := map[string]*models.FuncStatus{}

	rows, err := mem.GetStmt(`SELECT name FROM funcs`).Query()
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		f := models.FuncStatus{}
		err = rows.Scan(&f.Name)
		if err != nil {
			log.Fatal(err)
		}
		res[f.Name] = &f
	}

	rows, err = mem.GetStmt(`SELECT func, COUNT(*) FROM queue GROUP BY func`).Query()
	if err != nil {
		log.Fatal(err)
	}

	var name string
	var count int64

	for rows.Next() {
		err = rows.Scan(&name, &count)
		if err != nil {
			log.Fatal(err)
		}
		res[name].Jobs = count
	}

	rows, err = mem.GetStmt(`SELECT func, COUNT(*) FROM workers GROUP BY func`).Query()
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		err = rows.Scan(&name, &count)
		if err != nil {
			log.Fatal(err)
		}
		res[name].Workers = count
		// f.InProgress, _ = r.conn.LLen(r.ctx, "inprogress::"+fn).Result()
		// f.Jobs += f.InProgress
	}

	return res
}

func (mem *Mem) DeleteJob(ID []byte) error {
	_, err := mem.GetStmt("DELETE FROM queue WHERE `ID` = ?").Exec(ID)
	return err
}

func (mem *Mem) AddWorker(ID, fn string) {
	mem.GetStmt("INSERT INTO funcs VALUES(?)").Exec(fn)

	mem.GetStmt("INSERT INTO workers VALUES(?,?)").Exec(ID, fn)
}

func (mem *Mem) DeleteWorker(ID, fn string) {
	mem.GetStmt("DELETE FROM workers WHERE `ID` = ? AND `func` = ?").Exec(ID, fn)
}
