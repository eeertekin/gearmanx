package storage

import (
	"database/sql"
	"gearmanx/pkg/models"
	"log"
	"sync"

	// _ "github.com/mattn/go-sqlite3"
	_ "modernc.org/sqlite"
)

type Mem struct {
	DB        *sql.DB
	stmts     sync.Map
	func_list *LocalStorage
}

const (
	WAITING     = 1
	IN_PROGRESS = 2
	DONE        = 3
)

func NewMemBackend(addr string) (*Mem, error) {
	db, err := sql.Open("sqlite", "file:foobar.db?mode=memory&cache=shared&_journal_mode=WAL")

	// db, err := sql.Open("sqlite3", "file:foobar.db?mode=memory&cache=shared&_journal_mode=WAL")
	// db, err := sql.Open("sqlite3", "file:foobar.db?cache=shared")

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
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return nil, err
	}

	m := Mem{
		DB:        db,
		func_list: NewLocalStorage(),
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
	if !mem.func_list.IsSet(job.Func) {
		mem.func_list.Set(job.Func, true)
	}
	_, err := mem.GetStmt("INSERT INTO queue VALUES(?,?,?,?)").Exec(job.ID, job.Func, WAITING, job.Payload)
	if err != nil {
		log.Fatal(err)
	}

	return err
}

func (mem *Mem) GetJob(fn string) (job *models.Job) {
	row := mem.GetStmt(`
	UPDATE queue SET status = ? WHERE ID IN (
		SELECT ID FROM queue WHERE func = ? AND status = ? LIMIT 1
	)
	RETURNING ID, payload`).QueryRow(IN_PROGRESS, fn, WAITING)

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
}

func (mem *Mem) Status() map[string]*models.FuncStatus {
	res := map[string]*models.FuncStatus{}

	for _, fn := range mem.func_list.GetKeys() {
		res[fn] = &models.FuncStatus{
			Name: fn,
		}
	}

	rows, err := mem.GetStmt(`SELECT func, status, COUNT(*) FROM queue GROUP BY func, status`).Query()
	if err != nil {
		log.Fatal(err)
	}

	var name string
	var count int64
	var status int64

	for rows.Next() {
		err = rows.Scan(&name, &status, &count)
		if err != nil {
			log.Fatal(err)
		}
		if status == WAITING {
			res[name].Jobs = count
		} else if status == IN_PROGRESS {
			res[name].InProgress = count
			res[name].Jobs += count
		}
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
	}

	return res
}

func (mem *Mem) DeleteJob(ID []byte) error {
	// _, err := mem.GetStmt("DELETE FROM queue WHERE `ID` = ?").Exec(ID)
	_, err := mem.GetStmt("UPDATE queue SET `status` = ? WHERE `ID` = ?").Exec(DONE, ID)

	return err
}

func (mem *Mem) AddWorker(ID, fn string) {
	if !mem.func_list.IsSet(fn) {
		mem.func_list.Set(fn, true)
	}
	mem.GetStmt("INSERT INTO workers VALUES(?,?)").Exec(ID, fn)
}

func (mem *Mem) DeleteWorker(ID, fn string) {
	mem.GetStmt("DELETE FROM workers WHERE `ID` = ? AND `func` = ?").Exec(ID, fn)
}
