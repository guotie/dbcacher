package dbcacher

import (
	//"bytes"
	"database/sql"
	//"encoding/gob"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"reflect"
	"sync"
	"time"
)

const (
	DefaultFlushDB = 300
	DefaultExpires = 3600
)

type Modeler interface {
	RowToModel(row *sql.Row) error
	UpdateClause() (string, error)
	Decode([]byte) error
	Encode() ([]byte, error)
}

type expirestmr struct {
	fd      time.Duration // flush to db
	ed      time.Duration // delete from redis
	flushdb *time.Timer
	expires *time.Timer
}

type DbCacher struct {
	lock sync.Mutex
	db   *sql.DB
	pool *redis.Pool
	tmr  map[string]*expirestmr
}

var (
	dbc     DbCacher
	created bool
)

func Create(db *sql.DB, pool *redis.Pool) {
	if created {
		return
	}

	dbc = DbCacher{db: db,
		pool: pool,
		tmr:  make(map[string]*expirestmr),
	}

	created = true
}

func Get(m Modeler, table string, id int64, timeout int) error {
	t := reflect.TypeOf(m)
	if t.Kind() != reflect.Struct {
		//panic("Modeler m must be type struce!\n")
	}

	key := fmt.Sprintf("%s-%d", table, id)
	if timeout <= 0 {
		timeout = DefaultExpires
	}

	dbc.lock.Lock()
	val, ok1 := dbc.pool.Get().Do("HGET", table, id)
	if ok1 != nil || val == nil {
		// 在redis中没有找到该数据，从数据库中获取
		dbc.lock.Unlock()
		err := queryrow(m, table, id)
		if err != nil {
			return err
		}

		// 将结果保存到redis中
		err = Set(m, table, id, DefaultFlushDB)
		if err != nil {
			return err
		}

		// 设置该数据在redis中超时时间
		dbc.lock.Lock()
		etmr, _ := dbc.tmr[key]
		tmo := time.Duration(timeout) * time.Second
		etmr.ed = tmo
		if etmr.expires != nil {
			etmr.expires.Reset(tmo)
		} else {
			etmr.expires = time.AfterFunc(tmo, func() {
				dbc.lock.Lock()
				dbc.pool.Get().Do("HDel", table, key)
				dbc.lock.Unlock()
			})
		}
		dbc.lock.Unlock()
		return nil
	}

	etmr, ok2 := dbc.tmr[key]
	ed := time.Duration(timeout) * time.Second
	if ok2 {
		etmr.ed = ed
		etmr.expires.Reset(etmr.ed)
	} else {
		log.Printf("Not found timer for %s-%d\n", table, id)
		etmr = &expirestmr{ed: ed,
			expires: time.AfterFunc(ed, func() {
				dbc.lock.Lock()
				dbc.pool.Get().Do("HDel", table, key)
				dbc.lock.Unlock()
			}),
		}
		dbc.tmr[key] = etmr
	}
	dbc.lock.Unlock()

	err := m.Decode(val.([]byte))
	if err != nil {
		return err
	}
	return nil
}

// 将数据写入到redis中，并设置timer到期时，更新数据库
func Set(m Modeler, table string, id int64, timeout int) (err error) {
	var buf []byte

	buf, err = m.Encode()
	if err != nil {
		return err
	}

	clause, cerr := m.UpdateClause()
	key := fmt.Sprintf("%s-%d", table, id)
	if timeout <= 0 {
		timeout = DefaultFlushDB
	}

	// 写入到redis缓存中
	dbc.lock.Lock()
	_, err = dbc.pool.Get().Do("HSET", table, id, buf)
	if err != nil {
		log.Printf("HSET %s %d failed: %s!", table, id, err.Error())
	} else {
		if cerr != nil {
			goto unlock
		}
		// update 数据库操作
		flushTmo := time.Duration(timeout) * time.Second
		ftmr, ok := dbc.tmr[key]
		if !ok {
			ftmr = &expirestmr{fd: flushTmo}
			dbc.tmr[key] = ftmr
		}
		flushTmr := ftmr.flushdb

		flushTmr = ftmr.flushdb
		if flushTmr != nil {
			flushTmr.Reset(flushTmo)
		} else {
			flushTmr = time.AfterFunc(flushTmo, func() {
				println("exec update:", clause)
				_, err := dbc.db.Exec(clause)
				if err != nil {
					log.Printf("Exec update clause %s failed: %s\n",
						clause, err.Error())
				}
			})

		}

	}
unlock:
	dbc.lock.Unlock()

	return nil
}

// 立刻将数据写入到数据库中
func Flush(m Modeler, table string, id int) error {
	clause, cerr := m.UpdateClause()
	key := fmt.Sprintf("%s-%d", table, id)
	if cerr == nil {
		_, err := dbc.db.Exec(clause)
		if err != nil {
			log.Printf("Flush: exec clause %s failed: %s\n", clause, err.Error())
		}
	}
	dbc.lock.Lock()
	tmr, ok := dbc.tmr[key]
	if ok {
		if tmr.flushdb != nil {
			tmr.flushdb.Stop()
		}
	}
	dbc.lock.Unlock()
	return nil
}

func queryrow(m Modeler, table string, id int64) error {
	query := fmt.Sprintf("SELECT * FROM %s WHERE id=%d;", table, id)
	row := dbc.db.QueryRow(query)
	err := m.RowToModel(row)
	return err
}
