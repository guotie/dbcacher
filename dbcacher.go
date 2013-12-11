package dbcacher

import (
	//"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"reflect"
	"sync"
	"time"
)

type cacher interface {
	Get()
	Set()
}

type Modeler interface {
	RowToModel(row *sql.Row) error
	gob.GobDecoder
	gob.GobEncoder
}

type expirestmr struct {
	d   time.Duration
	tmr *time.Timer
}
type DbCacher struct {
	lock sync.Mutex
	db   *sql.DB
	pool *redis.Pool
	tmr  map[string]expirestmr
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
		tmr:  make(map[string]expirestmr),
	}

	created = true
}

func Get(m Modeler, table string, id int, timeout int) error {
	t := reflect.TypeOf(m)
	if t.Kind() != reflect.Struct {
		panic("Modeler m must be type struce!\n")
	}

	key := fmt.Sprintf("%s-%d", table, id)
	dbc.lock.Lock()
	val, ok1 := dbc.pool.Get().Do("HGET", table, id)
	if ok1 != nil || val == nil {
		dbc.lock.Unlock()
		err := queryrow(m, table, id)
		if err != nil {
			return err
		}
		var buf []byte
		buf, err = m.GobEncode()
		if err != nil {
			return err
		}

		// 写入到redis缓存中
		dbc.lock.Lock()
		_, err = dbc.pool.Get().Do("HSET", table, id, buf)
		if err != nil {
			log.Printf("HSET %s %d failed: %s!", table, id, err.Error())
		} else {
			tmo := time.Duration(timeout) * time.Second
			tmr := time.AfterFunc(tmo, func() {
				dbc.pool.Get().Do("HDEL", table, id)
			})
			dbc.tmr[key] = expirestmr{
				d:   tmo,
				tmr: tmr,
			}
		}
		dbc.lock.Unlock()
		return nil
	}

	etmr, ok2 := dbc.tmr[key]
	if ok2 {
		etmr.tmr.Reset(etmr.d)
	} else {
		log.Printf("Not found timer for %s-%d\n", table, id)
	}
	dbc.lock.Unlock()

	err := m.GobDecode(val.([]byte))
	if err != nil {
		return err
	}
	return nil
}

func queryrow(m Modeler, table string, id int) error {
	row := dbc.db.QueryRow(fmt.Sprintf("SELECT * FROM %s WHERE id=%d;", table, id))
	err := m.RowToModel(row)
	return err
}
