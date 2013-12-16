package dbcacher

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	"github.com/garyburd/redigo/redis"
	_ "github.com/lib/pq"
	"testing"
	"time"
)

type User struct {
	Id       int64
	Name     string
	Passwd   string
	Descp    string
	Created  int64
	Birthday int64
}

func (u *User) RowToModel(row *sql.Row) error {
	err := row.Scan(&u.Id, &u.Name, &u.Passwd,
		&u.Descp, &u.Created, &u.Birthday)
	println("rowtomodel: ", u.Id, u.Name, u.Passwd, u.Descp)
	return err
}

func (u *User) Decode(data []byte) error {
	println("Decode:", u.Id, u.Name, u.Passwd, u.Descp)
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)

	return dec.Decode(&u)
}

func (u *User) Encode() ([]byte, error) {
	var buf bytes.Buffer
	println("Encode:", u.Id, u.Name, u.Passwd, u.Descp)
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(u)
	if err != nil {
		println(err.Error())
		return nil, err
	}

	return buf.Bytes(), nil
}

func (u *User) UpdateClause() (string, error) {
	var clause = "UPDATE users SET %s='%s', %s='%s', %s='%s', %s='%d', %s='%d' WHERE id=%d"

	uc := fmt.Sprintf(clause, "name", u.Name, "passwd", u.Passwd,
		"descp", u.Descp, "created", u.Created, "birthday", u.Birthday, u.Id)
	return uc, nil
}

func openRedis() *redis.Pool {
	pool := &redis.Pool{
		MaxIdle:     10,
		IdleTimeout: 600 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "127.0.0.1:6379")
			if err != nil {
				panic(err)
				return nil, err
			}

			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
	return pool
}

func openDB() *sql.DB {
	db, err := sql.Open("postgres",
		"user=postgres dbname=pgtest password=action sslmode=disable")
	if err != nil {
		panic(err.Error())
	}
	drop := "drop table users;"
	db.Exec(drop)

	create := "create table if not exists users (id int8, name varchar(32), passwd varchar(64), descp varchar(200), created int8, birthday int8)"
	_, err = db.Exec(create)
	if err != nil {
		panic(err.Error())
	}
	_, err = db.Exec("insert into users values(1, 'guotie', '1234', 'a coder in china', 23, 2300)")
	if err != nil {
		panic(err.Error())
	}
	return db
}

func _test_Create(t *testing.T) {
	Create(openDB(), openRedis())
}

func _test_GetSet(t *testing.T) {
	u := User{}
	err := Get(&u, "users", 1, 0)
	if err != nil {
		t.Error(err.Error())
	}
	println(u.Id, u.Name, u.Descp)
	err = Get(&u, "users", 1, 0)
	if err != nil {
		t.Error(err.Error())
	}
	println(u.Id, u.Name, u.Descp)

	println("\n", "set testing")
	// 更新操作
	u.Name = "tiege"
	u.Descp = "我是铁哥"
	err = Set(&u, "users", 1, 2) // 2秒更新
	if err != nil {
		t.Error(err.Error())
	}
	//dbc.pool.Get().Do("HDEL", "users", 1)
	time.Sleep(time.Duration(1) * time.Second)
	err = Get(&u, "users", 1, 1)
	if err != nil {
		t.Error(err.Error())
	}
	println(u.Id, u.Name, u.Descp)

	time.Sleep(time.Duration(2) * time.Second)
	err = Get(&u, "users", 1, 0)
	if err != nil {
		t.Error(err.Error())
	}
	println(u.Id, u.Name, u.Descp)
}

func Test_DBC(t *testing.T) {
	_test_Create(t)
	_test_GetSet(t)
}
