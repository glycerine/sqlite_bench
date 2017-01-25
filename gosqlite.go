package main

import (
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"

	idem "github.com/glycerine/halter"
	"github.com/mattn/go-sqlite3"
)

type connPool struct {
	pool []*sqlite3.SQLiteConn

	mut sync.Mutex

	gotOne *idem.IdemCloseChan
}

func (p *connPool) Append(conn *sqlite3.SQLiteConn) {
	p.mut.Lock()
	defer p.mut.Unlock()
	p.pool = append(p.pool, conn)
	p.gotOne.Close()
}

func (p *connPool) Get() []*sqlite3.SQLiteConn {
	select {
	case <-time.After(1 * time.Second):
		return nil
	case <-p.gotOne.Chan:
		// good get lock and return a copy of
		// the slice header
	}
	p.mut.Lock()
	defer p.mut.Unlock()
	return p.pool
}

func newConnPool() *connPool {
	return &connPool{
		gotOne: idem.NewIdemCloseChan(),
	}
}

func withTxn(db *sql.DB, batchSize int, payload string) {
	t0 := time.Now()
	txn, err := db.Begin()
	panicOn(err)

	for i := 0; i < batchSize; i++ {
		_, err = txn.Exec(
			fmt.Sprintf("insert into frame (tm, val) values ('%s','%s');", time.Now().Format(time.RFC3339), payload))
		panicOn(err)

	}
	err = txn.Commit()
	panicOn(err)
	elap := time.Since(t0)
	p("batchsize=%v, with txn: elap=%v", batchSize, elap)
}

func withoutTxn(db *sql.DB, batchSize int, payload string) {
	t0 := time.Now()
	for i := 0; i < batchSize; i++ {
		_, err := db.Exec(
			fmt.Sprintf("insert into frame (tm, val) values ('%s','%s');", time.Now().Format(time.RFC3339), payload))
		panicOn(err)

	}
	elap := time.Since(t0)
	p("batchsize=%v, with withouttxn: elap=%v", batchSize, elap)
}

func main() {
	payload := ""
	for i := 0; i < 16; i++ {
		payload += fmt.Sprintf("%v", i%10)
	}

	for _, batchSize := range []int{1, 10, 100, 1000, 10000} {
		dba, dbb := setupDBs()
		withoutTxn(dba, batchSize, payload)
		withTxn(dbb, batchSize, payload)
		dba.Close()
		dbb.Close()
	}
	//conn := conns[0]

}

func setupDBs() (dba *sql.DB, dbb *sql.DB) {
	os.MkdirAll("./data/1234", 0755)
	a := "./data/1234/notxn.db"
	b := "./data/1234/withtxn.db"
	os.Create(a)
	os.Create(b)

	/*
		cp := newConnPool()
		sql.Register("sqlite3_with_hook",
			&sqlite3.SQLiteDriver{
				ConnectHook: func(conn *sqlite3.SQLiteConn) error {
					p("in ConnectHook callback")
					cp.Append(conn)
					return nil
				},
			})
	*/
	driver := "sqlite3"
	// driver := "sqlite3_with_hook"
	dba, err := sql.Open(driver, a)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	dbb, err = sql.Open(driver, b)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	//p("done with open")

	_, err = dba.Exec("CREATE TABLE `frame` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, `tm` DATETIME NOT NULL, `val` BLOB NOT NULL)")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	_, err = dbb.Exec("CREATE TABLE `frame` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, `tm` DATETIME NOT NULL, `val` BLOB NOT NULL)")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	//p("done with exec")
	/*
		conns := cp.Get()
		if conns == nil {
			panic("could not get sqlite connections")
		}
	*/
	return dba, dbb
}

func p(format string, a ...interface{}) {
	fmt.Printf("\n"+format+"\n", a...)
}

func panicOn(err error) {
	if err != nil {
		panic(err)
	}
}

/*
osx ssd results

with transaction vs. without txn.

batch size 1
elap=738.737µs vs. elap=767.952µs

batch size 10
elap=965.793µs vs. elap=5.181083ms

batch size 100
elap=2.629779ms  vs. elap=65.781325ms

batch size 1000
elap=18.752734ms  vs. elap=673.651937ms

batch size 10000
elap=156.5602ms  vs.  elap=6.533467682s

*/

/* OSX SSD driver:

batchsize=1, with withouttxn: elap=623.491µs
batchsize=1, with txn: elap=476.757µs

batchsize=10, with withouttxn: elap=5.570855ms
batchsize=10, with txn: elap=584.262µs

batchsize=100, with withouttxn: elap=64.369902ms
batchsize=100, with txn: elap=3.531113ms

batchsize=1000, with withouttxn: elap=693.016593ms
batchsize=1000, with txn: elap=14.771254ms

batchsize=10000, with withouttxn: elap=9.371305014s
batchsize=10000, with txn: elap=139.108072ms

*/
