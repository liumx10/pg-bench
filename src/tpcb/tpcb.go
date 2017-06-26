package tpcb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"io/ioutil"
	"math/rand"
	"os"
	"time"
)

type Configure struct {
	Branches         int
	TellerPerBranch  int
	AccountPerBranch int
	Duration         int
	Nthreads         int
}

var conf Configure

func checkErr(err error, msg string) {
	if err != nil {
		fmt.Println(msg)
		os.Exit(1)
	}
}
func dropTable(db *sql.DB) {
	_, err := db.Exec("drop table if EXISTS branch ")
	checkErr(err, "drop table failed")

	_, err = db.Exec("drop table if EXISTS account")
	checkErr(err, "drop table failed")

	_, err = db.Exec("drop table if EXISTS teller ")
	checkErr(err, "drop table failed")

	_, err = db.Exec("drop table if EXISTS history ")
	checkErr(err, "drop table failed")
}

func createTable(db *sql.DB) {
	_, err := db.Exec("CREATE table IF NOT EXISTS branch ( branch_id integer PRIMARY KEY, branch_balance bigint, padding char(88) )")
	checkErr(err, "create table failed")

	_, err = db.Exec("CREATE table IF NOT EXISTS account ( account_id integer PRIMARY KEY, branch_id integer, account_balance bigint, padding char(84) )")
	checkErr(err, "create table failed")

	_, err = db.Exec("CREATE table IF NOT EXISTS teller ( teller_id integer PRIMARY KEY, branch_id integer, teller_balance bigint, padding char(84) )")
	checkErr(err, "create table failed")

	_, err = db.Exec("CREATE table IF NOT EXISTS history ( account_id integer, teller_id integer, branch_id integer, amount bigint, padding char(30) )")
	checkErr(err, "create table failed")
}

func readConf(jsonfile string) {
	raw, err := ioutil.ReadFile(jsonfile)
	checkErr(err, "Read configuration file failed")

	err = json.Unmarshal(raw, &conf)
	checkErr(err, "Json file format error")
}

func InitBench(jsonfile string) {
	db, err := sql.Open("postgres", "host=localhost database=test sslmode=disable")
	checkErr(err, "connect to database failed")

	dropTable(db)
	createTable(db)

	readConf(jsonfile)

	aid := 0
	tid := 0

	for i := 0; i < conf.Branches; i++ {
		_, err = db.Exec(fmt.Sprintf("insert into branch (branch_id, branch_balance) values(%d, %d)", i, 0))
		checkErr(err, "insert branch failed")
		for j := 0; j < conf.TellerPerBranch; j++ {
			_, err = db.Exec(fmt.Sprintf("insert into teller (teller_id, branch_id, teller_balance) values(%d, %d, %d)", tid, i, 0))
			tid++
			checkErr(err, "insert teller failed")
		}
		for j := 0; j < conf.AccountPerBranch; j++ {
			_, err = db.Exec(fmt.Sprintf("insert into account (account_id, branch_id, account_balance) values(%d, %d, %d)", aid, i, 0))
			aid++
			checkErr(err, "insert account failed")
		}

		if (i*100)%conf.Branches == 0 {
			fmt.Printf("\rFinished: %d%%", 100*i/conf.Branches)
		}
	}
	fmt.Println("")
	db.Close()
}

func transaction(db *sql.DB) bool {
	bid := rand.Int() % conf.Branches
	tid := (rand.Int() % conf.TellerPerBranch) + bid*conf.TellerPerBranch
	aid := (rand.Int() % conf.AccountPerBranch) + bid*conf.AccountPerBranch
	delta := rand.Int63()%1000 - 500

	txn, err := db.Begin()
	if err != nil {
		return false
	}

	_, err = txn.Exec(fmt.Sprintf("update account set account_balance = account_balance+%d where account_id = %d", delta, aid))
	if err != nil {
		txn.Rollback()
		return false
	}

	_, err = txn.Exec(fmt.Sprintf("insert into history (account_id, teller_id, branch_id, amount) values(%d, %d, %d, %d)", aid, tid, bid, delta))
	if err != nil {
		txn.Rollback()
		return false
	}

	_, err = txn.Exec(fmt.Sprintf("update teller set teller_balance = teller_balance+%d where teller_id = %d", delta, tid))
	if err != nil {
		txn.Rollback()
		return false
	}

	_, err = txn.Exec(fmt.Sprintf("update branch set branch_balance =branch_balance+ %d where branch_id = %d", delta, tid))
	if err != nil {
		txn.Rollback()
		return false
	}

	txn.Commit()

	return true
}
func execute(ch chan int) {
	db, err := sql.Open("postgres", "host=localhost database=test sslmode=disable")
	checkErr(err, "connect to database failed")

	start := time.Now()
	txs := 0
	aborts := 0

	for time.Since(start).Seconds() < (float64)(conf.Duration) {
		for i := 0; i < 10; i++ {
			if transaction(db) {
				txs++
			} else {
				aborts++
			}
		}
	}

	ch <- txs
	ch <- aborts
}

func RunBench(jsonfile string) {
	readConf(jsonfile)

	var chs = make([]chan int, conf.Nthreads)
	for i := 0; i < conf.Nthreads; i++ {
		chs[i] = make(chan int)
		go execute(chs[i])
	}

	txs := 0
	aborts := 0
	for _, ch := range chs {
		txs += <-ch
		aborts += <-ch
	}

	fmt.Printf("Throughput: %.2f KTPS\n", float64(txs/conf.Duration)/1000.0)
	fmt.Printf("Abort ratio: %.2f%%\n", float64(aborts)*100/float64(aborts+txs))
}
