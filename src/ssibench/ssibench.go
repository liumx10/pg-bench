package ssibench

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

type Configure struct {
	TableSize  int
	ReadSize   int
	UpdateSize int
	Duration   int
	Nthreads   int
	ReadRatio  float64
}

var conf Configure

func checkErr(err error, msg string) {
	if err != nil {
		fmt.Println(msg)
		os.Exit(1)
	}
}

func table_name(id int) string {
	return "ssibench" + strconv.Itoa(id)
}

func random_table() string {
	return "ssibench" + strconv.Itoa(rand.Intn(3)+1)
}

func random_col() string {
	return "b_value_" + strconv.Itoa(rand.Intn(10)+1)
}

func random_string(length int) string {
	return "bilibili"
}

func random_condition(size int) string {
	var a []int
	for i := 0; i < size; i++ {
		xid := rand.Intn(conf.TableSize)
		contain := false
		for j := 0; j < len(a); j++ {
			if xid == a[j] {
				i--
				contain = true
				break
			}
		}
		if contain == false {
			a = append(a, xid)
		}
	}
	var str []string
	for i := 0; i < size; i++ {
		str = append(str, "b_int_key="+strconv.Itoa(a[i]))
	}
	return " where " + strings.Join(str, " OR ")
}

func read_only(db *sql.DB) int {
	xid := rand.Intn(conf.TableSize - conf.ReadSize)

	txn, err := db.BeginTx(context.Background(), nil)
	checkErr(err, "connect to database failed")

	var sum int
	query := `select sum(b_int) from ` + random_table() + ` where b_int_key > $1 and b_int_key < $2;`
	err = txn.QueryRow(query, xid, xid+conf.ReadSize).Scan(&sum)
	txn.Commit()
	if err != nil {
		return 0
	} else {
		return 1
	}
}

func read_update(db *sql.DB) int {
	read_xid := rand.Intn(conf.TableSize - conf.ReadSize)
	tableid := rand.Intn(3) + 1

	txn, err := db.Begin()
	if err != nil {
		return 0
	}

	var sum int
	query := fmt.Sprintf("select sum(b_int) from %s where b_int_key > %d and b_int_key < %d;",
		table_name(tableid), read_xid, read_xid+conf.ReadSize)

	err = txn.QueryRow(query).Scan(&sum)
	if err != nil {
		txn.Rollback()
		return 0
	}

	condition := random_condition(conf.UpdateSize)
	query = fmt.Sprintf("update %s set %s = '%s' %s",
		table_name((tableid%3)+1), random_col(), random_string(conf.UpdateSize), condition)
	//fmt.Println(query)
	_, err = txn.Exec(query)
	if err != nil {
		txn.Rollback()
		return 0
	}

	err = txn.Commit()
	if err != nil {
		return 0
	} else {
		return 1
	}
}

func readConf(jsonfile string) {
	raw, err := ioutil.ReadFile(jsonfile)
	checkErr(err, "Read configuration file failed")

	err = json.Unmarshal(raw, &conf)
	checkErr(err, "Json file format error")
}

func dropTable(db *sql.DB) {
	_, err := db.Exec("drop table if EXISTS ssibench1 ")
	checkErr(err, "drop table failed")

	_, err = db.Exec("drop table if EXISTS ssibench2")
	checkErr(err, "drop table failed")

	_, err = db.Exec("drop table if EXISTS ssibench3 ")
	checkErr(err, "drop table failed")
}

func createTable(db *sql.DB) {
	for i := 1; i < 4; i++ {
		_, err := db.Exec(fmt.Sprintf(`CREATE TABLE ssibench%d (b_int_key serial PRIMARY KEY, b_int integer, 
			b_value_1 varchar, 
			b_value_2 varchar, 
			b_value_3 varchar, 
			b_value_4 varchar, 
			b_value_5 varchar, 
			b_value_6 varchar, 
			b_value_7 varchar, 
			b_value_8 varchar, 
			b_value_9 varchar, 
			b_value_10 varchar 
			)`, i))
		checkErr(err, "create table failed")
	}
}
func InitBench(jsonfile string) {
	readConf(jsonfile)

	db, err := sql.Open("postgres", "host=localhost database=test sslmode=disable")
	checkErr(err, "connect to database failed")

	dropTable(db)
	createTable(db)

	for i := 1; i < 4; i++ {
		for j := 0; j < conf.TableSize; j++ {
			_, err := db.Exec(fmt.Sprintf(`insert into ssibench%d (b_int) values (%d)`, i, rand.Int()%100000000))
			checkErr(err, "Insert faild")
		}
	}

	db.Close()
}

func execute(ch chan int) {
	db, err := sql.Open("postgres", "host=localhost database=test sslmode=disable")
	checkErr(err, "connect to database failed")

	start := time.Now()
	txs := 0
	aborts := 0

	for time.Since(start).Seconds() < float64(conf.Duration) {
		for i := 0; i < 10; i++ {
			var res int
			if rand.Float64() < conf.ReadRatio {
				res = read_only(db)
			} else {
				res = read_update(db)
			}
			txs += res
			aborts += 1 - res
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

	fmt.Printf("Throughput: %.2f KTPS\n", float64(txs/conf.Duration)/1000)
	fmt.Printf("Abort ratio: %.2f%%\n", float64(aborts)*100/float64(aborts+txs))
}
