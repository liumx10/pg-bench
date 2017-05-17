package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"time"
)

func checkErr(err error) {
	if err != nil {
		panic(err)
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
	return "abcdefgh"
}
func random_condition(size int) string {
	var a []int
	for i := 0; i < size; i++ {
		xid := rand.Intn(100000)
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
	xid := rand.Intn(99900)

	var sum int
	query := `select sum(b_int) from ` + random_table() + ` where b_int_key > $1 and b_int_key < $2;`
	err := db.QueryRow(query, xid, xid+100).Scan(&sum)

	if err != nil {
		return 0
	} else {
		return 1
	}
}

func read_update(db *sql.DB) int {
	read_xid := rand.Intn(99980)
	tableid := rand.Intn(3) + 1

	txn, err := db.Begin()
	checkErr(err)

	var sum int
	query := `select sum(b_int) from ` + table_name(tableid) + ` where b_int_key > $1 and b_int_key < $2;`
	err = txn.QueryRow(query, read_xid, read_xid+100).Scan(&sum)
	if err != nil {
		txn.Rollback()
		return 0
	}

	condition := random_condition(10)
	query = `update ` + table_name(tableid%3+1) + ` set ` + random_col() + " = $1 " + condition

	_, err = txn.Exec(query, random_string(10))
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

func execute(ch chan int, ratio float64, duration float64) {
	db, err := sql.Open("postgres", "host=localhost database=test sslmode=disable")
	checkErr(err)

	start := time.Now()
	txs := 0

	for time.Since(start).Seconds() < duration {
		if rand.Float64() < ratio {
			txs += read_only(db)
		} else {
			txs += read_update(db)
		}
	}
	ch <- txs
}

func main() {
	runtime.GOMAXPROCS(90)
	nthreads := flag.Int("nthreads", 1, "Concurrent workers")
	duration := flag.Float64("duration", 5, "Test duration")
	read := flag.Float64("read", 0.5, "Read ratio (0~1)")
	flag.Parse()

	fmt.Println("nthreads: ", *nthreads, "duration: ", *duration)

	var chs = make([]chan int, *nthreads)
	for i := 0; i < *nthreads; i++ {
		chs[i] = make(chan int)
		go execute(chs[i], *read, *duration)
	}

	txs := 0
	for _, ch := range chs {
		txs += <-ch
	}

	fmt.Printf("Throughput: %.2f KTPS\n", float64(txs)/1000 / *duration)
}
