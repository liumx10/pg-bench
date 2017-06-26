package main

import (
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"runtime"
	"ssibench"
	"tpcb"
)

func main() {
	runtime.GOMAXPROCS(200)
	var conf string
	var init bool
	var bench string
	flag.StringVar(&bench, "bench", "ssibench", "Benchmark: ssibench/tpcb")
	flag.BoolVar(&init, "init", false, "Initilize tables")
	flag.StringVar(&conf, "configure", "tpcb.json", "Concurrent workers")

	flag.Parse()

	switch bench {
	case "ssibench":
		if conf == "tpcb.json" {
			conf = "ssibench.json"
		}

		if init {
			ssibench.InitBench(conf)
		} else {
			ssibench.RunBench(conf)
		}
		break
	case "tpcb":
		if init {
			tpcb.InitBench(conf)
		} else {
			tpcb.RunBench(conf)
		}
		break
	}

	fmt.Println("finished..")
}
