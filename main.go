package main

import (
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"runtime"
	"ssibench"
	"tpcb"
    "simple"
)

func main() {
	runtime.GOMAXPROCS(200)
	var conf string
	var init bool
	var bench string
	flag.StringVar(&bench, "bench", "default", "Benchmark: default/ssibench/tpcb")
	flag.BoolVar(&init, "init", false, "Initilize tables")

	flag.Parse()

	switch bench {
    case"default":
        conf = "default.json"
        if init{
            ssibench.InitBench(conf);
        }else{
            simple.RunBench(conf);
        }
        break
	case "ssibench":
        conf = "ssibench.json"
		if init {
			ssibench.InitBench(conf)
		} else {
			ssibench.RunBench(conf)
		}
		break
	case "tpcb":
        conf = "tpcb.json"
		if init {
			tpcb.InitBench(conf)
		} else {
			tpcb.RunBench(conf)
		}
		break
	}

	fmt.Println("finished..")
}
