package main

import (
	"flag"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/garyburd/redigo/redis"
)

type config struct {
	hostname string
	port     int
	socket   string
	password string
	clients  int
	requests int
	size     int
	dbnum    int
	keep     bool
}

type statistics struct {
	set int64
}

var (
	conf config
)

func init() {
	flag.IntVar(&conf.clients, "c", 50, "-c <clients>       Number of parallel connections (default 50)")
	flag.IntVar(&conf.requests, "n", 100000, "-n <requests>      Total number of requests (default 100000)")

	flag.Usage = func() {
		fmt.Printf(`Usage: redis-bench [-h <host>] [-p <port>] [-c <clients>] [-n <requests>] [-k <boolean>]

 -h <hostname>      Server hostname (default 127.0.0.1)
 -p <port>          Server port (default 6379)
 -s <socket>        Server socket (overrides host and port)
 -a <password>      Password for Redis Auth
 -c <clients>       Number of parallel connections (default 50)
 -n <requests>      Total number of requests (default 100000)
 -d <size>          Data size of SET/GET value in bytes (default 3)
 --dbnum <db>       SELECT the specified db number (default 0)
 -k <boolean>       1=keep alive 0=reconnect (default 1)
 -r <keyspacelen>   Use random keys for SET/GET/INCR, random values for SADD
  Using this option the benchmark will expand the string __rand_int__
  inside an argument with a 12 digits number in the specified range
  from 0 to keyspacelen-1. The substitution changes every time a command
  is executed. Default tests use this to hit random keys in the
  specified range.
 -P <numreq>        Pipeline <numreq> requests. Default 1 (no pipeline).
 -e                 If server replies with errors, show them on stdout.
                    (no more than 1 error per second is displayed)
 -q                 Quiet. Just show query/sec values
 --csv              Output in CSV format
 -l                 Loop. Run the tests forever
 -t <tests>         Only run the comma separated list of tests. The test
                    names are the same as the ones produced as output.
 -I                 Idle mode. Just open N idle connections and wait.

Examples:

 Run the benchmark with the default configuration against 127.0.0.1:6379:
   $ redis-benchmark

 Use 20 parallel clients, for a total of 100k requests, against 192.168.1.1:
   $ redis-benchmark -h 192.168.1.1 -p 6379 -n 100000 -c 20
 Fill 127.0.0.1:6379 with about 1 million keys only using the SET test:
   $ redis-benchmark -t set -n 1000000 -r 100000000

 Benchmark 127.0.0.1:6379 for a few commands producing CSV output:
   $ redis-benchmark -t ping,set,get -n 100000 --csv

 Benchmark a specific command line:
   $ redis-benchmark -r 10000 -n 10000 eval 'return redis.call(\"ping\")' 0

 Fill a list with 10000 random elements:
   $ redis-benchmark -r 10000 -n 10000 lpush mylist __rand_int__

 On user specified command lines __rand_int__ is replaced with a random integer
 with a range of values selected by the -r option.
`)
	}
}

func setBenchmark(stat *statistics, repeatNum int) {
	c, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		fmt.Println("Connect to redis error", err)
		return
	}

	for i := 0; i < repeatNum; i++ {
		c.Do("SET", "hello", fmt.Sprintf("world%d", i))
		atomic.AddInt64(&stat.set, 1)
	}

	defer c.Close()
}

func print(closeChan chan struct{}, stat *statistics) {
	period := 1
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	var last int64
	var now int64

	for {
		select {
		case <-ticker.C:
			now = atomic.LoadInt64(&stat.set)
			fmt.Printf("Set qps %d\n", (now-last)/(int64)(period))
			last = now
		case <-closeChan:
			return
		}
	}
}

func sanitizeFlag() {
	flag.Parse()

	if conf.clients < 0 || conf.clients > 3000 {
		conf.clients = 50
	}

	if conf.requests < 1000 {
		conf.requests = 100000
	}
}

func dumpConf() {
}

func main() {
	sanitizeFlag()

	runtime.GOMAXPROCS(runtime.NumCPU())

	var benchmakrWG sync.WaitGroup
	var wg sync.WaitGroup
	var stat statistics
	closeChan := make(chan struct{}, 1)

	for i := 0; i < conf.clients; i++ {
		benchmakrWG.Add(1)
		go func() {
			setBenchmark(&stat, conf.requests)
			benchmakrWG.Done()
		}()
	}

	wg.Add(1)
	go func() {
		benchmakrWG.Wait()
		close(closeChan)
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		print(closeChan, &stat)
		wg.Done()
	}()

	wg.Wait()
}
