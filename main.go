package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/garyburd/redigo/redis"
)

type config struct {
	hostname    string
	port        int
	socket      string
	password    string
	clientsNum  int
	requestsNum int
	size        int
	dbnum       int
	keep        bool
}

type statistics struct {
	set int64
}

var (
	conf    config
	clients []redis.Conn
)

func init() {
	flag.IntVar(&conf.clientsNum, "c", 50, "-c <clients>       Number of parallel connections (default 50)")
	flag.IntVar(&conf.requestsNum, "n", 100000, "-n <requests>      Total number of requests (default 100000)")
	flag.StringVar(&conf.hostname, "-h", "127.0.0.1", "-h <hostname>      Server hostname (default 127.0.0.1)")
	flag.IntVar(&conf.port, "-p", 6379, "-p <port>          Server port (default 6379)")

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
   $ redis-bench

 Use 20 parallel clients, for a total of 100k requests, against 192.168.1.1:
   $ redis-bench -h 192.168.1.1 -p 6379 -n 100000 -c 20
 Fill 127.0.0.1:6379 with about 1 million keys only using the SET test:
   $ redis-bench -t set -n 1000000 -r 100000000

 Benchmark 127.0.0.1:6379 for a few commands producing CSV output:
   $ redis-bench -t ping,set,get -n 100000 --csv

 Benchmark a specific command line:
   $ redis-bench -r 10000 -n 10000 eval 'return redis.call(\"ping\")' 0

 Fill a list with 10000 random elements:
   $ redis-bench -r 10000 -n 10000 lpush mylist __rand_int__

 On user specified command lines __rand_int__ is replaced with a random integer
 with a range of values selected by the -r option.
`)
	}
}

func setBenchmark(stat *statistics, clientNum int, repeatNum int) {
	client := clients[clientNum]
	for i := 0; i < repeatNum; i++ {
		client.Do("SET", "hello", fmt.Sprintf("world%d", i))
		atomic.AddInt64(&stat.set, 1)
	}
}

func pingBenchmark() {
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

	if conf.clientsNum < 0 || conf.clientsNum > 3000 {
		conf.clientsNum = 50
	}

	if conf.requestsNum < 1000 {
		conf.requestsNum = 100000
	}
}

func initClients() {
	clients = make([]redis.Conn, conf.clientsNum)
	for i := 0; i < conf.clientsNum; i++ {
		client, err := redis.Dial("tcp", fmt.Sprintf("%s:%d", conf.hostname, conf.port))
		if err != nil {
			fmt.Println("Connect to redis error", err)
			destroyClients(i)
			os.Exit(1)
		}
		clients[i] = client
	}
}

func destroyClients(activeClients int) {
	for i := 0; i < activeClients; i++ {
		clients[i].Close()
	}
}

func dumpConf() {
}

func main() {
	sanitizeFlag()
	initClients()
	defer destroyClients(conf.clientsNum)

	runtime.GOMAXPROCS(runtime.NumCPU())

	var benchmakrWG sync.WaitGroup
	var wg sync.WaitGroup
	var stat statistics
	closeChan := make(chan struct{}, 1)

	for i := 0; i < conf.clientsNum; i++ {
		benchmakrWG.Add(1)
		go func(clientNum int) {
			setBenchmark(&stat, clientNum, conf.requestsNum)
			benchmakrWG.Done()
		}(i)
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
