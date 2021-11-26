package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gomodule/redigo/redis"
)

type statistics struct {
	hit int64
}

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
	tests       string

	runnableBenchmarks map[string]Benchmark
}

var (
	conf    config
	clients []redis.Conn
)

func init() {
	flag.IntVar(&conf.clientsNum, "c", 50, "-c <clients>       Number of parallel connections (default 50)")
	flag.IntVar(&conf.requestsNum, "n", 100000, "-n <requests>      Total number of requests (default 100000)")
	flag.StringVar(&conf.hostname, "host", "127.0.0.1", "--host <hostname>      Server hostname (default 127.0.0.1)")
	flag.IntVar(&conf.port, "p", 6379, "-p <port>          Server port (default 6379)")
	flag.StringVar(&conf.password, "a", "", "-a <password>      Password for Redis Auth")
	flag.IntVar(&conf.dbnum, "dbnum", 0, "--dbnum <db>       SELECT the specified db number (default 0)")
	flag.StringVar(&conf.tests, "t", "", `-t <tests>         Only run the comma separated list of tests. The test
                    names are the same as the ones produced as output.`)

	flag.Usage = func() {
		fmt.Printf(`Usage: redis-bench [--host <host>] [--port <port>] [-c <clients>] [-n <requests>] [-k <boolean>]

 --host <hostname>      Server hostname (default 127.0.0.1)
 --port <port>          Server port (default 6379)
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

func print(benchmarkName string, closeChan chan struct{}, stat *statistics) {
	var period int64 = 100
	ticker := time.NewTicker(time.Duration(period) * time.Millisecond)
	defer ticker.Stop()
	var last int64
	var now int64

	for {
		select {
		case <-ticker.C:
			now = atomic.LoadInt64(&stat.hit)
			fmt.Printf("%s: %.3f\n", benchmarkName, float64(now-last)/(float64(period)/1000.0))
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

	if conf.dbnum < 0 {
		conf.dbnum = 0
	}
}

func sanitizeBenchmarks() {
	if conf.tests == "" {
		conf.runnableBenchmarks = Benchmarks
		return
	}

	conf.runnableBenchmarks = make(map[string]Benchmark)
	tests := strings.Split(conf.tests, ",")
	for _, benchmarkName := range tests {
		benchmarkName = strings.ToUpper(benchmarkName)
		if benchmark, ok := Benchmarks[benchmarkName]; ok {
			conf.runnableBenchmarks[benchmarkName] = benchmark
		}
	}
}

func createClient() (client redis.Conn, err error) {
	if client, err = redis.Dial("tcp", fmt.Sprintf("%s:%d", conf.hostname, conf.port)); err != nil {
		return
	}
	if conf.password != "" {
		if _, err = client.Do("AUTH", conf.password); err != nil {
			return
		}
	}
	if _, err = client.Do("SELECT", conf.dbnum); err != nil {
		return
	}
	return
}

func initClients() {
	clients = make([]redis.Conn, conf.clientsNum)
	for i := 0; i < conf.clientsNum; i++ {
		client, err := createClient()
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

func runBenchmark(benchmarkName string, benchmark Benchmark) {
	var benchmakrWG sync.WaitGroup
	var wg sync.WaitGroup
	var stat statistics
	closeChan := make(chan struct{}, 1)

	for i := 0; i < conf.clientsNum; i++ {
		benchmakrWG.Add(1)
		go func(clientNum int) {
			benchmark.Run(&stat, clients[clientNum], conf.requestsNum/conf.clientsNum)
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
		print(benchmarkName, closeChan, &stat)
		wg.Done()
	}()

	wg.Wait()
}

func runBenchmarks() {
	for benchmarkName, benchmark := range conf.runnableBenchmarks {
		runBenchmark(benchmarkName, benchmark)
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	sanitizeFlag()
	initClients()
	defer destroyClients(conf.clientsNum)
	sanitizeBenchmarks()

	runBenchmarks()
}
