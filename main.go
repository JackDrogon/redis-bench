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
	clients int
}

type statistics struct {
	set int64
}

var (
	conf config
)

func init() {
	flag.IntVar(&conf.clients, "c", 50, "-c <clients>       Number of parallel connections (default 50)")
}

func setBenchmark(stat *statistics, repeatNum int) {
	c, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		fmt.Println("Connect to redis error", err)
		return
	}

	for i := 0; i < repeatNum; i++ {
		c.Do("SET", "hello", "world")
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
			setBenchmark(&stat, 1000000)
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
