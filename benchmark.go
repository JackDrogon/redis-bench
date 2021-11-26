package main

import (
	"fmt"
	"sync/atomic"

	"github.com/gomodule/redigo/redis"
)

/// all benchmarks init by init()
var Benchmarks map[string]Benchmark

/// TODO(Drogon): need clone or client concurrent safety
type Benchmark interface {
	// TODO(Drogon): Add context
	Run(stat *statistics, client redis.Conn, repeatNum int)
}

type setBenchmark struct {
	key string
}

func (b *setBenchmark) Run(stat *statistics, client redis.Conn, repeatNum int) {
	for i := 0; i < repeatNum; i++ {
		client.Do("SET", b.key, fmt.Sprintf("world%d", i))
		atomic.AddInt64(&stat.hit, 1)
	}
}

func init() {
	Benchmarks = make(map[string]Benchmark)
	Benchmarks["SET"] = &setBenchmark{
		key: "Hello",
	}
	// benchmarks = map[string]benchmarkFun{
	// 	"PING": benchmarkWrapper(func(client redis.Conn) (reply interface{}, err error) { return client.Do("PING") }),
	// 	"SET":  setBenchmark,
	// 	"GET":  benchmarkWrapper(func(client redis.Conn) (reply interface{}, err error) { return client.Do("GET", "hello") }),
	// }
}
