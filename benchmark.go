package main

import (
	"fmt"
	"sync/atomic"

	"github.com/garyburd/redigo/redis"
)

type redisDo func(client redis.Conn) (reply interface{}, err error)

func benchmarkWrapper(do redisDo) benchmarkFun {
	return func(stat *statistics, clientNum int, repeatNum int) {
		client := clients[clientNum]
		for i := 0; i < repeatNum; i++ {
			do(client)
			atomic.AddInt64(&stat.hit, 1)
		}
	}
}

func setBenchmark(stat *statistics, clientNum int, repeatNum int) {
	client := clients[clientNum]
	for i := 0; i < repeatNum; i++ {
		client.Do("SET", "hello", fmt.Sprintf("world%d", i))
		atomic.AddInt64(&stat.hit, 1)
	}
}
