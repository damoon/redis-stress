package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/go-redis/redis"
	"golang.org/x/sync/semaphore"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	redisAddress  = kingpin.Flag("redisAddress", "Redis Host").Default("redis:6379").String()
	redisPassword = kingpin.Flag("redisPassword", "Redis Password").Default("").String()
	redisDatabase = kingpin.Flag("redisDatabase", "Redis Database").Default("0").Int()
	parallelism   = kingpin.Flag("parallelism", "Parallelism").Default("20000").Int64()
	verbose       = kingpin.Flag("verbose", "Verbose").Default("false").Bool()
)

func main() {
	kingpin.Parse()

	r := redis.NewClient(&redis.Options{
		Addr:     *redisAddress,
		Password: *redisPassword,
		DB:       *redisDatabase,
	})

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	sem := semaphore.NewWeighted(*parallelism)

	ch := make(chan struct{})

	go func() {
		for {
			sem.Acquire(context.Background(), 1)
			ch <- struct{}{}
		}
	}()

	rand.Seed(time.Now().UnixNano())
	i := rand.Int()

	for {
		select {

		case <-signals:
			log.Print("interrupt is detected")
			return

		case <-ch:
			i++
			go func(i int) {
				defer sem.Release(1)
				k := fmt.Sprintf("stress-%d", i)
				err := r.Set(k, k, 0).Err()
				if err != nil {
					log.Fatalf("failed to write to redis: %s", err)
				}
				err = r.Get(k).Err()
				if err != nil {
					log.Fatalf("failed to read from redis: %s", err)
				}
				if *verbose {
					log.Println(k)
				}
			}(i)
		}
	}

}
