package task

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-redis/redis/v8"

	"github.com/vmihailenco/taskq/v3"
	"github.com/vmihailenco/taskq/v3/redisq"
)

type RedisQueue struct {
	Name      string
	Queue     *redisq.Queue
	inner     *redis.Client
	closeChan chan struct{}
}

func NewClient() (*redis.Client, taskq.Factory, error) {
	dsn := "redis://localhost:6379"

	opts, err := redis.ParseURL(dsn)
	if err != nil {
		return nil, nil, err
	}

	Redis := redis.NewClient(opts)
	if err := Redis.
		Ping(context.Background()).
		Err(); err != nil {
		fmt.Print(err)
		return nil, nil, err
	}

	QueueFactory := redisq.NewFactory()
	return Redis, QueueFactory, nil
}

func NewQueue(c *redis.Client, factory taskq.Factory, name string) *RedisQueue {

	q := factory.RegisterQueue(&taskq.QueueOptions{
		Name:  name,
		Redis: c,
	})

	return &RedisQueue{
		Name:  name,
		inner: c,
		Queue: q.(*redisq.Queue),
	}
}

var SimpleTask = taskq.RegisterTask(&taskq.TaskOptions{
	Name: "printer",
	Handler: func(name string) error {
		fmt.Println("Hello", name)
		return nil
	},
})

func WaitSignal() os.Signal {
	ch := make(chan os.Signal, 2)
	signal.Notify(
		ch,
		syscall.SIGINT,
		syscall.SIGQUIT,
		syscall.SIGTERM,
	)
	for {
		sig := <-ch
		switch sig {
		case syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM:
			return sig
		}
	}
}
