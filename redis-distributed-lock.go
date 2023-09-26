package hexagon

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"time"
)

var (
	ErrTimeOut    = errors.New("time out")
	ErrLockFailed = errors.New("lock failed")
)

type Lock interface {
	Lock(ctx context.Context) error
	UnLock(ctx context.Context) error
	TryLock(ctx context.Context) error
}

type RedisLock struct {
	Key       string
	Value     string
	ttl       time.Duration
	rds       *redis.Client
	tryTime   time.Duration
	watchChan chan struct{}
}

func NewRedisLock(rds *redis.Client, key string, ttl time.Duration) Lock {
	return &RedisLock{
		Key:       key,
		Value:     uuid.New().String(),
		ttl:       ttl,
		rds:       rds,
		tryTime:   time.Millisecond * 50,
		watchChan: make(chan struct{}, 1),
	}
}

func (r RedisLock) TryLock(ctx context.Context) error {
	ok, err := r.rds.SetNX(ctx, r.Key, r.Value, r.ttl).Result()
	if err != nil {
		return err
	}
	if !ok {
		return ErrLockFailed
	}
	go r.watchExpire()
	return nil
}

func (r RedisLock) UnLock(ctx context.Context) error {
	err := luaUnLock.Run(ctx, r.rds, []string{r.Key}, r.Value).Err()
	close(r.watchChan)
	return err
}

var luaUnLock = redis.NewScript(`
	if redis.call("GET",KEYS[1]) == ARGV[1]
	then
		return redis.call("DEL",KEYS[1])
	else
		return 0
	end
`)

func (r RedisLock) watchExpire() {
	timeTicker := time.NewTicker(r.ttl / 3)
	for {
		select {
		case <-timeTicker.C:
			ctx, cancel := context.WithTimeout(context.TODO(), 500*time.Microsecond)
			err := r.rds.Expire(ctx, r.Key, r.ttl).Err()
			if err != nil {
				fmt.Println("RedisLock Expire failed, err:", err)
			}
			cancel()
		case <-r.watchChan:
			return
		}
	}
}

func (r RedisLock) Lock(ctx context.Context) error {
	err := r.TryLock(ctx)
	if err == nil {
		return nil
	}
	if !errors.Is(err, ErrLockFailed) {
		return err
	}
	interval := time.NewTicker(r.tryTime)
	defer interval.Stop()
	for {
		select {
		case <-interval.C:
			err = r.TryLock(ctx)
			if err == nil {
				return nil
			}
			if !errors.Is(err, ErrLockFailed) {
				return err
			}
		case <-ctx.Done():
			return ErrTimeOut
		}
	}
}
