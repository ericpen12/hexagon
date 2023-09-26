package hexagon

import (
	"context"
	"github.com/redis/go-redis/v9"
	. "github.com/smartystreets/goconvey/convey"
	"reflect"
	"testing"
	"time"
)

func getRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
}
func TestRedisLock_TryLock(t *testing.T) {
	t.Run("加锁测试", func(t *testing.T) {
		Convey("加锁", t, func() {
			ctx := context.TODO()
			key := "test-lock-try-lock"
			lock := NewRedisLock(getRedisClient(), key, 5*time.Second)
			err := lock.TryLock(ctx)
			So(err, ShouldBeNil)
			value := getRedisClient().Get(ctx, key).String()
			So(value, ShouldNotBeEmpty)
		})

		Convey("加锁-互斥", t, func() {
			ctx := context.TODO()
			key := "test-lock-try-lock-mutex"
			lock := NewRedisLock(getRedisClient(), key, 5*time.Second)
			err := lock.TryLock(ctx)
			So(err, ShouldBeNil)
			lock2 := NewRedisLock(getRedisClient(), key, 5*time.Second)
			err = lock2.TryLock(ctx)
			So(err, ShouldEqual, ErrLockFailed)
		})
	})
	t.Run("锁过期测试", func(t *testing.T) {
		Convey("锁过期测试", t, func() {
			ctx := context.TODO()
			key := "test-lock-try-lock-timeout"
			ttl := 1 * time.Second
			lock := NewRedisLock(getRedisClient(), key, ttl)
			err := lock.TryLock(ctx)
			So(err, ShouldBeNil)
			le := reflect.ValueOf(lock).Elem()
			ll := le.Interface().(RedisLock)
			close(ll.watchChan)

			lock2 := NewRedisLock(getRedisClient(), key, ttl)
			err = lock2.TryLock(ctx)
			So(err, ShouldEqual, ErrLockFailed)
			time.Sleep(ttl + time.Second)
			lock3 := NewRedisLock(getRedisClient(), key, ttl)
			err = lock3.TryLock(ctx)
			So(err, ShouldBeNil)
		})
		Convey("锁-续期", t, func() {
			ctx := context.TODO()
			key := "test-lock-try-lock-refresh-ttl"
			ttl := 1 * time.Second
			lock := NewRedisLock(getRedisClient(), key, ttl)
			err := lock.TryLock(ctx)
			So(err, ShouldBeNil)

			time.Sleep(2 * time.Second)
			ret, err := getRedisClient().TTL(ctx, key).Result()
			So(ret, ShouldBeGreaterThan, 100*time.Millisecond)
			So(err, ShouldBeNil)

			lock.UnLock(ctx)
			ret, err = getRedisClient().TTL(ctx, key).Result()
			So(ret, ShouldBeLessThan, 1*time.Millisecond)
			So(err, ShouldBeNil)
		})
	})
}

func TestRedisLock_UnLock(t *testing.T) {
	Convey("解锁", t, func() {
		Convey("解锁自己", func() {
			ctx := context.TODO()
			key := "test-lock-try-lock-self"
			rds := getRedisClient()
			lock := NewRedisLock(rds, key, 30*time.Second)
			err := lock.TryLock(ctx)
			So(err, ShouldBeNil)
			err = lock.UnLock(context.TODO())
			So(err, ShouldBeNil)
			value, err := rds.Get(ctx, key).Result()
			So(err, ShouldNotBeNil)
			So(value, ShouldBeEmpty)
		})
		Convey("解锁别人", func() {
			Convey("解锁自己", func() {
				ctx := context.TODO()
				key := "test-lock-try-lock-other"
				rds := getRedisClient()
				lock := NewRedisLock(rds, key, 30*time.Second)
				err := lock.TryLock(ctx)
				So(err, ShouldBeNil)
				lock2 := NewRedisLock(rds, key, 30*time.Second)
				err = lock2.UnLock(context.TODO())
				So(err, ShouldBeNil)
				value, err := rds.Get(ctx, key).Result()
				So(err, ShouldBeNil)
				So(value, ShouldNotBeEmpty)
			})
		})
	})
}

func TestRedisLock_Lock(t *testing.T) {
	Convey("上锁", t, func() {
		var (
			ctx = context.TODO()
			key = "test-lock-try-lock-success"
			rds = getRedisClient()
		)
		lock := NewRedisLock(rds, key, 1*time.Second)
		err := lock.Lock(ctx)
		So(err, ShouldBeNil)
		rLock := lock.(*RedisLock)
		result, err := rds.Get(ctx, key).Result()
		So(err, ShouldBeNil)
		So(result, ShouldEqual, rLock.Value)
	})
	Convey("上锁-阻塞", t, func() {
		var (
			ctx = context.TODO()
			key = "test-lock-try-lock-block"
			rds = getRedisClient()
		)
		lock := NewRedisLock(rds, key, 1*time.Second)
		err := lock.Lock(ctx)
		So(err, ShouldBeNil)
		lock2 := NewRedisLock(rds, key, 1*time.Second)
		err = lock2.TryLock(ctx)
		So(err, ShouldEqual, ErrLockFailed)
	})
	Convey("上锁-等待", t, func() {
		var (
			ctx = context.TODO()
			key = "test-lock-try-lock-wait"
			rds = getRedisClient()
		)
		lock := NewRedisLock(rds, key, 1*time.Second)
		err := lock.Lock(ctx)
		So(err, ShouldBeNil)
		go func() {
			time.Sleep(2 * time.Second)
			lock.UnLock(ctx)
		}()
		ctx, _ = context.WithTimeout(ctx, time.Second*5)
		lock2 := NewRedisLock(rds, key, 1*time.Second)
		err = lock2.Lock(ctx)
		So(err, ShouldBeNil)
	})
}
