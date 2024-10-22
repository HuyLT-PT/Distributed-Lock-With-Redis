package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

var redisService = redis.NewClient(&redis.Options{
	Addr:     "localhost:6379",
	Password: "123456",
})

func acquireLock(ctx context.Context, key string, value string, expiration time.Duration) (bool, error) {
	success, err := redisService.SetNX(ctx, key, value, expiration).Result()
	if err != nil {
		return false, err
	}
	return success, nil
}

func releaseLock(ctx context.Context, key string, value string) (bool, error) {
	luaScript := `
    if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("UNLINK", KEYS[1])
    else
        return 0
    end`

	result, err := redisService.Eval(ctx, luaScript, []string{key}, value).Result()
	if err != nil {
		return false, err
	}
	return result == int64(1), nil
}

func set(ctx context.Context, key string, value interface{}) error {
	err := redisService.Set(ctx, key, value, 10*time.Second).Err()
	if err != nil {
		return err
	}
	return nil
}

func get(ctx context.Context, key string) (string, error) {
	data, err := redisService.Get(ctx, key).Result()
	if err != nil {
		return "", err
	}
	return data, nil
}

func requestWithLock(wg *sync.WaitGroup, id int, ctx context.Context, key string, value string) {
	defer wg.Done()

	lockKey := key + ":lock"
	lockValue := fmt.Sprintf("lock-%d", id)

	acquired, err := acquireLock(ctx, lockKey, lockValue, 10*time.Second)
	if err != nil {
		log.Printf("Error acquiring lock for request %d: %v", id, err)
		return
	}

	if !acquired {
		log.Printf("Request %d could not acquire the lock", id)
		return
	}

	fmt.Printf("Request %d acquired the lock and setting the key\n", id)
	err = set(ctx, key, value)
	if err != nil {
		log.Printf("Error setting key for request %d: %v", id, err)
	}

	data, err := get(ctx, key)
	if err != nil {
		log.Printf("Error getting key for request %d: %v", id, err)
	} else {
		fmt.Printf("Request %d got the value from Redis: %s\n", id, data)
	}

	success, err := releaseLock(ctx, lockKey, lockValue)
	if err != nil || !success {
		log.Printf("Request %d failed to release the lock", id)
	} else {
		fmt.Printf("Request %d released the lock\n", id)
	}
}

func main() {
	fmt.Println("__________start_________")

	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(2)

	go requestWithLock(&wg, 1, ctx, "A", "Hello from request 1")
	go requestWithLock(&wg, 2, ctx, "A", "Hello from request 2")

	wg.Wait()

	fmt.Println("__________end_________")
}
