package queue

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type Queue struct {
	client *redis.Client
}

func New(ctx context.Context, addr string) (*Queue, error) {
	client := redis.NewClient(&redis.Options{Addr: addr})
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}
	return &Queue{client: client}, nil
}

func (q *Queue) Close() error {
	return q.client.Close()
}

func (q *Queue) Push(ctx context.Context, queueName string, data []byte) error {
	return q.client.LPush(ctx, queueName, data).Err()
}

func (q *Queue) LLen(ctx context.Context, queueName string) (int64, error) {
	return q.client.LLen(ctx, queueName).Result()
}

func (q *Queue) LRange(ctx context.Context, queueName string, start, stop int64) ([]string, error) {
	return q.client.LRange(ctx, queueName, start, stop).Result()
}
