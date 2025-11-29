package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	redis "github.com/redis/go-redis/v9"
)

type Job struct {
	ID        string  `json:"id"`
	VideoPath string  `json:"video_path"`
	Start     float64 `json:"start"`
	End       float64 `json:"end"`
	Query     string  `json:"query"`
}

func main() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6380"
	}
	ctx := context.Background()
	opt := &redis.Options{Addr: addr}
	r := redis.NewClient(opt)
	defer r.Close()

	if err := r.Ping(ctx).Err(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to ping redis: %v\n", err)
		os.Exit(1)
	}

	job := Job{
		ID:        fmt.Sprintf("job-%d", time.Now().Unix()),
		VideoPath: "s3://scene-uploads/example-movie.mp4",
		Start:     12.5,
		End:       20.0,
		Query:     "I'll be back",
	}
	b, _ := json.Marshal(job)

	// push to the left so workers using LPOP will get it
	if err := r.LPush(ctx, "scene_jobs", string(b)).Err(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to push job: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("pushed job: %s\n", b)
}
