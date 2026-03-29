package main

import (
	"context"
	"log"

	"github.com/gauravghodinde/scenextractor/orchestrator/internal/api"
	"github.com/gauravghodinde/scenextractor/orchestrator/internal/config"
	"github.com/gauravghodinde/scenextractor/orchestrator/internal/queue"
	"github.com/gauravghodinde/scenextractor/orchestrator/internal/repository"
	"github.com/gauravghodinde/scenextractor/orchestrator/internal/storage"
)

func main() {
	cfg := config.Load()
	ctx := context.Background()

	q, err := queue.New(ctx, cfg.RedisAddr)
	if err != nil {
		log.Fatalf("redis init failed: %v", err)
	}
	defer q.Close()

	repo, err := repository.New(ctx, cfg.PGURL)
	if err != nil {
		log.Fatalf("postgres init failed: %v", err)
	}
	defer repo.Close()

	store, err := storage.New(cfg.MinioEndpointHost, cfg.MinioAccess, cfg.MinioSecret, cfg.BucketUploads, cfg.MinioSecure)
	if err != nil {
		log.Fatalf("minio init failed: %v", err)
	}

	server := api.NewServer(cfg, repo, q, store)

	log.Printf("starting orchestrator on :%s", cfg.Port)
	if err := server.Start(); err != nil {
		log.Fatal(err)
	}
}
