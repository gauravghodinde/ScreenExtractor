package config

import (
	"log"
	"net/url"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	Port              string
	RedisAddr         string
	PGURL             string
	MinioEndpoint     string
	MinioEndpointHost string
	MinioSecure       bool
	MinioAccess       string
	MinioSecret       string
	BucketUploads     string
	WorkerSearch      string
}

func Load() *Config {
	_ = godotenv.Load("../local/.env.orchestrator")

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "redis:6379"
	}

	pgHost := os.Getenv("PG_HOST")
	if pgHost == "" {
		pgHost = "postgres"
	}
	pgPort := os.Getenv("PG_PORT")
	if pgPort == "" {
		pgPort = "5432"
	}
	pgUser := os.Getenv("PG_USER")
	if pgUser == "" {
		pgUser = "scene_user"
	}
	pgPass := os.Getenv("PG_PASS")
	if pgPass == "" {
		pgPass = "scene_pass"
	}
	pgDB := os.Getenv("PG_DB")
	if pgDB == "" {
		pgDB = "scene_seeker"
	}

	pgURL := "postgres://" + pgUser + ":" + pgPass + "@" + pgHost + ":" + pgPort + "/" + pgDB + "?sslmode=disable"

	minioEndpoint := os.Getenv("MINIO_ENDPOINT")
	if minioEndpoint == "" {
		minioEndpoint = "minio:9000"
	}

	bucketUploads := os.Getenv("BUCKET_UPLOADS")
	if bucketUploads == "" {
		bucketUploads = "scene-uploads"
	}
	minioAccess := os.Getenv("MINIO_ACCESS_KEY")
	minioSecret := os.Getenv("MINIO_SECRET_KEY")

	endpointHost := minioEndpoint
	secure := false
	if strings.HasPrefix(minioEndpoint, "http://") || strings.HasPrefix(minioEndpoint, "https://") {
		u, err := url.Parse(minioEndpoint)
		if err != nil {
			log.Fatalf("invalid MINIO_ENDPOINT: %v", err)
		}
		endpointHost = u.Host
		if u.Scheme == "https" {
			secure = true
		}
	}

	workerSearch := os.Getenv("WORKER_SEARCH_ADDR")
	if workerSearch == "" {
		workerSearch = "http://localhost:50052"
	}

	return &Config{
		Port:              port,
		RedisAddr:         redisAddr,
		PGURL:             pgURL,
		MinioEndpoint:     minioEndpoint,
		MinioEndpointHost: endpointHost,
		MinioSecure:       secure,
		MinioAccess:       minioAccess,
		MinioSecret:       minioSecret,
		BucketUploads:     bucketUploads,
		WorkerSearch:      workerSearch,
	}
}
