package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	neturl "net/url"
)

type JobRequest struct {
	VideoPath string  `json:"video_path" binding:"required"`
	Start     float64 `json:"start"`
	End       float64 `json:"end"`
	Query     string  `json:"query"`
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Redis config
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "redis:6379"
	}

	// Postgres config
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

	ctx := context.Background()

	// connect to redis
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis ping failed: %v", err)
	}
	defer rdb.Close()

	// connect to postgres
	pgURL := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", pgUser, pgPass, pgHost, pgPort, pgDB)
	pgpool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		log.Fatalf("postgres connect failed: %v", err)
	}
	defer pgpool.Close()

	// connect to minio (S3)
	minioEndpoint := os.Getenv("MINIO_ENDPOINT")
	if minioEndpoint == "" {
		minioEndpoint = "minio:9000"
	}
	minioAccess := os.Getenv("MINIO_ACCESS_KEY")
	minioSecret := os.Getenv("MINIO_SECRET_KEY")

	// Allow fully-qualified endpoints (e.g. http://localhost:9000) by parsing the URL
	// and extracting the host and scheme to determine Secure flag. The MinIO SDK
	// expects the endpoint to be host[:port] (no scheme/path).
	endpointHost := minioEndpoint
	secure := false
	if strings.HasPrefix(minioEndpoint, "http://") || strings.HasPrefix(minioEndpoint, "https://") {
		u, err := neturl.Parse(minioEndpoint)
		if err != nil {
			log.Fatalf("invalid MINIO_ENDPOINT: %v", err)
		}
		endpointHost = u.Host
		if u.Scheme == "https" {
			secure = true
		}
	}

	minioClient, err := minio.New(endpointHost, &minio.Options{
		Creds:  credentials.NewStaticV4(minioAccess, minioSecret, ""),
		Secure: secure,
	})
	if err != nil {
		log.Fatalf("minio init failed: %v", err)
	}

	r := gin.Default()

	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	r.POST("/jobs", func(c *gin.Context) {
		var req JobRequest
		// Read raw body so we can log it on errors
		raw, err := c.GetRawData()
		if err != nil {
			log.Printf("failed to read request body: %v", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read body"})
			return
		}
		log.Printf("/jobs request body: %s", string(raw))

		if err := json.Unmarshal(raw, &req); err != nil {
			log.Printf("json unmarshal error: %v; raw=%s", err, string(raw))
			// Try to parse as form data as a fallback (some clients may POST form-encoded)
			// Reset request body (we already consumed it with GetRawData) so ParseForm can read it
			c.Request.Body = io.NopCloser(bytes.NewReader(raw))
			if err := c.Request.ParseForm(); err == nil {
				// use FormValue which reads from POST and URL query
				v := c.Request.FormValue("video_path")
				if v != "" {
					req.VideoPath = v
					if s := c.Request.FormValue("start"); s != "" {
						if f, err := strconv.ParseFloat(s, 64); err == nil {
							req.Start = f
						}
					}
					if s := c.Request.FormValue("end"); s != "" {
						if f, err := strconv.ParseFloat(s, 64); err == nil {
							req.End = f
						}
					}
					req.Query = c.Request.FormValue("query")
				}
			}
			// validate that required field is present
			if req.VideoPath == "" {
				// As a last-resort fallback, attempt to parse simple "key:value" tokens from the raw body
				rawStr := string(raw)
				parts := strings.Fields(rawStr)
				for _, p := range parts {
					if kv := strings.SplitN(p, ":", 2); len(kv) == 2 {
						k := strings.TrimSpace(kv[0])
						v := strings.TrimSpace(kv[1])
						switch k {
						case "video_path":
							req.VideoPath = v
						case "start":
							if f, err := strconv.ParseFloat(v, 64); err == nil {
								req.Start = f
							}
						case "end":
							if f, err := strconv.ParseFloat(v, 64); err == nil {
								req.End = f
							}
						case "query":
							req.Query = v
						}
					}
				}
				if req.VideoPath == "" {
					c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body or missing video_path"})
					return
				}
			}
		}

		id := uuid.New().String()

		// prepare details JSON
		details := map[string]interface{}{
			"video_path": req.VideoPath,
			"start":      req.Start,
			"end":        req.End,
			"query":      req.Query,
		}
		detailsBytes, err := json.Marshal(details)
		if err != nil {
			log.Printf("failed to marshal details: %v; details=%v", err, details)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to marshal details"})
			return
		}

		// insert job metadata into postgres
		ctxPg, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, err = pgpool.Exec(ctxPg, "INSERT INTO jobs (id, status, details) VALUES ($1, $2, $3)", id, "queued", detailsBytes)
		if err != nil {
			log.Printf("pg insert error: %v; id=%s details=%s", err, id, string(detailsBytes))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create job metadata"})
			return
		}

		// push job into redis queue
		msg := map[string]interface{}{
			"id":         id,
			"video_path": req.VideoPath,
			"start":      req.Start,
			"end":        req.End,
			"query":      req.Query,
		}
		msgBytes, err := json.Marshal(msg)
		if err != nil {
			log.Printf("failed to marshal redis message: %v; msg=%v", err, msg)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to marshal job message"})
			return
		}
		log.Printf("pushing job to redis: %s", string(msgBytes))
		if err := rdb.LPush(ctx, "scene_jobs", msgBytes).Err(); err != nil {
			log.Printf("redis push error: %v; msg=%s", err, string(msgBytes))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue job"})
			return
		}
		// Log current queue length and a preview of the head elements for debugging
		if ll, err := rdb.LLen(ctx, "scene_jobs").Result(); err == nil {
			log.Printf("scene_jobs length after push: %d", ll)
			if lr, err := rdb.LRange(ctx, "scene_jobs", 0, -1).Result(); err == nil {
				for i, v := range lr {
					if i < 5 {
						log.Printf("scene_jobs[%d]=%s", i, v)
					}
				}
			} else {
				log.Printf("failed to LRANGE scene_jobs: %v", err)
			}
		} else {
			log.Printf("failed to LLen scene_jobs: %v", err)
		}

		// GET /jobs/:id/result will use this minio client (minioClient) to presign object URLs

		c.JSON(http.StatusCreated, gin.H{"id": id})
	})

	// Return a presigned URL for the job result object (if available)
	r.GET("/jobs/:id/result", func(c *gin.Context) {
		id := c.Param("id")
		ctxPg, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		var result string
		err := pgpool.QueryRow(ctxPg, "select details->>'result' from jobs where id=$1", id).Scan(&result)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "job not found or no result"})
			return
		}
		if result == "" {
			c.JSON(http.StatusNotFound, gin.H{"error": "result not available"})
			return
		}

		// result is expected like s3://bucket/key
		if !strings.HasPrefix(result, "s3://") {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "unexpected result url format"})
			return
		}
		rest := strings.TrimPrefix(result, "s3://")
		parts := strings.SplitN(rest, "/", 2)
		if len(parts) != 2 {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "invalid s3 path"})
			return
		}
		bucket := parts[0]
		object := parts[1]

		// Stream the object through the orchestrator to the client. This avoids
		// presigning/signature issues when clients access MinIO via a different
		// hostname than the one the container uses to reach the MinIO server.
		// First stat the object to obtain size/content-type.
		info, err := minioClient.StatObject(ctx, bucket, object, minio.StatObjectOptions{})
		if err != nil {
			log.Printf("failed to stat object: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to stat object"})
			return
		}

		obj, err := minioClient.GetObject(ctx, bucket, object, minio.GetObjectOptions{})
		if err != nil {
			log.Printf("failed to get object: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get object"})
			return
		}

		// Set response headers
		c.Header("Content-Type", info.ContentType)
		c.Header("Content-Length", fmt.Sprintf("%d", info.Size))
		c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", object))

		// Stream the object to the client
		if _, err := io.Copy(c.Writer, obj); err != nil {
			log.Printf("error streaming object to client: %v", err)
		}
	})

	log.Printf("starting orchestrator on :%s", port)
	if err := r.Run(":" + port); err != nil {
		log.Fatal(err)
	}
}
