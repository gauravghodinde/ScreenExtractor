package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	neturl "net/url"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/redis/go-redis/v9"
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

	// bucket names
	bucketUploads := os.Getenv("BUCKET_UPLOADS")
	if bucketUploads == "" {
		bucketUploads = "scene-uploads"
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

	// address of worker search API (internal to compose network)
	workerSearch := os.Getenv("WORKER_SEARCH_ADDR")
	if workerSearch == "" {
		workerSearch = "http://worker:50052"
	}

	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	r.POST("/jobs", func(c *gin.Context) {
		var req JobRequest

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

	// Stream upload: client streams bytes to orchestrator which streams them to MinIO
	// Required headers: Content-Length (for streaming without buffering). Optional: X-Filename for original filename.
	r.POST("/upload", func(c *gin.Context) {
		contentLenStr := c.GetHeader("Content-Length")
		if contentLenStr == "" {
			c.JSON(http.StatusLengthRequired, gin.H{"error": "Content-Length header required for streaming upload"})
			return
		}
		contentLen, err := strconv.ParseInt(contentLenStr, 10, 64)
		if err != nil || contentLen <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid Content-Length"})
			return
		}

		filename := c.GetHeader("X-Filename")
		if filename == "" {
			// try query param
			filename = c.Query("filename")
		}
		// generate id/object name
		id := uuid.New().String()
		objectName := id
		if filename != "" {
			// sanitize filename by keeping only the extension
			if idx := strings.LastIndex(filename, "."); idx != -1 {
				ext := filename[idx:]
				objectName = id + ext
			}
		}

		ctxMin := context.Background()
		// stream directly from request body to MinIO
		uploaded, err := minioClient.PutObject(ctxMin, bucketUploads, objectName, c.Request.Body, contentLen, minio.PutObjectOptions{ContentType: c.ContentType()})
		if err != nil {
			log.Printf("minio upload failed: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to upload to storage"})
			return
		}
		log.Printf("uploaded object: %s size=%d", uploaded.Key, uploaded.Size)

		// create job record in Postgres
		// jobID := uuid.New().String()
		jobID := uploaded.Key
		details := map[string]interface{}{"video_path": fmt.Sprintf("s3://%s/%s", bucketUploads, objectName)}
		detailsBytes, _ := json.Marshal(details)
		ctxPg, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if _, err := pgpool.Exec(ctxPg, "INSERT INTO jobs (id, status, details) VALUES ($1, $2, $3)", jobID, "pending", detailsBytes); err != nil {
			log.Printf("failed to insert job metadata: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create job metadata"})
			return
		}

		// enqueue into redis queue:queue:transcription
		msg := map[string]interface{}{"id": jobID, "video_path": fmt.Sprintf("s3://%s/%s", bucketUploads, objectName)}
		msgBytes, _ := json.Marshal(msg)
		if err := rdb.LPush(ctx, "queue:transcription", msgBytes).Err(); err != nil {
			log.Printf("failed to push job to redis: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue job"})
			return
		}

		c.JSON(http.StatusCreated, gin.H{"id": jobID, "s3_path": fmt.Sprintf("s3://%s/%s", bucketUploads, objectName)})
	})

	// POST /clip/search - Find matching subtitle segment and create a clip job
	// Uses the new MinIO-based vector search with job_id parameter
	// Request JSON: { "query": "text to search", "job_id": "...", "top_k": 1, "padding": 2.0 }
	r.POST("/clip/search", func(c *gin.Context) {
		var body struct {
			Query   string  `json:"query" binding:"required"`
			JobID   string  `json:"job_id" binding:"required"`
			TopK    int     `json:"top_k"`
			Padding float64 `json:"padding"`
		}
		if err := c.BindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request: query and job_id are required"})
			return
		}
		if body.TopK <= 0 {
			body.TopK = 1
		}

		// Call worker search API with job_id parameter for MinIO-based search
		reqPayload := map[string]interface{}{
			"query":  body.Query,
			"job_id": body.JobID,
			"top_k":  body.TopK,
		}
		reqBytes, _ := json.Marshal(reqPayload)
		client := &http.Client{Timeout: 30 * time.Second}
		resp, err := client.Post(workerSearch+"/search", "application/json", bytes.NewReader(reqBytes))
		if err != nil {
			log.Printf("failed to call worker search api: %v", err)
			c.JSON(http.StatusBadGateway, gin.H{"error": "failed to contact worker search"})
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			log.Printf("worker search returned status %d: %s", resp.StatusCode, string(bodyBytes))
			c.JSON(http.StatusBadGateway, gin.H{"error": "worker search error"})
			return
		}

		var wRes struct {
			Results []struct {
				ID    interface{}            `json:"id"`
				Text  string                 `json:"text"`
				Meta  map[string]interface{} `json:"meta"`
				Score float64                `json:"score"`
			} `json:"results"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&wRes); err != nil {
			log.Printf("failed to decode worker search response: %v", err)
			c.JSON(http.StatusBadGateway, gin.H{"error": "invalid worker response"})
			return
		}
		if len(wRes.Results) == 0 {
			c.JSON(http.StatusNotFound, gin.H{"error": "no matching segments found"})
			return
		}

		// Extract timestamp from top result
		// Helper function to parse timestamp strings in various formats
		parseTimestamp := func(ts string) float64 {
			// Try parsing HH:MM:SS.microseconds format first (e.g., "0:01:08.443000")
			parts := strings.Split(ts, ":")
			if len(parts) == 3 {
				hours := 0
				minutes := 0
				seconds := 0.0

				fmt.Sscanf(parts[0], "%d", &hours)
				fmt.Sscanf(parts[1], "%d", &minutes)
				fmt.Sscanf(parts[2], "%f", &seconds)

				return float64(hours*3600+minutes*60) + seconds
			}

			// Try parsing simple float
			if f, err := strconv.ParseFloat(ts, 64); err == nil {
				return f
			}

			return 0.0
		}

		top := wRes.Results[0]
		segStart := 0.0
		segEnd := 0.0

		if v, ok := top.Meta["start"]; ok {
			switch t := v.(type) {
			case float64:
				segStart = t
			case float32:
				segStart = float64(t)
			case string:
				segStart = parseTimestamp(t)
			}
		}
		if v, ok := top.Meta["end"]; ok {
			switch t := v.(type) {
			case float64:
				segEnd = t
			case float32:
				segEnd = float64(t)
			case string:
				segEnd = parseTimestamp(t)
			}
		}

		// Apply padding
		padding := body.Padding
		if padding < 0 {
			padding = 0
		}
		clipStart := segStart - padding
		if clipStart < 0 {
			clipStart = 0
		}
		clipEnd := segEnd + padding

		// Get the video_path from the job database
		ctxPg, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		// Try to get video_path from details, fallback to result if available
		var videoPath string
		var videoPaths3 sql.NullString
		var resultPath sql.NullString

		err2 := pgpool.QueryRow(ctxPg, "SELECT COALESCE(details->>'video_path', '') as vp, COALESCE(details->>'result', '') as rp FROM jobs WHERE id=$1", body.JobID).Scan(&videoPaths3, &resultPath)
		if err2 != nil {
			log.Printf("failed to fetch original job video_path: %v", err2)
			c.JSON(http.StatusBadRequest, gin.H{"error": "original job not found"})
			return
		}

		// Use video_path if present, otherwise try to extract from result
		if videoPaths3.Valid && videoPaths3.String != "" {
			videoPath = videoPaths3.String
		} else if resultPath.Valid && resultPath.String != "" {
			// result is in format "srt://bucket/key" or "s3://bucket/key"
			result := resultPath.String
			if strings.HasPrefix(result, "srt://") {
				// Extract the S3 path from srt:// format
				videoPath = "s3://" + strings.TrimPrefix(result, "srt://")
			} else if strings.HasPrefix(result, "s3://") {
				videoPath = result
			}
		}

		if videoPath == "" {
			log.Printf("job %s has no video_path in details or result", body.JobID)
			c.JSON(http.StatusBadRequest, gin.H{"error": "job has no video_path"})
			return
		}

		// Create a new clip job
		clipJobID := uuid.New().String()
		clipDetails := map[string]interface{}{
			"video_path": videoPath,
			"start":      clipStart,
			"end":        clipEnd,
			"query":      body.Query,
			"source_job": body.JobID,
		}
		clipDetailsBytes, _ := json.Marshal(clipDetails)
		if _, err := pgpool.Exec(ctxPg, "INSERT INTO jobs (id, status, details) VALUES ($1, $2, $3)", clipJobID, "queued", clipDetailsBytes); err != nil {
			log.Printf("failed to insert clip job: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create clip job"})
			return
		}

		// Enqueue clip job into redis scene_jobs queue
		clipMsg := map[string]interface{}{
			"id":         clipJobID,
			"video_path": videoPath,
			"start":      clipStart,
			"end":        clipEnd,
		}
		clipMsgBytes, _ := json.Marshal(clipMsg)
		if err := rdb.LPush(ctx, "scene_jobs", clipMsgBytes).Err(); err != nil {
			log.Printf("failed to enqueue clip job: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue clip job"})
			return
		}

		c.JSON(http.StatusAccepted, gin.H{
			"clip_job_id": clipJobID,
			"clip_start":  clipStart,
			"clip_end":    clipEnd,
			"source_job":  body.JobID,
			"query":       body.Query,
		})
	})

	// List jobs with optional pagination and status filter
	// Query params: ?limit=10&offset=0&status=done
	r.GET("/jobs", func(c *gin.Context) {
		limitStr := c.DefaultQuery("limit", "20")
		offsetStr := c.DefaultQuery("offset", "0")
		statusFilter := c.Query("status")

		limit, err := strconv.Atoi(limitStr)
		if err != nil || limit <= 0 || limit > 100 {
			limit = 20
		}
		offset, err := strconv.Atoi(offsetStr)
		if err != nil || offset < 0 {
			offset = 0
		}

		ctxPg, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		var rows pgx.Rows
		if statusFilter != "" {
			rows, err = pgpool.Query(ctxPg, "SELECT id, status, details->>'result', created_at FROM jobs WHERE status=$1 ORDER BY created_at DESC LIMIT $2 OFFSET $3", statusFilter, limit, offset)
		} else {
			rows, err = pgpool.Query(ctxPg, "SELECT id, status, details->>'result', created_at FROM jobs ORDER BY created_at DESC LIMIT $1 OFFSET $2", limit, offset)
		}
		if err != nil {
			log.Printf("jobs list query failed: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to query jobs"})
			return
		}
		defer rows.Close()

		var result []gin.H
		for rows.Next() {
			var id string
			var status string
			var resultPath string
			var createdAt time.Time
			if err := rows.Scan(&id, &status, &resultPath, &createdAt); err != nil {
				log.Printf("failed to scan job row: %v", err)
				continue
			}
			result = append(result, gin.H{"id": id, "status": status, "result": resultPath, "created_at": createdAt})
		}
		if err := rows.Err(); err != nil {
			log.Printf("rows iteration error: %v", err)
		}
		c.JSON(http.StatusOK, gin.H{"jobs": result})
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
