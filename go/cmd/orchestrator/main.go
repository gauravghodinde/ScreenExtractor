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

	// Proxy vector search to the worker's Chroma-backed search API
	r.POST("/search", func(c *gin.Context) {
		// read the incoming JSON body
		raw, err := c.GetRawData()
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read body"})
			return
		}

		// forward to worker search API
		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Post(workerSearch+"/search", "application/json", bytes.NewReader(raw))
		if err != nil {
			log.Printf("failed to call worker search api: %v", err)
			c.JSON(http.StatusBadGateway, gin.H{"error": "failed to contact worker search"})
			return
		}
		defer resp.Body.Close()

		// copy status code and body
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("failed to read worker response: %v", err)
			c.JSON(http.StatusBadGateway, gin.H{"error": "failed to read worker response"})
			return
		}
		c.Data(resp.StatusCode, resp.Header.Get("Content-Type"), body)
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
		jobID := uuid.New().String()
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

	// Query text -> find matching subtitle segment via worker's Chroma search,
	// then enqueue a job to extract a clip around the top hit.
	// Request JSON: { "query": "text to search", "clip_duration": 20 }
	r.POST("/query_clip", func(c *gin.Context) {
		var body struct {
			Query        string  `json:"query"`
			TopK         int     `json:"top_k"`
			ClipDuration float64 `json:"clip_duration"`
		}
		if err := c.BindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json body"})
			return
		}
		if body.Query == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "query is required"})
			return
		}
		if body.TopK <= 0 {
			body.TopK = 5
		}
		if body.ClipDuration <= 0 {
			// default to 20 seconds (between the requested 15-30s)
			body.ClipDuration = 20.0
		}

		// call worker search API directly
		reqPayload := map[string]interface{}{"query": body.Query, "top_k": body.TopK}
		reqBytes, _ := json.Marshal(reqPayload)
		client := &http.Client{Timeout: 10 * time.Second}
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
				ID       string                 `json:"id"`
				Text     string                 `json:"text"`
				Meta     map[string]interface{} `json:"meta"`
				Distance float64                `json:"distance"`
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

		top := wRes.Results[0]
		// extract metadata: expected keys job_id, start, end
		matchedJobID := ""
		segStart := 0.0
		segEnd := 0.0
		if v, ok := top.Meta["job_id"]; ok {
			if s, ok2 := v.(string); ok2 {
				matchedJobID = s
			}
		}
		if v, ok := top.Meta["start"]; ok {
			switch t := v.(type) {
			case float64:
				segStart = t
			case float32:
				segStart = float64(t)
			case int:
				segStart = float64(t)
			case int64:
				segStart = float64(t)
			case string:
				if f, err := strconv.ParseFloat(t, 64); err == nil {
					segStart = f
				}
			}
		}
		if v, ok := top.Meta["end"]; ok {
			switch t := v.(type) {
			case float64:
				segEnd = t
			case float32:
				segEnd = float64(t)
			case int:
				segEnd = float64(t)
			case int64:
				segEnd = float64(t)
			case string:
				if f, err := strconv.ParseFloat(t, 64); err == nil {
					segEnd = f
				}
			}
		}

		if matchedJobID == "" {
			log.Printf("worker returned hit without job_id metadata: %+v", top.Meta)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "incomplete hit metadata"})
			return
		}

		// lookup original job to obtain video_path
		ctxPg2, cancel2 := context.WithTimeout(ctx, 5*time.Second)
		defer cancel2()
		var videoPath string
		if err := pgpool.QueryRow(ctxPg2, "select details->>'video_path' from jobs where id=$1", matchedJobID).Scan(&videoPath); err != nil {
			log.Printf("failed to lookup source job %s: %v", matchedJobID, err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to lookup source video"})
			return
		}

		// compute clip window centered on the matched segment midpoint
		segMid := (segStart + segEnd) / 2.0
		half := body.ClipDuration / 2.0
		clipStart := segMid - half
		if clipStart < 0 {
			clipStart = 0
		}
		clipEnd := clipStart + body.ClipDuration

		// create new job to extract the clip
		newID := uuid.New().String()
		details := map[string]interface{}{"video_path": videoPath, "start": clipStart, "end": clipEnd, "query": body.Query, "source_job": matchedJobID}
		detailsBytes, _ := json.Marshal(details)
		ctxPg3, cancel3 := context.WithTimeout(ctx, 5*time.Second)
		defer cancel3()
		if _, err := pgpool.Exec(ctxPg3, "INSERT INTO jobs (id, status, details) VALUES ($1, $2, $3)", newID, "queued", detailsBytes); err != nil {
			log.Printf("failed to insert clip job: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create clip job"})
			return
		}
		// enqueue
		msg := map[string]interface{}{"id": newID, "video_path": videoPath, "start": clipStart, "end": clipEnd, "query": body.Query}
		msgBytes, _ := json.Marshal(msg)
		if err := rdb.LPush(ctx, "scene_jobs", msgBytes).Err(); err != nil {
			log.Printf("failed to enqueue clip job: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue clip job"})
			return
		}

		c.JSON(http.StatusAccepted, gin.H{"job_id": newID, "clip_start": clipStart, "clip_end": clipEnd, "source_job": matchedJobID})
	})

	// Fallback: simple substring search over SRT transcripts stored in the results bucket.
	// This is a lightweight alternative to vector search for prototyping.
	r.POST("/query_clip_fallback", func(c *gin.Context) {
		var body struct {
			Query        string  `json:"query"`
			ClipDuration float64 `json:"clip_duration"`
			LimitSRTs    int     `json:"limit_srts"`
		}
		if err := c.BindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json body"})
			return
		}
		if body.Query == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "query is required"})
			return
		}
		if body.ClipDuration <= 0 {
			body.ClipDuration = 20.0
		}
		if body.LimitSRTs <= 0 {
			body.LimitSRTs = 50
		}

		// list objects in results bucket and look for .srt files
		bucket := "scene-results"
		opts := minio.ListObjectsOptions{Recursive: true}
		cnt := 0
		found := false
		var matchedJob string
		var segStart, segEnd float64

		ctxMin := context.Background()
		for obj := range minioClient.ListObjects(ctxMin, bucket, opts) {
			if obj.Err != nil {
				log.Printf("minio list object error: %v", obj.Err)
				continue
			}
			if !strings.HasSuffix(obj.Key, ".srt") {
				continue
			}
			cnt++
			if cnt > body.LimitSRTs {
				break
			}

			// read object into memory (SRTs are small)
			rc, err := minioClient.GetObject(ctxMin, bucket, obj.Key, minio.GetObjectOptions{})
			if err != nil {
				log.Printf("failed to get srt %s: %v", obj.Key, err)
				continue
			}
			data, err := io.ReadAll(rc)
			rc.Close()
			if err != nil {
				log.Printf("failed to read srt %s: %v", obj.Key, err)
				continue
			}
			content := string(data)
			// split into blocks by blank line
			blocks := strings.Split(content, "\n\n")
			qlow := strings.ToLower(body.Query)
			for _, b := range blocks {
				lines := strings.Split(strings.TrimSpace(b), "\n")
				if len(lines) < 3 {
					continue
				}
				// time line is usually second line
				timeLine := strings.TrimSpace(lines[1])
				// content is the rest
				txt := strings.Join(lines[2:], " ")
				if strings.Contains(strings.ToLower(txt), qlow) {
					// parse timeLine like "00:00:50,000 --> 00:00:52,000"
					parts := strings.Split(timeLine, "-->")
					if len(parts) != 2 {
						continue
					}
					parse := func(ts string) float64 {
						ts = strings.TrimSpace(ts)
						// format HH:MM:SS,mmm
						hms := strings.Split(ts, ":")
						if len(hms) != 3 {
							return 0.0
						}
						hrs := 0
						mins := 0
						secs := 0
						msecs := 0
						fmt.Sscanf(hms[0], "%d", &hrs)
						fmt.Sscanf(hms[1], "%d", &mins)
						secParts := strings.Split(hms[2], ",")
						if len(secParts) >= 1 {
							fmt.Sscanf(secParts[0], "%d", &secs)
						}
						if len(secParts) >= 2 {
							fmt.Sscanf(secParts[1], "%d", &msecs)
						}
						return float64(hrs*3600+mins*60+secs) + float64(msecs)/1000.0
					}
					s := parse(parts[0])
					e := parse(parts[1])
					matchedJob = strings.TrimSuffix(obj.Key, ".srt")
					segStart = s
					segEnd = e
					found = true
					break
				}
			}
			if found {
				break
			}
		}

		if !found {
			c.JSON(http.StatusNotFound, gin.H{"error": "no matching subtitle segment found"})
			return
		}

		// lookup original job to obtain video_path
		ctxPg2, cancel2 := context.WithTimeout(ctx, 5*time.Second)
		defer cancel2()
		var videoPath string
		if err := pgpool.QueryRow(ctxPg2, "select details->>'video_path' from jobs where id=$1", matchedJob).Scan(&videoPath); err != nil {
			log.Printf("failed to lookup source job %s: %v", matchedJob, err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to lookup source video"})
			return
		}

		// compute clip window centered on the matched segment midpoint
		segMid := (segStart + segEnd) / 2.0
		half := body.ClipDuration / 2.0
		clipStart := segMid - half
		if clipStart < 0 {
			clipStart = 0
		}
		clipEnd := clipStart + body.ClipDuration

		// create new job to extract the clip
		newID := uuid.New().String()
		details := map[string]interface{}{"video_path": videoPath, "start": clipStart, "end": clipEnd, "query": body.Query, "source_job": matchedJob}
		detailsBytes, _ := json.Marshal(details)
		ctxPg3, cancel3 := context.WithTimeout(ctx, 5*time.Second)
		defer cancel3()
		if _, err := pgpool.Exec(ctxPg3, "INSERT INTO jobs (id, status, details) VALUES ($1, $2, $3)", newID, "queued", detailsBytes); err != nil {
			log.Printf("failed to insert clip job: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create clip job"})
			return
		}
		// enqueue
		msg := map[string]interface{}{"id": newID, "video_path": videoPath, "start": clipStart, "end": clipEnd, "query": body.Query}
		msgBytes, _ := json.Marshal(msg)
		if err := rdb.LPush(ctx, "scene_jobs", msgBytes).Err(); err != nil {
			log.Printf("failed to enqueue clip job: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue clip job"})
			return
		}

		c.JSON(http.StatusAccepted, gin.H{"job_id": newID, "clip_start": clipStart, "clip_end": clipEnd, "source_job": matchedJob})
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
