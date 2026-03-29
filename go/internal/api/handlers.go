package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
)

type JobRequest struct {
	VideoPath string  `json:"video_path" binding:"required"`
	Start     float64 `json:"start"`
	End       float64 `json:"end"`
	Query     string  `json:"query"`
}

func (s *Server) handleHealthz(c *gin.Context) {
	c.JSON(200, gin.H{"status": "ok"})
}

func (s *Server) handleCreateJob(c *gin.Context) {
	var req JobRequest

	raw, err := c.GetRawData()
	if err != nil {
		log.Printf("failed to read request body: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read body"})
		return
	}

	if err := json.Unmarshal(raw, &req); err != nil {
		c.Request.Body = io.NopCloser(bytes.NewReader(raw))
		if err := c.Request.ParseForm(); err == nil {
			if v := c.Request.FormValue("video_path"); v != "" {
				req.VideoPath = v
				if str := c.Request.FormValue("start"); str != "" {
					if f, err := strconv.ParseFloat(str, 64); err == nil {
						req.Start = f
					}
				}
				if str := c.Request.FormValue("end"); str != "" {
					if f, err := strconv.ParseFloat(str, 64); err == nil {
						req.End = f
					}
				}
				req.Query = c.Request.FormValue("query")
			}
		}
		if req.VideoPath == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body or missing video_path"})
			return
		}
	}

	id := uuid.New().String()

	details := map[string]interface{}{
		"video_path": req.VideoPath,
		"start":      req.Start,
		"end":        req.End,
		"query":      req.Query,
	}
	detailsBytes, err := json.Marshal(details)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to marshal details"})
		return
	}

	ctx := c.Request.Context()
	if err := s.repo.CreateJob(ctx, id, "queued", detailsBytes); err != nil {
		log.Printf("pg insert error: %v; id=%s details=%s", err, id, string(detailsBytes))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create job metadata"})
		return
	}

	msg := map[string]interface{}{
		"id":         id,
		"video_path": req.VideoPath,
		"start":      req.Start,
		"end":        req.End,
		"query":      req.Query,
	}
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to marshal job message"})
		return
	}
	if err := s.q.Push(ctx, "scene_jobs", msgBytes); err != nil {
		log.Printf("redis push error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue job"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"id": id})
}

func (s *Server) handleUpload(c *gin.Context) {
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
		filename = c.Query("filename")
	}
	id := uuid.New().String()
	objectName := id
	if filename != "" {
		if idx := strings.LastIndex(filename, "."); idx != -1 {
			objectName = id + filename[idx:]
		}
	}

	ctx := context.Background()
	uploaded, err := s.store.PutObject(ctx, s.cfg.BucketUploads, objectName, c.Request.Body, contentLen, minio.PutObjectOptions{ContentType: c.ContentType()})
	if err != nil {
		log.Printf("minio upload failed: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to upload to storage"})
		return
	}

	jobID := uploaded.Key
	details := map[string]interface{}{"video_path": fmt.Sprintf("s3://%s/%s", s.cfg.BucketUploads, objectName)}
	detailsBytes, _ := json.Marshal(details)

	if err := s.repo.CreateJob(ctx, jobID, "pending", detailsBytes); err != nil {
		log.Printf("failed to insert job metadata: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create job metadata"})
		return
	}

	msg := map[string]interface{}{"id": jobID, "video_path": fmt.Sprintf("s3://%s/%s", s.cfg.BucketUploads, objectName)}
	msgBytes, _ := json.Marshal(msg)
	if err := s.q.Push(ctx, "queue:transcription", msgBytes); err != nil {
		log.Printf("failed to push job to redis: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue job"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"id": jobID, "s3_path": fmt.Sprintf("s3://%s/%s", s.cfg.BucketUploads, objectName)})
}

func (s *Server) handleSearchClip(c *gin.Context) {
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

	reqPayload := map[string]interface{}{
		"query":  body.Query,
		"job_id": body.JobID,
		"top_k":  body.TopK,
	}
	reqBytes, _ := json.Marshal(reqPayload)
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Post(s.cfg.WorkerSearch+"/search", "application/json", bytes.NewReader(reqBytes))
	if err != nil {
		log.Printf("failed to call worker search api: %v", err)
		c.JSON(http.StatusBadGateway, gin.H{"error": "failed to contact worker search"})
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
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

	parseTimestamp := func(ts string) float64 {
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

	padding := body.Padding
	if padding < 0 {
		padding = 0
	}
	clipStart := segStart - padding
	if clipStart < 0 {
		clipStart = 0
	}
	clipEnd := segEnd + padding

	ctx := context.Background()
	videoPath, resultPath, err := s.repo.GetJobResultAndVideoPath(ctx, body.JobID)
	if err != nil {
		log.Printf("failed to fetch original job paths: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "original job not found"})
		return
	}

	if videoPath == "" && resultPath != "" {
		if strings.HasPrefix(resultPath, "srt://") {
			videoPath = "s3://" + strings.TrimPrefix(resultPath, "srt://")
		} else if strings.HasPrefix(resultPath, "s3://") {
			videoPath = resultPath
		}
	}

	if videoPath == "" {
		log.Printf("job %s has no video_path in details or result", body.JobID)
		c.JSON(http.StatusBadRequest, gin.H{"error": "job has no video_path"})
		return
	}

	clipJobID := uuid.New().String()
	clipDetails := map[string]interface{}{
		"video_path": videoPath,
		"start":      clipStart,
		"end":        clipEnd,
		"query":      body.Query,
		"source_job": body.JobID,
	}
	clipDetailsBytes, _ := json.Marshal(clipDetails)
	if err := s.repo.CreateJob(ctx, clipJobID, "queued", clipDetailsBytes); err != nil {
		log.Printf("failed to insert clip job: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create clip job"})
		return
	}

	clipMsg := map[string]interface{}{
		"id":         clipJobID,
		"video_path": videoPath,
		"start":      clipStart,
		"end":        clipEnd,
	}
	clipMsgBytes, _ := json.Marshal(clipMsg)
	if err := s.q.Push(ctx, "scene_jobs", clipMsgBytes); err != nil {
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
}

func (s *Server) handleListJobs(c *gin.Context) {
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

	ctx := c.Request.Context()
	jobs, err := s.repo.GetJobs(ctx, statusFilter, limit, offset)
	if err != nil {
		log.Printf("jobs list query failed: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to query jobs"})
		return
	}

	var result []gin.H
	for _, j := range jobs {
		result = append(result, gin.H{"id": j.ID, "status": j.Status, "result": j.Result, "created_at": j.CreatedAt})
	}
	c.JSON(http.StatusOK, gin.H{"jobs": result})
}

func (s *Server) handleGetJobResult(c *gin.Context) {
	id := c.Param("id")
	ctx := context.Background()

	result, err := s.repo.GetJobResult(ctx, id)
	if err != nil || result == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found or no result"})
		return
	}

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

	info, err := s.store.StatObject(ctx, bucket, object, minio.StatObjectOptions{})
	if err != nil {
		log.Printf("failed to stat object: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to stat object"})
		return
	}

	obj, err := s.store.GetObject(ctx, bucket, object, minio.GetObjectOptions{})
	if err != nil {
		log.Printf("failed to get object: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get object"})
		return
	}
	defer obj.Close()

	c.Header("Content-Type", info.ContentType)
	c.Header("Content-Length", fmt.Sprintf("%d", info.Size))
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", object))

	if _, err := io.Copy(c.Writer, obj); err != nil {
		log.Printf("error streaming object to client: %v", err)
	}
}
