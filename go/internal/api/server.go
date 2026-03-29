package api

import (
	"github.com/gin-gonic/gin"

	"github.com/gauravghodinde/scenextractor/orchestrator/internal/config"
	"github.com/gauravghodinde/scenextractor/orchestrator/internal/queue"
	"github.com/gauravghodinde/scenextractor/orchestrator/internal/repository"
	"github.com/gauravghodinde/scenextractor/orchestrator/internal/storage"
)

type Server struct {
	router *gin.Engine
	cfg    *config.Config
	repo   *repository.Repository
	q      *queue.Queue
	store  *storage.Storage
}

func NewServer(cfg *config.Config, repo *repository.Repository, q *queue.Queue, store *storage.Storage) *Server {
	s := &Server{
		router: gin.Default(),
		cfg:    cfg,
		repo:   repo,
		q:      q,
		store:  store,
	}
	s.registerRoutes()
	return s
}

func (s *Server) registerRoutes() {
	s.router.GET("/healthz", s.handleHealthz)
	s.router.POST("/jobs", s.handleCreateJob)
	s.router.POST("/upload", s.handleUpload)
	s.router.POST("/clip/search", s.handleSearchClip)
	s.router.GET("/jobs", s.handleListJobs)
	s.router.GET("/jobs/:id/result", s.handleGetJobResult)
}

func (s *Server) Start() error {
	return s.router.Run(":" + s.cfg.Port)
}
