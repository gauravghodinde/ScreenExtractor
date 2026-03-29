package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, pgURL string) (*Repository, error) {
	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		return nil, err
	}
	return &Repository{pool: pool}, nil
}

func (r *Repository) Close() {
	r.pool.Close()
}

func (r *Repository) CreateJob(ctx context.Context, id, status string, details []byte) error {
	ctxPg, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := r.pool.Exec(ctxPg, "INSERT INTO jobs (id, status, details) VALUES ($1, $2, $3)", id, status, details)
	return err
}

type JobRow struct {
	ID        string
	Status    string
	Result    string
	CreatedAt time.Time
}

func (r *Repository) GetJobs(ctx context.Context, status string, limit, offset int) ([]JobRow, error) {
	ctxPg, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var rows pgx.Rows
	var err error
	if status != "" {
		rows, err = r.pool.Query(ctxPg, "SELECT id, status, COALESCE(details->>'result', ''), created_at FROM jobs WHERE status=$1 ORDER BY created_at DESC LIMIT $2 OFFSET $3", status, limit, offset)
	} else {
		rows, err = r.pool.Query(ctxPg, "SELECT id, status, COALESCE(details->>'result', ''), created_at FROM jobs ORDER BY created_at DESC LIMIT $1 OFFSET $2", limit, offset)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []JobRow
	for rows.Next() {
		var j JobRow
		if err := rows.Scan(&j.ID, &j.Status, &j.Result, &j.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, j)
	}
	return result, rows.Err()
}

func (r *Repository) GetJobResultAndVideoPath(ctx context.Context, id string) (videoPath, resultPath string, err error) {
	ctxPg, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var vp, rp *string
	err = r.pool.QueryRow(ctxPg, "SELECT details->>'video_path', details->>'result' FROM jobs WHERE id=$1", id).Scan(&vp, &rp)
	if err != nil {
		return "", "", err
	}
	if vp != nil {
		videoPath = *vp
	}
	if rp != nil {
		resultPath = *rp
	}
	return videoPath, resultPath, nil
}

func (r *Repository) GetJobResult(ctx context.Context, id string) (string, error) {
	ctxPg, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var result *string
	err := r.pool.QueryRow(ctxPg, "SELECT details->>'result' FROM jobs WHERE id=$1", id).Scan(&result)
	if err != nil {
		return "", err
	}
	if result == nil {
		return "", fmt.Errorf("result not available")
	}
	return *result, nil
}
