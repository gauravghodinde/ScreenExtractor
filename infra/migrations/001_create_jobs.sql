-- Migration: 001_create_jobs.sql
-- Creates a simple jobs table to track processing state for Scene-Seeker

CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  details JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Optional: update `updated_at` on row change
CREATE OR REPLACE FUNCTION touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_touch_updated_at ON jobs;
CREATE TRIGGER trigger_touch_updated_at
BEFORE UPDATE ON jobs
FOR EACH ROW
EXECUTE FUNCTION touch_updated_at();
