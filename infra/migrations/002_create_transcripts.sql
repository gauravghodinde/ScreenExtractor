-- Migration: 002_create_transcripts.sql
-- Creates a transcripts (segments) table to store subtitle/transcript segments
-- Includes a tsvector column for fast full-text search and an optional embedding JSONB

CREATE TABLE IF NOT EXISTS transcripts (
  id TEXT PRIMARY KEY,
  video_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  start_time DOUBLE PRECISION NOT NULL,
  end_time DOUBLE PRECISION NOT NULL,
  text TEXT NOT NULL,
  text_tsv tsvector,
  embedding JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Trigger to keep tsvector in sync
CREATE OR REPLACE FUNCTION transcripts_tsv_trigger() RETURNS trigger AS $$
BEGIN
  NEW.text_tsv := to_tsvector('english', coalesce(NEW.text, ''));
  RETURN NEW;
END
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_transcripts_tsv ON transcripts;
CREATE TRIGGER trigger_transcripts_tsv
BEFORE INSERT OR UPDATE ON transcripts
FOR EACH ROW EXECUTE FUNCTION transcripts_tsv_trigger();

-- GIN index for fast full-text search
CREATE INDEX IF NOT EXISTS idx_transcripts_text_tsv ON transcripts USING GIN (text_tsv);
