-- Migration: 002_add_row_errors
-- Adds row-level error tracking to file_processing and replaces the
-- unused 'pending' status with 'to_review' for files that exceed the
-- max-error threshold and require human intervention before retrying.

ALTER TABLE file_processing
    ADD COLUMN IF NOT EXISTS row_errors JSONB NOT NULL DEFAULT '[]';

ALTER TABLE file_processing
    DROP CONSTRAINT IF EXISTS file_processing_status_check;

ALTER TABLE file_processing
    ADD CONSTRAINT file_processing_status_check
    CHECK (status IN ('processing','done','failed','to_review'));
