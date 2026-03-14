-- Migration: 001_create_transactions
-- Creates the three core tables for the txn-processor service.

-- Stores account owner information (account_id + contact email).
CREATE TABLE IF NOT EXISTS accounts (
    account_id  TEXT        PRIMARY KEY,
    email       TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tracks the processing lifecycle of each file ingestion run.
-- idempotency_key prevents re-processing the same file (e.g. S3 ETag or SHA256).
-- checkpoint_row allows resuming from the last successfully processed row.
-- heartbeat_at is updated periodically so other workers can detect stale/dead locks.
CREATE TABLE IF NOT EXISTS file_processing (
    idempotency_key TEXT        PRIMARY KEY,
    account_id      TEXT        NOT NULL REFERENCES accounts(account_id),
    status          TEXT        NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending','processing','done','failed')),
    checkpoint_row  INTEGER     NOT NULL DEFAULT 0,
    heartbeat_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Stores the aggregated summary produced after a file is fully processed.
-- summary is JSONB so the payload shape can evolve without schema migrations.
CREATE TABLE IF NOT EXISTS file_summary (
    idempotency_key TEXT        PRIMARY KEY REFERENCES file_processing(idempotency_key),
    account_id      TEXT        NOT NULL REFERENCES accounts(account_id),
    email_sent      BOOLEAN     NOT NULL DEFAULT FALSE,
    summary         JSONB       NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for fast lookup of all summaries belonging to an account.
CREATE INDEX IF NOT EXISTS idx_file_summary_account_id ON file_summary(account_id);
