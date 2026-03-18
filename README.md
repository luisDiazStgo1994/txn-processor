# txn-processor

A Go service that processes CSV transaction files, computes per-account summaries (balance, monthly averages), persists results in PostgreSQL, and delivers an HTML report via email using the Brevo API.

## Architecture

```
cmd/main.go          → thin entry point (wiring only)
internal/
  parser/            → CSV reader with row-level validation
  aggregator/        → pure-function summary logic with checkpointing
  storage/           → PostgreSQL repository (interface-based, swappable)
  sender/            → email delivery (Brevo REST API + SMTP fallback)
  orchestrator/      → pipeline coordinator (parse → aggregate → persist → email)
config/              → environment-based configuration
migrations/          → SQL schema (auto-runs on first Postgres boot)
templates/           → HTML email template
data/                → sample CSV file
```

### Key Design Decisions

- **Interface-driven DI** — every external dependency (DB, email) is behind an interface, making it easy to swap implementations (e.g., Postgres → DynamoDB) or mock in tests.
- **Pure summary logic** — the aggregator takes transactions in and returns a `Summary` struct. No side effects, fully unit-testable.
- **Checkpointed processing** — large files are processed with periodic DB checkpoints and heartbeat-based lock reclamation, so a crash mid-file resumes from the last checkpoint.
- **Row error tracking** — invalid rows are accumulated and stored as JSONB. If errors exceed a configurable threshold, the file is marked `to_review` for human intervention instead of silently dropping data.
- **Docker-first, Lambda-ready** — `cmd/main.go` reads from local disk; a future `cmd/lambda/main.go` would read from S3 using the same internal packages.
- **Multi-stage Dockerfile** — build stage compiles the binary, run stage uses a minimal Alpine image with no Go toolchain.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)
- A [Brevo](https://www.brevo.com/) account (free tier works) — you need an API key for transactional emails.

## Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/luisDiazStgo1994/txn-processor.git
cd txn-processor
```

### 2. Create your `.env` file

Copy the example below and fill in your values (use .env.example as reference. MUST edit RECIPIENT_EMAIL and BREVO_API_KEY):

```bash
# .env

# Database (defaults work with the docker-compose Postgres)
DB_HOST=localhost
DB_PORT=5432
DB_USER=stori
DB_PASSWORD=stori
DB_NAME=storidb

# Brevo transactional email
BREVO_API_KEY=your-brevo-api-key
BREVO_SENDER_EMAIL=storinotifications@gmail.com
BREVO_SENDER_NAME=Stori

# App settings
RECIPIENT_EMAIL=your-email@example.com
TRANSACTIONS_FILE=/app/data/ACC-001_2025-01_2026-03_txns.csv
ACCOUNT_ID=ACC-001

# Optional tuning (shown with defaults)
# PIPELINE_TIMEOUT_SECS=120
# CHECKPOINT_INTERVAL=100
# HEARTBEAT_TIMEOUT_SECS=20
# MAX_ROW_ERRORS=10
```

> **Note:** `DB_HOST` and `TRANSACTIONS_FILE` are overridden inside the container by `docker-compose.yml`, so the values above are mainly for reference.

### 3. Place your CSV file

The `data/` directory is **not** copied into the Docker image — it is mounted as a volume at runtime (`./data:/app/data`). Place your CSV file in the local `data/` directory and the container will read it directly from there. Expected format:

```csv
Id,Date,Transaction
0,05/01/2025,+60.50
1,08/01/2025,-10.30
2,12/01/2025,-20.46
3,15/01/2025,+10.00
```

- **Date**: `DD/MM/YYYY`
- **Transaction**: `+` = credit, `-` = debit

### 4. Run the project

```bash
docker-compose down -v && docker-compose up --build
```

This will:
1. Start a PostgreSQL 15 container and run the migrations automatically.
2. Build the Go binary in a multi-stage Docker image.
3. Execute the transaction processor, which parses the CSV, stores results, and sends the summary email.

> Use `docker-compose down -v` to wipe the Postgres volume if you need a clean database (e.g., after schema changes). Without `-v`, data persists across restarts.

### 5. Check your email

You should receive an HTML email with the transaction summary at the address you set in `RECIPIENT_EMAIL`.

## Running Tests

Requires **Go 1.22+** installed locally.

```bash
go test ./...
```

## CSV Processing Details

| Scenario | Behavior |
|---|---|
| Valid row | Parsed and included in summary |
| Invalid row (bad date, bad amount, missing fields) | Skipped, logged as a `RowError` |
| Invalid rows ≤ `MAX_ROW_ERRORS` | File completes with status `done`, email includes a note about skipped rows |
| Invalid rows > `MAX_ROW_ERRORS` | File marked as `to_review`, processing stops, no email sent |
| Duplicate file run (same file + account) | Idempotent — resumes from checkpoint, skips email if already sent |
| Crash mid-processing | Heartbeat expires → lock released → next run resumes from last checkpoint |

## Project Structure

| File | Purpose |
|---|---|
| `cmd/main.go` | Entry point — loads config, wires dependencies, runs pipeline |
| `config/config.go` | Reads environment variables with validation |
| `internal/parser/` | CSV parsing with `Parser` interface |
| `internal/aggregator/` | Computes balance, monthly averages, tracks row errors |
| `internal/storage/` | `Repository` interface + Postgres implementation |
| `internal/sender/` | `Sender` interface + Brevo (production) and SMTP (fallback) |
| `internal/orchestrator/` | Coordinates the full pipeline |
| `migrations/` | PostgreSQL DDL (runs on first container boot) |
| `templates/email.html` | HTML email template with Go `text/template` syntax |
