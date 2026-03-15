package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/luisDiazStgo1994/txn-processor/config"
	"github.com/luisDiazStgo1994/txn-processor/internal/email"
	"github.com/luisDiazStgo1994/txn-processor/internal/orchestrator"
	"github.com/luisDiazStgo1994/txn-processor/internal/parser"
	"github.com/luisDiazStgo1994/txn-processor/internal/storage"
)

func main() {
	if err := run(); err != nil {
		slog.Error("txn-processor failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	// ctx is cancelled on SIGINT/SIGTERM or when the pipeline timeout expires.
	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	timeout := time.Duration(cfg.PipelineTimeoutSecs) * time.Second
	ctx, cancel := context.WithTimeout(sigCtx, timeout)
	defer cancel()

	repo, err := storage.NewPostgresRepository(cfg.DB.DSN())
	if err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	defer func() {
		if err := repo.Close(); err != nil {
			slog.Warn("closing db connection pool", "error", err)
		}
	}()

	sender, err := email.NewEmailSender(cfg.SMTP, "templates/email.html")
	if err != nil {
		return fmt.Errorf("email: %w", err)
	}

	f, err := os.Open(cfg.TransactionsFile)
	if err != nil {
		return fmt.Errorf("open %q: %w", cfg.TransactionsFile, err)
	}
	defer f.Close()

	p := parser.NewCsvParser(f)

	orch := orchestrator.New(repo, sender, cfg.CheckpointInterval)
	if err := orch.Run(ctx, p, cfg.TransactionsFile, cfg.AccountID, cfg.RecipientEmail); err != nil {
		return fmt.Errorf("run: %w", err)
	}

	slog.Info("pipeline complete", "account_id", cfg.AccountID)
	return nil
}
