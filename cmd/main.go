package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/luisDiazStgo1994/txn-processor/config"
	"github.com/luisDiazStgo1994/txn-processor/internal/email"
	"github.com/luisDiazStgo1994/txn-processor/internal/orchestrator"
	"github.com/luisDiazStgo1994/txn-processor/internal/parser"
	"github.com/luisDiazStgo1994/txn-processor/internal/storage"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("txn-processor: %v", err)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	timeout := time.Duration(cfg.PipelineTimeoutSecs) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	repo, err := storage.NewPostgresRepository(cfg.DB.DSN())
	if err != nil {
		return fmt.Errorf("storage: %w", err)
	}

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

	orch := orchestrator.New(repo, sender)
	if err := orch.Run(ctx, p, cfg.TransactionsFile, cfg.AccountID, cfg.RecipientEmail); err != nil {
		return fmt.Errorf("run: %w", err)
	}

	log.Printf("pipeline complete for account %s", cfg.AccountID)
	return nil
}
