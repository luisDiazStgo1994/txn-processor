package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/luisDiazStgo1994/txn-processor/config"
	"github.com/luisDiazStgo1994/txn-processor/internal/email"
	"github.com/luisDiazStgo1994/txn-processor/internal/orchestrator"
	"github.com/luisDiazStgo1994/txn-processor/internal/storage"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("txn-processor: %v", err)
	}
}

func run() error {
	ctx := context.Background()

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

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

	orch := orchestrator.New(repo, sender)
	if err := orch.Run(ctx, f, cfg.TransactionsFile, cfg.AccountID, cfg.RecipientEmail); err != nil {
		return fmt.Errorf("run: %w", err)
	}

	log.Printf("pipeline complete for account %s", cfg.AccountID)
	return nil
}
