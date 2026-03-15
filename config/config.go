package config

import (
	"fmt"
	"os"
	"strconv"
)

type DBConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Name     string
}

func (c DBConfig) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		c.Host, c.Port, c.User, c.Password, c.Name,
	)
}

type SMTPConfig struct {
	Host     string
	Port     int
	User     string
	Password string
}

type AppConfig struct {
	DB                  DBConfig
	SMTP                SMTPConfig
	RecipientEmail      string
	TransactionsFile    string
	AccountID           string
	PipelineTimeoutSecs int
	CheckpointInterval  int // CHECKPOINT_INTERVAL rows between mid-file DB flushes
}

func Load() (AppConfig, error) {
	dbPort, err := strconv.Atoi(getEnv("DB_PORT", "5432"))
	if err != nil {
		return AppConfig{}, fmt.Errorf("config: invalid DB_PORT: %w", err)
	}

	smtpPort, err := strconv.Atoi(getEnv("SMTP_PORT", "587"))
	if err != nil {
		return AppConfig{}, fmt.Errorf("config: invalid SMTP_PORT: %w", err)
	}

	cfg := AppConfig{
		DB: DBConfig{
			Host:     getEnv("DB_HOST", "localhost"),
			Port:     dbPort,
			User:     getEnv("DB_USER", "stori"),
			Password: getEnv("DB_PASSWORD", "stori"),
			Name:     getEnv("DB_NAME", "storidb"),
		},
		SMTP: SMTPConfig{
			Host:     getEnv("SMTP_HOST", "smtp.gmail.com"),
			Port:     smtpPort,
			User:     os.Getenv("SMTP_USER"),
			Password: os.Getenv("SMTP_PASSWORD"),
		},
		RecipientEmail:   os.Getenv("RECIPIENT_EMAIL"),
		TransactionsFile: getEnv("TRANSACTIONS_FILE", "/data/txns.csv"),
		AccountID:           getEnv("ACCOUNT_ID", "ACC-001"),
		PipelineTimeoutSecs: func() int {
			v, _ := strconv.Atoi(getEnv("PIPELINE_TIMEOUT_SECS", "120"))
			return v
		}(),
		CheckpointInterval: func() int {
			v, _ := strconv.Atoi(getEnv("CHECKPOINT_INTERVAL", "100"))
			return v
		}(),
	}

	if err := cfg.validate(); err != nil {
		return AppConfig{}, err
	}

	return cfg, nil
}

// validate checks that all required fields are non-empty after loading.
func (c AppConfig) validate() error {
	required := map[string]string{
		"SMTP_USER":       c.SMTP.User,
		"SMTP_PASSWORD":   c.SMTP.Password,
		"RECIPIENT_EMAIL": c.RecipientEmail,
	}
	for key, val := range required {
		if val == "" {
			return fmt.Errorf("config: required env var %q is not set", key)
		}
	}
	if c.CheckpointInterval <= 0 {
		return fmt.Errorf("config: CHECKPOINT_INTERVAL must be > 0, got %d", c.CheckpointInterval)
	}
	return nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
