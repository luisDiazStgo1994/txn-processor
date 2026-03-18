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

type BrevoConfig struct {
	APIKey      string
	SenderEmail string
	SenderName  string
}

type AppConfig struct {
	DB                   DBConfig
	Brevo                BrevoConfig
	EmailTemplatePath    string
	RecipientEmail       string
	TransactionsFile     string
	AccountID            string
	PipelineTimeoutSecs  int
	CheckpointInterval   int // CHECKPOINT_INTERVAL rows between mid-file DB flushes
	HeartbeatTimeoutSecs int // HEARTBEAT_TIMEOUT_SECS before a stale lock is reclaimed
	MaxRowErrors         int // MAX_ROW_ERRORS threshold before marking file as to_review
}

func Load() (AppConfig, error) {
	dbPort, err := strconv.Atoi(getEnv("DB_PORT", "5432"))
	if err != nil {
		return AppConfig{}, fmt.Errorf("config: invalid DB_PORT: %w", err)
	}

	cfg := AppConfig{
		DB: DBConfig{
			Host:     getEnv("DB_HOST", "localhost"),
			Port:     dbPort,
			User:     getEnv("DB_USER", "stori"),
			Password: getEnv("DB_PASSWORD", "stori"),
			Name:     getEnv("DB_NAME", "storidb"),
		},
		Brevo: BrevoConfig{
			APIKey:      os.Getenv("BREVO_API_KEY"),
			SenderEmail: getEnv("BREVO_SENDER_EMAIL", "storinotifications@gmail.com"),
			SenderName:  getEnv("BREVO_SENDER_NAME", "noreply"),
		},
		RecipientEmail:    os.Getenv("RECIPIENT_EMAIL"),
		TransactionsFile:  os.Getenv("TRANSACTIONS_FILE"),
		AccountID:         os.Getenv("ACCOUNT_ID"),
		EmailTemplatePath: getEnv("EMAIL_TEMPLATE_PATH", "templates/email.html"),
		PipelineTimeoutSecs: func() int {
			v, _ := strconv.Atoi(getEnv("PIPELINE_TIMEOUT_SECS", "120"))
			return v
		}(),
		CheckpointInterval: func() int {
			v, _ := strconv.Atoi(getEnv("CHECKPOINT_INTERVAL", "100"))
			return v
		}(),
		HeartbeatTimeoutSecs: func() int {
			v, _ := strconv.Atoi(getEnv("HEARTBEAT_TIMEOUT_SECS", "20"))
			return v
		}(),
		MaxRowErrors: func() int {
			v, _ := strconv.Atoi(getEnv("MAX_ROW_ERRORS", "10"))
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
		"BREVO_API_KEY":     c.Brevo.APIKey,
		"RECIPIENT_EMAIL":   c.RecipientEmail,
		"TRANSACTIONS_FILE": c.TransactionsFile,
		"ACCOUNT_ID":        c.AccountID,
	}
	for key, val := range required {
		if val == "" {
			return fmt.Errorf("config: required env var %q is not set", key)
		}
	}
	if c.CheckpointInterval <= 0 {
		return fmt.Errorf("config: CHECKPOINT_INTERVAL must be > 0, got %d", c.CheckpointInterval)
	}
	if c.HeartbeatTimeoutSecs <= 0 {
		return fmt.Errorf("config: HEARTBEAT_TIMEOUT_SECS must be > 0, got %d", c.HeartbeatTimeoutSecs)
	}
	if c.PipelineTimeoutSecs <= 0 {
		return fmt.Errorf("config: PIPELINE_TIMEOUT_SECS must be > 0, got %d", c.PipelineTimeoutSecs)
	}
	if c.MaxRowErrors < 0 {
		return fmt.Errorf("config: MAX_ROW_ERRORS must be >= 0, got %d", c.MaxRowErrors)
	}
	return nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
