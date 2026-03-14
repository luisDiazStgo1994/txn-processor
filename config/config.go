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
	DB               DBConfig
	SMTP             SMTPConfig
	RecipientEmail   string
	TransactionsFile string
	AccountID        string
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
			User:     mustEnv("SMTP_USER"),
			Password: mustEnv("SMTP_PASSWORD"),
		},
		RecipientEmail:   mustEnv("RECIPIENT_EMAIL"),
		TransactionsFile: getEnv("TRANSACTIONS_FILE", "/data/txns.csv"),
		AccountID:        getEnv("ACCOUNT_ID", "ACC-001"),
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic(fmt.Sprintf("config: required env var %q is not set", key))
	}
	return v
}
