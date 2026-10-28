package database

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
	SSLMode  string
}

func ConnectPostgres(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database, cfg.SSLMode)

	poolConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("error parsing pgx connection string: %w", err)
	}

	poolConfig.MaxConns = 50
	poolConfig.MinConns = 10
	poolConfig.MaxConnLifetime = 30 * time.Minute
	poolConfig.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("error establishing pgx connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("database ping failed: %w", err)
	}

	return pool, nil
}

func RunMigrations(ctx context.Context, pool *pgxpool.Pool, migrationFilePath string) error {
	content, err := os.ReadFile(migrationFilePath)
	if err != nil {
		return fmt.Errorf("failed to read migration file %s: %w", migrationFilePath, err)
	}

	_, err = pool.Exec(ctx, string(content))
	if err != nil {
		return fmt.Errorf("failed to apply migration: %w", err)
	}
	return nil
}
