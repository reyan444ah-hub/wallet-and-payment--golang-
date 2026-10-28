package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/distributed-wallet/platform/pkg/api"
	"github.com/distributed-wallet/platform/pkg/database"
	"github.com/distributed-wallet/platform/pkg/ratelimit"
	"github.com/distributed-wallet/platform/pkg/telemetry"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// PostgreSQL
	dbHost := getEnv("DB_HOST", "localhost")
	dbPort := getEnv("DB_PORT", "5432")
	dbUser := getEnv("DB_USER", "postgres")
	dbPass := getEnv("DB_PASSWORD", "postgres")
	dbName := getEnv("DB_NAME", "wallet_ledger")

	pool, err := database.ConnectPostgres(ctx, database.Config{
		Host:     dbHost,
		Port:     dbPort,
		User:     dbUser,
		Password: dbPass,
		Database: dbName,
		SSLMode:  "disable",
	})
	if err != nil {
		logger.Fatal("failed to connect to postgres", zap.Error(err))
	}
	defer pool.Close()

	// Auto-run database migrations
	migrationPath := getEnv("MIGRATION_PATH", "migrations/000001_init_schema.sql")
	if err := database.RunMigrations(ctx, pool, migrationPath); err != nil {
		logger.Warn("migration notice (tables may already exist)", zap.Error(err))
	}

	// Redis
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	// Rate limiter (fails open for maximum payment availability if Redis degrades)
	limiter := ratelimit.NewLimiter(rdb, false)

	// Metrics
	metrics := telemetry.InitMetrics("wallet_platform")

	jwtSecret := getEnv("JWT_SECRET", "super-secret-production-grade-key-1234567890")
	server := api.NewServer(pool, limiter, metrics, logger, jwtSecret)

	port := getEnv("PORT", "8080")
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%s", port),
		Handler:      server.Router(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		logger.Info("REST API Server running", zap.String("port", port))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("server listen error", zap.Error(err))
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("shutting down REST API server gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown failed", zap.Error(err))
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
