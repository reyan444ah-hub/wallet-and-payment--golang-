package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/distributed-wallet/platform/pkg/database"
	"github.com/distributed-wallet/platform/pkg/reconciliation"
	"github.com/distributed-wallet/platform/pkg/telemetry"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbHost := getEnv("DB_HOST", "localhost")
	dbPort := getEnv("DB_PORT", "5432")
	dbUser := getEnv("DB_USER", "postgres")
	dbPass := os.Getenv("DB_PASSWORD")
	if dbPass == "" {
		logger.Fatal("DB_PASSWORD environment variable is required")
	}
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

	auditor := reconciliation.NewAuditor(pool, logger)
	metrics := telemetry.InitMetrics("wallet_platform")
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := pool.Ping(r.Context()); err != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprintln(w, "ready")
	})
	healthMux.Handle("/metrics", promhttp.Handler())
	healthServer := &http.Server{
		Addr:              ":8081",
		Handler:           healthMux,
		ReadHeaderTimeout: 2 * time.Second,
	}
	healthErr := make(chan error, 1)
	go func() {
		if err := healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			healthErr <- err
		}
	}()
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := healthServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("reconciliation health server shutdown failed", zap.Error(err))
		}
	}()

	logger.Info("starting periodic reconciliation auditor daemon...")

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// Run initial audit on boot
	runAudit(ctx, auditor, logger, metrics)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-sigChan:
			logger.Info("shutting down reconciliation worker...")
			return
		case err := <-healthErr:
			logger.Error("reconciliation health server failed", zap.Error(err))
			return
		case <-ticker.C:
			runAudit(ctx, auditor, logger, metrics)
		}
	}
}

func runAudit(ctx context.Context, auditor *reconciliation.Auditor, logger *zap.Logger, metrics *telemetry.Metrics) {
	discrepancies, err := auditor.RunAudit(ctx)
	if err != nil {
		logger.Error("reconciliation audit run failed", zap.Error(err))
		metrics.ReconciliationErrors.Inc()
		return
	}

	if len(discrepancies) > 0 {
		metrics.ReconciliationErrors.Add(float64(len(discrepancies)))
		logger.Warn("reconciliation detected discrepancies!", zap.Int("count", len(discrepancies)))
		for _, d := range discrepancies {
			logger.Warn("discrepancy detail", zap.String("type", d.Type), zap.String("desc", d.Description))
		}
	} else {
		logger.Info("reconciliation audit completed: all balances match ledger & all journals balance")
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
