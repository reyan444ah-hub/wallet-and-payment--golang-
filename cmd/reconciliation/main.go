package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/distributed-wallet/platform/pkg/database"
	"github.com/distributed-wallet/platform/pkg/reconciliation"
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

	auditor := reconciliation.NewAuditor(pool, logger)

	logger.Info("starting periodic reconciliation auditor daemon...")

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// Run initial audit on boot
	runAudit(ctx, auditor, logger)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-sigChan:
			logger.Info("shutting down reconciliation worker...")
			return
		case <-ticker.C:
			runAudit(ctx, auditor, logger)
		}
	}
}

func runAudit(ctx context.Context, auditor *reconciliation.Auditor, logger *zap.Logger) {
	discrepancies, err := auditor.RunAudit(ctx)
	if err != nil {
		logger.Error("reconciliation audit run failed", zap.Error(err))
		return
	}

	if len(discrepancies) > 0 {
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
