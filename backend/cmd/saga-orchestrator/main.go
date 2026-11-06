package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/distributed-wallet/platform/pkg/database"
	"github.com/distributed-wallet/platform/pkg/outbox"
	"github.com/distributed-wallet/platform/pkg/saga"
	"github.com/distributed-wallet/platform/pkg/telemetry"
	"github.com/distributed-wallet/platform/pkg/webhooks"
	riskv1 "github.com/distributed-wallet/platform/proto/risk/v1"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

	// NATS JetStream
	natsURL := getEnv("NATS_URL", nats.DefaultURL)
	nc, err := nats.Connect(natsURL)
	if err != nil {
		logger.Fatal("failed to connect to nats", zap.Error(err))
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		logger.Fatal("failed to create jetstream context", zap.Error(err))
	}

	// Ensure NATS Stream exists
	if _, err := js.StreamInfo("TRANSFERS"); errors.Is(err, nats.ErrStreamNotFound) {
		if _, err := js.AddStream(&nats.StreamConfig{
			Name:     "TRANSFERS",
			Subjects: []string{"transfer.*.*"},
			Storage:  nats.FileStorage,
		}); err != nil {
			logger.Fatal("failed to create transfers stream", zap.Error(err))
		}
	} else if err != nil {
		logger.Fatal("failed to inspect transfers stream", zap.Error(err))
	}

	// gRPC Risk Client
	riskAddr := getEnv("RISK_SERVICE_ADDR", "localhost:50051")
	conn, err := grpc.Dial(riskAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Fatal("failed to connect to risk service", zap.Error(err))
	}
	defer conn.Close()
	riskClient := riskv1.NewRiskServiceClient(conn)

	// Webhooks Dispatcher
	webhookEncryptionKey, err := webhooks.ParseEncryptionKey(os.Getenv("WEBHOOK_ENCRYPTION_KEY"))
	if err != nil {
		logger.Fatal("WEBHOOK_ENCRYPTION_KEY is invalid", zap.Error(err))
	}
	webhookDispatcher := webhooks.NewDispatcher(pool, logger, webhookEncryptionKey)
	metrics := telemetry.InitMetrics("wallet_platform")

	// Saga Orchestrator
	orchestrator := saga.NewOrchestrator(pool, js, riskClient, webhookDispatcher, logger, metrics)

	// Outbox Relay Worker
	relay := outbox.NewRelay(pool, js, logger, metrics)
	go relay.StartRelay(ctx, 500*time.Millisecond)

	// Webhook Delivery Worker
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				webhookDispatcher.ProcessPendingDeliveries(ctx)
			}
		}
	}()

	// Resume Incomplete Sagas (Crash Recovery on Startup)
	orchestrator.StartRecovery(ctx, 30*time.Second)

	// Start consuming saga events
	if err := orchestrator.StartConsumer(ctx); err != nil {
		logger.Fatal("failed starting saga consumer", zap.Error(err))
	}

	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := pool.Ping(r.Context()); err != nil || !nc.IsConnected() {
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

	logger.Info("Saga Orchestrator and Outbox Relay running successfully")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigChan:
	case err := <-healthErr:
		logger.Error("orchestrator health server failed", zap.Error(err))
	}

	logger.Info("shutting down saga orchestrator...")
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := healthServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("orchestrator health server shutdown failed", zap.Error(err))
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
