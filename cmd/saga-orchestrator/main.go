package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/distributed-wallet/platform/pkg/database"
	"github.com/distributed-wallet/platform/pkg/outbox"
	"github.com/distributed-wallet/platform/pkg/saga"
	"github.com/distributed-wallet/platform/pkg/webhooks"
	riskv1 "github.com/distributed-wallet/platform/proto/risk/v1"
	"github.com/nats-io/nats.go"
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
	_, _ = js.AddStream(&nats.StreamConfig{
		Name:     "TRANSFERS",
		Subjects: []string{"transfer.*.*"},
	})

	// gRPC Risk Client
	riskAddr := getEnv("RISK_SERVICE_ADDR", "localhost:50051")
	conn, err := grpc.Dial(riskAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Fatal("failed to connect to risk service", zap.Error(err))
	}
	defer conn.Close()
	riskClient := riskv1.NewRiskServiceClient(conn)

	// Webhooks Dispatcher
	webhookDispatcher := webhooks.NewDispatcher(pool, logger)

	// Saga Orchestrator
	orchestrator := saga.NewOrchestrator(pool, js, riskClient, webhookDispatcher, logger)

	// Outbox Relay Worker
	relay := outbox.NewRelay(pool, js, logger)
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
	orchestrator.ResumeIncompleteSagas(ctx)

	// Start consuming saga events
	if err := orchestrator.StartConsumer(ctx); err != nil {
		logger.Fatal("failed starting saga consumer", zap.Error(err))
	}

	logger.Info("Saga Orchestrator and Outbox Relay running successfully")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("shutting down saga orchestrator...")
	cancel()
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
