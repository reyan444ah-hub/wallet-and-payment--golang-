package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/distributed-wallet/platform/pkg/risk"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	portStr := os.Getenv("GRPC_PORT")
	if portStr == "" {
		portStr = "50051"
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		logger.Fatal("GRPC_PORT must be a valid TCP port", zap.Error(err))
	}
	if port < 1 || port > 65535 {
		logger.Fatal("GRPC_PORT must be between 1 and 65535")
	}

	server, lis, err := risk.StartRiskService(port, logger)
	if err != nil {
		logger.Fatal("failed to start gRPC Risk Service", zap.Error(err))
	}
	defer lis.Close()

	fmt.Printf("Risk service running on :%d\n", port)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("shutting down risk service gracefully...")
	server.GracefulStop()
}
