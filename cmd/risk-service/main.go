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
		port = 50051
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
