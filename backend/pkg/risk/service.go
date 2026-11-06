package risk

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	riskv1 "github.com/distributed-wallet/platform/proto/risk/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

type Server struct {
	riskv1.UnimplementedRiskServiceServer
	logger       *zap.Logger
	mu           sync.RWMutex
	delay        time.Duration
	failRequests bool
	maxAmount    int64
}

func NewRiskServer(logger *zap.Logger) *Server {
	return &Server{
		logger:    logger,
		maxAmount: 100000000, // 1,000,000.00 minor units
	}
}

func (s *Server) SetSimulatedDelay(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delay = d
}

func (s *Server) SetSimulatedFailure(fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failRequests = fail
}

func (s *Server) EvaluateTransferRisk(ctx context.Context, req *riskv1.EvaluateRiskRequest) (*riskv1.EvaluateRiskResponse, error) {
	s.mu.RLock()
	delay := s.delay
	fail := s.failRequests
	maxAmt := s.maxAmount
	s.mu.RUnlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if fail {
		return nil, fmt.Errorf("risk service temporarily unavailable (simulated outage)")
	}

	s.logger.Info("evaluating transfer risk",
		zap.String("transfer_id", req.TransferId),
		zap.Int64("amount", req.Amount),
		zap.String("currency", req.Currency))

	// Enforce strict limit checks
	if req.Amount > maxAmt {
		return &riskv1.EvaluateRiskResponse{
			Decision:  riskv1.RiskDecision_RISK_DECISION_REJECTED,
			Reason:    "Transfer amount exceeds single-transaction risk ceiling",
			RiskScore: 95,
		}, nil
	}

	// Dynamic risk score calculation
	score := int32(rand.Intn(30)) // low risk default
	return &riskv1.EvaluateRiskResponse{
		Decision:  riskv1.RiskDecision_RISK_DECISION_APPROVED,
		Reason:    "Risk checks passed successfully",
		RiskScore: score,
	}, nil
}

func StartRiskService(port int, logger *zap.Logger) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to listen on port %d: %w", port, err)
	}

	grpcServer := grpc.NewServer()
	server := NewRiskServer(logger)
	riskv1.RegisterRiskServiceServer(grpcServer, server)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	go func() {
		logger.Info("gRPC Risk Service listening", zap.Int("port", port))
		if err := grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			logger.Error("gRPC Risk Service error", zap.Error(err))
		}
	}()

	return grpcServer, lis, nil
}
