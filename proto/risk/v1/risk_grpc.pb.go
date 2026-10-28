package riskv1

import (
	"context"

	"google.golang.org/grpc"
)

type RiskDecision int32

const (
	RiskDecision_RISK_DECISION_UNSPECIFIED RiskDecision = 0
	RiskDecision_RISK_DECISION_APPROVED    RiskDecision = 1
	RiskDecision_RISK_DECISION_REJECTED    RiskDecision = 2
)

type EvaluateRiskRequest struct {
	TransferId          string `json:"transfer_id,omitempty"`
	OrganizationId      string `json:"organization_id,omitempty"`
	SourceWalletId      string `json:"source_wallet_id,omitempty"`
	DestinationWalletId string `json:"destination_wallet_id,omitempty"`
	Amount              int64  `json:"amount,omitempty"`
	Currency            string `json:"currency,omitempty"`
}

type EvaluateRiskResponse struct {
	Decision  RiskDecision `json:"decision,omitempty"`
	Reason    string       `json:"reason,omitempty"`
	RiskScore int32        `json:"risk_score,omitempty"`
}

type RiskServiceClient interface {
	EvaluateTransferRisk(ctx context.Context, in *EvaluateRiskRequest, opts ...grpc.CallOption) (*EvaluateRiskResponse, error)
}

type riskServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewRiskServiceClient(cc grpc.ClientConnInterface) RiskServiceClient {
	return &riskServiceClient{cc}
}

func (c *riskServiceClient) EvaluateTransferRisk(ctx context.Context, in *EvaluateRiskRequest, opts ...grpc.CallOption) (*EvaluateRiskResponse, error) {
	out := new(EvaluateRiskResponse)
	err := c.cc.Invoke(ctx, "/risk.v1.RiskService/EvaluateTransferRisk", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

type RiskServiceServer interface {
	EvaluateTransferRisk(context.Context, *EvaluateRiskRequest) (*EvaluateRiskResponse, error)
	mustEmbedUnimplementedRiskServiceServer()
}

type UnimplementedRiskServiceServer struct{}

func (UnimplementedRiskServiceServer) EvaluateTransferRisk(context.Context, *EvaluateRiskRequest) (*EvaluateRiskResponse, error) {
	return nil, nil
}
func (UnimplementedRiskServiceServer) mustEmbedUnimplementedRiskServiceServer() {}

func RegisterRiskServiceServer(s grpc.ServiceRegistrar, srv RiskServiceServer) {
	s.RegisterService(&RiskService_ServiceDesc, srv)
}

var RiskService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "risk.v1.RiskService",
	HandlerType: (*RiskServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "EvaluateTransferRisk",
			Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
				in := new(EvaluateRiskRequest)
				if err := dec(in); err != nil {
					return nil, err
				}
				if interceptor == nil {
					return srv.(RiskServiceServer).EvaluateTransferRisk(ctx, in)
				}
				info := &grpc.UnaryServerInfo{
					Server:     srv,
					FullMethod: "/risk.v1.RiskService/EvaluateTransferRisk",
				}
				handler := func(ctx context.Context, req interface{}) (interface{}, error) {
					return srv.(RiskServiceServer).EvaluateTransferRisk(ctx, req.(*EvaluateRiskRequest))
				}
				return interceptor(ctx, in, info, handler)
			},
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/risk/v1/risk.proto",
}
