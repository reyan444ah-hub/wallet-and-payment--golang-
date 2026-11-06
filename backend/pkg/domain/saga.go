package domain

import (
	"time"

	"github.com/google/uuid"
)

type TransferStatus string

const (
	TransferStatusPending    TransferStatus = "PENDING"
	TransferStatusProcessing TransferStatus = "PROCESSING"
	TransferStatusCompleted  TransferStatus = "COMPLETED"
	TransferStatusFailed     TransferStatus = "FAILED"
	TransferStatusReversed   TransferStatus = "REVERSED"
)

type SagaStep string

const (
	StepReserveFunds SagaStep = "RESERVE_FUNDS"
	StepRiskCheck    SagaStep = "RISK_CHECK"
	StepPostLedger   SagaStep = "POST_LEDGER"
	StepReleaseHold  SagaStep = "RELEASE_HOLD"
	StepCompleted    SagaStep = "COMPLETED"
	StepCompensate   SagaStep = "COMPENSATE"
)

type Transfer struct {
	ID                        uuid.UUID      `json:"id"`
	OrganizationID            uuid.UUID      `json:"organization_id"`
	DestinationOrganizationID uuid.UUID      `json:"destination_organization_id"`
	SourceWalletID            uuid.UUID      `json:"source_wallet_id"`
	DestinationWalletID       uuid.UUID      `json:"destination_wallet_id"`
	Currency                  string         `json:"currency"`
	Amount                    int64          `json:"amount"`     // minor units
	FeeAmount                 int64          `json:"fee_amount"` // minor units
	Status                    TransferStatus `json:"status"`
	SagaStep                  SagaStep       `json:"saga_step"`
	FailureReason             string         `json:"failure_reason,omitempty"`
	FencingToken              int64          `json:"fencing_token"`
	IdempotencyKey            string         `json:"idempotency_key"`
	CreatedAt                 time.Time      `json:"created_at"`
	UpdatedAt                 time.Time      `json:"updated_at"`
}

type SagaStepLog struct {
	ID           uuid.UUID `json:"id"`
	TransferID   uuid.UUID `json:"transfer_id"`
	Step         SagaStep  `json:"step"`
	Status       string    `json:"status"` // STARTED, COMPLETED, FAILED, COMPENSATED
	Payload      any       `json:"payload,omitempty"`
	ErrorMessage string    `json:"error_message,omitempty"`
	FencingToken int64     `json:"fencing_token"`
	CreatedAt    time.Time `json:"created_at"`
}
