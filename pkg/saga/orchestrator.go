package saga

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/distributed-wallet/platform/pkg/domain"
	"github.com/distributed-wallet/platform/pkg/inbox"
	"github.com/distributed-wallet/platform/pkg/webhooks"
	riskv1 "github.com/distributed-wallet/platform/proto/risk/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type Orchestrator struct {
	db         *pgxpool.Pool
	js         nats.JetStreamContext
	riskClient riskv1.RiskServiceClient
	webhook    *webhooks.Dispatcher
	inbox      *inbox.InboxManager
	logger     *zap.Logger
}

func NewOrchestrator(
	db *pgxpool.Pool,
	js nats.JetStreamContext,
	riskClient riskv1.RiskServiceClient,
	webhook *webhooks.Dispatcher,
	logger *zap.Logger,
) *Orchestrator {
	return &Orchestrator{
		db:         db,
		js:         js,
		riskClient: riskClient,
		webhook:    webhook,
		inbox:      inbox.NewInboxManager(db),
		logger:     logger,
	}
}

// StartConsumer listens to transfer saga trigger events
func (o *Orchestrator) StartConsumer(ctx context.Context) error {
	// Durable push consumer on transfers stream
	sub, err := o.js.Subscribe("transfer.created.*", func(msg *nats.Msg) {
		msgID := msg.Header.Get("Nats-Msg-Id")
		if msgID == "" {
			msgID = string(msg.Data)
		}

		// Check inbox deduplication
		tx, err := o.db.Begin(ctx)
		if err == nil {
			dedupErr := o.inbox.CheckAndRecordInTx(ctx, tx, msgID, "saga_orchestrator")
			if dedupErr == inbox.ErrDuplicateMessage {
				_ = tx.Rollback(ctx)
				o.logger.Info("ignoring duplicate message in saga orchestrator", zap.String("msg_id", msgID))
				_ = msg.Ack()
				return
			}
			_ = tx.Commit(ctx)
		}

		transferIDStr := string(msg.Data)
		transferID, parseErr := uuid.Parse(transferIDStr)
		if parseErr != nil {
			o.logger.Error("invalid transfer id in saga message", zap.String("raw", transferIDStr))
			_ = msg.Ack()
			return
		}

		err = o.ExecuteSaga(ctx, transferID)
		if err != nil {
			o.logger.Error("saga execution error", zap.String("transfer_id", transferID.String()), zap.Error(err))
			// JetStream will redeliver according to policy
			_ = msg.Nak()
			return
		}

		_ = msg.Ack()
	}, nats.Durable("saga_orchestrator"), nats.ManualAck())

	if err != nil {
		return fmt.Errorf("failed to subscribe saga orchestrator: %w", err)
	}

	go func() {
		<-ctx.Done()
		_ = sub.Unsubscribe()
	}()

	return nil
}

// ExecuteSaga steps through the state machine idempotently and handles crash recovery
func (o *Orchestrator) ExecuteSaga(ctx context.Context, transferID uuid.UUID) error {
	transfer, err := o.loadTransfer(ctx, transferID)
	if err != nil {
		return err
	}

	if transfer.Status == domain.TransferStatusCompleted ||
		transfer.Status == domain.TransferStatusFailed ||
		transfer.Status == domain.TransferStatusReversed {
		o.logger.Info("saga already in terminal state", zap.String("transfer_id", transferID.String()), zap.String("status", string(transfer.Status)))
		return nil
	}

	o.logger.Info("executing saga step",
		zap.String("transfer_id", transfer.ID.String()),
		zap.String("current_step", string(transfer.SagaStep)))

	// Step 1: Hold has already been placed atomically when transfer was accepted (RESERVE_FUNDS).
	// Step 2: Risk / Limit Check
	if transfer.SagaStep == domain.StepReserveFunds || transfer.SagaStep == domain.StepRiskCheck {
		riskApproved, reason, err := o.performRiskCheck(ctx, transfer)
		if err != nil {
			o.logger.Warn("risk service error or timeout", zap.Error(err))
			return err // Trigger retry
		}

		if !riskApproved {
			o.logger.Warn("risk check rejected transfer", zap.String("reason", reason))
			return o.compensateAndFail(ctx, transfer, fmt.Sprintf("Risk rejected: %s", reason))
		}

		if err := o.updateSagaStep(ctx, transfer.ID, domain.StepPostLedger, transfer.FencingToken); err != nil {
			return err
		}
		transfer.SagaStep = domain.StepPostLedger
		transfer.FencingToken++
	}

	// Step 3: Post Double-Entry Ledger Entries atomically
	if transfer.SagaStep == domain.StepPostLedger {
		if err := o.postDoubleEntryLedger(ctx, transfer); err != nil {
			o.logger.Error("failed posting ledger entries", zap.Error(err))
			return o.compensateAndFail(ctx, transfer, fmt.Sprintf("Ledger post failed: %s", err.Error()))
		}

		if err := o.updateSagaStep(ctx, transfer.ID, domain.StepReleaseHold, transfer.FencingToken); err != nil {
			return err
		}
		transfer.SagaStep = domain.StepReleaseHold
		transfer.FencingToken++
	}

	// Step 4: Release hold and mark completed
	if transfer.SagaStep == domain.StepReleaseHold {
		if err := o.finalizeTransfer(ctx, transfer); err != nil {
			return err
		}
		transfer.FencingToken++
	}

	// Step 5: Webhook delivery
	_ = o.webhook.QueueWebhook(ctx, transfer.OrganizationID, "transfer.completed", map[string]any{
		"transfer_id": transfer.ID,
		"status":      "COMPLETED",
		"amount":      transfer.Amount,
		"currency":    transfer.Currency,
	})

	return nil
}

func (o *Orchestrator) loadTransfer(ctx context.Context, transferID uuid.UUID) (*domain.Transfer, error) {
	var t domain.Transfer
	err := o.db.QueryRow(ctx, `
		SELECT id, organization_id, source_wallet_id, destination_wallet_id, currency, amount, fee_amount,
		       status, saga_step, failure_reason, fencing_token, idempotency_key, created_at, updated_at
		FROM transfers WHERE id = $1
	`, transferID).Scan(
		&t.ID, &t.OrganizationID, &t.SourceWalletID, &t.DestinationWalletID, &t.Currency, &t.Amount, &t.FeeAmount,
		&t.Status, &t.SagaStep, &t.FailureReason, &t.FencingToken, &t.IdempotencyKey, &t.CreatedAt, &t.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func (o *Orchestrator) performRiskCheck(ctx context.Context, t *domain.Transfer) (bool, string, error) {
	callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	resp, err := o.riskClient.EvaluateTransferRisk(callCtx, &riskv1.EvaluateRiskRequest{
		TransferId:          t.ID.String(),
		OrganizationId:      t.OrganizationID.String(),
		SourceWalletId:      t.SourceWalletID.String(),
		DestinationWalletId: t.DestinationWalletID.String(),
		Amount:              t.Amount,
		Currency:            t.Currency,
	})
	if err != nil {
		return false, "", err
	}

	return resp.Decision == riskv1.RiskDecision_RISK_DECISION_APPROVED, resp.Reason, nil
}

// postDoubleEntryLedger creates the balanced journal entries:
// DR: Source Wallet (Total = Amount + Fee)
// CR: Destination Wallet (Amount)
// CR: Platform Fees Account (Fee)
// Total DR == Total CR (Double-Entry Invariant)
func (o *Orchestrator) postDoubleEntryLedger(ctx context.Context, t *domain.Transfer) error {
	tx, err := o.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Fetch system fee wallet for currency
	var feeWalletID uuid.UUID
	err = tx.QueryRow(ctx, `
		SELECT id FROM wallets 
		WHERE organization_id = $1 AND currency = $2 AND account_type = 'fees'
		LIMIT 1
	`, t.OrganizationID, t.Currency).Scan(&feeWalletID)
	if err != nil {
		// Fallback: auto-create fee system wallet if not present
		feeWalletID = uuid.New()
		_, err = tx.Exec(ctx, `
			INSERT INTO wallets (id, organization_id, currency, account_type, balance, hold_balance)
			VALUES ($1, $2, $3, 'fees', 0, 0)
			ON CONFLICT (id) DO NOTHING
		`, feeWalletID, t.OrganizationID, t.Currency)
		if err != nil {
			return fmt.Errorf("failed to ensure fee wallet: %w", err)
		}
	}

	// Idempotency check: if journal entry already exists for this transfer, skip re-posting
	var existingJournalID uuid.UUID
	err = tx.QueryRow(ctx, `
		SELECT id FROM journal_entries 
		WHERE reference_id = $1 AND reference_type = 'transfer'
		LIMIT 1
	`, t.ID).Scan(&existingJournalID)
	if err == nil {
		// Journal already created on previous attempt; commit and return nil
		return tx.Commit(ctx)
	}

	// 1. Create Journal Entry
	journalID := uuid.New()
	_, err = tx.Exec(ctx, `
		INSERT INTO journal_entries (id, organization_id, reference_id, reference_type, currency, description)
		VALUES ($1, $2, $3, 'transfer', $4, $5)
	`, journalID, t.OrganizationID, t.ID, t.Currency, fmt.Sprintf("Transfer %s", t.ID.String()))
	if err != nil {
		return err
	}

	entries := []domain.LedgerEntry{
		{
			ID:             uuid.New(),
			JournalEntryID: journalID,
			WalletID:       t.SourceWalletID,
			Direction:      domain.DirectionDebit,
			Amount:         t.Amount + t.FeeAmount,
		},
		{
			ID:             uuid.New(),
			JournalEntryID: journalID,
			WalletID:       t.DestinationWalletID,
			Direction:      domain.DirectionCredit,
			Amount:         t.Amount,
		},
	}
	if t.FeeAmount > 0 {
		entries = append(entries, domain.LedgerEntry{
			ID:             uuid.New(),
			JournalEntryID: journalID,
			WalletID:       feeWalletID,
			Direction:      domain.DirectionCredit,
			Amount:         t.FeeAmount,
		})
	}

	// Strict verification before DB insertion
	if err := domain.ValidateJournalBalance(entries); err != nil {
		return fmt.Errorf("invariant violation: %w", err)
	}

	// Insert entries into strictly append-only ledger_entries table
	for _, entry := range entries {
		_, err = tx.Exec(ctx, `
			INSERT INTO ledger_entries (id, journal_entry_id, wallet_id, direction, amount)
			VALUES ($1, $2, $3, $4, $5)
		`, entry.ID, entry.JournalEntryID, entry.WalletID, entry.Direction, entry.Amount)
		if err != nil {
			return fmt.Errorf("failed inserting ledger entry: %w", err)
		}
	}

	// Update cached running balances with deterministic lock ordering by UUID to prevent deadlocks
	firstID, secondID := t.SourceWalletID, t.DestinationWalletID
	if firstID.String() > secondID.String() {
		firstID, secondID = secondID, firstID
	}

	_, _ = tx.Exec(ctx, `SELECT id FROM wallets WHERE id IN ($1, $2) ORDER BY id FOR UPDATE`, firstID, secondID)

	// Update Source: balance decreases by (amount + fee), hold decreases by (amount + fee)
	_, err = tx.Exec(ctx, `
		UPDATE wallets 
		SET balance = balance - $1, hold_balance = hold_balance - $1, updated_at = NOW()
		WHERE id = $2
	`, t.Amount+t.FeeAmount, t.SourceWalletID)
	if err != nil {
		return err
	}

	// Update Destination: balance increases by amount
	_, err = tx.Exec(ctx, `
		UPDATE wallets 
		SET balance = balance + $1, updated_at = NOW()
		WHERE id = $2
	`, t.Amount, t.DestinationWalletID)
	if err != nil {
		return err
	}

	// Update Fees Account if applicable
	if t.FeeAmount > 0 {
		_, err = tx.Exec(ctx, `
			UPDATE wallets 
			SET balance = balance + $1, updated_at = NOW()
			WHERE id = $2
		`, t.FeeAmount, feeWalletID)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

func (o *Orchestrator) finalizeTransfer(ctx context.Context, t *domain.Transfer) error {
	res, err := o.db.Exec(ctx, `
		UPDATE transfers
		SET status = 'COMPLETED', saga_step = 'COMPLETED', fencing_token = fencing_token + 1, updated_at = NOW()
		WHERE id = $1 AND fencing_token = $2
	`, t.ID, t.FencingToken)
	if err != nil {
		return err
	}
	if res.RowsAffected() == 0 {
		return errors.New("fencing token mismatch: transfer updated by another worker")
	}
	return nil
}

// compensateAndFail implements compensation: releases fund hold on source wallet and marks transfer FAILED
func (o *Orchestrator) compensateAndFail(ctx context.Context, t *domain.Transfer, reason string) error {
	tx, err := o.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Release hold on source wallet
	_, err = tx.Exec(ctx, `
		UPDATE wallets
		SET hold_balance = GREATEST(0, hold_balance - $1), updated_at = NOW()
		WHERE id = $2
	`, t.Amount+t.FeeAmount, t.SourceWalletID)
	if err != nil {
		return err
	}

	// Update transfer status
	_, err = tx.Exec(ctx, `
		UPDATE transfers
		SET status = 'FAILED', saga_step = 'COMPENSATE', failure_reason = $1, fencing_token = fencing_token + 1, updated_at = NOW()
		WHERE id = $2
	`, reason, t.ID)
	if err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	// Webhook notification for failure
	_ = o.webhook.QueueWebhook(ctx, t.OrganizationID, "transfer.failed", map[string]any{
		"transfer_id":    t.ID,
		"status":         "FAILED",
		"failure_reason": reason,
	})

	return nil
}

func (o *Orchestrator) updateSagaStep(ctx context.Context, transferID uuid.UUID, nextStep domain.SagaStep, fencingToken int64) error {
	res, err := o.db.Exec(ctx, `
		UPDATE transfers
		SET saga_step = $1, fencing_token = fencing_token + 1, updated_at = NOW()
		WHERE id = $2 AND fencing_token = $3
	`, nextStep, transferID, fencingToken)
	if err != nil {
		return err
	}
	if res.RowsAffected() == 0 {
		return errors.New("fencing token conflict during saga step update")
	}

	_, _ = o.db.Exec(ctx, `
		INSERT INTO saga_step_logs (transfer_id, step, status, fencing_token)
		VALUES ($1, $2, 'COMPLETED', $3)
	`, transferID, nextStep, fencingToken+1)

	return nil
}

// ResumeIncompleteSagas sweeps database for stuck or interrupted sagas on service startup
func (o *Orchestrator) ResumeIncompleteSagas(ctx context.Context) {
	rows, err := o.db.Query(ctx, `
		SELECT id FROM transfers 
		WHERE status IN ('PENDING', 'PROCESSING') 
		  AND updated_at < NOW() - INTERVAL '30 seconds'
		ORDER BY created_at ASC
		LIMIT 100
	`)
	if err != nil {
		o.logger.Error("failed querying incomplete sagas", zap.Error(err))
		return
	}
	defer rows.Close()

	var stuckIDs []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err == nil {
			stuckIDs = append(stuckIDs, id)
		}
	}

	for _, id := range stuckIDs {
		o.logger.Info("resuming interrupted saga", zap.String("transfer_id", id.String()))
		go func(tid uuid.UUID) {
			_ = o.ExecuteSaga(context.Background(), tid)
		}(id)
	}
}
