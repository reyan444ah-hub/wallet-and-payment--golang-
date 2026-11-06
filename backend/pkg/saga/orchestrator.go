package saga

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/distributed-wallet/platform/pkg/domain"
	"github.com/distributed-wallet/platform/pkg/inbox"
	"github.com/distributed-wallet/platform/pkg/telemetry"
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
	metrics    *telemetry.Metrics
}

func NewOrchestrator(
	db *pgxpool.Pool,
	js nats.JetStreamContext,
	riskClient riskv1.RiskServiceClient,
	webhook *webhooks.Dispatcher,
	logger *zap.Logger,
	metrics *telemetry.Metrics,
) *Orchestrator {
	return &Orchestrator{
		db:         db,
		js:         js,
		riskClient: riskClient,
		webhook:    webhook,
		inbox:      inbox.NewInboxManager(db),
		logger:     logger,
		metrics:    metrics,
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
		messageCtx := ctx
		if correlationID := msg.Header.Get("X-Correlation-ID"); correlationID != "" {
			messageCtx = telemetry.ContextWithCorrelationID(ctx, correlationID)
			o.logger.Info("received transfer saga event",
				zap.String("transfer_id", string(msg.Data)),
				zap.String("correlation_id", correlationID))
		}

		transferIDStr := string(msg.Data)
		var decodedTransferID string
		if err := json.Unmarshal(msg.Data, &decodedTransferID); err == nil {
			transferIDStr = decodedTransferID
		}
		transferID, parseErr := uuid.Parse(transferIDStr)
		if parseErr != nil {
			o.logger.Error("invalid transfer id in saga message", zap.String("subject", msg.Subject))
			if err := o.recordDeadLetter(messageCtx, msg, msgID, "invalid transfer ID"); err != nil {
				o.logger.Error("failed to persist invalid saga message to dead-letter store", zap.Error(err))
				nakSagaMessageWithBackoff(msg)
				return
			}
			_ = msg.Term()
			return
		}

		if err := o.ExecuteSaga(messageCtx, transferID); err != nil {
			o.logger.Error("saga execution error", zap.String("transfer_id", transferID.String()), zap.Error(err))
			metadata, metadataErr := msg.Metadata()
			if metadataErr == nil && metadata.NumDelivered >= 5 {
				if dlqErr := o.recordDeadLetter(messageCtx, msg, msgID, err.Error()); dlqErr == nil {
					_ = msg.Term()
					return
				} else {
					o.logger.Error("failed to persist saga message to dead-letter store", zap.Error(dlqErr))
				}
			}
			nakSagaMessageWithBackoff(msg)
			return
		}

		tx, err := o.db.Begin(messageCtx)
		if err != nil {
			o.logger.Error("failed to start inbox transaction", zap.Error(err))
			nakSagaMessageWithBackoff(msg)
			return
		}
		dedupErr := o.inbox.CheckAndRecordInTx(messageCtx, tx, msgID, "saga_orchestrator")
		if dedupErr != nil && dedupErr != inbox.ErrDuplicateMessage {
			_ = tx.Rollback(messageCtx)
			o.logger.Error("failed to record processed saga message", zap.Error(dedupErr))
			nakSagaMessageWithBackoff(msg)
			return
		}
		if err := tx.Commit(messageCtx); err != nil {
			o.logger.Error("failed to commit processed saga message", zap.Error(err))
			nakSagaMessageWithBackoff(msg)
			return
		}
		if dedupErr == inbox.ErrDuplicateMessage {
			o.logger.Info("saga message was concurrently completed", zap.String("msg_id", msgID))
		}
		if err := msg.Ack(); err != nil {
			o.logger.Error("failed acknowledging saga message", zap.Error(err))
		}
	}, nats.Durable("saga_orchestrator"), nats.ManualAck(), nats.AckWait(15*time.Second))

	if err != nil {
		return fmt.Errorf("failed to subscribe saga orchestrator: %w", err)
	}

	go func() {
		<-ctx.Done()
		_ = sub.Unsubscribe()
	}()

	return nil
}

func sagaRetryDelay(deliveryCount uint64) time.Duration {
	const (
		baseDelay = time.Second
		maxDelay  = 30 * time.Second
	)
	if deliveryCount == 0 {
		deliveryCount = 1
	}
	exponent := min(deliveryCount-1, uint64(5))
	delay := baseDelay * time.Duration(uint64(1)<<exponent)
	if delay > maxDelay {
		delay = maxDelay
	}
	return delay + time.Duration(rand.Int63n(int64(delay/2)+1))
}

func nakSagaMessageWithBackoff(msg *nats.Msg) {
	deliveryCount := uint64(1)
	if metadata, err := msg.Metadata(); err == nil {
		deliveryCount = metadata.NumDelivered
	}
	_ = msg.NakWithDelay(sagaRetryDelay(deliveryCount))
}

func (o *Orchestrator) recordDeadLetter(ctx context.Context, msg *nats.Msg, messageID, reason string) error {
	metadata, err := msg.Metadata()
	if err != nil {
		return fmt.Errorf("read dead-letter delivery metadata: %w", err)
	}
	tx, err := o.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin dead-letter transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
		INSERT INTO dead_letter_messages
			(source, message_id, subject, payload, delivery_count, error_message)
		VALUES ('saga_orchestrator', $1, $2, $3, $4, $5)
		ON CONFLICT (source, message_id) DO NOTHING
	`, messageID, msg.Subject, msg.Data, metadata.NumDelivered, reason); err != nil {
		return fmt.Errorf("store dead-letter message: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit dead-letter message: %w", err)
	}
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
		return o.queueTerminalWebhook(ctx, transfer)
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

		if err := o.updateSagaStep(ctx, transfer.ID, transfer.SagaStep, domain.StepPostLedger, transfer.FencingToken); err != nil {
			return err
		}
		transfer.SagaStep = domain.StepPostLedger
		transfer.FencingToken++
	}

	// Step 3: Post Double-Entry Ledger Entries atomically
	if transfer.SagaStep == domain.StepPostLedger {
		if err := o.postDoubleEntryLedger(ctx, transfer); err != nil {
			o.logger.Error("failed posting ledger entries", zap.Error(err))
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
		transfer.Status = domain.TransferStatusCompleted
		transfer.SagaStep = domain.StepCompleted
		transfer.FencingToken++
	}

	return o.queueTerminalWebhook(ctx, transfer)
}

func (o *Orchestrator) queueTerminalWebhook(ctx context.Context, transfer *domain.Transfer) error {
	organizations := []uuid.UUID{transfer.OrganizationID}
	if transfer.DestinationOrganizationID != uuid.Nil &&
		transfer.DestinationOrganizationID != transfer.OrganizationID {
		organizations = append(organizations, transfer.DestinationOrganizationID)
	}
	switch transfer.Status {
	case domain.TransferStatusCompleted:
		payload := map[string]any{
			"transfer_id": transfer.ID,
			"status":      "COMPLETED",
			"amount":      transfer.Amount,
			"currency":    transfer.Currency,
		}
		for _, organizationID := range organizations {
			if err := o.webhook.QueueWebhook(ctx, organizationID, "transfer.completed", payload); err != nil {
				return err
			}
		}
	case domain.TransferStatusFailed:
		payload := map[string]any{
			"transfer_id":    transfer.ID,
			"status":         "FAILED",
			"failure_reason": transfer.FailureReason,
		}
		for _, organizationID := range organizations {
			if err := o.webhook.QueueWebhook(ctx, organizationID, "transfer.failed", payload); err != nil {
				return err
			}
		}
	}
	return nil
}

func (o *Orchestrator) loadTransfer(ctx context.Context, transferID uuid.UUID) (*domain.Transfer, error) {
	var t domain.Transfer
	err := o.db.QueryRow(ctx, `
		SELECT id, organization_id, destination_organization_id, source_wallet_id, destination_wallet_id, currency, amount, fee_amount,
		       status, saga_step, COALESCE(failure_reason, ''), fencing_token, idempotency_key, created_at, updated_at
		FROM transfers WHERE id = $1
	`, transferID).Scan(
		&t.ID, &t.OrganizationID, &t.DestinationOrganizationID, &t.SourceWalletID, &t.DestinationWalletID, &t.Currency, &t.Amount, &t.FeeAmount,
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
		TransferId:                t.ID.String(),
		OrganizationId:            t.OrganizationID.String(),
		SourceWalletId:            t.SourceWalletID.String(),
		DestinationWalletId:       t.DestinationWalletID.String(),
		Amount:                    t.Amount,
		Currency:                  t.Currency,
		DestinationOrganizationId: t.DestinationOrganizationID.String(),
	})
	if err != nil {
		return false, "", err
	}

	return resp.Decision == riskv1.RiskDecision_RISK_DECISION_APPROVED, resp.Reason, nil
}

func (o *Orchestrator) postDoubleEntryLedger(ctx context.Context, t *domain.Transfer) error {
	tx, err := o.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var currentStatus domain.TransferStatus
	var currentStep domain.SagaStep
	var currentToken int64
	var destinationOrganizationID uuid.UUID
	err = tx.QueryRow(ctx, `
		SELECT status, saga_step, fencing_token, destination_organization_id
		FROM transfers WHERE id = $1 FOR UPDATE
	`, t.ID).Scan(&currentStatus, &currentStep, &currentToken, &destinationOrganizationID)
	if err != nil {
		return fmt.Errorf("lock transfer for ledger posting: %w", err)
	}
	if currentStatus != domain.TransferStatusPending && currentStatus != domain.TransferStatusProcessing {
		return fmt.Errorf("transfer is no longer active: %s", currentStatus)
	}
	if currentStep != domain.StepPostLedger || currentToken != t.FencingToken {
		return errors.New("stale saga worker cannot post transfer ledger")
	}
	if destinationOrganizationID != t.DestinationOrganizationID {
		return errors.New("transfer destination organization changed")
	}
	if t.Amount <= 0 || t.FeeAmount < 0 || t.Amount > math.MaxInt64-t.FeeAmount {
		return errors.New("transfer amount is outside the supported range")
	}

	crossOrganization := t.OrganizationID != destinationOrganizationID
	var feeWalletID, sourceClearingWalletID, destinationClearingWalletID uuid.UUID
	if t.FeeAmount > 0 {
		if _, err := tx.Exec(ctx, `
			INSERT INTO wallets (organization_id, currency, account_type, balance, hold_balance)
			VALUES ($1, $2, 'fees', 0, 0)
			ON CONFLICT (organization_id, currency, account_type)
			WHERE account_type IN ('external_funding', 'fees', 'suspense', 'inter_org_clearing')
			DO NOTHING
		`, t.OrganizationID, t.Currency); err != nil {
			return fmt.Errorf("ensure fee wallet: %w", err)
		}
		if err := tx.QueryRow(ctx, `
			SELECT id FROM wallets
			WHERE organization_id = $1 AND currency = $2 AND account_type = 'fees'
		`, t.OrganizationID, t.Currency).Scan(&feeWalletID); err != nil {
			return fmt.Errorf("load fee wallet: %w", err)
		}
	}
	if crossOrganization {
		clearingOrganizations := []uuid.UUID{t.OrganizationID, destinationOrganizationID}
		sort.Slice(clearingOrganizations, func(i, j int) bool {
			return clearingOrganizations[i].String() < clearingOrganizations[j].String()
		})
		for _, organizationID := range clearingOrganizations {
			if _, err := tx.Exec(ctx, `
				INSERT INTO wallets (organization_id, currency, account_type, balance, hold_balance)
				VALUES ($1, $2, 'inter_org_clearing', 0, 0)
				ON CONFLICT (organization_id, currency, account_type)
				WHERE account_type IN ('external_funding', 'fees', 'suspense', 'inter_org_clearing')
				DO NOTHING
			`, organizationID, t.Currency); err != nil {
				return fmt.Errorf("ensure inter-organization clearing wallet: %w", err)
			}
		}
		if err := tx.QueryRow(ctx, `
			SELECT id FROM wallets
			WHERE organization_id = $1 AND currency = $2 AND account_type = 'inter_org_clearing'
		`, t.OrganizationID, t.Currency).Scan(&sourceClearingWalletID); err != nil {
			return fmt.Errorf("load sender clearing wallet: %w", err)
		}
		if err := tx.QueryRow(ctx, `
			SELECT id FROM wallets
			WHERE organization_id = $1 AND currency = $2 AND account_type = 'inter_org_clearing'
		`, destinationOrganizationID, t.Currency).Scan(&destinationClearingWalletID); err != nil {
			return fmt.Errorf("load recipient clearing wallet: %w", err)
		}
	}

	walletIDs := []uuid.UUID{t.SourceWalletID, t.DestinationWalletID}
	if t.FeeAmount > 0 {
		walletIDs = append(walletIDs, feeWalletID)
	}
	if crossOrganization {
		walletIDs = append(walletIDs, sourceClearingWalletID, destinationClearingWalletID)
	}
	type lockedWallet struct {
		organizationID                uuid.UUID
		currency, status, accountType string
	}
	locked := make(map[uuid.UUID]lockedWallet, len(walletIDs))
	walletRows, err := tx.Query(ctx, `
		SELECT id, organization_id, currency, status, account_type
		FROM wallets
		WHERE id = ANY($1)
		ORDER BY id
		FOR UPDATE
	`, walletIDs)
	if err != nil {
		return fmt.Errorf("lock transfer wallets: %w", err)
	}
	for walletRows.Next() {
		var id uuid.UUID
		var wallet lockedWallet
		if err := walletRows.Scan(&id, &wallet.organizationID, &wallet.currency, &wallet.status, &wallet.accountType); err != nil {
			walletRows.Close()
			return fmt.Errorf("scan transfer wallet: %w", err)
		}
		locked[id] = wallet
	}
	if err := walletRows.Err(); err != nil {
		walletRows.Close()
		return fmt.Errorf("iterate transfer wallets: %w", err)
	}
	walletRows.Close()
	if len(locked) != len(walletIDs) {
		return errors.New("transfer references a missing wallet")
	}
	for _, id := range []uuid.UUID{t.SourceWalletID, t.DestinationWalletID} {
		wallet := locked[id]
		if wallet.currency != t.Currency || wallet.accountType != string(domain.AccountCustomer) ||
			wallet.status != string(domain.WalletStatusActive) {
			return errors.New("transfer wallets must be active customer accounts in the transfer currency")
		}
	}
	if locked[t.SourceWalletID].organizationID != t.OrganizationID ||
		locked[t.DestinationWalletID].organizationID != destinationOrganizationID {
		return errors.New("transfer wallet organization does not match the recorded transfer")
	}
	if t.FeeAmount > 0 && (locked[feeWalletID].currency != t.Currency ||
		locked[feeWalletID].accountType != string(domain.AccountFees) ||
		locked[feeWalletID].organizationID != t.OrganizationID) {
		return errors.New("fee wallet does not match the transfer currency")
	}
	if crossOrganization {
		for id, expectedOrganization := range map[uuid.UUID]uuid.UUID{
			sourceClearingWalletID:      t.OrganizationID,
			destinationClearingWalletID: destinationOrganizationID,
		} {
			wallet := locked[id]
			if wallet.organizationID != expectedOrganization || wallet.currency != t.Currency ||
				wallet.accountType != string(domain.AccountInterOrgClearing) {
				return errors.New("inter-organization clearing wallet does not match its organization and currency")
			}
		}
	}

	type journalPosting struct {
		organizationID uuid.UUID
		description    string
		entries        []domain.LedgerEntry
	}
	makeEntry := func(walletID uuid.UUID, direction domain.Direction, amount int64) domain.LedgerEntry {
		return domain.LedgerEntry{ID: uuid.New(), WalletID: walletID, Direction: direction, Amount: amount}
	}
	postings := []journalPosting{}
	if crossOrganization {
		senderEntries := []domain.LedgerEntry{
			makeEntry(t.SourceWalletID, domain.DirectionDebit, t.Amount+t.FeeAmount),
			makeEntry(sourceClearingWalletID, domain.DirectionCredit, t.Amount),
		}
		if t.FeeAmount > 0 {
			senderEntries = append(senderEntries, makeEntry(feeWalletID, domain.DirectionCredit, t.FeeAmount))
		}
		receiverEntries := []domain.LedgerEntry{
			makeEntry(destinationClearingWalletID, domain.DirectionDebit, t.Amount),
			makeEntry(t.DestinationWalletID, domain.DirectionCredit, t.Amount),
		}
		postings = append(postings,
			journalPosting{t.OrganizationID, "Cross-organization transfer sender side " + t.ID.String(), senderEntries},
			journalPosting{destinationOrganizationID, "Cross-organization transfer recipient side " + t.ID.String(), receiverEntries},
		)
	} else {
		entries := []domain.LedgerEntry{
			makeEntry(t.SourceWalletID, domain.DirectionDebit, t.Amount+t.FeeAmount),
			makeEntry(t.DestinationWalletID, domain.DirectionCredit, t.Amount),
		}
		if t.FeeAmount > 0 {
			entries = append(entries, makeEntry(feeWalletID, domain.DirectionCredit, t.FeeAmount))
		}
		postings = append(postings, journalPosting{t.OrganizationID, "Transfer " + t.ID.String(), entries})
	}

	for _, posting := range postings {
		if err := domain.ValidateJournalBalance(posting.entries); err != nil {
			return fmt.Errorf("invariant violation: %w", err)
		}
		journalID := uuid.New()
		if _, err := tx.Exec(ctx, `
			INSERT INTO journal_entries (id, organization_id, reference_id, reference_type, currency, description)
			VALUES ($1, $2, $3, 'transfer', $4, $5)
		`, journalID, posting.organizationID, t.ID, t.Currency, posting.description); err != nil {
			return fmt.Errorf("create transfer journal: %w", err)
		}
		for _, entry := range posting.entries {
			if _, err := tx.Exec(ctx, `
				INSERT INTO ledger_entries (id, journal_entry_id, wallet_id, direction, amount)
				VALUES ($1, $2, $3, $4, $5)
			`, entry.ID, journalID, entry.WalletID, entry.Direction, entry.Amount); err != nil {
				return fmt.Errorf("insert transfer ledger entry: %w", err)
			}
		}
	}

	result, err := tx.Exec(ctx, `
		UPDATE wallets 
		SET balance = balance - $1, hold_balance = hold_balance - $1, updated_at = NOW()
		WHERE id = $2 AND balance >= $1 AND hold_balance >= $1
	`, t.Amount+t.FeeAmount, t.SourceWalletID)
	if err != nil {
		return fmt.Errorf("debit source wallet and release hold: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("source wallet balance or hold is insufficient")
	}

	result, err = tx.Exec(ctx, `
		UPDATE wallets 
		SET balance = balance + $1, updated_at = NOW()
		WHERE id = $2
	`, t.Amount, t.DestinationWalletID)
	if err != nil {
		return fmt.Errorf("credit destination wallet: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("destination wallet is missing")
	}

	if t.FeeAmount > 0 {
		result, err = tx.Exec(ctx, `
			UPDATE wallets 
			SET balance = balance + $1, updated_at = NOW()
			WHERE id = $2
		`, t.FeeAmount, feeWalletID)
		if err != nil {
			return fmt.Errorf("credit fee wallet: %w", err)
		}
		if result.RowsAffected() != 1 {
			return errors.New("fee wallet is missing")
		}
	}
	if crossOrganization {
		result, err = tx.Exec(ctx, `
			UPDATE wallets SET balance = balance + $1, updated_at = NOW()
			WHERE id = $2 AND organization_id = $3
		`, t.Amount, sourceClearingWalletID, t.OrganizationID)
		if err != nil || result.RowsAffected() != 1 {
			return errors.New("failed to credit sender clearing wallet")
		}
		result, err = tx.Exec(ctx, `
			UPDATE wallets SET balance = balance - $1, updated_at = NOW()
			WHERE id = $2 AND organization_id = $3
		`, t.Amount, destinationClearingWalletID, destinationOrganizationID)
		if err != nil || result.RowsAffected() != 1 {
			return errors.New("failed to debit recipient clearing wallet")
		}
	}

	result, err = tx.Exec(ctx, `
		UPDATE transfers
		SET status = 'PROCESSING', saga_step = 'RELEASE_HOLD',
		    fencing_token = fencing_token + 1, updated_at = NOW()
		WHERE id = $1 AND fencing_token = $2 AND saga_step = 'POST_LEDGER'
		  AND status IN ('PENDING', 'PROCESSING')
	`, t.ID, t.FencingToken)
	if err != nil || result.RowsAffected() != 1 {
		return errors.New("fencing token conflict while posting transfer ledger")
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO saga_step_logs (transfer_id, step, status, fencing_token)
		VALUES ($1, $2, 'COMPLETED', $3)
	`, t.ID, domain.StepReleaseHold, t.FencingToken+1); err != nil {
		return fmt.Errorf("record ledger saga step: %w", err)
	}

	return tx.Commit(ctx)
}

func (o *Orchestrator) finalizeTransfer(ctx context.Context, t *domain.Transfer) error {
	tx, err := o.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transfer finalization: %w", err)
	}
	defer tx.Rollback(ctx)

	res, err := tx.Exec(ctx, `
		UPDATE transfers
		SET status = 'COMPLETED', saga_step = 'COMPLETED', fencing_token = fencing_token + 1, updated_at = NOW()
		WHERE id = $1 AND fencing_token = $2 AND status = 'PROCESSING' AND saga_step = 'RELEASE_HOLD'
	`, t.ID, t.FencingToken)
	if err != nil {
		return err
	}
	if res.RowsAffected() != 1 {
		return errors.New("fencing token mismatch: transfer updated by another worker")
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO saga_step_logs (transfer_id, step, status, fencing_token)
		VALUES ($1, $2, 'COMPLETED', $3)
	`, t.ID, domain.StepCompleted, t.FencingToken+1); err != nil {
		return fmt.Errorf("record transfer completion: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transfer completion: %w", err)
	}
	o.metrics.TransfersTotal.WithLabelValues(t.Currency, string(domain.TransferStatusCompleted)).Inc()
	o.metrics.SagaDurationSeconds.WithLabelValues(string(domain.TransferStatusCompleted)).Observe(time.Since(t.CreatedAt).Seconds())
	return nil
}

// compensateAndFail implements compensation: releases fund hold on source wallet and marks transfer FAILED
func (o *Orchestrator) compensateAndFail(ctx context.Context, t *domain.Transfer, reason string) error {
	tx, err := o.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var status domain.TransferStatus
	var failureReason *string
	var currentToken int64
	err = tx.QueryRow(ctx, `
	SELECT status, failure_reason, fencing_token FROM transfers WHERE id = $1 FOR UPDATE
	`, t.ID).Scan(&status, &failureReason, &currentToken)
	if err != nil {
		return fmt.Errorf("lock transfer for compensation: %w", err)
	}
	if status == domain.TransferStatusFailed {
		t.Status = status
		if failureReason != nil {
			t.FailureReason = *failureReason
		}
		return o.queueTerminalWebhook(ctx, t)
	}
	if status == domain.TransferStatusCompleted || status == domain.TransferStatusReversed {
		return fmt.Errorf("cannot compensate terminal transfer in state %s", status)
	}
	if currentToken != t.FencingToken {
		return errors.New("stale saga worker cannot compensate transfer")
	}
	if t.Amount <= 0 || t.FeeAmount < 0 || t.Amount > math.MaxInt64-t.FeeAmount {
		return errors.New("transfer hold amount is outside the supported range")
	}

	result, err := tx.Exec(ctx, `
	UPDATE wallets
	SET hold_balance = hold_balance - $1, updated_at = NOW()
	WHERE id = $2 AND organization_id = $3 AND hold_balance >= $1
	`, t.Amount+t.FeeAmount, t.SourceWalletID, t.OrganizationID)
	if err != nil {
		return fmt.Errorf("release transfer hold: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("transfer hold is missing or smaller than the compensation amount")
	}

	result, err = tx.Exec(ctx, `
	UPDATE transfers
	SET status = 'FAILED', saga_step = 'COMPENSATE', failure_reason = $1, fencing_token = fencing_token + 1, updated_at = NOW()
	WHERE id = $2 AND fencing_token = $3
	  AND status IN ('PENDING', 'PROCESSING')
	`, reason, t.ID, t.FencingToken)
	if err != nil {
		return fmt.Errorf("mark transfer failed: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("fencing token conflict while compensating transfer")
	}
	if _, err := tx.Exec(ctx, `
	INSERT INTO saga_step_logs (transfer_id, step, status, error_message, fencing_token)
	VALUES ($1, $2, 'COMPENSATED', $3, $4)
	`, t.ID, domain.StepCompensate, reason, t.FencingToken+1); err != nil {
		return fmt.Errorf("record transfer compensation: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	t.Status = domain.TransferStatusFailed
	t.SagaStep = domain.StepCompensate
	t.FailureReason = reason
	t.FencingToken++
	o.metrics.TransfersTotal.WithLabelValues(t.Currency, string(domain.TransferStatusFailed)).Inc()
	o.metrics.SagaDurationSeconds.WithLabelValues(string(domain.TransferStatusFailed)).Observe(time.Since(t.CreatedAt).Seconds())
	return o.queueTerminalWebhook(ctx, t)
}

func (o *Orchestrator) updateSagaStep(ctx context.Context, transferID uuid.UUID, currentStep, nextStep domain.SagaStep, fencingToken int64) error {
	tx, err := o.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin saga transition: %w", err)
	}
	defer tx.Rollback(ctx)

	res, err := tx.Exec(ctx, `
	UPDATE transfers
	SET status = 'PROCESSING', saga_step = $1,
	    fencing_token = fencing_token + 1, updated_at = NOW()
	WHERE id = $2 AND fencing_token = $3 AND saga_step = $4
	  AND status IN ('PENDING', 'PROCESSING')
	`, nextStep, transferID, fencingToken, currentStep)
	if err != nil {
		return fmt.Errorf("update saga state: %w", err)
	}
	if res.RowsAffected() != 1 {
		return errors.New("fencing token conflict during saga step update")
	}

	if _, err := tx.Exec(ctx, `
	INSERT INTO saga_step_logs (transfer_id, step, status, fencing_token)
	VALUES ($1, $2, 'COMPLETED', $3)
	`, transferID, nextStep, fencingToken+1); err != nil {
		return fmt.Errorf("record saga step: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit saga transition: %w", err)
	}
	return nil
}

// ResumeIncompleteSagas sweeps the database for interrupted sagas.
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
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			o.logger.Error("failed to scan incomplete saga", zap.Error(err))
			return
		}
		stuckIDs = append(stuckIDs, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		o.logger.Error("failed to iterate incomplete sagas", zap.Error(err))
		return
	}
	rows.Close()

	for _, id := range stuckIDs {
		o.logger.Info("resuming interrupted saga", zap.String("transfer_id", id.String()))
		go func(tid uuid.UUID) {
			if err := o.ExecuteSaga(ctx, tid); err != nil {
				o.logger.Error("saga recovery attempt failed", zap.String("transfer_id", tid.String()), zap.Error(err))
			}
		}(id)
	}
}

func (o *Orchestrator) StartRecovery(ctx context.Context, interval time.Duration) {
	o.ResumeIncompleteSagas(ctx)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				o.ResumeIncompleteSagas(ctx)
			}
		}
	}()
}
