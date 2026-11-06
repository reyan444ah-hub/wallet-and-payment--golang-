package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/distributed-wallet/platform/pkg/telemetry"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type Relay struct {
	db      *pgxpool.Pool
	js      nats.JetStreamContext
	logger  *zap.Logger
	metrics *telemetry.Metrics
}

func NewRelay(db *pgxpool.Pool, js nats.JetStreamContext, logger *zap.Logger, metrics *telemetry.Metrics) *Relay {
	return &Relay{
		db:      db,
		js:      js,
		logger:  logger,
		metrics: metrics,
	}
}

// QueueEventInTx writes an event to the outbox table as part of an active DB transaction
func QueueEventInTx(ctx context.Context, tx pgx.Tx, topic, partitionKey string, payload any) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal outbox event payload: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (id, topic, partition_key, correlation_id, payload, status, retry_count, next_retry_at, created_at)
		VALUES ($1, $2, $3, $4, $5, 'PENDING', 0, NOW(), NOW())
	`, uuid.New(), topic, partitionKey, telemetry.CorrelationIDFromContext(ctx), payloadBytes)

	return err
}

// StartRelay runs a background worker loop publishing outbox rows to NATS JetStream
func (r *Relay) StartRelay(ctx context.Context, pollInterval time.Duration) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("stopping transactional outbox relay worker")
			return
		case <-ticker.C:
			r.publishPendingEvents(ctx)
		}
	}
}

func (r *Relay) publishPendingEvents(ctx context.Context) {
	var pending int64
	if err := r.db.QueryRow(ctx, `SELECT COUNT(*) FROM outbox_events WHERE status = 'PENDING'`).Scan(&pending); err != nil {
		r.logger.Error("failed to measure pending outbox events", zap.Error(err))
	} else {
		r.metrics.OutboxLagGauge.Set(float64(pending))
	}

	tx, err := r.db.Begin(ctx)
	if err != nil {
		r.logger.Error("failed starting transaction for outbox claim", zap.Error(err))
		return
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'PENDING', claimed_at = NULL
		WHERE status = 'PROCESSING'
		  AND (claimed_at IS NULL OR claimed_at < NOW() - INTERVAL '30 seconds')
	`); err != nil {
		r.logger.Error("failed to recover abandoned outbox claims", zap.Error(err))
		return
	}

	rows, err := tx.Query(ctx, `
		SELECT id, topic, partition_key, COALESCE(correlation_id, ''), payload, retry_count
		FROM outbox_events
		WHERE status = 'PENDING' AND next_retry_at <= NOW()
		ORDER BY created_at ASC
		LIMIT 50
		FOR UPDATE SKIP LOCKED
	`)
	if err != nil {
		r.logger.Error("failed to query pending outbox events", zap.Error(err))
		return
	}
	defer rows.Close()

	type OutboxRow struct {
		ID            uuid.UUID
		Topic         string
		PartitionKey  string
		CorrelationID string
		Payload       []byte
		RetryCount    int
	}

	var batch []OutboxRow
	for rows.Next() {
		var item OutboxRow
		if err := rows.Scan(&item.ID, &item.Topic, &item.PartitionKey, &item.CorrelationID, &item.Payload, &item.RetryCount); err != nil {
			rows.Close()
			r.logger.Error("failed to scan pending outbox event", zap.Error(err))
			return
		}
		batch = append(batch, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		r.logger.Error("failed to iterate pending outbox events", zap.Error(err))
		return
	}
	rows.Close()

	if len(batch) == 0 {
		return
	}

	// Atomically mark claimed events as PROCESSING so other concurrent relay workers ignore them
	claimedIDs := make([]uuid.UUID, len(batch))
	for i, b := range batch {
		claimedIDs[i] = b.ID
	}
	_, err = tx.Exec(ctx, `
		UPDATE outbox_events SET status = 'PROCESSING', claimed_at = NOW()
		WHERE id = ANY($1) AND status = 'PENDING'
	`, claimedIDs)
	if err != nil {
		r.logger.Error("failed marking outbox events as processing", zap.Error(err))
		return
	}

	if err := tx.Commit(ctx); err != nil {
		r.logger.Error("failed committing outbox event claims", zap.Error(err))
		return
	}

	for _, item := range batch {
		msg := &nats.Msg{
			Subject: fmt.Sprintf("%s.%s", item.Topic, item.PartitionKey),
			Data:    item.Payload,
			Header:  nats.Header{},
		}
		msg.Header.Set("Nats-Msg-Id", item.ID.String()) // JetStream message deduplication ID
		if item.CorrelationID != "" {
			msg.Header.Set("X-Correlation-ID", item.CorrelationID)
		}

		_, err := r.js.PublishMsg(msg)
		if err != nil {
			r.logger.Warn("failed to publish outbox event to JetStream", zap.String("id", item.ID.String()), zap.Error(err))
			r.handlePublishFailure(ctx, item.ID, item.RetryCount, err.Error())
			continue
		}

		// Mark as PUBLISHED
		result, err := r.db.Exec(ctx, `
			UPDATE outbox_events
			SET status = 'PUBLISHED', published_at = NOW(), claimed_at = NULL
			WHERE id = $1 AND status = 'PROCESSING'
		`, item.ID)
		if err != nil || result.RowsAffected() != 1 {
			r.logger.Error("failed to mark outbox event published", zap.String("id", item.ID.String()), zap.Error(err))
		}
	}
}

func (r *Relay) handlePublishFailure(ctx context.Context, id uuid.UUID, retryCount int, errMsg string) {
	r.metrics.OutboxRetriesTotal.Inc()
	const maxRetries = 5
	if retryCount >= maxRetries-1 {
		r.metrics.DLQMessagesTotal.Inc()
		if _, err := r.db.Exec(ctx, `
			UPDATE outbox_events
			SET status = 'DEAD_LETTER', last_error = $1, claimed_at = NULL
			WHERE id = $2 AND status = 'PROCESSING'
		`, errMsg, id); err != nil {
			r.logger.Error("failed to dead-letter outbox event", zap.String("id", id.String()), zap.Error(err))
		}
		return
	}

	backoff := time.Duration(1<<uint(retryCount)) * 500 * time.Millisecond
	backoff += time.Duration(rand.Int63n(int64(backoff/2 + 1)))
	nextRetry := time.Now().Add(backoff)

	if _, err := r.db.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'PENDING', claimed_at = NULL, retry_count = retry_count + 1,
		    last_error = $1, next_retry_at = $2
		WHERE id = $3 AND status = 'PROCESSING'
	`, errMsg, nextRetry, id); err != nil {
		r.logger.Error("failed to schedule outbox retry", zap.String("id", id.String()), zap.Error(err))
	}
}
