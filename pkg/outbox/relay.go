package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type Relay struct {
	db     *pgxpool.Pool
	js     nats.JetStreamContext
	logger *zap.Logger
}

func NewRelay(db *pgxpool.Pool, js nats.JetStreamContext, logger *zap.Logger) *Relay {
	return &Relay{
		db:     db,
		js:     js,
		logger: logger,
	}
}

// QueueEventInTx writes an event to the outbox table as part of an active DB transaction
func QueueEventInTx(ctx context.Context, tx pgx.Tx, topic, partitionKey string, payload any) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal outbox event payload: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (id, topic, partition_key, payload, status, retry_count, next_retry_at, created_at)
		VALUES ($1, $2, $3, $4, 'PENDING', 0, NOW(), NOW())
	`, uuid.New(), topic, partitionKey, payloadBytes)

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
	rows, err := r.db.Query(ctx, `
		SELECT id, topic, partition_key, payload, retry_count
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
		ID           uuid.UUID
		Topic        string
		PartitionKey string
		Payload      []byte
		RetryCount   int
	}

	var batch []OutboxRow
	for rows.Next() {
		var item OutboxRow
		if err := rows.Scan(&item.ID, &item.Topic, &item.PartitionKey, &item.Payload, &item.RetryCount); err == nil {
			batch = append(batch, item)
		}
	}

	for _, item := range batch {
		msg := &nats.Msg{
			Subject: fmt.Sprintf("%s.%s", item.Topic, item.PartitionKey),
			Data:    item.Payload,
			Header:  nats.Header{},
		}
		msg.Header.Set("Nats-Msg-Id", item.ID.String()) // JetStream message deduplication ID

		_, err := r.js.PublishMsg(msg)
		if err != nil {
			r.logger.Warn("failed to publish outbox event to JetStream", zap.String("id", item.ID.String()), zap.Error(err))
			r.handlePublishFailure(ctx, item.ID, item.RetryCount, err.Error())
			continue
		}

		// Mark as PUBLISHED
		_, _ = r.db.Exec(ctx, `
			UPDATE outbox_events
			SET status = 'PUBLISHED', published_at = NOW()
			WHERE id = $1
		`, item.ID)
	}
}

func (r *Relay) handlePublishFailure(ctx context.Context, id uuid.UUID, retryCount int, errMsg string) {
	const maxRetries = 5
	if retryCount >= maxRetries {
		_, _ = r.db.Exec(ctx, `
			UPDATE outbox_events
			SET status = 'DEAD_LETTER', last_error = $1
			WHERE id = $2
		`, errMsg, id)
		return
	}

	// Exponential backoff
	backoff := time.Duration(1<<uint(retryCount)) * 500 * time.Millisecond
	nextRetry := time.Now().Add(backoff)

	_, _ = r.db.Exec(ctx, `
		UPDATE outbox_events
		SET retry_count = retry_count + 1, last_error = $1, next_retry_at = $2
		WHERE id = $3
	`, errMsg, nextRetry, id)
}
