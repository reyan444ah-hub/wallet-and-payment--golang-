package webhooks

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type Dispatcher struct {
	db     *pgxpool.Pool
	logger *zap.Logger
	client *http.Client
}

func NewDispatcher(db *pgxpool.Pool, logger *zap.Logger) *Dispatcher {
	return &Dispatcher{
		db:     db,
		logger: logger,
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// SignPayload generates an HMAC-SHA256 signature for payload + timestamp
func SignPayload(secret string, timestamp int64, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	message := fmt.Sprintf("%d.%s", timestamp, string(payload))
	mac.Write([]byte(message))
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifySignature verifies the HMAC signature and timestamp freshness (replay attack prevention)
func VerifySignature(secret, signatureHeader string, timestamp int64, payload []byte, tolerance time.Duration) bool {
	if time.Since(time.Unix(timestamp, 0)).Abs() > tolerance {
		return false // Replay window exceeded
	}

	expectedSig := SignPayload(secret, timestamp, payload)
	return hmac.Equal([]byte(signatureHeader), []byte(expectedSig))
}

// QueueWebhook inserts a delivery record into webhook_deliveries
func (d *Dispatcher) QueueWebhook(ctx context.Context, orgID uuid.UUID, eventType string, payload any) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	rows, err := d.db.Query(ctx, `SELECT id, secret, url FROM webhooks WHERE organization_id = $1 AND is_active = true AND $2 = ANY(events)`, orgID, eventType)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var webhookID uuid.UUID
		var secret, url string
		if err := rows.Scan(&webhookID, &secret, &url); err != nil {
			continue
		}

		now := time.Now().Unix()
		sig := SignPayload(secret, now, payloadBytes)

		_, err = d.db.Exec(ctx, `
			INSERT INTO webhook_deliveries (webhook_id, event_type, payload, signature, status, next_retry_at)
			VALUES ($1, $2, $3, $4, 'PENDING', NOW())
		`, webhookID, eventType, payloadBytes, sig)
		if err != nil {
			d.logger.Error("failed to queue webhook delivery", zap.Error(err), zap.String("webhook_id", webhookID.String()))
		}
	}

	return nil
}

// DispatchWorker processes pending webhook deliveries with exponential backoff and atomic claim
func (d *Dispatcher) ProcessPendingDeliveries(ctx context.Context) {
	tx, err := d.db.Begin(ctx)
	if err != nil {
		d.logger.Error("error starting transaction for pending webhooks", zap.Error(err))
		return
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT wd.id, wd.webhook_id, w.url, w.secret, wd.event_type, wd.payload, wd.attempt
		FROM webhook_deliveries wd
		JOIN webhooks w ON wd.webhook_id = w.id
		WHERE wd.status = 'PENDING' AND (wd.next_retry_at IS NULL OR wd.next_retry_at <= NOW())
		LIMIT 20
		FOR UPDATE SKIP LOCKED
	`)
	if err != nil {
		d.logger.Error("error fetching pending webhooks", zap.Error(err))
		return
	}
	defer rows.Close()

	type DeliveryItem struct {
		ID        uuid.UUID
		WebhookID uuid.UUID
		URL       string
		Secret    string
		EventType string
		Payload   []byte
		Attempt   int
	}

	var items []DeliveryItem
	for rows.Next() {
		var item DeliveryItem
		if err := rows.Scan(&item.ID, &item.WebhookID, &item.URL, &item.Secret, &item.EventType, &item.Payload, &item.Attempt); err == nil {
			items = append(items, item)
		}
	}
	rows.Close()

	if len(items) == 0 {
		return
	}

	// Atomically mark claimed items as PROCESSING so other workers skip them
	claimedIDs := make([]uuid.UUID, len(items))
	for i, item := range items {
		claimedIDs[i] = item.ID
	}
	_, err = tx.Exec(ctx, `UPDATE webhook_deliveries SET status = 'PROCESSING' WHERE id = ANY($1)`, claimedIDs)
	if err != nil {
		d.logger.Error("failed to mark webhooks as processing", zap.Error(err))
		return
	}

	if err := tx.Commit(ctx); err != nil {
		d.logger.Error("failed to commit webhook claims", zap.Error(err))
		return
	}

	for _, item := range items {
		go d.sendSingleWebhook(context.Background(), item.ID, item.URL, item.Secret, item.EventType, item.Payload, item.Attempt)
	}
}

func (d *Dispatcher) sendSingleWebhook(ctx context.Context, deliveryID uuid.UUID, url, secret, eventType string, payload []byte, attempt int) {
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(payload))
	if err != nil {
		d.markDeliveryFailed(ctx, deliveryID, attempt, 0, err.Error())
		return
	}

	// Sign payload with the exact timestamp sent in the header
	timestamp := time.Now().Unix()
	sig := SignPayload(secret, timestamp, payload)

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Webhook-Event", eventType)
	req.Header.Set("X-Webhook-Signature", sig)
	req.Header.Set("X-Webhook-Timestamp", fmt.Sprintf("%d", timestamp))

	resp, err := d.client.Do(req)
	if err != nil {
		d.markDeliveryFailed(ctx, deliveryID, attempt, 0, err.Error())
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		_, _ = d.db.Exec(ctx, `UPDATE webhook_deliveries SET status = 'SUCCESS', response_status = $1, signature = $2 WHERE id = $3`, resp.StatusCode, sig, deliveryID)
	} else {
		d.markDeliveryFailed(ctx, deliveryID, attempt, resp.StatusCode, fmt.Sprintf("HTTP %d", resp.StatusCode))
	}
}

func (d *Dispatcher) markDeliveryFailed(ctx context.Context, deliveryID uuid.UUID, attempt int, statusCode int, errMsg string) {
	const maxRetries = 5
	if attempt >= maxRetries {
		_, _ = d.db.Exec(ctx, `UPDATE webhook_deliveries SET status = 'FAILED', response_status = $1, error_message = $2 WHERE id = $3`, statusCode, errMsg, deliveryID)
		return
	}

	// Exponential backoff: 2s, 4s, 8s, 16s...
	backoff := time.Duration(1<<uint(attempt)) * 2 * time.Second
	nextRetry := time.Now().Add(backoff)

	_, _ = d.db.Exec(ctx, `
		UPDATE webhook_deliveries 
		SET attempt = attempt + 1, response_status = $1, error_message = $2, next_retry_at = $3
		WHERE id = $4
	`, statusCode, errMsg, nextRetry, deliveryID)
}
