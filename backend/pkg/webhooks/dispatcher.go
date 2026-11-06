package webhooks

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type Dispatcher struct {
	db            *pgxpool.Pool
	logger        *zap.Logger
	client        *http.Client
	semaphore     chan struct{}
	encryptionKey []byte
}

func NewDispatcher(db *pgxpool.Pool, logger *zap.Logger, encryptionKey []byte) *Dispatcher {
	transport := &http.Transport{
		Proxy: nil,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
			if err != nil {
				return nil, err
			}
			for _, candidate := range ips {
				if isPublicIP(candidate.IP) {
					return (&net.Dialer{Timeout: 3 * time.Second}).DialContext(
						ctx, network, net.JoinHostPort(candidate.IP.String(), port),
					)
				}
			}
			return nil, fmt.Errorf("webhook host resolves only to non-public IP addresses")
		},
	}
	return &Dispatcher{
		db:            db,
		logger:        logger,
		semaphore:     make(chan struct{}, 20),
		encryptionKey: encryptionKey,
		client: &http.Client{
			Timeout:   5 * time.Second,
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

func isPublicIP(ip net.IP) bool {
	if ip == nil || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast() {
		return false
	}
	if ip4 := ip.To4(); ip4 != nil {
		sharedCarrier := net.ParseIP("100.64.0.0").To4()
		if ip4[0] == sharedCarrier[0] && ip4[1] >= 64 && ip4[1] <= 127 {
			return false
		}
	}
	return true
}

func ValidateWebhookURL(raw string) error {
	parsed, err := url.ParseRequestURI(raw)
	if err != nil {
		return fmt.Errorf("invalid webhook URL: %w", err)
	}
	if parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.User != nil ||
		parsed.Fragment != "" {
		return errors.New("webhook URL must be an HTTPS URL without user information or a fragment")
	}
	if strings.EqualFold(parsed.Hostname(), "localhost") {
		return errors.New("webhook URL cannot target localhost")
	}
	if ip := net.ParseIP(parsed.Hostname()); ip != nil && !isPublicIP(ip) {
		return errors.New("webhook URL cannot target a non-public IP address")
	}
	return nil
}

// SignPayload is retained for consumers that use the timestamp-only signature format.
func SignPayload(secret string, timestamp int64, payload []byte) string {
	return sign(secret, fmt.Sprintf("%d.%s", timestamp, payload))
}

func SignDelivery(secret string, deliveryID uuid.UUID, timestamp int64, payload []byte) string {
	return sign(secret, fmt.Sprintf("%d.%s.%s", timestamp, deliveryID, payload))
}

func sign(secret, message string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(message))
	return hex.EncodeToString(mac.Sum(nil))
}

func VerifySignature(secret, signatureHeader string, timestamp int64, payload []byte, tolerance time.Duration) bool {
	if time.Since(time.Unix(timestamp, 0)).Abs() > tolerance {
		return false
	}
	return hmac.Equal([]byte(signatureHeader), []byte(SignPayload(secret, timestamp, payload)))
}

func VerifyDeliverySignature(secret, signatureHeader string, deliveryID uuid.UUID, timestamp int64, payload []byte, tolerance time.Duration) bool {
	if deliveryID == uuid.Nil || time.Since(time.Unix(timestamp, 0)).Abs() > tolerance {
		return false
	}
	return hmac.Equal([]byte(signatureHeader), []byte(SignDelivery(secret, deliveryID, timestamp, payload)))
}

func (d *Dispatcher) QueueWebhook(ctx context.Context, orgID uuid.UUID, eventType string, payload any) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal webhook payload: %w", err)
	}

	tx, err := d.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin webhook queue transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id, secret
		FROM webhooks
		WHERE organization_id = $1 AND is_active = TRUE AND $2 = ANY(events)
	`, orgID, eventType)
	if err != nil {
		return fmt.Errorf("query registered webhooks: %w", err)
	}
	type registeredWebhook struct {
		id     uuid.UUID
		secret string
	}
	var webhooks []registeredWebhook
	for rows.Next() {
		var webhook registeredWebhook
		if err := rows.Scan(&webhook.id, &webhook.secret); err != nil {
			rows.Close()
			return fmt.Errorf("scan registered webhook: %w", err)
		}
		webhooks = append(webhooks, webhook)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate registered webhooks: %w", err)
	}
	rows.Close()

	now := time.Now().Unix()
	for _, webhook := range webhooks {
		secret, err := DecryptSecret(d.encryptionKey, webhook.secret)
		if err != nil {
			return fmt.Errorf("decrypt webhook secret for %s: %w", webhook.id, err)
		}
		deliveryID := uuid.New()
		signature := SignDelivery(secret, deliveryID, now, payloadBytes)
		if _, err := tx.Exec(ctx, `
			INSERT INTO webhook_deliveries
				(id, webhook_id, event_type, payload, signature, status, next_retry_at)
			VALUES ($1, $2, $3, $4, $5, 'PENDING', NOW())
			ON CONFLICT DO NOTHING
		`, deliveryID, webhook.id, eventType, payloadBytes, signature); err != nil {
			return fmt.Errorf("queue webhook delivery for %s: %w", webhook.id, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit webhook queue transaction: %w", err)
	}
	return nil
}

func (d *Dispatcher) ProcessPendingDeliveries(ctx context.Context) {
	tx, err := d.db.Begin(ctx)
	if err != nil {
		d.logger.Error("error starting transaction for pending webhooks", zap.Error(err))
		return
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		UPDATE webhook_deliveries
		SET status = 'PENDING', claimed_at = NULL
		WHERE status = 'PROCESSING'
		  AND (claimed_at IS NULL OR claimed_at < NOW() - INTERVAL '30 seconds')
	`); err != nil {
		d.logger.Error("failed to recover abandoned webhook claims", zap.Error(err))
		return
	}

	rows, err := tx.Query(ctx, `
		SELECT wd.id, wd.webhook_id, w.url, w.secret, wd.event_type, wd.payload, wd.attempt
		FROM webhook_deliveries wd
		JOIN webhooks w ON wd.webhook_id = w.id
		WHERE w.is_active = TRUE AND wd.status = 'PENDING'
		  AND (wd.next_retry_at IS NULL OR wd.next_retry_at <= NOW())
		ORDER BY wd.created_at
		LIMIT 20
		FOR UPDATE OF wd SKIP LOCKED
	`)
	if err != nil {
		d.logger.Error("error fetching pending webhooks", zap.Error(err))
		return
	}
	type deliveryItem struct {
		id        uuid.UUID
		webhookID uuid.UUID
		url       string
		secret    string
		eventType string
		payload   []byte
		attempt   int
	}
	var items []deliveryItem
	for rows.Next() {
		var item deliveryItem
		if err := rows.Scan(&item.id, &item.webhookID, &item.url, &item.secret, &item.eventType, &item.payload, &item.attempt); err != nil {
			rows.Close()
			d.logger.Error("failed to scan pending webhook", zap.Error(err))
			return
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		d.logger.Error("failed to iterate pending webhooks", zap.Error(err))
		return
	}
	rows.Close()
	if len(items) == 0 {
		return
	}

	ids := make([]uuid.UUID, len(items))
	for i := range items {
		ids[i] = items[i].id
	}
	if _, err := tx.Exec(ctx, `
		UPDATE webhook_deliveries
		SET status = 'PROCESSING', claimed_at = NOW()
		WHERE id = ANY($1) AND status = 'PENDING'
	`, ids); err != nil {
		d.logger.Error("failed to claim webhook deliveries", zap.Error(err))
		return
	}
	if err := tx.Commit(ctx); err != nil {
		d.logger.Error("failed to commit webhook claims", zap.Error(err))
		return
	}

	for _, item := range items {
		secret, err := DecryptSecret(d.encryptionKey, item.secret)
		if err != nil {
			d.logger.Error("failed to decrypt pending webhook secret",
				zap.String("delivery_id", item.id.String()), zap.Error(err))
			d.markDeliveryFailed(ctx, item.id, item.attempt, 0, err.Error())
			continue
		}
		select {
		case d.semaphore <- struct{}{}:
		case <-ctx.Done():
			return
		}
		go func(item deliveryItem, secret string) {
			defer func() { <-d.semaphore }()
			d.sendSingleWebhook(ctx, item.id, item.url, secret, item.eventType, item.payload, item.attempt)
		}(item, secret)
	}
}

func (d *Dispatcher) sendSingleWebhook(ctx context.Context, deliveryID uuid.UUID, rawURL, secret, eventType string, payload []byte, attempt int) {
	if err := ValidateWebhookURL(rawURL); err != nil {
		d.markDeliveryFailed(ctx, deliveryID, attempt, 0, err.Error())
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, bytes.NewReader(payload))
	if err != nil {
		d.markDeliveryFailed(ctx, deliveryID, attempt, 0, err.Error())
		return
	}

	timestamp := time.Now().Unix()
	signature := SignDelivery(secret, deliveryID, timestamp, payload)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Webhook-Event", eventType)
	req.Header.Set("X-Webhook-ID", deliveryID.String())
	req.Header.Set("X-Webhook-Signature", signature)
	req.Header.Set("X-Webhook-Timestamp", strconv.FormatInt(timestamp, 10))

	resp, err := d.client.Do(req)
	if err != nil {
		d.markDeliveryFailed(ctx, deliveryID, attempt, 0, err.Error())
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		result, err := d.db.Exec(ctx, `
			UPDATE webhook_deliveries
			SET status = 'SUCCESS', claimed_at = NULL, response_status = $1, signature = $2
			WHERE id = $3 AND status = 'PROCESSING'
		`, resp.StatusCode, signature, deliveryID)
		if err != nil || result.RowsAffected() != 1 {
			d.logger.Error("failed to mark webhook delivery successful",
				zap.String("delivery_id", deliveryID.String()), zap.Error(err))
		}
		return
	}
	d.markDeliveryFailed(ctx, deliveryID, attempt, resp.StatusCode, fmt.Sprintf("HTTP %d", resp.StatusCode))
}

func (d *Dispatcher) markDeliveryFailed(ctx context.Context, deliveryID uuid.UUID, attempt int, statusCode int, errMsg string) {
	const maxAttempts = 5
	if attempt >= maxAttempts {
		if _, err := d.db.Exec(ctx, `
			UPDATE webhook_deliveries
			SET status = 'FAILED', claimed_at = NULL, response_status = $1, error_message = $2
			WHERE id = $3 AND status = 'PROCESSING'
		`, statusCode, errMsg, deliveryID); err != nil {
			d.logger.Error("failed to mark webhook delivery terminal",
				zap.String("delivery_id", deliveryID.String()), zap.Error(err))
		}
		return
	}

	backoff := time.Duration(1<<uint(max(attempt-1, 0))) * 2 * time.Second
	backoff += time.Duration(rand.Int63n(int64(backoff/2 + 1)))
	nextRetry := time.Now().Add(backoff)
	if _, err := d.db.Exec(ctx, `
		UPDATE webhook_deliveries
		SET status = 'PENDING', claimed_at = NULL, attempt = attempt + 1,
		    response_status = $1, error_message = $2, next_retry_at = $3
		WHERE id = $4 AND status = 'PROCESSING'
	`, statusCode, errMsg, nextRetry, deliveryID); err != nil {
		d.logger.Error("failed to schedule webhook retry",
			zap.String("delivery_id", deliveryID.String()), zap.Error(err))
	}
}
