package webhooks

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

func TestDispatcherSignsRetriesAndDeduplicatesTransferWebhook(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set TEST_DATABASE_URL to run webhook delivery integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	adminPool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to integration PostgreSQL: %v", err)
	}
	schema := "webhook_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		adminPool.Close()
		t.Fatalf("create isolated webhook test schema: %v", err)
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		_, _ = adminPool.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		adminPool.Close()
		t.Fatalf("parse test database configuration: %v", err)
	}
	if config.ConnConfig.RuntimeParams == nil {
		config.ConnConfig.RuntimeParams = make(map[string]string)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema + ",public"
	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		_, _ = adminPool.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		adminPool.Close()
		t.Fatalf("connect using isolated webhook schema: %v", err)
	}
	t.Cleanup(func() {
		db.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = adminPool.Exec(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE")
		adminPool.Close()
	})
	if _, err := db.Exec(ctx, `
		CREATE TABLE webhooks (
			id UUID PRIMARY KEY,
			organization_id UUID NOT NULL,
			url TEXT NOT NULL,
			secret VARCHAR(255) NOT NULL,
			events TEXT[] NOT NULL,
			is_active BOOLEAN NOT NULL DEFAULT TRUE
		)
	`); err != nil {
		t.Fatalf("create webhook integration table: %v", err)
	}
	if _, err := db.Exec(ctx, `
		CREATE TABLE webhook_deliveries (
			id UUID PRIMARY KEY,
			webhook_id UUID NOT NULL REFERENCES webhooks(id),
			event_type VARCHAR(100) NOT NULL,
			payload JSONB NOT NULL,
			signature VARCHAR(255) NOT NULL,
			response_status INT,
			error_message TEXT,
			attempt INT NOT NULL DEFAULT 1,
			status VARCHAR(50) NOT NULL,
			next_retry_at TIMESTAMPTZ,
			claimed_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`); err != nil {
		t.Fatalf("create webhook delivery integration table: %v", err)
	}
	if _, err := db.Exec(ctx, `
		CREATE UNIQUE INDEX webhook_test_transfer_delivery_unique
			ON webhook_deliveries(webhook_id, event_type, (payload->>'transfer_id'))
			WHERE payload ? 'transfer_id'
	`); err != nil {
		t.Fatalf("create webhook deduplication index: %v", err)
	}

	const webhookSecret = "integration-webhook-signing-secret"
	encryptionKey := []byte("0123456789abcdef0123456789abcdef")
	encryptedSecret, err := EncryptSecret(encryptionKey, webhookSecret)
	if err != nil {
		t.Fatalf("encrypt integration webhook secret: %v", err)
	}
	orgID, webhookID, transferID := uuid.New(), uuid.New(), uuid.New()

	var requests atomic.Int32
	verifiedDeliveryIDs := make(chan uuid.UUID, 4)
	rejectedSignatures := make(chan string, 4)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}
		deliveryID, err := uuid.Parse(r.Header.Get("X-Webhook-ID"))
		if err != nil {
			http.Error(w, "invalid delivery id", http.StatusBadRequest)
			return
		}
		timestamp, err := strconv.ParseInt(r.Header.Get("X-Webhook-Timestamp"), 10, 64)
		if err != nil {
			rejectedSignatures <- fmt.Sprintf("invalid timestamp header: %v", err)
			http.Error(w, "invalid webhook timestamp", http.StatusUnauthorized)
			return
		}
		if !VerifyDeliverySignature(webhookSecret, r.Header.Get("X-Webhook-Signature"), deliveryID, timestamp, body, time.Minute) {
			rejectedSignatures <- fmt.Sprintf("signature=%s expected=%s body=%s",
				r.Header.Get("X-Webhook-Signature"),
				SignDelivery(webhookSecret, deliveryID, timestamp, body),
				body,
			)
			http.Error(w, "invalid webhook signature", http.StatusUnauthorized)
			return
		}
		if r.Header.Get("X-Webhook-Event") != "transfer.completed" {
			http.Error(w, "invalid event type", http.StatusBadRequest)
			return
		}
		verifiedDeliveryIDs <- deliveryID
		if requests.Add(1) == 1 {
			http.Error(w, "temporary failure", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			if address == "webhook.example.test:443" {
				address = server.Listener.Addr().String()
			}
			return (&net.Dialer{Timeout: time.Second}).DialContext(ctx, network, address)
		},
	}
	defer transport.CloseIdleConnections()

	if _, err := db.Exec(ctx, `
		INSERT INTO webhooks (id, organization_id, url, secret, events)
		VALUES ($1, $2, 'https://webhook.example.test/receive', $3, ARRAY['transfer.completed'])
	`, webhookID, orgID, encryptedSecret); err != nil {
		t.Fatalf("register integration webhook: %v", err)
	}
	dispatcher := NewDispatcher(db, zap.NewNop(), encryptionKey)
	dispatcher.client = &http.Client{
		Timeout:   3 * time.Second,
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	payload := map[string]any{
		"transfer_id": transferID,
		"status":      "COMPLETED",
		"amount":      500,
		"currency":    "USD",
	}
	if err := dispatcher.QueueWebhook(ctx, orgID, "transfer.completed", payload); err != nil {
		t.Fatalf("queue webhook: %v", err)
	}
	if err := dispatcher.QueueWebhook(ctx, orgID, "transfer.completed", payload); err != nil {
		t.Fatalf("queue duplicate webhook: %v", err)
	}
	var deliveryCount int
	if err := db.QueryRow(ctx, `
		SELECT COUNT(*) FROM webhook_deliveries WHERE webhook_id = $1
	`, webhookID).Scan(&deliveryCount); err != nil {
		t.Fatal(err)
	}
	if deliveryCount != 1 {
		t.Fatalf("duplicate webhook event created %d deliveries, want 1", deliveryCount)
	}
	var deliveryID uuid.UUID
	if err := db.QueryRow(ctx, `
		SELECT id FROM webhook_deliveries WHERE webhook_id = $1
	`, webhookID).Scan(&deliveryID); err != nil {
		t.Fatal(err)
	}

	dispatcher.ProcessPendingDeliveries(ctx)
	select {
	case firstDeliveryID := <-verifiedDeliveryIDs:
		if firstDeliveryID != deliveryID {
			t.Fatalf("first webhook request used delivery ID %s, want %s", firstDeliveryID, deliveryID)
		}
	case detail := <-rejectedSignatures:
		t.Fatalf("webhook receiver rejected signature: %s", detail)
	case <-time.After(5 * time.Second):
		var status string
		var attempt int
		var deliveryError *string
		if err := db.QueryRow(ctx, `
			SELECT status, attempt, error_message FROM webhook_deliveries WHERE id = $1
		`, deliveryID).Scan(&status, &attempt, &deliveryError); err != nil {
			t.Fatalf("webhook request timed out and delivery query failed: %v", err)
		}
		errorText := ""
		if deliveryError != nil {
			errorText = *deliveryError
		}
		t.Fatalf("webhook request timed out: status=%s attempt=%d error=%s", status, attempt, errorText)
	}
	waitForWebhookState(t, db, ctx, deliveryID, "PENDING", 2)

	var retryAt time.Time
	if err := db.QueryRow(ctx, `
		SELECT next_retry_at FROM webhook_deliveries WHERE id = $1
	`, deliveryID).Scan(&retryAt); err != nil {
		t.Fatal(err)
	}
	if !retryAt.After(time.Now()) {
		t.Fatalf("failed delivery retry timestamp %s is not in the future", retryAt)
	}
	if delay := time.Until(retryAt); delay > 0 {
		time.Sleep(delay)
	}

	dispatcher.ProcessPendingDeliveries(ctx)
	secondDeliveryID := waitForVerifiedDelivery(t, verifiedDeliveryIDs)
	if deliveryID != secondDeliveryID {
		t.Fatalf("retry changed webhook delivery ID from %s to %s", deliveryID, secondDeliveryID)
	}
	waitForWebhookState(t, db, ctx, deliveryID, "SUCCESS", 2)
	if requests.Load() != 2 {
		t.Fatalf("webhook receiver got %d attempts, want 2", requests.Load())
	}
}

func waitForVerifiedDelivery(t *testing.T, deliveries <-chan uuid.UUID) uuid.UUID {
	t.Helper()
	select {
	case deliveryID := <-deliveries:
		return deliveryID
	case <-time.After(5 * time.Second):
		t.Fatal("webhook dispatcher did not contact the HTTPS receiver")
		return uuid.Nil
	}
}

func waitForWebhookState(t *testing.T, db *pgxpool.Pool, ctx context.Context, deliveryID uuid.UUID, expected string, expectedAttempt int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var status string
		var attempt int
		if err := db.QueryRow(ctx, `
			SELECT status, attempt FROM webhook_deliveries WHERE id = $1
		`, deliveryID).Scan(&status, &attempt); err != nil {
			t.Fatal(err)
		}
		if status == expected && attempt == expectedAttempt {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	var status string
	var attempt int
	if err := db.QueryRow(ctx, `
		SELECT status, attempt FROM webhook_deliveries WHERE id = $1
	`, deliveryID).Scan(&status, &attempt); err != nil {
		t.Fatal(err)
	}
	t.Fatalf("webhook delivery state is %s/%d, want %s/%d",
		status, attempt, expected, expectedAttempt)
}
