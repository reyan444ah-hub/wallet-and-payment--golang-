package outbox

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/distributed-wallet/platform/pkg/telemetry"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

func TestRelayRecoversCommittedClaimAndPublishesPendingEvent(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	natsURL := os.Getenv("TEST_NATS_URL")
	if databaseURL == "" || natsURL == "" {
		t.Skip("set TEST_DATABASE_URL and TEST_NATS_URL to run outbox recovery integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	adminPool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to integration PostgreSQL: %v", err)
	}
	schema := "outbox_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		adminPool.Close()
		t.Fatalf("create isolated outbox test schema: %v", err)
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
		t.Fatalf("connect using isolated outbox schema: %v", err)
	}
	t.Cleanup(func() {
		db.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = adminPool.Exec(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE")
		adminPool.Close()
	})
	if _, err := db.Exec(ctx, `
		CREATE TABLE outbox_events (
			id UUID PRIMARY KEY,
			topic TEXT NOT NULL,
			partition_key TEXT NOT NULL,
			correlation_id TEXT,
			payload JSONB NOT NULL,
			status TEXT NOT NULL,
			retry_count INT NOT NULL,
			last_error TEXT,
			next_retry_at TIMESTAMPTZ NOT NULL,
			claimed_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL,
			published_at TIMESTAMPTZ
		)
	`); err != nil {
		t.Fatalf("create outbox test table: %v", err)
	}

	nc, err := nats.Connect(natsURL, nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatalf("connect to test NATS: %v", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("create test JetStream context: %v", err)
	}

	eventID := uuid.New()
	streamName := "OUTBOX_TEST_" + strings.ToUpper(strings.ReplaceAll(uuid.NewString(), "-", ""))
	subject := "outbox.recovery." + eventID.String()
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{subject},
		Storage:  nats.MemoryStorage,
	}); err != nil {
		t.Fatalf("create isolated JetStream stream: %v", err)
	}
	defer func() { _ = js.DeleteStream(streamName) }()

	received := make(chan *nats.Msg, 1)
	subscription, err := js.Subscribe(subject, func(msg *nats.Msg) {
		received <- msg
		_ = msg.Ack()
	}, nats.Durable("outbox-recovery-"+eventID.String()), nats.ManualAck(), nats.BindStream(streamName))
	if err != nil {
		t.Fatalf("subscribe to outbox recovery subject: %v", err)
	}
	defer subscription.Unsubscribe()

	payload, err := json.Marshal(map[string]string{"event_id": eventID.String()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `
		INSERT INTO outbox_events
			(id, topic, partition_key, payload, status, retry_count, next_retry_at, claimed_at, created_at)
		VALUES ($1, 'outbox.recovery', $2, $3, 'PROCESSING', 0,
		        NOW() - INTERVAL '1 minute', NOW() - INTERVAL '1 minute', NOW() - INTERVAL '2 minutes')
	`, eventID, eventID.String(), payload); err != nil {
		t.Fatalf("commit event before simulated publisher crash: %v", err)
	}

	relay := NewRelay(db, js, zap.NewNop(), telemetry.InitMetrics("wallet_outbox_test"))
	relay.publishPendingEvents(ctx)

	select {
	case message := <-received:
		var actualPayload map[string]string
		if err := json.Unmarshal(message.Data, &actualPayload); err != nil {
			t.Fatalf("decode published event payload: %v", err)
		}
		if message.Subject != subject || actualPayload["event_id"] != eventID.String() {
			t.Fatalf("published event subject=%q payload=%s", message.Subject, message.Data)
		}
		if message.Header.Get("Nats-Msg-Id") != eventID.String() {
			t.Fatalf("published message ID is %q, want %s",
				message.Header.Get("Nats-Msg-Id"), eventID)
		}
	case <-time.After(5 * time.Second):
		var status string
		var lastError *string
		if err := db.QueryRow(ctx, `
			SELECT status, last_error FROM outbox_events WHERE id = $1
		`, eventID).Scan(&status, &lastError); err != nil {
			t.Fatalf("publisher restart did not publish the outbox event (status query failed: %v)", err)
		}
		t.Fatalf("publisher restart did not deliver the outbox event: status=%s last_error=%v", status, lastError)
	}

	var status string
	var publishedAt *time.Time
	if err := db.QueryRow(ctx, `
		SELECT status, published_at FROM outbox_events WHERE id = $1
	`, eventID).Scan(&status, &publishedAt); err != nil {
		t.Fatal(err)
	}
	if status != "PUBLISHED" || publishedAt == nil {
		t.Fatalf("recovered outbox event status=%s published_at=%v", status, publishedAt)
	}
}
