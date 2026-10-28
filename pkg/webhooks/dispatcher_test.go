package webhooks_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/distributed-wallet/platform/pkg/webhooks"
)

func TestWebhookSignatureVerification_Valid(t *testing.T) {
	secret := "secret-webhook-key-32-chars-long!"
	now := time.Now().Unix()
	payload, _ := json.Marshal(map[string]any{
		"event":  "transfer.completed",
		"amount": 5000,
	})

	signature := webhooks.SignPayload(secret, now, payload)

	valid := webhooks.VerifySignature(secret, signature, now, payload, 5*time.Minute)
	if !valid {
		t.Fatal("expected signature verification to succeed")
	}
}

func TestWebhookSignatureVerification_InvalidSecret(t *testing.T) {
	secret := "correct-secret-key-32-chars-long"
	wrongSecret := "wrong-secret-key-32-chars-long!!"
	now := time.Now().Unix()
	payload := []byte(`{"event":"transfer.completed"}`)

	signature := webhooks.SignPayload(secret, now, payload)

	valid := webhooks.VerifySignature(wrongSecret, signature, now, payload, 5*time.Minute)
	if valid {
		t.Fatal("verification should fail when evaluated against wrong secret")
	}
}

func TestWebhookSignatureVerification_ModifiedPayload(t *testing.T) {
	secret := "secret-webhook-key-32-chars-long!"
	now := time.Now().Unix()
	originalPayload := []byte(`{"event":"transfer.completed","amount":5000}`)
	tamperedPayload := []byte(`{"event":"transfer.completed","amount":999999}`)

	signature := webhooks.SignPayload(secret, now, originalPayload)

	valid := webhooks.VerifySignature(secret, signature, now, tamperedPayload, 5*time.Minute)
	if valid {
		t.Fatal("verification should fail when payload has been modified/tampered")
	}
}

func TestWebhookSignatureVerification_StaleTimestamp(t *testing.T) {
	secret := "secret-webhook-key-32-chars-long!"
	staleTimestamp := time.Now().Add(-10 * time.Minute).Unix()
	payload := []byte(`{"event":"transfer.completed"}`)

	signature := webhooks.SignPayload(secret, staleTimestamp, payload)

	// Tolerance window of 5 minutes: stale timestamp must be rejected
	valid := webhooks.VerifySignature(secret, signature, staleTimestamp, payload, 5*time.Minute)
	if valid {
		t.Fatal("verification should reject stale timestamp beyond tolerance window (replay attack prevention)")
	}
}
