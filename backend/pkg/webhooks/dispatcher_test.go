package webhooks_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/distributed-wallet/platform/pkg/webhooks"
	"github.com/google/uuid"
)

func TestWebhookSignatureVerification_Valid(t *testing.T) {
	secret := "secret-webhook-key-32-chars-long!"
	now := time.Now().Unix()
	payload, _ := json.Marshal(map[string]any{"event": "transfer.completed", "amount": 5000})
	signature := webhooks.SignPayload(secret, now, payload)

	if !webhooks.VerifySignature(secret, signature, now, payload, 5*time.Minute) {
		t.Fatal("expected signature verification to succeed")
	}
}

func TestWebhookSignatureVerification_InvalidSecret(t *testing.T) {
	now := time.Now().Unix()
	payload := []byte(`{"event":"transfer.completed"}`)
	signature := webhooks.SignPayload("correct-secret-key-32-chars-long", now, payload)
	if webhooks.VerifySignature("wrong-secret-key-32-chars-long!!", signature, now, payload, 5*time.Minute) {
		t.Fatal("verification should fail for a different secret")
	}
}

func TestWebhookSignatureVerification_ModifiedPayload(t *testing.T) {
	secret := "secret-webhook-key-32-chars-long!"
	now := time.Now().Unix()
	payload := []byte(`{"event":"transfer.completed","amount":5000}`)
	signature := webhooks.SignPayload(secret, now, payload)
	if webhooks.VerifySignature(secret, signature, now, []byte(`{"event":"transfer.completed","amount":999999}`), 5*time.Minute) {
		t.Fatal("verification should fail for a modified payload")
	}
}

func TestWebhookSignatureVerification_StaleTimestamp(t *testing.T) {
	secret := "secret-webhook-key-32-chars-long!"
	staleTimestamp := time.Now().Add(-10 * time.Minute).Unix()
	payload := []byte(`{"event":"transfer.completed"}`)
	signature := webhooks.SignPayload(secret, staleTimestamp, payload)
	if webhooks.VerifySignature(secret, signature, staleTimestamp, payload, 5*time.Minute) {
		t.Fatal("verification should reject stale timestamps")
	}
}

func TestWebhookDeliverySignature_BindsDeliveryID(t *testing.T) {
	secret := "secret-webhook-key-32-chars-long!"
	deliveryID := uuid.New()
	timestamp := time.Now().Unix()
	payload := []byte(`{"event":"transfer.completed"}`)
	signature := webhooks.SignDelivery(secret, deliveryID, timestamp, payload)
	if !webhooks.VerifyDeliverySignature(secret, signature, deliveryID, timestamp, payload, time.Minute) {
		t.Fatal("expected signature for the original delivery ID to validate")
	}
	if webhooks.VerifyDeliverySignature(secret, signature, uuid.New(), timestamp, payload, time.Minute) {
		t.Fatal("signature must not validate for a different delivery ID")
	}
}

func TestValidateWebhookURL_RejectsUnsafeTargets(t *testing.T) {
	for _, target := range []string{
		"http://example.com/hook",
		"https://127.0.0.1/hook",
		"https://user:pass@example.com/hook",
	} {
		if err := webhooks.ValidateWebhookURL(target); err == nil {
			t.Errorf("expected unsafe webhook URL %q to be rejected", target)
		}
	}
	if err := webhooks.ValidateWebhookURL("https://example.com/hook"); err != nil {
		t.Fatalf("expected public HTTPS URL to be accepted: %v", err)
	}
}

func TestWebhookSecretEncryption_RoundTrip(t *testing.T) {
	key := make([]byte, 32)
	secret := "this is a webhook secret with adequate entropy"
	encrypted, err := webhooks.EncryptSecret(key, secret)
	if err != nil {
		t.Fatal(err)
	}
	if encrypted == secret {
		t.Fatal("webhook secret was stored without encryption")
	}
	decrypted, err := webhooks.DecryptSecret(key, encrypted)
	if err != nil {
		t.Fatal(err)
	}
	if decrypted != secret {
		t.Fatal("secret mismatch after decryption")
	}
	wrongKey := make([]byte, 32)
	wrongKey[0] = 1
	if _, err := webhooks.DecryptSecret(wrongKey, encrypted); err == nil {
		t.Fatal("wrong encryption key should not decrypt the secret")
	}
}
