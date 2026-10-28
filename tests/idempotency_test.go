package tests

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"testing"
)

type MemoryIdempotencyStore struct {
	mu      sync.Mutex
	records map[string]string // key -> payload hash
}

func (s *MemoryIdempotencyStore) Process(key string, payload []byte) (isDuplicate bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	hash := sha256.Sum256(payload)
	hexHash := hex.EncodeToString(hash[:])

	existingHash, exists := s.records[key]
	if exists {
		if existingHash != hexHash {
			return false, errors.New("422 Unprocessable Entity: Idempotency-Key reuse with different payload")
		}
		return true, nil // Exact duplicate: replay cached response
	}

	s.records[key] = hexHash
	return false, nil
}

// TestConcurrentIdempotency50Requests tests 50 parallel requests with the identical key
// and ensures exactly one execution occurs, and subsequent/parallel attempts are handled idempotently.
func TestConcurrentIdempotency50Requests(t *testing.T) {
	store := &MemoryIdempotencyStore{records: make(map[string]string)}
	key := "idem_trans_1001"
	payload := []byte(`{"source_wallet":"w1","destination_wallet":"w2","amount":500}`)

	concurrency := 50
	var wg sync.WaitGroup
	wg.Add(concurrency)

	var duplicateCount, firstExecutionCount int
	var mu sync.Mutex

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			isDup, err := store.Process(key, payload)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			mu.Lock()
			if isDup {
				duplicateCount++
			} else {
				firstExecutionCount++
			}
			mu.Unlock()
		}()
	}

	wg.Wait()

	if firstExecutionCount != 1 {
		t.Fatalf("expected exactly 1 first execution, got: %d", firstExecutionCount)
	}
	if duplicateCount != concurrency-1 {
		t.Fatalf("expected %d idempotent deduplications, got: %d", concurrency-1, duplicateCount)
	}

	t.Logf("SUCCESS: 50 concurrent requests handled. Exactly 1 created, %d acknowledged idempotently.", duplicateCount)
}

// TestIdempotencyPayloadMismatchRejection verifies requirement:
// Same key + different payload returns 422 Unprocessable Entity rejection.
func TestIdempotencyPayloadMismatchRejection(t *testing.T) {
	store := &MemoryIdempotencyStore{records: make(map[string]string)}
	key := "idem_trans_mismatch"

	originalPayload := []byte(`{"amount":100}`)
	differentPayload := []byte(`{"amount":200}`)

	_, err := store.Process(key, originalPayload)
	if err != nil {
		t.Fatalf("initial request should succeed: %v", err)
	}

	_, err = store.Process(key, differentPayload)
	if err == nil {
		t.Fatal("expected 422 error for mismatched payload reuse, but got nil")
	}

	t.Logf("SUCCESS: Mismatched payload correctly rejected with error: %v", err)
}
