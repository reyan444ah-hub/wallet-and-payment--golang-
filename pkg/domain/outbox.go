package domain

import (
	"time"

	"github.com/google/uuid"
)

type OutboxStatus string

const (
	OutboxStatusPending    OutboxStatus = "PENDING"
	OutboxStatusPublished  OutboxStatus = "PUBLISHED"
	OutboxStatusFailed     OutboxStatus = "FAILED"
	OutboxStatusDeadLetter OutboxStatus = "DEAD_LETTER"
)

type OutboxEvent struct {
	ID           uuid.UUID    `json:"id"`
	Topic        string       `json:"topic"`
	PartitionKey string       `json:"partition_key"`
	Payload      []byte       `json:"payload"`
	Status       OutboxStatus `json:"status"`
	RetryCount   int          `json:"retry_count"`
	LastError    string       `json:"last_error,omitempty"`
	NextRetryAt  time.Time    `json:"next_retry_at"`
	CreatedAt    time.Time    `json:"created_at"`
	PublishedAt  *time.Time   `json:"published_at,omitempty"`
}

type InboxRecord struct {
	MessageID     string    `json:"message_id"`
	ConsumerGroup string    `json:"consumer_group"`
	ProcessedAt   time.Time `json:"processed_at"`
}
