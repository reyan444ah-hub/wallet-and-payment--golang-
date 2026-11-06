package inbox

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrDuplicateMessage = errors.New("message has already been processed by consumer group")

type InboxManager struct {
	db *pgxpool.Pool
}

func NewInboxManager(db *pgxpool.Pool) *InboxManager {
	return &InboxManager{db: db}
}

// CheckAndRecord processes the message within a transaction, returning ErrDuplicateMessage if already seen
func (m *InboxManager) CheckAndRecordInTx(ctx context.Context, tx pgx.Tx, messageID, consumerGroup string) error {
	result, err := tx.Exec(ctx, `
		INSERT INTO inbox_records (message_id, consumer_group, processed_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (message_id, consumer_group) DO NOTHING
	`, messageID, consumerGroup)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return ErrDuplicateMessage
	}

	return nil
}
