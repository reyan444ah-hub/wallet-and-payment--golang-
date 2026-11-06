package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/distributed-wallet/platform/pkg/database"
	"github.com/distributed-wallet/platform/pkg/outbox"
	"github.com/distributed-wallet/platform/pkg/reconciliation"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		return
	}

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	ctx := context.Background()

	dbHost := getEnv("DB_HOST", "localhost")
	dbPort := getEnv("DB_PORT", "5432")
	dbUser := getEnv("DB_USER", "postgres")
	dbPass := os.Getenv("DB_PASSWORD")
	if dbPass == "" {
		fmt.Println("DB_PASSWORD environment variable is required")
		os.Exit(1)
	}
	dbName := getEnv("DB_NAME", "wallet_ledger")

	pool, err := database.ConnectPostgres(ctx, database.Config{
		Host:     dbHost,
		Port:     dbPort,
		User:     dbUser,
		Password: dbPass,
		Database: dbName,
		SSLMode:  "disable",
	})
	if err != nil {
		fmt.Printf("Database connection failed: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	command := os.Args[1]

	switch command {
	case "reconcile":
		auditor := reconciliation.NewAuditor(pool, logger)
		discrepancies, err := auditor.RunAudit(ctx)
		if err != nil {
			fmt.Printf("Error running reconciliation: %v\n", err)
			os.Exit(1)
		}
		if len(discrepancies) == 0 {
			fmt.Println("SUCCESS: All journal entries balance and wallet balances match ledger entries perfectly.")
		} else {
			fmt.Printf("DISCREPANCIES DETECTED (%d found):\n", len(discrepancies))
			for i, d := range discrepancies {
				fmt.Printf("[%d] %s (ID: %s): %s\n", i+1, d.Type, d.ID, d.Description)
			}
		}

	case "dlq:list":
		rows, err := pool.Query(ctx, `
			SELECT id, topic, partition_key, retry_count, last_error, created_at 
			FROM outbox_events WHERE status = 'DEAD_LETTER' ORDER BY created_at DESC
		`)
		if err != nil {
			fmt.Printf("Failed listing DLQ: %v\n", err)
			return
		}
		fmt.Println("--- DEAD LETTER QUEUE (DLQ) MESSAGES ---")
		for rows.Next() {
			var id uuid.UUID
			var topic, partKey, lastErr string
			var retries int
			var createdAt time.Time
			if err := rows.Scan(&id, &topic, &partKey, &retries, &lastErr, &createdAt); err != nil {
				fmt.Printf("Failed reading outbox DLQ row: %v\n", err)
				rows.Close()
				os.Exit(1)
			}
			fmt.Printf("ID: %s | Topic: %s | Retries: %d | Error: %s\n", id, topic, retries, lastErr)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			fmt.Printf("Failed reading outbox DLQ: %v\n", err)
			os.Exit(1)
		}
		rows.Close()

		sagaRows, err := pool.Query(ctx, `
			SELECT id, subject, delivery_count, error_message, created_at
			FROM dead_letter_messages WHERE source = 'saga_orchestrator' AND replayed_at IS NULL
			ORDER BY created_at DESC
		`)
		if err != nil {
			fmt.Printf("Failed listing saga DLQ: %v\n", err)
			os.Exit(1)
		}
		for sagaRows.Next() {
			var id uuid.UUID
			var subject, lastErr string
			var deliveries int64
			var createdAt time.Time
			if err := sagaRows.Scan(&id, &subject, &deliveries, &lastErr, &createdAt); err != nil {
				sagaRows.Close()
				fmt.Printf("Failed reading saga DLQ row: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("ID: %s | Subject: %s | Deliveries: %d | Error: %s\n", id, subject, deliveries, lastErr)
		}
		if err := sagaRows.Err(); err != nil {
			sagaRows.Close()
			fmt.Printf("Failed reading saga DLQ: %v\n", err)
			os.Exit(1)
		}
		sagaRows.Close()

	case "dlq:replay":
		if len(os.Args) < 3 {
			fmt.Println("Usage: wallet-cli dlq:replay <event_id|all>")
			return
		}
		target := os.Args[2]
		if target == "all" {
			res, err := pool.Exec(ctx, `UPDATE outbox_events SET status = 'PENDING', retry_count = 0, next_retry_at = NOW() WHERE status = 'DEAD_LETTER'`)
			if err != nil {
				fmt.Printf("Error replaying DLQ: %v\n", err)
				return
			}
			sagaRows, err := pool.Query(ctx, `
				SELECT id FROM dead_letter_messages
				WHERE source = 'saga_orchestrator' AND replayed_at IS NULL
				ORDER BY created_at
			`)
			if err != nil {
				fmt.Printf("Failed loading saga DLQ messages: %v\n", err)
				os.Exit(1)
			}
			var ids []uuid.UUID
			for sagaRows.Next() {
				var id uuid.UUID
				if err := sagaRows.Scan(&id); err != nil {
					sagaRows.Close()
					fmt.Printf("Failed reading saga DLQ message: %v\n", err)
					os.Exit(1)
				}
				ids = append(ids, id)
			}
			if err := sagaRows.Err(); err != nil {
				sagaRows.Close()
				fmt.Printf("Failed reading saga DLQ messages: %v\n", err)
				os.Exit(1)
			}
			sagaRows.Close()
			for _, id := range ids {
				if err := replaySagaMessage(ctx, pool, id); err != nil {
					fmt.Printf("Failed replaying saga DLQ message %s: %v\n", id, err)
					os.Exit(1)
				}
			}
			fmt.Printf("Replayed %d outbox events and %d saga messages\n", res.RowsAffected(), len(ids))
		} else {
			eventID, err := uuid.Parse(target)
			if err != nil {
				fmt.Println("Invalid event UUID")
				return
			}
			res, err := pool.Exec(ctx, `UPDATE outbox_events SET status = 'PENDING', retry_count = 0, next_retry_at = NOW() WHERE id = $1`, eventID)
			if err != nil {
				fmt.Printf("Error replaying event: %v\n", err)
				return
			}
			if res.RowsAffected() == 0 {
				if err := replaySagaMessage(ctx, pool, eventID); err != nil {
					fmt.Printf("No replayable DLQ message found: %v\n", err)
					os.Exit(1)
				}
				fmt.Printf("Replayed saga message %s\n", eventID)
			} else {
				fmt.Printf("Replayed outbox event %s\n", eventID)
			}
		}

	case "corrupt:balance":
		// Deliberate corruption hook for test demonstration
		if len(os.Args) < 3 {
			fmt.Println("Usage: wallet-cli corrupt:balance <wallet_uuid>")
			return
		}
		wID, err := uuid.Parse(os.Args[2])
		if err != nil {
			fmt.Println("Invalid wallet UUID")
			return
		}
		_, err = pool.Exec(ctx, `UPDATE wallets SET balance = balance + 50000 WHERE id = $1`, wID)
		if err != nil {
			fmt.Printf("Database rejected the corruption attempt: %v\n", err)
			return
		}
		fmt.Printf("Artificially corrupted balance for wallet %s. Run 'reconcile' to verify detection.\n", wID)

	default:
		printUsage()
	}
}

func replaySagaMessage(ctx context.Context, pool *pgxpool.Pool, messageID uuid.UUID) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var payload []byte
	err = tx.QueryRow(ctx, `
			SELECT payload FROM dead_letter_messages
			WHERE id = $1 AND source = 'saga_orchestrator' AND replayed_at IS NULL
			FOR UPDATE
		`, messageID).Scan(&payload)
	if err != nil {
		return err
	}
	transferID, err := uuid.Parse(string(payload))
	if err != nil {
		return fmt.Errorf("dead-letter payload is not a transfer UUID: %w", err)
	}
	var sourceWalletID uuid.UUID
	var status string
	err = tx.QueryRow(ctx, `
			SELECT source_wallet_id, status FROM transfers WHERE id = $1
		`, transferID).Scan(&sourceWalletID, &status)
	if err != nil {
		return fmt.Errorf("load transfer for DLQ replay: %w", err)
	}
	if status != "PENDING" && status != "PROCESSING" {
		return fmt.Errorf("transfer is not recoverable in state %s", status)
	}
	if err := outbox.QueueEventInTx(ctx, tx, "transfer.created", sourceWalletID.String(), transferID.String()); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
			UPDATE dead_letter_messages SET replayed_at = NOW()
			WHERE id = $1 AND replayed_at IS NULL
		`, messageID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func printUsage() {
	fmt.Println("Wallet & Payment Ledger CLI")
	fmt.Println("Commands:")
	fmt.Println("  reconcile             Run full ledger integrity audit")
	fmt.Println("  dlq:list              List dead letter queue outbox messages")
	fmt.Println("  dlq:replay <id|all>   Replay dead letter messages")
	fmt.Println("  corrupt:balance <id>  Artificially corrupt wallet balance to test reconciliation detector")
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
