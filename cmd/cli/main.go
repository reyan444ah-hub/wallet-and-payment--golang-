package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/distributed-wallet/platform/pkg/database"
	"github.com/distributed-wallet/platform/pkg/reconciliation"
	"github.com/google/uuid"
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
	dbPass := getEnv("DB_PASSWORD", "postgres")
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
		defer rows.Close()

		fmt.Println("--- DEAD LETTER QUEUE (DLQ) MESSAGES ---")
		for rows.Next() {
			var id uuid.UUID
			var topic, partKey, lastErr string
			var retries int
			var createdAt time.Time
			_ = rows.Scan(&id, &topic, &partKey, &retries, &lastErr, &createdAt)
			fmt.Printf("ID: %s | Topic: %s | Retries: %d | Error: %s\n", id, topic, retries, lastErr)
		}

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
			fmt.Printf("Replayed %d DLQ messages\n", res.RowsAffected())
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
			fmt.Printf("Replayed event %s (affected: %d)\n", eventID, res.RowsAffected())
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
			fmt.Printf("Failed: %v\n", err)
			return
		}
		fmt.Printf("Artificially corrupted balance for wallet %s (added $500 discrepancy). Run 'reconcile' to verify detection.\n", wID)

	default:
		printUsage()
	}
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
