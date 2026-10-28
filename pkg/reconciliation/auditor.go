package reconciliation

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type Discrepancy struct {
	Type        string `json:"type"` // "UNBALANCED_JOURNAL", "BALANCE_MISMATCH", "MISSING_LEDGER_TRANSFER", "STUCK_SAGA"
	ID          string `json:"id"`
	Description string `json:"description"`
}

type Auditor struct {
	db     *pgxpool.Pool
	logger *zap.Logger
}

func NewAuditor(db *pgxpool.Pool, logger *zap.Logger) *Auditor {
	return &Auditor{db: db, logger: logger}
}

// RunAudit performs end-to-end ledger reconciliation
func (a *Auditor) RunAudit(ctx context.Context) ([]Discrepancy, error) {
	var discrepancies []Discrepancy

	// 1. Check: Every journal entry set must balance (sum debits == sum credits)
	rows, err := a.db.Query(ctx, `
		SELECT journal_entry_id, 
		       COALESCE(SUM(CASE WHEN direction = 'DR' THEN amount ELSE 0 END), 0) AS total_dr,
		       COALESCE(SUM(CASE WHEN direction = 'CR' THEN amount ELSE 0 END), 0) AS total_cr
		FROM ledger_entries
		GROUP BY journal_entry_id
		HAVING COALESCE(SUM(CASE WHEN direction = 'DR' THEN amount ELSE 0 END), 0) != 
		       COALESCE(SUM(CASE WHEN direction = 'CR' THEN amount ELSE 0 END), 0)
	`)
	if err == nil {
		for rows.Next() {
			var jID uuid.UUID
			var dr, cr int64
			if err := rows.Scan(&jID, &dr, &cr); err == nil {
				discrepancies = append(discrepancies, Discrepancy{
					Type:        "UNBALANCED_JOURNAL",
					ID:          jID.String(),
					Description: fmt.Sprintf("Journal %s is unbalanced: DR=%d, CR=%d", jID, dr, cr),
				})
			}
		}
		rows.Close()
	}

	// 2. Check: Wallet balances match the sum of ledger entries
	// Note: Customer wallets: Credits - Debits = Current Balance
	walletRows, err := a.db.Query(ctx, `
		SELECT w.id, w.currency, w.balance,
		       COALESCE(SUM(CASE WHEN le.direction = 'CR' THEN le.amount ELSE -le.amount END), 0) AS calculated_balance
		FROM wallets w
		LEFT JOIN ledger_entries le ON w.id = le.wallet_id
		WHERE w.account_type = 'customer'
		GROUP BY w.id, w.currency, w.balance
		HAVING w.balance != COALESCE(SUM(CASE WHEN le.direction = 'CR' THEN le.amount ELSE -le.amount END), 0)
	`)
	if err == nil {
		for walletRows.Next() {
			var wID uuid.UUID
			var curr string
			var recordedBal, calcBal int64
			if err := walletRows.Scan(&wID, &curr, &recordedBal, &calcBal); err == nil {
				discrepancies = append(discrepancies, Discrepancy{
					Type:        "BALANCE_MISMATCH",
					ID:          wID.String(),
					Description: fmt.Sprintf("Wallet %s (%s) balance mismatch: recorded=%d, ledger_sum=%d", wID, curr, recordedBal, calcBal),
				})
			}
		}
		walletRows.Close()
	}

	// 3. Check: No completed transfer lacks ledger entries
	missingLedgerRows, err := a.db.Query(ctx, `
		SELECT t.id 
		FROM transfers t
		WHERE t.status = 'COMPLETED'
		  AND NOT EXISTS (
		      SELECT 1 FROM journal_entries j WHERE j.reference_id = t.id AND j.reference_type = 'transfer'
		  )
	`)
	if err == nil {
		for missingLedgerRows.Next() {
			var tID uuid.UUID
			if err := missingLedgerRows.Scan(&tID); err == nil {
				discrepancies = append(discrepancies, Discrepancy{
					Type:        "MISSING_LEDGER_TRANSFER",
					ID:          tID.String(),
					Description: fmt.Sprintf("Completed transfer %s has no corresponding journal entry", tID),
				})
			}
		}
		missingLedgerRows.Close()
	}

	// 4. Check: No saga is stuck beyond threshold (e.g. > 5 minutes in PENDING/PROCESSING)
	stuckSagaRows, err := a.db.Query(ctx, `
		SELECT id, saga_step, updated_at
		FROM transfers
		WHERE status IN ('PENDING', 'PROCESSING')
		  AND updated_at < NOW() - INTERVAL '5 minutes'
	`)
	if err == nil {
		for stuckSagaRows.Next() {
			var tID uuid.UUID
			var step string
			var updatedAt time.Time
			if err := stuckSagaRows.Scan(&tID, &step, &updatedAt); err == nil {
				discrepancies = append(discrepancies, Discrepancy{
					Type:        "STUCK_SAGA",
					ID:          tID.String(),
					Description: fmt.Sprintf("Transfer saga %s stuck in step %s since %s", tID, step, updatedAt.Format(time.RFC3339)),
				})
			}
		}
		stuckSagaRows.Close()
	}

	// Record audit run in database
	discBytes, _ := json.Marshal(discrepancies)
	status := "SUCCESS"
	if len(discrepancies) > 0 {
		status = "DISCREPANCIES_FOUND"
	}

	_, _ = a.db.Exec(ctx, `
		INSERT INTO reconciliation_runs (status, discrepancies, started_at, completed_at)
		VALUES ($1, $2, NOW(), NOW())
	`, status, discBytes)

	return discrepancies, nil
}
